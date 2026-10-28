package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/terraform-exec/tfexec"

	tfengine "github.com/karavy/terraform-engine"
	"github.com/karavy/terraform-engine/s3utils"
)

// unsavedSuffix names, after the state key, the record of the resources an apply began to
// create and that are not in the state: "<state key>.unsaved-resources". The operator reads it
// (internal/controller/terraform.UnsavedResources has the same format) and applies nothing more
// to the state until someone accepts it.
const unsavedSuffix = ".unsaved-resources"

// unsavedRecord is the content of the record.
type unsavedRecord struct {
	ID        string            `json:"ID"`
	Resources []unsavedResource `json:"Resources"`
	Who       string            `json:"Who"`
	Workflow  string            `json:"Workflow"`
	Step      string            `json:"Step"`
	Created   time.Time         `json:"Created"`
}

type unsavedResource struct {
	Address string `json:"Address"`
	// Error is the error OpenTofu reported for the resource, if any: none when the apply was
	// interrupted while creating it.
	Error string `json:"Error,omitempty"`
}

// refusedCreate matches the error of an OpenStack API that refused a create outright (a 4xx
// answer to the request that creates): nothing was created. Errors after the create was
// accepted, network errors and interruptions do not match.
var refusedCreate = regexp.MustCompile(`when accessing \[(POST|PUT) [^\]]+\], but got 4[0-9]{2}`)

// applyWatcher prints the machine-readable output of "apply -json" as the readable messages it
// carries, and records which resources the apply began and finished creating.
type applyWatcher struct {
	out     io.Writer
	mu      sync.Mutex
	partial []byte
	started map[string]bool
	done    map[string]bool
	errs    map[string]string
}

func newApplyWatcher(out io.Writer) *applyWatcher {
	return &applyWatcher{out: out, started: map[string]bool{}, done: map[string]bool{}, errs: map[string]string{}}
}

type applyEvent struct {
	Message string `json:"@message"`
	Type    string `json:"type"`
	Hook    struct {
		Resource struct {
			Addr string `json:"addr"`
		} `json:"resource"`
		Action string `json:"action"`
	} `json:"hook"`
	Diagnostic *struct {
		Severity string `json:"severity"`
		Summary  string `json:"summary"`
		Detail   string `json:"detail"`
		Address  string `json:"address"`
	} `json:"diagnostic"`
}

func (w *applyWatcher) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.partial = append(w.partial, p...)
	for {
		line, rest, found := bytes.Cut(w.partial, []byte("\n"))
		if !found {
			break
		}
		w.partial = rest
		w.line(line)
	}
	return len(p), nil
}

func (w *applyWatcher) line(line []byte) {
	if len(bytes.TrimSpace(line)) == 0 {
		return
	}
	var event applyEvent
	if err := json.Unmarshal(line, &event); err != nil {
		_, _ = fmt.Fprintf(w.out, "%s\n", line)
		return
	}
	addr := event.Hook.Resource.Addr
	switch event.Type {
	case "apply_start":
		if event.Hook.Action == "create" {
			w.started[addr] = true
		}
	case "apply_complete":
		if event.Hook.Action == "create" {
			w.done[addr] = true
		}
	case "diagnostic":
		if d := event.Diagnostic; d != nil {
			if d.Severity == "error" && d.Address != "" {
				w.errs[d.Address] = strings.TrimSpace(d.Summary + " " + d.Detail)
			}
			_, _ = fmt.Fprintf(w.out, "%s\n%s\n", event.Message, d.Detail)
			return
		}
	}
	_, _ = fmt.Fprintf(w.out, "%s\n", event.Message)
}

// unsaved returns the resources the apply began to create, did not finish creating, and that
// are not in the saved state, except those whose create the API refused.
func (w *applyWatcher) unsaved(inState map[string]bool) []unsavedResource {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []unsavedResource
	for addr := range w.started {
		if w.done[addr] || inState[addr] {
			continue
		}
		if refusedCreate.MatchString(w.errs[addr]) {
			log.Printf("The API refused to create %s: nothing was created", addr)
			continue
		}
		out = append(out, unsavedResource{Address: addr, Error: w.errs[addr]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address < out[j].Address })
	return out
}

// watchedApply runs "apply -json" and, when it fails, records in the state storage the
// resources it may have created without saving them in the state (see unsavedSuffix).
func watchedApply(
	ctx context.Context, terraform *tfexec.Terraform, backend tfengine.S3Backend, opts ...tfexec.ApplyOption,
) error {
	watcher := newApplyWatcher(os.Stdout)
	err := terraform.ApplyJSON(ctx, watcher, opts...)
	terraform.SetStdout(os.Stdout)
	if err == nil {
		return nil
	}
	// The run may be stopping: the record must still be written.
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if recordErr := recordUnsaved(recordCtx, watcher, backend); recordErr != nil {
		log.Printf("Could not check the state for resources created but not saved: %v", recordErr)
	}
	return err
}

func recordUnsaved(ctx context.Context, watcher *applyWatcher, backend tfengine.S3Backend) error {
	client := storageClient(backend)
	raw, err := s3utils.ReadObject(ctx, client, backend.Bucket, backend.Key)
	if err != nil && !errors.Is(err, s3utils.ErrObjectNotFound) {
		return fmt.Errorf("read the state: %w", err)
	}
	inState, err := stateAddresses(raw)
	if err != nil {
		return err
	}
	resources := watcher.unsaved(inState)
	if len(resources) == 0 {
		return nil
	}
	record := unsavedRecord{Resources: resources, Who: "runner@" + hostname(), Created: time.Now().UTC()}
	id := make([]byte, 8)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	record.ID = hex.EncodeToString(id)
	var metadata struct{ Namespace, Workflow, Step string }
	if err := json.Unmarshal([]byte(os.Getenv("TF_RENDERED_METADATA")), &metadata); err == nil && metadata.Workflow != "" {
		record.Workflow, record.Step = metadata.Namespace+"/"+metadata.Workflow, metadata.Step
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := s3utils.WriteObject(ctx, client, backend.Bucket, backend.Key+unsavedSuffix, data); err != nil {
		return err
	}
	addresses := make([]string, 0, len(resources))
	for _, resource := range resources {
		addresses = append(addresses, resource.Address)
	}
	log.Printf("Resources possibly created but not saved in the state, recorded as %s: %s",
		record.ID, strings.Join(addresses, ", "))
	return nil
}

// stateAddresses returns the addresses of the managed resource instances of a state.
func stateAddresses(raw []byte) (map[string]bool, error) {
	out := map[string]bool{}
	if len(raw) == 0 {
		return out, nil
	}
	var state struct {
		Resources []struct {
			Module    string `json:"module"`
			Mode      string `json:"mode"`
			Type      string `json:"type"`
			Name      string `json:"name"`
			Instances []struct {
				IndexKey any `json:"index_key"`
			} `json:"instances"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("the state is not valid JSON: %w", err)
	}
	for _, resource := range state.Resources {
		if resource.Mode != "managed" {
			continue
		}
		base := resource.Type + "." + resource.Name
		if resource.Module != "" {
			base = resource.Module + "." + base
		}
		for _, instance := range resource.Instances {
			switch key := instance.IndexKey.(type) {
			case nil:
				out[base] = true
			case float64:
				out[base+"["+strconv.FormatFloat(key, 'f', -1, 64)+"]"] = true
			case string:
				out[base+"["+strconv.Quote(key)+"]"] = true
			}
		}
	}
	return out, nil
}

// hostname is the pod name, which the operator traces back to the Job.
func hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return name
}
