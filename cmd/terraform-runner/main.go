package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/terraform-exec/tfexec"
	tfengine "github.com/karavy/terraform-engine"
	"github.com/karavy/terraform-engine/s3utils"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Terraform workflow step failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			// The operator reads it from the pod status (see terminationMessagePath).
			_ = os.WriteFile("/dev/termination-log", []byte(timeLimitMessage), 0o644)
		}
		os.Exit(1)
	}
}

// timeLimitMessage is the termination message of a runner that stopped OpenTofu at its time
// limit (TF_STOP_AFTER_SECONDS), before the Job's deadline.
const timeLimitMessage = "Stopped at its time limit"

// stopContext is cancelled on SIGTERM or SIGINT, and at TF_STOP_AFTER_SECONDS: the operator
// sets it before the Job's deadline, so that OpenTofu is interrupted, saves the state and
// releases the lock while the pod still has time, instead of being killed.
func stopContext() (context.Context, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	seconds, err := strconv.Atoi(os.Getenv("TF_STOP_AFTER_SECONDS"))
	if err != nil || seconds <= 0 {
		return ctx, stop
	}
	limited, cancel := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	return limited, func() { cancel(); stop() }
}

func run() error {
	workspace := envOr("WORKSPACE_DIR", "/workspace")
	templates := envOr("TEMPLATE_DIR", "/templates")
	variablesFile := envOr("VARIABLES_FILE", "/input/terraform.auto.tfvars.json")
	variablesBytes, err := os.ReadFile(variablesFile)
	if err != nil {
		return fmt.Errorf("read mapped Terraform variables: %w", err)
	}
	var variables map[string]any
	if err := json.Unmarshal(variablesBytes, &variables); err != nil || variables == nil {
		return errors.New("mapped Terraform variables must be a JSON object")
	}
	if err := os.MkdirAll(workspace, 0o750); err != nil {
		return fmt.Errorf("create Terraform workspace: %w", err)
	}
	if err := removeStaleConfiguration(workspace); err != nil {
		return err
	}
	files, err := os.ReadDir(templates)
	if err != nil {
		return fmt.Errorf("read Terraform template ConfigMap: %w", err)
	}
	if len(files) == 0 {
		return errors.New("template ConfigMap for Terraform is empty")
	}
	for _, file := range files {
		if file.IsDir() || strings.HasPrefix(file.Name(), "..") || filepath.Base(file.Name()) != file.Name() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(templates, file.Name()))
		if err != nil {
			return fmt.Errorf("read Terraform template %q: %w", file.Name(), err)
		}
		destination := filepath.Join(workspace, file.Name())
		if err := os.WriteFile(destination, contents, 0o640); err != nil {
			return fmt.Errorf("copy Terraform template %q: %w", file.Name(), err)
		}
		if strings.HasSuffix(file.Name(), ".tf.tmpl") {
			if err := tfengine.RenderTemplate(destination, variables); err != nil {
				return fmt.Errorf("render Terraform template %q: %w", file.Name(), err)
			}
		}
	}
	if _, err := findTerraformFile(workspace); err != nil {
		return err
	}
	variablesBytes, err = json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("encode Terraform variables: %w", err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "terraform.auto.tfvars.json"), variablesBytes, 0o600); err != nil {
		return fmt.Errorf("write Terraform variable file: %w", err)
	}
	// The state lives in S3-compatible storage under a stable key, never in the workspace,
	// so a later run, possibly of another workflow, continues from it. The backend is an
	// override file: it wins over any backend the template declares. Without a complete
	// configuration the run must fail rather than fall back to a local state that is lost.
	backend := tfengine.S3Backend{
		Endpoint: os.Getenv("TF_STATE_ENDPOINT"), Bucket: os.Getenv("TF_STATE_BUCKET"),
		Region: os.Getenv("TF_STATE_REGION"), Key: os.Getenv("TF_STATE_KEY"),
	}
	if _, err := tfengine.WriteS3BackendOverride(workspace, backend); err != nil {
		return fmt.Errorf("configure the Terraform state backend: %w", err)
	}
	ctx, stop := stopContext()
	defer stop()
	// Before init, so that the files of a run that fails are kept too.
	if err := storeRenderedFiles(ctx, workspace, backend); err != nil {
		return err
	}
	terraform, err := tfengine.CreateTerraform(workspace)
	if err != nil {
		return fmt.Errorf("initialize OpenTofu: %w", err)
	}
	// The backend is rewritten on every run, so init always runs; it is cheap once the
	// providers are cached in the workspace.
	if err := tfengine.RunInit(terraform, ctx); err != nil {
		return fmt.Errorf("OpenTofu init failed: %w", err)
	}
	switch mode := envOr("TF_MODE", "apply"); mode {
	case "apply":
		if err := apply(ctx, terraform, backend); err != nil {
			return fmt.Errorf("OpenTofu plan/apply failed: %w", err)
		}
		return nil
	case "plan":
		return savePlan(ctx, terraform, newPlanStorage(backend))
	case "apply-plan":
		return applySavedPlan(ctx, terraform, newPlanStorage(backend), backend)
	case "destroy":
		log.Printf("Destroying the resources of state %s", backend.Key)
		if err := terraform.Destroy(ctx); err != nil {
			return fmt.Errorf("OpenTofu destroy failed: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown TF_MODE %q", mode)
	}
}

// planStorage keeps saved plans in the state storage, with the credentials of the state, at
// TF_PLAN_KEY: the plan itself, its readable text (".txt") and its summary (".json").
type planStorage struct {
	client      *s3.Client
	bucket, key string
	file        string
}

func newPlanStorage(backend tfengine.S3Backend) planStorage {
	return planStorage{
		client: storageClient(backend),
		bucket: backend.Bucket, key: os.Getenv("TF_PLAN_KEY"),
		file: filepath.Join(os.TempDir(), "step.tfplan"),
	}
}

// savePlan plans without applying, and stores the plan, its text and its summary.
func savePlan(ctx context.Context, terraform *tfexec.Terraform, storage planStorage) error {
	if storage.key == "" {
		return errors.New("TF_PLAN_KEY is not set")
	}
	summary, text, err := tfengine.RunPlan(terraform, ctx, storage.file)
	if err != nil {
		return fmt.Errorf("OpenTofu plan failed: %w", err)
	}
	plan, err := os.ReadFile(storage.file)
	if err != nil {
		return fmt.Errorf("read the saved plan: %w", err)
	}
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return err
	}
	objects := map[string][]byte{storage.key: plan, storage.key + ".txt": []byte(text), storage.key + ".json": summaryJSON}
	for key, data := range objects {
		if err := s3utils.WriteObject(ctx, storage.client, storage.bucket, key, data); err != nil {
			return fmt.Errorf("store the plan: %w", err)
		}
	}
	return nil
}

// applySavedPlan applies exactly the plan stored at TF_PLAN_KEY.
func applySavedPlan(
	ctx context.Context, terraform *tfexec.Terraform, storage planStorage, backend tfengine.S3Backend,
) error {
	if storage.key == "" {
		return errors.New("TF_PLAN_KEY is not set")
	}
	plan, err := s3utils.ReadObject(ctx, storage.client, storage.bucket, storage.key)
	if err != nil {
		return fmt.Errorf("read the approved plan: %w", err)
	}
	if err := os.WriteFile(storage.file, plan, 0o600); err != nil {
		return fmt.Errorf("write the approved plan: %w", err)
	}
	log.Printf("Applying the approved plan")
	if err := watchedApply(ctx, terraform, backend, tfexec.DirOrPlan(storage.file)); err != nil {
		return fmt.Errorf("OpenTofu apply of the approved plan failed: %w", err)
	}
	log.Printf("Approved plan applied")
	return nil
}

// apply plans, and applies only when the plan has changes (see watchedApply).
func apply(ctx context.Context, terraform *tfexec.Terraform, backend tfengine.S3Backend) error {
	changes, err := terraform.Plan(ctx)
	if err != nil {
		return err
	}
	if !changes {
		log.Printf("No changes to apply")
		return nil
	}
	log.Printf("Applying the changes")
	if err := watchedApply(ctx, terraform, backend); err != nil {
		return err
	}
	log.Printf("Changes applied")
	return nil
}

func removeStaleConfiguration(workspace string) error {
	entries, err := os.ReadDir(workspace)
	if err != nil {
		return fmt.Errorf("read Terraform workspace: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".tf") || strings.HasSuffix(name, ".tf.tmpl") || name == "terraform.auto.tfvars.json" {
			if err := os.RemoveAll(filepath.Join(workspace, name)); err != nil {
				return fmt.Errorf("remove stale Terraform file %q: %w", name, err)
			}
		}
	}
	return nil
}

// storageClient connects to the state storage with the credentials of the state.
func storageClient(backend tfengine.S3Backend) *s3.Client {
	return s3utils.NewClient(
		backend.Endpoint, backend.Region, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"),
	)
}

// storeRenderedFiles stores in the state storage, under TF_RENDERED_PREFIX, the files the step
// runs (see renderedObjects). Without a prefix it stores nothing.
func storeRenderedFiles(ctx context.Context, workspace string, backend tfengine.S3Backend) error {
	prefix := os.Getenv("TF_RENDERED_PREFIX")
	if prefix == "" {
		return nil
	}
	objects, err := renderedObjects(workspace, prefix, os.Getenv("TF_RENDERED_METADATA"), time.Now())
	if err != nil {
		return err
	}
	client := storageClient(backend)
	for key, data := range objects {
		if err := s3utils.WriteObject(ctx, client, backend.Bucket, key, data); err != nil {
			return fmt.Errorf("store the rendered Terraform files: %w", err)
		}
	}
	log.Printf("Stored %d rendered Terraform files under %s", len(objects)-1, prefix)
	return nil
}

// renderedFile describes one stored file in the manifest.
type renderedFile struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int    `json:"size"`
}

// renderedObjects returns, by object key under prefix, the files of the workspace that the
// step runs: the .tf files, as rendered from the template, and terraform.auto.tfvars.json. It
// adds manifest.json: the metadata the operator gave (which workflow, template, step,
// revision and state), the time and the list of files with their SHA-256.
func renderedObjects(workspace, prefix, metadata string, now time.Time) (map[string][]byte, error) {
	manifest := map[string]any{}
	if metadata != "" {
		if err := json.Unmarshal([]byte(metadata), &manifest); err != nil {
			return nil, fmt.Errorf("TF_RENDERED_METADATA is not a JSON object: %w", err)
		}
	}
	entries, err := os.ReadDir(workspace)
	if err != nil {
		return nil, fmt.Errorf("read Terraform workspace: %w", err)
	}
	objects := map[string][]byte{}
	files := []renderedFile{}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || !strings.HasSuffix(name, ".tf") && name != "terraform.auto.tfvars.json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(workspace, name))
		if err != nil {
			return nil, fmt.Errorf("read rendered Terraform file %q: %w", name, err)
		}
		sum := sha256.Sum256(data)
		objects[prefix+name] = data
		files = append(files, renderedFile{Name: name, SHA256: hex.EncodeToString(sum[:]), Size: len(data)})
	}
	manifest["renderedAt"] = now.UTC().Format(time.RFC3339)
	manifest["files"] = files
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	objects[prefix+"manifest.json"] = encoded
	return objects, nil
}

func findTerraformFile(workspace string) (string, error) {
	entries, err := os.ReadDir(workspace)
	if err != nil {
		return "", err
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tf") {
			paths = append(paths, filepath.Join(workspace, entry.Name()))
		}
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return "", errors.New("template ConfigMap for Terraform must contain at least one .tf or .tf.tmpl file")
	}
	return paths[0], nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
