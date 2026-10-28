// Package mocksamples generates, from the real samples, the copies that run against the mock
// services of cmd/mock-api: the FortiGate (p2p) workflow against the mock admin portal, and the
// Virtuozzo Terraform workflow against the mock OpenStack. The copies differ from the real
// samples only in where they send requests, which credentials they use, their names and their
// Terraform states, so a test against the mock tests the workflow that runs for real.
package mocksamples

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/yaml"
)

const (
	// PortalURL is the base of the real admin portal endpoints, MockPortalURL that of the mock.
	PortalURL = "https://dev-portal.admin.dinova.cloud:8453/api/v1"
	// The mock portal is called over HTTPS: the operator sends the API key only over HTTPS (see
	// hack/mock-api-tls.sh for the test CA it trusts).
	MockPortalURL = "https://mock-api.apicall-debug.svc.cluster.local:8443/portal/api/v1"
	// MockPortalKey is the API key the mock portal accepts (see cmd/mock-api/portal.go).
	MockPortalKey = "mock-portal-key"
	// MockAuthURL is the Keystone endpoint of the mock OpenStack, with the credentials it accepts.
	MockAuthURL  = "http://mock-api.apicall-debug.svc.cluster.local:8080/identity/v3"
	MockUser     = "admin"
	MockPassword = "Password01!"

	suffix = "-mock"
)

// File is a generated manifest, with its path relative to the mock folder.
type File struct {
	Name    string
	Content []byte
}

type object = map[string]any

// P2P returns the mock copy of the FortiGate workflow in dir (config/samples/p2p-workflow).
func P2P(dir string) ([]File, error) {
	templates, err := read(dir, "apicalltemplates.yaml")
	if err != nil {
		return nil, err
	}
	names := map[string]string{}
	for _, template := range templates {
		meta := template["metadata"].(object)
		name := meta["name"].(string)
		names[name] = name + suffix
		meta["name"] = name + suffix
		label(meta, "p2p-workflow-mock")
		spec := template["spec"].(object)
		endpoint, _ := spec["endpoint"].(string)
		if !strings.HasPrefix(endpoint, PortalURL) {
			return nil, fmt.Errorf("ApiCallTemplate %s does not call the portal at %s: %s", name, PortalURL, endpoint)
		}
		spec["endpoint"] = MockPortalURL + strings.TrimPrefix(endpoint, PortalURL)
		delete(spec, "debug")
		if auth, ok := spec["authentication"].(object); ok {
			if ref, ok := auth["tokenSecretRef"].(object); ok {
				ref["name"] = "portal-api-key" + suffix
			}
		}
	}
	workflowTemplates, err := read(dir, "workflowtemplate.yaml")
	if err != nil {
		return nil, err
	}
	for _, template := range workflowTemplates {
		meta := template["metadata"].(object)
		meta["name"] = meta["name"].(string) + suffix
		label(meta, "p2p-workflow-mock")
		spec := template["spec"].(object)
		for _, key := range []string{"steps", "deleteSteps"} {
			steps, _ := spec[key].([]any)
			for _, step := range steps {
				api, ok := step.(object)["api"].(object)
				if !ok {
					continue
				}
				delete(api, "debug")
				if err := rename(api, names); err != nil {
					return nil, err
				}
				if poll, ok := api["poll"].(object); ok {
					if err := rename(poll, names); err != nil {
						return nil, err
					}
					if recover, ok := poll["recover"].(object); ok {
						if err := rename(recover, names); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	workflows, err := read(dir, "workflow.yaml")
	if err != nil {
		return nil, err
	}
	for _, workflow := range workflows {
		meta := workflow["metadata"].(object)
		meta["name"] = meta["name"].(string) + suffix
		label(meta, "p2p-workflow-mock")
		for _, run := range workflow["spec"].(object)["templates"].([]any) {
			ref := run.(object)["templateRef"].(object)
			ref["name"] = ref["name"].(string) + suffix
		}
	}
	key := object{"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
		"metadata":   object{"name": "portal-api-key" + suffix, "labels": object{"app.kubernetes.io/name": "latch", "app.kubernetes.io/component": "p2p-workflow-mock"}},
		"stringData": object{"api-key": MockPortalKey}}
	return files("p2p-workflow", []part{
		{"apicalltemplates.yaml", "apicalltemplates.yaml", templates},
		{"workflowtemplate.yaml", "workflowtemplate.yaml", workflowTemplates},
		{"workflow.yaml", "workflow.yaml", workflows},
		{"apikey.yaml", "", []object{key}},
	}, []string{"apicalltemplates.yaml", "workflowtemplate.yaml", "apikey.yaml"},
		"kubectl create -f config/samples/p2p-workflow/mock/workflow.yaml")
}

// Terraform returns the mock copy of the Virtuozzo workflow in dir
// (config/samples/terraform-workflow). The TerraformTemplates and their ConfigMap are not copied:
// the mock runs the real Terraform code.
func Terraform(dir string) ([]File, error) {
	secrets, err := read(dir, "step-virtuozzo-vm-secret.yaml")
	if err != nil {
		return nil, err
	}
	credentials := map[string]string{}
	for _, secret := range secrets {
		meta := secret["metadata"].(object)
		credentials[meta["name"].(string)] = meta["name"].(string) + suffix
		meta["name"] = meta["name"].(string) + suffix
		label(meta, "terraform-workflow-mock")
		data := secret["stringData"].(object)
		if _, ok := data["TF_VAR_os_auth_url"]; !ok {
			return nil, fmt.Errorf("secret %s has no TF_VAR_os_auth_url", meta["name"])
		}
		data["TF_VAR_os_auth_url"], data["TF_VAR_os_username"], data["TF_VAR_os_password"] = MockAuthURL, MockUser, MockPassword
	}
	templates, err := read(dir, "virtuozzo-vm-workflowtemplate.yaml")
	if err != nil {
		return nil, err
	}
	names := map[string]string{}
	for _, template := range templates {
		meta := template["metadata"].(object)
		names[meta["name"].(string)] = meta["name"].(string) + suffix
		meta["name"] = meta["name"].(string) + suffix
		label(meta, "terraform-workflow-mock")
		for _, step := range template["spec"].(object)["steps"].([]any) {
			tf, ok := step.(object)["terraform"].(object)
			if !ok {
				continue
			}
			state := tf["state"].(object)
			key := state["key"].(string)
			first, rest, _ := strings.Cut(key, "/")
			state["key"] = first + suffix + "/" + rest
			secretsUsed, _ := tf["envFromSecrets"].([]any)
			for i, name := range secretsUsed {
				if renamed, ok := credentials[name.(string)]; ok {
					secretsUsed[i] = renamed
				}
			}
		}
	}
	var runs []part
	for _, file := range []string{"virtuozzo-vm-workflow.yaml", "virtuozzo-vm-instance.yaml"} {
		objects, err := read(dir, file)
		if err != nil {
			return nil, err
		}
		for _, obj := range objects {
			meta := obj["metadata"].(object)
			meta["name"] = meta["name"].(string) + suffix
			label(meta, "terraform-workflow-mock")
			spec := obj["spec"].(object)
			if ref, ok := spec["templateRef"].(object); ok {
				ref["name"] = names[ref["name"].(string)]
			}
			if stages, ok := spec["templates"].([]any); ok {
				for _, stage := range stages {
					ref := stage.(object)["templateRef"].(object)
					ref["name"] = names[ref["name"].(string)]
				}
			}
		}
		runs = append(runs, part{strings.TrimPrefix(file, "virtuozzo-vm-"), file, objects})
	}
	return files("terraform-workflow", append([]part{
		{"secret.yaml", "step-virtuozzo-vm-secret.yaml", secrets},
		{"workflowtemplate.yaml", "virtuozzo-vm-workflowtemplate.yaml", templates},
	}, runs...), []string{"secret.yaml", "workflowtemplate.yaml"},
		"kubectl create -f config/samples/terraform-workflow/mock/workflow.yaml (one run), or\n#   kubectl apply -f config/samples/terraform-workflow/mock/instance.yaml (kept applied)")
}

type part struct {
	name, source string
	objects      []object
}

func files(sample string, parts []part, resources []string, run string) ([]File, error) {
	var out []File
	for _, p := range parts {
		var buf bytes.Buffer
		buf.WriteString("# Generated by `go run ./hack/gen-mock-samples`")
		if p.source != "" {
			buf.WriteString(" from ../" + p.source)
		}
		buf.WriteString(": do not edit.\n")
		for i, obj := range p.objects {
			raw, err := yaml.Marshal(obj)
			if err != nil {
				return nil, err
			}
			if i > 0 {
				buf.WriteString("---\n")
			}
			buf.Write(raw)
		}
		out = append(out, File{Name: p.name, Content: buf.Bytes()})
	}
	var k bytes.Buffer
	fmt.Fprintf(&k, "# Generated by `go run ./hack/gen-mock-samples`: do not edit.\n"+
		"# The %s sample against the mock services of cmd/mock-api (docs/tests). Apply with\n"+
		"#   kubectl apply -k config/samples/%s/mock/\n# then run it with\n#   %s\nresources:\n", sample, sample, run)
	for _, resource := range resources {
		fmt.Fprintf(&k, "  - %s\n", resource)
	}
	return append(out, File{Name: "kustomization.yaml", Content: k.Bytes()}), nil
}

func label(meta object, component string) {
	labels, _ := meta["labels"].(object)
	if labels == nil {
		labels = object{}
		meta["labels"] = labels
	}
	labels["app.kubernetes.io/component"] = component
}

func rename(holder object, names map[string]string) error {
	ref, ok := holder["templateRef"].(object)
	if !ok {
		return nil
	}
	renamed, ok := names[ref["name"].(string)]
	if !ok {
		return fmt.Errorf("templateRef %v is not an ApiCallTemplate of the sample", ref["name"])
	}
	ref["name"] = renamed
	return nil
}

// read returns the objects of a multi-document YAML file.
func read(dir, name string) ([]object, error) {
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}
	var out []object
	for doc := range bytes.SplitSeq(raw, []byte("\n---")) {
		var obj object
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if len(obj) > 0 {
			out = append(out, obj)
		}
	}
	return out, nil
}
