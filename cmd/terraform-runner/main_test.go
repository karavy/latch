package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRenderedObjects(t *testing.T) {
	workspace := t.TempDir()
	for name, content := range map[string]string{
		"main.tf": "resource {}", "vm-1.tf": "vm 1", "backend_override.tf": "backend",
		"terraform.auto.tfvars.json": `{"a":1}`,
		"notes.txt":                  "not run", ".terraform.lock.hcl": "lock",
	} {
		if err := os.WriteFile(filepath.Join(workspace, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(workspace, ".terraform", "x.tf"), 0o750); err != nil {
		t.Fatal(err)
	}
	prefix := "ns/rendered/wf-uid/tpl/vm/"
	now := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	objects, err := renderedObjects(workspace, prefix, `{"workflow":"wf","step":"vm"}`, now)
	if err != nil {
		t.Fatal(err)
	}
	stored := []string{"main.tf", "vm-1.tf", "backend_override.tf", "terraform.auto.tfvars.json", "manifest.json"}
	for _, name := range stored {
		if _, ok := objects[prefix+name]; !ok {
			t.Errorf("%s is not stored", name)
		}
	}
	if len(objects) != 5 {
		t.Errorf("stored %d objects, want 5", len(objects))
	}
	if string(objects[prefix+"main.tf"]) != "resource {}" {
		t.Errorf("main.tf = %q", objects[prefix+"main.tf"])
	}
	var manifest struct {
		Workflow, Step, RenderedAt string
		Files                      []renderedFile
	}
	if err := json.Unmarshal(objects[prefix+"manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Workflow != "wf" || manifest.Step != "vm" ||
		manifest.RenderedAt != "2026-10-26T10:00:00Z" || len(manifest.Files) != 4 {
		t.Errorf("manifest = %+v", manifest)
	}
	sum := sha256.Sum256([]byte("resource {}"))
	for _, file := range manifest.Files {
		if file.Name == "main.tf" && (file.Size != 11 || file.SHA256 != hex.EncodeToString(sum[:])) {
			t.Errorf("main.tf in the manifest: %+v", file)
		}
	}
	if _, err := renderedObjects(workspace, prefix, `[1]`, time.Now()); err == nil {
		t.Error("metadata that is not an object was accepted")
	}
}
