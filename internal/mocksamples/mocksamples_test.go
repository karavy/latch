package mocksamples

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The mock copies in the repository must be what the generator makes from the real samples
// now: after changing a real sample, run `go run ./hack/gen-mock-samples`.
func TestMockSamplesAreUpToDate(t *testing.T) {
	for dir, generate := range map[string]func(string) ([]File, error){
		"../../config/samples/p2p-workflow":       P2P,
		"../../config/samples/terraform-workflow": Terraform,
	} {
		files, err := generate(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			stored, err := os.ReadFile(filepath.Join(dir, "mock", file.Name))
			if err != nil || !bytes.Equal(stored, file.Content) {
				t.Errorf("%s/mock/%s is not up to date: run go run ./hack/gen-mock-samples", dir, file.Name)
			}
		}
	}
}

func TestP2PMockOnlyChangesWhereItCalls(t *testing.T) {
	files, err := P2P("../../config/samples/p2p-workflow")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		text := string(file.Content)
		if strings.Contains(text, PortalURL) || strings.Contains(text, "debug: true") {
			t.Errorf("%s still calls the real portal or has debug", file.Name)
		}
		if file.Name == "workflowtemplate.yaml" && strings.Count(text, "-mock") < 13 {
			t.Errorf("the workflow template does not use the mock ApiCallTemplates:\n%s", text)
		}
	}
}
