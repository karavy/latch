package terraform

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/karavy/terraform-engine/s3utils"
)

// TestRunnerKeepsStateInS3AcrossRuns runs the real cmd/terraform-runner with OpenTofu against
// an S3-compatible storage, as two Jobs of two different workflows would: separate empty
// working directories, the same state key, a changed variable. It then reads the outputs with
// the code the operator uses. It needs TFSTATE_TEST_S3_ENDPOINT, TFSTATE_TEST_S3_ACCESS_KEY and
// TFSTATE_TEST_S3_SECRET_KEY (for example a local MinIO) and the tofu binary; otherwise it skips.
func TestRunnerKeepsStateInS3AcrossRuns(t *testing.T) {
	endpoint, access, secret := os.Getenv("TFSTATE_TEST_S3_ENDPOINT"), os.Getenv("TFSTATE_TEST_S3_ACCESS_KEY"), os.Getenv("TFSTATE_TEST_S3_SECRET_KEY")
	if endpoint == "" || access == "" || secret == "" {
		t.Skip("TFSTATE_TEST_S3_ENDPOINT/ACCESS_KEY/SECRET_KEY not set")
	}
	if _, err := exec.LookPath("tofu"); err != nil {
		t.Skip("tofu not found in PATH")
	}
	ctx := context.Background()
	const bucket, region = "tfstate-operator-test", "us-east-1"
	client := s3utils.NewClient(endpoint, region, access, secret)
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil && !strings.Contains(err.Error(), "BucketAlready") {
		t.Fatalf("creating the bucket: %v", err)
	}
	key := ObjectKey("default", "vdc/"+strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))+"/network")

	runner := filepath.Join(t.TempDir(), "terraform-runner")
	build := exec.Command("go", "build", "-o", runner, "./cmd/terraform-runner")
	build.Dir = filepath.Join("..", "..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the runner: %v\n%s", err, output)
	}

	// The template declares a LOCAL backend on purpose: the runner's override must win,
	// otherwise the state would stay in the workspace and be lost with the Job.
	templates := t.TempDir()
	files := map[string]string{
		"main.tf.tmpl": `
terraform {
  backend "local" {
    path = "should-not-be-used.tfstate"
  }
}
variable "network_name" {
  type = string
}
resource "terraform_data" "network" {
  input = "<FGT_NETWORK_NAME>"
}
output "network_id" {
  value = terraform_data.network.id
}
output "network_name" {
  value = var.network_name
}
output "admin_password" {
  value     = "pw-${var.network_name}"
  sensitive = true
}
`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(templates, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	backend := StateConfig{Endpoint: endpoint, Bucket: bucket, Region: region}.Backend(key)

	run := func(networkName string) (workspace string) {
		t.Helper()
		workspace = t.TempDir()
		input := filepath.Join(t.TempDir(), "terraform.auto.tfvars.json")
		if err := os.WriteFile(input, []byte(`{"network_name":"`+networkName+`"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command(runner)
		command.Env = append(os.Environ(),
			"WORKSPACE_DIR="+workspace, "TEMPLATE_DIR="+templates, "VARIABLES_FILE="+input,
			"TF_STATE_ENDPOINT="+backend.Endpoint, "TF_STATE_BUCKET="+backend.Bucket, "TF_STATE_REGION="+backend.Region, "TF_STATE_KEY="+key,
			"AWS_ACCESS_KEY_ID="+access, "AWS_SECRET_ACCESS_KEY="+secret, "TF_IN_AUTOMATION=true", "HOME="+t.TempDir(),
		)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("runner for %q failed: %v\n%s", networkName, err, output)
		}
		return workspace
	}

	firstWorkspace := run("lan-one")
	secondWorkspace := run("lan-two") // another Job: a new empty workspace, a changed variable

	for _, workspace := range []string{firstWorkspace, secondWorkspace} {
		if _, err := os.Stat(filepath.Join(workspace, "should-not-be-used.tfstate")); err == nil {
			t.Errorf("a local state was written in %s: the template's own backend was not overridden", workspace)
		}
		if _, err := os.Stat(filepath.Join(workspace, "terraform.tfstate")); err == nil {
			t.Errorf("a local terraform.tfstate exists in %s: the state must live only in the storage", workspace)
		}
	}

	reader := S3StateReader{Endpoint: endpoint, Region: region, Bucket: bucket, AccessKeyID: access, SecretAccessKey: secret}
	state, err := reader.Read(ctx, key)
	if err != nil {
		t.Fatalf("the state is not in the storage: %v", err)
	}
	document, sensitive, err := StateOutputs(state)
	if err != nil {
		t.Fatal(err)
	}
	var outputs map[string]string
	if err := json.Unmarshal(document, &outputs); err != nil {
		t.Fatal(err)
	}
	if outputs["network_name"] != "lan-two" || outputs["admin_password"] != "pw-lan-two" {
		t.Errorf("outputs after the second run = %v, want the changed variable", outputs)
	}
	if len(sensitive) != 1 || sensitive[0] != "admin_password" {
		t.Errorf("sensitive outputs = %v, want [admin_password]", sensitive)
	}
	// The same resource was modified: it keeps the id the provider gave it the first time.
	var parsed struct {
		Resources []struct {
			Instances []struct {
				Attributes struct{ ID string } `json:"attributes"`
			} `json:"instances"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(state, &parsed); err != nil || len(parsed.Resources) != 1 || len(parsed.Resources[0].Instances) != 1 {
		t.Fatalf("expected exactly one resource in the state: %v %s", err, state)
	}
	if outputs["network_id"] != parsed.Resources[0].Instances[0].Attributes.ID {
		t.Errorf("network_id output %q differs from the resource id", outputs["network_id"])
	}
	if _, err := reader.Read(ctx, ObjectKey("another-namespace", "vdc/"+strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))+"/network")); err == nil {
		t.Error("the same step key in another namespace must be a different state")
	}
}
