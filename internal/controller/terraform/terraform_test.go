package terraform

import (
	"strings"
	"testing"

	tfengine "github.com/karavy/terraform-engine"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

func testState() tfengine.S3Backend {
	return tfengine.S3Backend{Endpoint: "http://minio.storage:9000", Bucket: "tfstate", Region: "us-east-1", Key: "default/vdc/acme.tfstate"}
}

func templateWith(files ...dinovav1alpha1.TerraformFile) *dinovav1alpha1.TerraformTemplate {
	template := &dinovav1alpha1.TerraformTemplate{Spec: dinovav1alpha1.TerraformTemplateSpec{Files: files}}
	template.Name = "vdc"
	return template
}

func TestConfigMapDataKeepsFilesByName(t *testing.T) {
	data, err := ConfigMapData(templateWith(
		dinovav1alpha1.TerraformFile{Name: "main.tf.tmpl", Content: `value = "<FGT_VDCID>"`},
		dinovav1alpha1.TerraformFile{Name: "providers.tf", Content: "terraform {}"},
	))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 2 || data["main.tf.tmpl"] != `value = "<FGT_VDCID>"` || data["providers.tf"] != "terraform {}" {
		t.Fatalf("unexpected data: %v", data)
	}
}

func TestConfigMapDataRejectsUnsafeTemplates(t *testing.T) {
	for name, template := range map[string]*dinovav1alpha1.TerraformTemplate{
		"no files":        templateWith(),
		"path traversal":  templateWith(dinovav1alpha1.TerraformFile{Name: "../main.tf", Content: "x"}),
		"subdirectory":    templateWith(dinovav1alpha1.TerraformFile{Name: "modules/main.tf", Content: "x"}),
		"hidden file":     templateWith(dinovav1alpha1.TerraformFile{Name: ".main.tf", Content: "x"}),
		"not terraform":   templateWith(dinovav1alpha1.TerraformFile{Name: "script.sh", Content: "x"}),
		"state file name": templateWith(dinovav1alpha1.TerraformFile{Name: "terraform.tfstate", Content: "x"}),
		"duplicate":       templateWith(dinovav1alpha1.TerraformFile{Name: "a.tf", Content: "x"}, dinovav1alpha1.TerraformFile{Name: "a.tf", Content: "y"}),
		"empty content":   templateWith(dinovav1alpha1.TerraformFile{Name: "a.tf"}),
	} {
		if _, err := ConfigMapData(template); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestNewJobIsIsolatedAndWiredToItsInputs(t *testing.T) {
	job, err := NewJob(JobConfig{
		Namespace: "ns", Name: "job", StepName: "network", State: testState(), RunnerImage: "runner:test", WorkspacePVC: "pvc",
		TemplateConfigMap: "files", InputSecret: "vars", TimeoutSeconds: 60,
		EnvFromSecrets: []string{"cloud-credentials"},
	})
	if err != nil {
		t.Fatal(err)
	}
	pod := job.Spec.Template.Spec
	container := pod.Containers[0]
	if container.Image != "runner:test" || *job.Spec.ActiveDeadlineSeconds != 60 || *job.Spec.BackoffLimit != 0 {
		t.Errorf("unexpected container or limits: %+v", job.Spec)
	}
	if *pod.AutomountServiceAccountToken || !*container.SecurityContext.ReadOnlyRootFilesystem || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Error("the job must not mount a service account token, write to the root filesystem or escalate")
	}
	claims := map[string]string{}
	for _, volume := range pod.Volumes {
		switch {
		case volume.PersistentVolumeClaim != nil:
			claims[volume.Name] = volume.PersistentVolumeClaim.ClaimName
		case volume.ConfigMap != nil:
			claims[volume.Name] = volume.ConfigMap.Name
		case volume.Secret != nil:
			claims[volume.Name] = volume.Secret.SecretName
		}
	}
	if claims["workspace"] != "pvc" || claims["templates"] != "files" || claims["input"] != "vars" {
		t.Errorf("volumes not wired to the step inputs: %v", claims)
	}
	if len(container.EnvFrom) != 1 || container.EnvFrom[0].SecretRef.Name != "cloud-credentials" {
		t.Errorf("credentials Secret not exposed: %+v", container.EnvFrom)
	}
	var variablesFile, workspace string
	for _, env := range container.Env {
		switch env.Name {
		case "VARIABLES_FILE":
			variablesFile = env.Value
		case "WORKSPACE_DIR":
			workspace = env.Value
		}
	}
	env := map[string]string{}
	for _, variable := range container.Env {
		env[variable.Name] = variable.Value
	}
	for name, want := range map[string]string{
		"TF_STATE_ENDPOINT": "http://minio.storage:9000", "TF_STATE_BUCKET": "tfstate",
		"TF_STATE_REGION": "us-east-1", "TF_STATE_KEY": "default/vdc/acme.tfstate",
	} {
		if env[name] != want {
			t.Errorf("%s = %q, want %q", name, env[name], want)
		}
	}
	if workspace != "/workspace/network" {
		t.Errorf("WORKSPACE_DIR = %q, want the step's own folder", workspace)
	}
	if !strings.HasSuffix(variablesFile, "/"+VariablesKey) {
		t.Errorf("VARIABLES_FILE = %q", variablesFile)
	}
}

func TestNewJobDefaultsAndRequiresARunnerImage(t *testing.T) {
	job, err := NewJob(JobConfig{RunnerImage: "runner:test", StepName: "apply", State: testState()})
	if err != nil || *job.Spec.ActiveDeadlineSeconds != int64(DefaultTimeoutSeconds) {
		t.Fatalf("default timeout not applied: %v %v", job, err)
	}
	if _, err := NewJob(JobConfig{}); err == nil {
		t.Fatal("a missing runner image must be an error")
	}
}

func TestNewJobGivesEachStepItsOwnFolder(t *testing.T) {
	folders := map[string]bool{}
	for _, step := range []string{"network", "cluster", "a"} {
		job, err := NewJob(JobConfig{RunnerImage: "runner:test", StepName: step, State: testState()})
		if err != nil {
			t.Fatal(err)
		}
		for _, env := range job.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "WORKSPACE_DIR" {
				folders[env.Value] = true
			}
		}
	}
	if len(folders) != 3 {
		t.Errorf("steps share a workspace folder: %v", folders)
	}
}

func TestNewJobRejectsStepNamesThatCouldEscapeTheWorkspace(t *testing.T) {
	for _, step := range []string{"", "..", "../other", "a/b", "Upper", "-lead", "trail-", "with space"} {
		if _, err := NewJob(JobConfig{RunnerImage: "runner:test", StepName: step, State: testState()}); err == nil {
			t.Errorf("step name %q accepted", step)
		}
	}
}

func TestNewJobRefusesAnInvalidStateBackend(t *testing.T) {
	for name, state := range map[string]tfengine.S3Backend{
		"empty":         {},
		"bad key":       {Endpoint: "http://s3:9000", Bucket: "tfstate", Region: "r", Key: "a/../b"},
		"bad endpoint":  {Endpoint: "s3:9000", Bucket: "tfstate", Region: "r", Key: "a"},
		"missing":       {Endpoint: "http://s3:9000", Bucket: "tfstate", Region: "r"},
		"capital block": {Endpoint: "http://s3:9000", Bucket: "TfState", Region: "r", Key: "a"},
	} {
		if _, err := NewJob(JobConfig{RunnerImage: "runner:test", StepName: "apply", State: state}); err == nil {
			t.Errorf("%s: a job without a valid state backend would use a local state that is lost", name)
		}
	}
}
