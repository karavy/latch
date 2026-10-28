package workflow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	tfengine "github.com/karavy/terraform-engine"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

// The p2p sample is what people copy: its references must hold, and a Workflow that gives
// the values it shows must run it.
func TestP2PSampleIsConsistent(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "config", "samples", "p2p-workflow")
	read := func(name string, into any) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.UnmarshalStrict(raw, into); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	template := &dinovav1alpha1.WorkflowTemplate{}
	workflow := &dinovav1alpha1.Workflow{}
	read("workflowtemplate.yaml", template)
	read("workflow.yaml", workflow)

	if err := validateTemplate(template); err != nil {
		t.Fatalf("template references: %v", err)
	}
	if len(workflow.Spec.Templates) == 0 {
		t.Fatal("the sample workflow runs nothing")
	}
	stage := workflow.Spec.Templates[0]
	if stage.TemplateRef.Name != template.Name {
		t.Fatalf("the workflow runs %q, the sample template is %q", stage.TemplateRef.Name, template.Name)
	}
	if _, err := effectiveParameters(template, stage); err != nil {
		t.Fatalf("parameters of the sample workflow: %v", err)
	}
}

// The deliver step builds the body the portal expects, and puts env_id in the path.
func TestP2PDeliverRequest(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "config", "samples", "p2p-workflow")
	raw, err := os.ReadFile(filepath.Join(dir, "apicalltemplates.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var call *dinovav1alpha1.ApiCallTemplate
	for document := range strings.SplitSeq(string(raw), "\n---\n") {
		candidate := &dinovav1alpha1.ApiCallTemplate{}
		if err := yaml.UnmarshalStrict([]byte(document), candidate); err != nil {
			t.Fatal(err)
		}
		if candidate.Name == "p2p-fortigate-deliver" {
			call = candidate
		}
	}
	if call == nil || call.Spec.Method != "POST" {
		t.Fatalf("p2p-fortigate-deliver must be a POST: %+v", call)
	}
	templateRaw, err := os.ReadFile(filepath.Join(dir, "workflowtemplate.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	template := &dinovav1alpha1.WorkflowTemplate{}
	if err := yaml.UnmarshalStrict(templateRaw, template); err != nil {
		t.Fatal(err)
	}
	var inputs []dinovav1alpha1.WorkflowInput
	for _, step := range template.Spec.Steps {
		if step.Name == "deliver" {
			inputs = step.API.Inputs
		}
	}
	values := map[string]string{
		"parameter.vmName": "fw-acme-01", "parameter.vcloudOrgName": "Dinova-Cloud", "parameter.clientName": "ACME",
		"parameter.rifContratto": "CNT.26.0001.01", "parameter.cpuCount": "2", "parameter.ramGb": "4", "parameter.vdcCpuMhz": "4000", "parameter.vdcRamMb": "8192", "parameter.vdcStorageMb": "61440",
		"parameter.vdcCpuReservationPct": "0", "parameter.vdcRamReservationPct": "10", "parameter.vdcName": "ACME-VDC",
		"get-env.env_id": "6", "get-orgs.org_id": `"urn:vcloud:org:1"`,
		"create-segment.nsx_segment_id": `"P2P-ACME-NET"`, "create-segment.nsx_segment_name": `"P2P-ACME-NET"`,
		"create-segment.nsx_segment_network": `"178.239.21.34/31"`,
		"get-flexvm-configs.config_id":       `3619`, "get-storage-profiles.storage_profile": `"DATA4_ENT"`, "get-templates.template_id": `4`,
		"free-subnet.tenant_ip": `"178.239.21.35"`, "free-subnet.nsx_ip": `"178.239.21.34"`,
	}
	resolve := func(input dinovav1alpha1.WorkflowInput) ([]byte, error) {
		if input.Parameter != "" {
			return json.Marshal(values["parameter."+input.Parameter])
		}
		value, ok := values[input.Step+"."+input.Output]
		if !ok {
			t.Errorf("input %s.%s has no value in the test", input.Step, input.Output)
		}
		return []byte(value), nil
	}
	endpoint, err := applyPathInputs(call.Spec.Endpoint, inputs, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://dev-portal.admin.dinova.cloud:8453/api/v1/fortigate-vm/vcloud/6/deliver"; endpoint != want {
		t.Errorf("endpoint = %s, want %s", endpoint, want)
	}
	body, err := applyInputs([]byte(call.Spec.Body), inputs, resolve)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"vm_name":"fw-acme-01","template_id":4,"org_name":"Dinova-Cloud","org_id":"urn:vcloud:org:1",
		"vdc_name":"ACME-VDC","create_vdc":true,"nsx_segment_id":"P2P-ACME-NET","nsx_segment_name":"P2P-ACME-NET",
		"nsx_segment_network":"178.239.21.34/31","assigned_ip":"178.239.21.35","netmask":"255.255.255.254",
		"gateway":"178.239.21.34","flexvm_config_id":"3619","cliente":"ACME","rif_contratto":"CNT.26.0001.01",
		"cpu_count":2,"ram_gb":4,"vdc_cpu_mhz":4000,"vdc_ram_mb":8192,"vdc_storage_mb":61440,"vdc_cpu_reservation_pct":0,"vdc_ram_reservation_pct":10,"storage_profile":"DATA4_ENT"}`
	var gotJSON, wantJSON map[string]any
	if err := json.Unmarshal(body, &gotJSON); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &wantJSON); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotJSON, wantJSON) {
		t.Errorf("body = %s\nwant   %s", body, want)
	}
}

// The Virtuozzo VM sample is what people copy: its references must hold, each step must have its
// own Terraform files and its own state, and the list of VMs that a Workflow gives as a string
// must reach the templates as a list, one VM for each element.
func TestVirtuozzoVMSampleIsConsistent(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "config", "samples", "terraform-workflow")
	readFile := func(name string) string {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	read := func(name string, into any) {
		t.Helper()
		if err := yaml.UnmarshalStrict([]byte(readFile(name)), into); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	templates := map[string]*dinovav1alpha1.TerraformTemplate{}
	for document := range strings.SplitSeq(readFile("virtuozzo-vm-terraformtemplate.yaml"), "\n---\n") {
		candidate := &dinovav1alpha1.TerraformTemplate{}
		if err := yaml.UnmarshalStrict([]byte(document), candidate); err != nil {
			t.Fatal(err)
		}
		templates[candidate.Name] = candidate
	}
	configMap := &corev1.ConfigMap{}
	template := &dinovav1alpha1.WorkflowTemplate{}
	workflow := &dinovav1alpha1.Workflow{}
	read("step-virtuozzo-vm-configmap.yaml", configMap)
	if configMap.Immutable == nil || !*configMap.Immutable {
		t.Fatal("the ConfigMap with the Terraform files must be immutable, or the operator refuses it")
	}
	read("virtuozzo-vm-workflowtemplate.yaml", template)
	read("virtuozzo-vm-workflow.yaml", workflow)

	lookup := func(ref dinovav1alpha1.WorkflowConfigMapKeyReference) (string, error) {
		value, ok := configMap.Data[ref.Key]
		if !ok || ref.Name != configMap.Name {
			return "", fmt.Errorf("the sample ConfigMap has no %s/%s", ref.Name, ref.Key)
		}
		return value, nil
	}
	if err := validateTemplate(template); err != nil {
		t.Fatalf("template references: %v", err)
	}
	stage := workflow.Spec.Templates[0]
	if stage.TemplateRef.Name != template.Name {
		t.Fatalf("the workflow runs %q, the sample template is %q", stage.TemplateRef.Name, template.Name)
	}
	parameters, err := effectiveParameters(template, stage)
	if err != nil {
		t.Fatalf("parameters of the sample workflow: %v", err)
	}
	instance := &dinovav1alpha1.WorkflowInstance{}
	read("virtuozzo-vm-instance.yaml", instance)
	checkSampleInstance(t, instance, template)
	if got := []string{template.Spec.Steps[0].Name, template.Spec.Steps[1].Name}; len(template.Spec.Steps) != 2 || got[0] != "network" || got[1] != "vm" {
		t.Fatalf("the steps must be network, then vm: %v", got)
	}

	// Each step has its own state: two steps with one key would change each other's resources.
	states := map[string]string{}
	rendered := map[string]string{}
	for _, step := range template.Spec.Steps {
		if step.Terraform == nil {
			t.Fatalf("step %s is not a Terraform step", step.Name)
		}
		given := map[string]string{}
		for _, parameter := range parameters {
			given[parameter.Name] = parameter.Value
		}
		stateKey, err := renderStateKey(step.Terraform.State.Key, resolve(nil), given)
		if err != nil {
			t.Fatalf("state key of step %s: %v", step.Name, err)
		}
		if !strings.Contains(stateKey, given["clientName"]) {
			t.Fatalf("the state key %q of step %s does not belong to the client %q", stateKey, step.Name, given["clientName"])
		}
		if other, ok := states[stateKey]; ok {
			t.Fatalf("steps %s and %s share the state %q", other, step.Name, stateKey)
		}
		states[stateKey] = step.Name
		terraformTemplate := templates[step.Terraform.TemplateRef.Name]
		if terraformTemplate == nil {
			t.Fatalf("step %s runs %q, which the sample does not define", step.Name, step.Terraform.TemplateRef.Name)
		}
		files, err := terraform.ConfigMapData(terraformTemplate, lookup)
		if err != nil {
			t.Fatalf("the TerraformTemplate of step %s does not match its ConfigMap: %v", step.Name, err)
		}

		values := map[string]string{}
		for _, parameter := range parameters {
			values["parameter."+parameter.Name] = parameter.Value
		}
		body, err := applyInputs([]byte(`{}`), step.Terraform.Inputs, resolverFrom(values))
		if err != nil {
			t.Fatal(err)
		}
		var variables map[string]any
		if err := json.Unmarshal(body, &variables); err != nil {
			t.Fatal(err)
		}
		if step.Name == "vm" {
			if want := []any{"vm-1", "vm-2"}; !reflect.DeepEqual(variables["vms"], want) {
				t.Fatalf("vms = %v, want %v", variables["vms"], want)
			}
		}

		for name, content := range renderStepFiles(t, step.Name, files, variables) {
			rendered[step.Name+"/"+name] = content
		}
	}
	checkVMFiles(t, rendered)
	if _, ok := rendered["network/compute.tf"]; ok {
		t.Error("the network step must not have the VM files")
	}
}

// renderStepFiles writes the files of a step into a workspace, renders the *.tf.tmpl ones like the
// runner does, and returns the content of every file the step would run, by name.
func renderStepFiles(t *testing.T, step string, files map[string]string, variables map[string]any) map[string]string {
	t.Helper()
	workspace := t.TempDir()
	for name, content := range files {
		path := filepath.Join(workspace, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(name, ".tf.tmpl") {
			if err := tfengine.RenderTemplate(path, variables); err != nil {
				t.Fatalf("step %s: render %s: %v", step, name, err)
			}
		}
	}
	entries, err := os.ReadDir(workspace)
	if err != nil {
		t.Fatal(err)
	}
	rendered := map[string]string{}
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(workspace, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "<FGT_") {
			t.Errorf("step %s: %s still has a placeholder:\n%s", step, entry.Name(), raw)
		}
		rendered[entry.Name()] = string(raw)
	}
	return rendered
}

// checkVMFiles checks that every VM file of the vm step has a resource for each VM of the list.
func checkVMFiles(t *testing.T, rendered map[string]string) {
	t.Helper()
	for _, name := range []string{"compute.tf", "floatingip.tf", "outputs.tf"} {
		for _, vm := range []string{"vm-1", "vm-2"} {
			if !strings.Contains(rendered["vm/"+name], `"`+vm+`"`) {
				t.Errorf("vm/%s has nothing for %s:\n%s", name, vm, rendered["vm/"+name])
			}
		}
	}
}

// checkSampleInstance checks that the sample WorkflowInstance runs the sample template with the
// parameters it declares, and that the template has Terraform steps only, as an instance needs.
func checkSampleInstance(t *testing.T, instance *dinovav1alpha1.WorkflowInstance, template *dinovav1alpha1.WorkflowTemplate) {
	t.Helper()
	if instance.Spec.TemplateRef.Name != template.Name {
		t.Fatalf("the instance runs %q, the sample template is %q", instance.Spec.TemplateRef.Name, template.Name)
	}
	stage := dinovav1alpha1.WorkflowTemplateRun{Name: "run", TemplateRef: instance.Spec.TemplateRef, Parameters: instance.Spec.Parameters}
	if _, err := effectiveParameters(template, stage); err != nil {
		t.Fatalf("parameters of the sample instance: %v", err)
	}
	for _, step := range template.Spec.Steps {
		if step.Terraform == nil {
			t.Fatalf("step %s is not a Terraform step: an instance refuses the template", step.Name)
		}
	}
}

// The Google DNS zone sample: its references must hold, the state must belong to the client's
// zone under its parent, and every variable the step sets must be declared by its files.
func TestGoogleDNSZoneSampleIsConsistent(t *testing.T) {
	dir := filepath.Join("..", "..", "..", "config", "samples", "google-dns-zone")
	read := func(name string, into any) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := yaml.UnmarshalStrict(raw, into); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	configMap := &corev1.ConfigMap{}
	terraformTemplate := &dinovav1alpha1.TerraformTemplate{}
	template := &dinovav1alpha1.WorkflowTemplate{}
	workflow := &dinovav1alpha1.Workflow{}
	read("step-google-dns-zone-configmap.yaml", configMap)
	read("google-dns-zone-terraformtemplate.yaml", terraformTemplate)
	read("google-dns-zone-workflowtemplate.yaml", template)
	read("google-dns-zone-workflow.yaml", workflow)
	if configMap.Immutable == nil || !*configMap.Immutable {
		t.Fatal("the ConfigMap with the Terraform files must be immutable, or the operator refuses it")
	}
	if err := validateTemplate(template); err != nil {
		t.Fatalf("template references: %v", err)
	}
	stage := workflow.Spec.Templates[0]
	if stage.TemplateRef.Name != template.Name {
		t.Fatalf("the workflow runs %q, the sample template is %q", stage.TemplateRef.Name, template.Name)
	}
	parameters, err := effectiveParameters(template, stage)
	if err != nil {
		t.Fatalf("parameters of the sample workflow: %v", err)
	}
	if len(template.Spec.Steps) != 1 || template.Spec.Steps[0].Terraform == nil {
		t.Fatal("the zone and its delegation must be one Terraform step")
	}
	step := template.Spec.Steps[0].Terraform
	if step.TemplateRef.Name != terraformTemplate.Name {
		t.Fatalf("the step runs %q, the sample TerraformTemplate is %q", step.TemplateRef.Name, terraformTemplate.Name)
	}
	files, err := terraform.ConfigMapData(terraformTemplate, func(ref dinovav1alpha1.WorkflowConfigMapKeyReference) (string, error) {
		value, ok := configMap.Data[ref.Key]
		if !ok || ref.Name != configMap.Name {
			return "", fmt.Errorf("the sample ConfigMap has no %s/%s", ref.Name, ref.Key)
		}
		return value, nil
	})
	if err != nil {
		t.Fatalf("the TerraformTemplate does not match its ConfigMap: %v", err)
	}

	given := map[string]string{}
	values := map[string]string{}
	for _, parameter := range parameters {
		given[parameter.Name] = parameter.Value
		values["parameter."+parameter.Name] = parameter.Value
	}
	stateKey, err := renderStateKey(step.State.Key, resolve(nil), given)
	if err != nil {
		t.Fatalf("state key: %v", err)
	}
	if want := "google-dns-zone/k8s-test-dinova-services/acme"; stateKey != want {
		t.Errorf("state key = %q, want %q", stateKey, want)
	}
	body, err := applyInputs([]byte(`{}`), step.Inputs, resolverFrom(values))
	if err != nil {
		t.Fatal(err)
	}
	var variables map[string]any
	if err := json.Unmarshal(body, &variables); err != nil {
		t.Fatal(err)
	}
	if variables["client_name"] != "acme" || variables["parent_zone"] != "k8s-test-dinova-services" {
		t.Errorf("variables = %v", variables)
	}
	for name := range variables {
		if !strings.Contains(files["variables.tf"], `variable "`+name+`"`) {
			t.Errorf("the step sets %s, which variables.tf does not declare", name)
		}
	}
	for _, output := range step.Outputs {
		if !strings.Contains(files["outputs.tf"], `output "`+strings.TrimPrefix(output.From, "/")+`"`) {
			t.Errorf("output %s reads %s, which outputs.tf does not declare", output.Name, output.From)
		}
	}

	// The destroy sample must reach the same state as the run that created the zone.
	destroy := &dinovav1alpha1.Workflow{}
	read("google-dns-zone-destroy.yaml", destroy)
	if destroy.Spec.TerraformMode != "Destroy" {
		t.Errorf("the destroy sample has terraformMode %q", destroy.Spec.TerraformMode)
	}
	if !reflect.DeepEqual(destroy.Spec.Templates, workflow.Spec.Templates) {
		t.Errorf("the destroy sample runs %+v, not the template and parameters that created the zone %+v", destroy.Spec.Templates, workflow.Spec.Templates)
	}
}

// The pipeline of a new Kubernetes cluster is what people copy to chain stages: every stage must
// run a template of the sample, give each of its required parameters, and read outputs that
// the steps it names publish.
func TestNewK8sClusterPipelineSampleIsConsistent(t *testing.T) {
	samples := filepath.Join("..", "..", "..", "config", "samples")
	templates := map[string]*dinovav1alpha1.WorkflowTemplate{}
	for _, file := range []string{filepath.Join("newk8scluster", "templates.yaml"), filepath.Join("google-dns-zone", "google-dns-zone-workflowtemplate.yaml")} {
		raw, err := os.ReadFile(filepath.Join(samples, file))
		if err != nil {
			t.Fatal(err)
		}
		for document := range strings.SplitSeq(string(raw), "\n---\n") {
			template := &dinovav1alpha1.WorkflowTemplate{}
			if err := yaml.UnmarshalStrict([]byte(document), template); err != nil {
				t.Fatalf("%s: %v", file, err)
			}
			if err := validateTemplate(template); err != nil {
				t.Fatalf("template %s: %v", template.Name, err)
			}
			templates[template.Name] = template
		}
	}
	raw, err := os.ReadFile(filepath.Join(samples, "newk8scluster", "pipeline.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	pipeline := &dinovav1alpha1.WorkflowPipeline{}
	if err := yaml.UnmarshalStrict(raw, pipeline); err != nil {
		t.Fatal(err)
	}
	stageTemplates := map[string]*dinovav1alpha1.WorkflowTemplate{}
	for _, stage := range pipeline.Spec.Stages {
		ref := dinovav1alpha1.WorkflowResourceReference{}
		if stage.Instance != nil {
			ref = stage.Instance.TemplateRef
		} else {
			ref = stage.Workflow.TemplateRef
		}
		template, ok := templates[ref.Name]
		if !ok {
			t.Fatalf("stage %s runs %q, which is not a template of the sample", stage.Name, ref.Name)
		}
		stageTemplates[stage.Name] = template
	}
	for _, stage := range pipeline.Spec.Stages {
		template := stageTemplates[stage.Name]
		given := map[string]bool{}
		for _, parameter := range pipeline.Spec.Parameters {
			given[parameter.Name] = true
		}
		for _, parameter := range stage.Parameters {
			given[parameter.Name] = true
			from := parameter.ValueFrom
			if from == nil {
				continue
			}
			if !strings.Contains(" "+strings.Join(stage.DependsOn, " ")+" ", " "+from.Stage+" ") {
				t.Errorf("stage %s reads stage %s without depending on it", stage.Name, from.Stage)
			}
			published := false
			for _, step := range stageTemplates[from.Stage].Spec.Steps {
				if step.Name == from.Step {
					published = outputNames(stepOutputs(step))[from.Output]
				}
			}
			if !published {
				t.Errorf("stage %s reads output %s of step %s of stage %s, which it does not publish", stage.Name, from.Output, from.Step, from.Stage)
			}
		}
		declared := map[string]bool{}
		for _, parameter := range template.Spec.Parameters {
			declared[parameter.Name] = true
			if parameter.Default == nil && !given[parameter.Name] {
				t.Errorf("stage %s does not give parameter %s of %s", stage.Name, parameter.Name, template.Name)
			}
		}
		for _, parameter := range stage.Parameters {
			if !declared[parameter.Name] {
				t.Errorf("stage %s gives parameter %s, which %s does not declare", stage.Name, parameter.Name, template.Name)
			}
		}
	}
}
