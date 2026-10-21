package workflow

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
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
