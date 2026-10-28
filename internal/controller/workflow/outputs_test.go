package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

type (
	output = dinovav1alpha1.WorkflowOutput
	input  = dinovav1alpha1.WorkflowInput
)

func TestExtractOutputs(t *testing.T) {
	response := []byte(`{"id":"vdc-1","nested":{"count":3,"ready":true},"list":["a","b"]}`)
	values, err := extractOutputs(response, []output{
		{Name: "id", From: "/id"},
		{Name: "count", From: "/nested/count"},
		{Name: "second", From: "/list/1"},
		{Name: "region", From: "/region", DefaultJSON: `"eu-1"`},
		{Name: "nothing", From: "/missing", DefaultJSON: `null`},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"id": `"vdc-1"`, "count": `3`, "second": `"b"`, "region": `"eu-1"`, "nothing": `null`}
	for name, expected := range want {
		if string(values[name]) != expected {
			t.Errorf("output %s = %s, want %s", name, values[name], expected)
		}
	}
}

func TestExtractOutputsFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		response string
		outputs  []output
		reason   string
	}{
		"missing without default": {`{}`, []output{{Name: "id", From: "/id"}}, reasonOutputMissing},
		"response not JSON":       {`not json`, []output{{Name: "id", From: "/id"}}, "OutputExtractionFailed"},
		"invalid default":         {`{}`, []output{{Name: "id", From: "/id", DefaultJSON: `{bad`}}, "OutputExtractionFailed"},
		"value too large":         {fmt.Sprintf(`{"big":%q}`, strings.Repeat("x", maxOutputValueBytes)), []output{{Name: "big", From: "/big"}}, "OutputTooLarge"},
	} {
		_, err := extractOutputs([]byte(tc.response), tc.outputs)
		var failure *outputFailure
		if !errors.As(err, &failure) || failure.reason != tc.reason {
			t.Errorf("%s: error = %v, want reason %s", name, err, tc.reason)
		}
	}
	if values, err := extractOutputs([]byte(`not json`), nil); err != nil || values != nil {
		t.Errorf("no declared outputs must not parse the response: %v %v", values, err)
	}
}

func resolverFrom(values map[string]string) inputResolver {
	return func(in input) ([]byte, error) {
		switch {
		case in.Parameter != "":
			return json.Marshal(values["parameter."+in.Parameter])
		case in.ConfigMapKeyRef != nil:
			return json.Marshal(values[in.ConfigMapKeyRef.Name+"/"+in.ConfigMapKeyRef.Key])
		}
		if value, ok := values[in.Step+"."+in.Output]; ok {
			return []byte(value), nil
		}
		return nil, fmt.Errorf("no output %s.%s", in.Step, in.Output)
	}
}

func TestApplyInputsSetsValuesIntoBody(t *testing.T) {
	body, err := applyInputs([]byte(`{"request":{},"static":"kept"}`), []input{
		{Step: "create-vdc", Output: "id", To: "/request/vdcId"},
		{Step: "create-vdc", Output: "count", To: "/count"},
		{Step: "other", Output: "ready", To: "/ready"},
	}, resolverFrom(map[string]string{"create-vdc.id": `"vdc-1"`, "create-vdc.count": `3`, "other.ready": `true`}))
	if err != nil {
		t.Fatal(err)
	}
	var actual map[string]any
	if err := json.Unmarshal(body, &actual); err != nil {
		t.Fatal(err)
	}
	request := actual["request"].(map[string]any)
	if request["vdcId"] != "vdc-1" || actual["count"] != float64(3) || actual["ready"] != true || actual["static"] != "kept" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestApplyInputsWithoutInputsKeepsBodyUntouched(t *testing.T) {
	body, err := applyInputs([]byte(`not even json`), nil, nil)
	if err != nil || string(body) != `not even json` {
		t.Fatalf("body = %q, error = %v", body, err)
	}
}

func TestApplyInputsRejectsInvalidUse(t *testing.T) {
	resolve := resolverFrom(map[string]string{"a.x": `"v"`, "a.y": `"w"`, "a.bad": `{`})
	for name, tc := range map[string]struct {
		body   string
		inputs []input
	}{
		"unknown output":           {`{}`, []input{{Step: "a", Output: "zzz", To: "/x"}}},
		"duplicate target":         {`{}`, []input{{Step: "a", Output: "x", To: "/t"}, {Step: "a", Output: "y", To: "/t"}}},
		"body is not an object":    {`[]`, []input{{Step: "a", Output: "x", To: "/t"}}},
		"body is not JSON":         {`text`, []input{{Step: "a", Output: "x", To: "/t"}}},
		"parent is not an object":  {`{"n":"str"}`, []input{{Step: "a", Output: "x", To: "/n/t"}}},
		"stored value is not JSON": {`{}`, []input{{Step: "a", Output: "bad", To: "/t"}}},
	} {
		if _, err := applyInputs([]byte(tc.body), tc.inputs, resolve); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func apiStep(name string, outputs ...output) dinovav1alpha1.WorkflowStep {
	return dinovav1alpha1.WorkflowStep{Name: name, API: &dinovav1alpha1.WorkflowAPIStep{Outputs: outputs}}
}

func TestValidateReferences(t *testing.T) {
	first := apiStep("first", output{Name: "id", From: "/id"})
	withInput := func(name string, in ...input) dinovav1alpha1.WorkflowStep {
		step := apiStep(name)
		step.API.Inputs = in
		return step
	}
	polling := apiStep("polling", output{Name: "op", From: "/op"})
	polling.API.Poll = &dinovav1alpha1.WorkflowPoll{
		Inputs:  []input{{Step: "polling", Output: "op", To: "/op"}},
		Outputs: []output{{Name: "result", From: "/result"}},
	}
	terraform := dinovav1alpha1.WorkflowStep{Name: "tf", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
		Inputs: []input{{Step: "polling", Output: "result", To: "/result"}},
	}}

	valid := [][]dinovav1alpha1.WorkflowStep{
		{first, withInput("second", input{Step: "first", Output: "id", To: "/id"})},
		{first, withInput("second"), withInput("third", input{Step: "first", Output: "id", To: "/id"})},
		{polling, terraform},
	}
	for i, steps := range valid {
		if err := validateReferences(steps); err != nil {
			t.Errorf("valid case %d rejected: %v", i, err)
		}
	}

	badPollSelf := apiStep("polling", output{Name: "op", From: "/op"})
	badPollSelf.API.Poll = &dinovav1alpha1.WorkflowPoll{
		Inputs:  []input{{Step: "polling", Output: "result", To: "/x"}},
		Outputs: []output{{Name: "result", From: "/result"}},
	}
	badTerraform := dinovav1alpha1.WorkflowStep{Name: "tf", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
		Inputs: []input{{Step: "first", Output: "nope", To: "/x"}},
	}}
	invalid := map[string][]dinovav1alpha1.WorkflowStep{
		"unknown step":               {first, withInput("second", input{Step: "ghost", Output: "id", To: "/id"})},
		"undeclared output":          {first, withInput("second", input{Step: "first", Output: "nope", To: "/id"})},
		"later step":                 {withInput("second", input{Step: "first", Output: "id", To: "/id"}), first},
		"own output without a poll":  {withInput("self", input{Step: "self", Output: "id", To: "/id"})},
		"poll uses its own poll out": {badPollSelf},
		"duplicate step name":        {first, first},
		"duplicate output name":      {apiStep("dup", output{Name: "a", From: "/a"}, output{Name: "a", From: "/b"})},
		"terraform undeclared":       {first, badTerraform},
	}
	for name, steps := range invalid {
		if err := validateReferences(steps); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func terraformStep(name, key string, inputs []input, outputs ...output) dinovav1alpha1.WorkflowStep {
	return dinovav1alpha1.WorkflowStep{Name: name, Terraform: &dinovav1alpha1.WorkflowTerraformStep{
		State: dinovav1alpha1.WorkflowTerraformState{Key: key}, Inputs: inputs, Outputs: outputs,
	}}
}

func TestValidateReferencesCoversTerraformStateKeysAndOutputs(t *testing.T) {
	create := apiStep("create-vdc", output{Name: "name", From: "/name"})
	net := terraformStep("net", "vdc/{{steps.create-vdc.outputs.name}}/net", nil, output{Name: "network_id", From: "/network_id"})
	for i, steps := range [][]dinovav1alpha1.WorkflowStep{
		{create, net},
		// a Terraform step may use the outputs of an earlier Terraform step, as variables and in its key
		{create, net, terraformStep("cluster", "vdc/{{steps.create-vdc.outputs.name}}/{{steps.net.outputs.network_id}}",
			[]input{{Step: "net", Output: "network_id", To: "/network_id"}})},
		{terraformStep("solo", "static/key", nil)},
	} {
		if err := validateReferences(steps); err != nil {
			t.Errorf("valid case %d rejected: %v", i, err)
		}
	}
	for name, steps := range map[string][]dinovav1alpha1.WorkflowStep{
		"key uses an unknown step":                  {create, terraformStep("net", "vdc/{{steps.ghost.outputs.name}}", nil)},
		"key uses an undeclared value":              {create, terraformStep("net", "vdc/{{steps.create-vdc.outputs.nope}}", nil)},
		"key uses a later step":                     {terraformStep("net", "vdc/{{steps.create-vdc.outputs.name}}", nil), create},
		"key uses its own output":                   {terraformStep("net", "vdc/{{steps.net.outputs.id}}", nil, output{Name: "id", From: "/id"})},
		"input from an undeclared terraform output": {net, terraformStep("cluster", "k", []input{{Step: "net", Output: "nope", To: "/x"}})},
		"terraform declares a duplicate output":     {terraformStep("net", "k", nil, output{Name: "a", From: "/a"}, output{Name: "a", From: "/b"})},
	} {
		if err := validateReferences(steps); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func TestSensitiveDeclaredFollowsTheTerraformOutputThePointerReadsFrom(t *testing.T) {
	declared := []output{
		{Name: "adminPassword", From: "/admin_password"}, // the name differs from the Terraform output
		{Name: "dbPassword", From: "/db/password"},       // a field of a sensitive object output
		{Name: "networkId", From: "/network_id"},
		{Name: "firstIP", From: "/ips/0"},
	}
	got := sensitiveDeclared(declared, []string{"admin_password", "db"})
	if len(got) != 2 || got[0] != "adminPassword" || got[1] != "dbPassword" {
		t.Fatalf("sensitive declared outputs = %v, want [adminPassword dbPassword]", got)
	}
	if got := sensitiveDeclared(declared, nil); len(got) != 0 {
		t.Fatalf("nothing is sensitive: %v", got)
	}
}

func configLookup(values map[string]string) conditionResolver {
	return func(condition dinovav1alpha1.WorkflowCondition) (any, error) {
		ref := condition.EqualsFrom
		if value, ok := values[ref.Name+"/"+ref.Key]; ok {
			return value, nil
		}
		return nil, &outputFailure{"ConfigMapKeyMissing", "missing " + ref.Name + "/" + ref.Key}
	}
}

func TestExtractOutputsSelectsByConfigMapValue(t *testing.T) {
	response := []byte(`[{"id":"1","name":"BLK-ALTRO"},{"id":"2","name":"BLK-AUTOMAZIONE-P2P"},{"id":"3","name":"BLK-AUTOMAZIONE-P2P"}]`)
	byName := output{Name: "blockId", From: "/id", Select: &dinovav1alpha1.WorkflowSelect{Where: []dinovav1alpha1.WorkflowCondition{
		{Field: "/name", EqualsFrom: &dinovav1alpha1.WorkflowConfigMapKeyReference{Name: "cfg", Key: "blockName"}},
	}}}
	values, err := extractOutputsWith(response, []output{byName}, configLookup(map[string]string{"cfg/blockName": "BLK-AUTOMAZIONE-P2P"}))
	if err != nil {
		t.Fatal(err)
	}
	if string(values["blockId"]) != `"2"` {
		t.Errorf("blockId = %s, want the first match, \"2\"", values["blockId"])
	}

	_, err = extractOutputsWith(response, []output{byName}, configLookup(map[string]string{"cfg/blockName": "BLK-INESISTENTE"}))
	var failure *outputFailure
	if !errors.As(err, &failure) || failure.reason != reasonOutputMissing {
		t.Errorf("no match: error = %v, want OutputMissing", err)
	}

	_, err = extractOutputsWith(response, []output{byName}, nil)
	if !errors.As(err, &failure) || failure.reason != "OutputExtractionFailed" {
		t.Errorf("no lookup: error = %v, want OutputExtractionFailed", err)
	}
}

func TestExtractOutputsSelectsFirstAvailableAndPublishesTheElement(t *testing.T) {
	response := []byte(`{"subnets":[{"cidr":"10.0.0.0/31","available":false},{"cidr":"10.0.0.2/31","available":true},{"cidr":"10.0.0.4/31","available":true}]}`)
	free := output{Name: "subnet", Select: &dinovav1alpha1.WorkflowSelect{
		Array: "/subnets",
		Where: []dinovav1alpha1.WorkflowCondition{{Field: "/available", EqualsJSON: `true`}},
	}}
	values, err := extractOutputs(response, []output{free, {Name: "cidr", From: "/cidr", Select: free.Select}})
	if err != nil {
		t.Fatal(err)
	}
	if string(values["subnet"]) != `{"available":true,"cidr":"10.0.0.2/31"}` {
		t.Errorf("subnet = %s", values["subnet"])
	}
	if string(values["cidr"]) != `"10.0.0.2/31"` {
		t.Errorf("cidr = %s", values["cidr"])
	}

	none := output{Name: "subnet", Select: free.Select, DefaultJSON: `null`}
	values, err = extractOutputs([]byte(`{"subnets":[{"available":false}]}`), []output{none})
	if err != nil || string(values["subnet"]) != `null` {
		t.Errorf("no available subnet with a default: %s %v", values["subnet"], err)
	}

	_, err = extractOutputs([]byte(`{"subnets":"x"}`), []output{free})
	var failure *outputFailure
	if !errors.As(err, &failure) || failure.reason != "OutputExtractionFailed" {
		t.Errorf("not an array: error = %v", err)
	}
}

func TestApplyQueryInputs(t *testing.T) {
	resolve := resolverFrom(map[string]string{"blocks.id": `"a b&c"`, "blocks.n": `7`, "blocks.obj": `{}`})
	endpoint, err := applyQueryInputs("https://portal.example/api/v1/x?mask=31&block_id=old", []input{
		{Step: "blocks", Output: "id", ToQuery: "block_id"},
		{Step: "blocks", Output: "n", ToQuery: "n"},
	}, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://portal.example/api/v1/x?block_id=a+b%26c&mask=31&n=7"; endpoint != want {
		t.Errorf("endpoint = %s, want %s", endpoint, want)
	}
	if same, _ := applyQueryInputs("https://portal.example/x", []input{{Step: "blocks", Output: "id", To: "/id"}}, resolve); same != "https://portal.example/x" {
		t.Errorf("body inputs must leave the endpoint alone: %s", same)
	}
	for name, inputs := range map[string][]input{
		"object value": {{Step: "blocks", Output: "obj", ToQuery: "o"}},
		"duplicate":    {{Step: "blocks", Output: "id", ToQuery: "p"}, {Step: "blocks", Output: "n", ToQuery: "p"}},
		"unknown":      {{Step: "blocks", Output: "nope", ToQuery: "p"}},
	} {
		if _, err := applyQueryInputs("https://portal.example/x", inputs, resolve); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	// Body inputs must not trip over query ones.
	body, err := applyInputs([]byte(`{"k":1}`), []input{{Step: "blocks", Output: "id", ToQuery: "block_id"}}, resolve)
	if err != nil || string(body) != `{"k":1}` {
		t.Errorf("query input changed the body: %s %v", body, err)
	}
}

func TestValidateReferencesRejectsQueryInputsOnTerraform(t *testing.T) {
	steps := []dinovav1alpha1.WorkflowStep{
		apiStep("a", output{Name: "id", From: "/id"}),
		terraformStep("t", "k", []input{{Step: "a", Output: "id", ToQuery: "x"}}),
	}
	if err := validateReferences(steps); err == nil || !strings.Contains(err.Error(), "toQuery") {
		t.Errorf("error = %v, want a toQuery rejection", err)
	}
}

func TestApplyInputsFromParametersAndConfigMaps(t *testing.T) {
	resolve := resolverFrom(map[string]string{"parameter.clientName": "ACME", "cfg/envName": "vcloud-r1.dinova.cloud"})
	inputs := []input{
		{Parameter: "clientName", ToQuery: "client_name"},
		{ConfigMapKeyRef: &dinovav1alpha1.WorkflowConfigMapKeyReference{Name: "cfg", Key: "envName"}, ToQuery: "env_name"},
	}
	endpoint, err := applyQueryInputs("https://portal.example/segments/name-check", inputs, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://portal.example/segments/name-check?client_name=ACME&env_name=vcloud-r1.dinova.cloud"; endpoint != want {
		t.Errorf("endpoint = %s, want %s", endpoint, want)
	}
}

func TestValidateParameters(t *testing.T) {
	step := func(name string) dinovav1alpha1.WorkflowStep {
		return dinovav1alpha1.WorkflowStep{Name: "s", API: &dinovav1alpha1.WorkflowAPIStep{
			Inputs: []input{{Parameter: name, ToQuery: "q"}},
		}}
	}
	declared := []dinovav1alpha1.WorkflowTemplateParameter{{Name: "clientName"}}
	ok := dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{step("clientName")}, Parameters: declared}
	if err := validateParameters(ok); err != nil {
		t.Errorf("declared parameter rejected: %v", err)
	}
	bad := dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{step("other")}, Parameters: declared}
	if err := validateParameters(bad); err == nil || !strings.Contains(err.Error(), "other") {
		t.Errorf("undeclared parameter accepted: %v", err)
	}
}

func TestCheckExpectations(t *testing.T) {
	ok := []dinovav1alpha1.WorkflowExpectation{{Pointer: "/ok", EqualsJSON: "true", Message: "Name not available"}}
	for name, tc := range map[string]struct {
		response string
		expect   []dinovav1alpha1.WorkflowExpectation
		want     string // substring of the failure, empty for success
	}{
		"holds":            {`{"ok":true}`, ok, ""},
		"no expectations":  {`not json`, nil, ""},
		"does not hold":    {`{"ok":false,"secret":"s3"}`, ok, "Name not available: Response field /ok"},
		"missing field":    {`{}`, ok, "has no field /ok"},
		"not json":         {`nope`, ok, "not valid JSON"},
		"wrong type":       {`{"ok":"true"}`, ok, "did not have the expected value"},
		"second one fails": {`{"ok":true,"n":1}`, append(append([]dinovav1alpha1.WorkflowExpectation(nil), ok...), dinovav1alpha1.WorkflowExpectation{Pointer: "/n", EqualsJSON: "2"}), "/n"},
	} {
		err := checkExpectations([]byte(tc.response), tc.expect)
		var failure *outputFailure
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: unexpected failure %v", name, err)
		case tc.want != "" && (!errors.As(err, &failure) || failure.reason != "ExpectationFailed" || !strings.Contains(failure.message, tc.want)):
			t.Errorf("%s: error = %v, want ExpectationFailed containing %q", name, err, tc.want)
		case failure != nil && strings.Contains(failure.message, "s3"):
			t.Errorf("%s: the response value leaked into the message: %s", name, failure.message)
		}
	}
}

func TestSelectComparesWithAnEarlierOutputExactly(t *testing.T) {
	response := []byte(`[
		{"name":"P2P-ACME2-SEG","switchId":"wrong","network":"1.1.1.0/31"},
		{"name":"P2P-ACME-SEG-OLD","switchId":"wrong2","network":"2.2.2.0/31"},
		{"name":"P2P-ACME-SEG","switchId":"sw-1","gateway_cidr":"178.239.21.34/31","network":"178.239.21.34/31"}]`)
	byName := &dinovav1alpha1.WorkflowSelect{Where: []dinovav1alpha1.WorkflowCondition{
		{Field: "/name", EqualsOutput: &dinovav1alpha1.WorkflowOutputReference{Step: "create", Output: "segName"}},
	}}
	resolve := func(condition dinovav1alpha1.WorkflowCondition) (any, error) {
		if condition.EqualsOutput.Step != "create" {
			return nil, errors.New("unexpected step")
		}
		return "P2P-ACME-SEG", nil
	}
	values, err := extractOutputsWith(response, []output{
		{Name: "id", From: "/switchId", Select: byName},
		{Name: "network", From: "/network", Select: byName},
	}, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if string(values["id"]) != `"sw-1"` || string(values["network"]) != `"178.239.21.34/31"` {
		t.Errorf("selected the wrong segment: %s %s", values["id"], values["network"])
	}
	_, err = extractOutputsWith([]byte(`[{"name":"P2P-ACME-SEG-OLD","switchId":"x"}]`), []output{{Name: "id", From: "/switchId", Select: byName}}, resolve)
	var failure *outputFailure
	if !errors.As(err, &failure) || failure.reason != reasonOutputMissing {
		t.Errorf("a name that only contains the key must not match: %v", err)
	}
}

func TestValidateReferencesChecksSelectOutputs(t *testing.T) {
	compare := func(step string) []output {
		return []output{{Name: "id", From: "/id", Select: &dinovav1alpha1.WorkflowSelect{Where: []dinovav1alpha1.WorkflowCondition{
			{Field: "/name", EqualsOutput: &dinovav1alpha1.WorkflowOutputReference{Step: step, Output: "segName"}},
		}}}}
	}
	earlier := []dinovav1alpha1.WorkflowStep{apiStep("create", output{Name: "segName", From: "/seg_name"}), apiStep("get", compare("create")...)}
	if err := validateReferences(earlier); err != nil {
		t.Errorf("comparison with an earlier output rejected: %v", err)
	}
	for name, steps := range map[string][]dinovav1alpha1.WorkflowStep{
		"later step":   {apiStep("get", compare("create")...), apiStep("create", output{Name: "segName", From: "/seg_name"})},
		"same step":    {apiStep("get", compare("get")...)},
		"unknown step": {apiStep("get", compare("nope")...)},
	} {
		if err := validateReferences(steps); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestApplyPathInputs(t *testing.T) {
	resolve := resolverFrom(map[string]string{"cfg/envId": "7", "parameter.name": "a b/c"})
	cfg := &dinovav1alpha1.WorkflowConfigMapKeyReference{Name: "cfg", Key: "envId"}
	endpoint, err := applyPathInputs("https://portal.example:8453/api/v1/vcloud/{env_id}/importable-segments?ignore_suffix_filter=true",
		[]input{{ConfigMapKeyRef: cfg, ToPath: "env_id"}}, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://portal.example:8453/api/v1/vcloud/7/importable-segments?ignore_suffix_filter=true"; endpoint != want {
		t.Errorf("endpoint = %s, want %s", endpoint, want)
	}
	escaped, err := applyPathInputs("https://portal.example/x/{name}", []input{{Parameter: "name", ToPath: "name"}}, resolve)
	if err != nil || escaped != "https://portal.example/x/a%20b%2Fc" {
		t.Errorf("value must be escaped as one path segment: %s %v", escaped, err)
	}
	if same, _ := applyPathInputs("https://portal.example/x", []input{{Step: "a", Output: "b", To: "/b"}}, resolve); same != "https://portal.example/x" {
		t.Errorf("body inputs must leave the endpoint alone: %s", same)
	}
	for name, tc := range map[string]struct {
		endpoint string
		inputs   []input
	}{
		"no placeholder":      {"https://portal.example/x", []input{{ConfigMapKeyRef: cfg, ToPath: "env_id"}}},
		"placeholder in host": {"https://{env_id}.example", []input{{ConfigMapKeyRef: cfg, ToPath: "env_id"}}},
		"duplicate":           {"https://portal.example/{env_id}", []input{{ConfigMapKeyRef: cfg, ToPath: "env_id"}, {ConfigMapKeyRef: cfg, ToPath: "env_id"}}},
	} {
		if _, err := applyPathInputs(tc.endpoint, tc.inputs, resolve); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestSelectComparesWithAParameter(t *testing.T) {
	run := &templateRun{owner: &dinovav1alpha1.Workflow{}, parameters: []dinovav1alpha1.WorkflowParameter{{Name: "blockName", Value: "BLK-AUTOMAZIONE-P2P"}}}
	byName := output{Name: "blockId", From: "/id", Select: &dinovav1alpha1.WorkflowSelect{Array: "/blocks", Where: []dinovav1alpha1.WorkflowCondition{
		{Field: "/name", EqualsParameter: "blockName"},
	}}}
	response := []byte(`{"blocks":[{"id":"1","name":"BLK-AUTOMAZIONE-P2P-OLD"},{"id":"2","name":"BLK-AUTOMAZIONE-P2P"}]}`)
	values, err := extractOutputsWith(response, []output{byName}, (&Handler{}).conditionResolver(context.Background(), run))
	if err != nil || string(values["blockId"]) != `"2"` {
		t.Fatalf("blockId = %s, %v; want the exact match, \"2\"", values["blockId"], err)
	}

	run.parameters = nil
	_, err = extractOutputsWith(response, []output{byName}, (&Handler{}).conditionResolver(context.Background(), run))
	var failure *outputFailure
	if !errors.As(err, &failure) || failure.reason != "ParameterMissing" {
		t.Errorf("undeclared parameter: error = %v, want ParameterMissing", err)
	}

	spec := dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{apiStep("blocks", byName)}}
	if err := validateParameters(spec); err == nil || !strings.Contains(err.Error(), "blockName") {
		t.Errorf("a select on an undeclared parameter passed validation: %v", err)
	}
	spec.Parameters = []dinovav1alpha1.WorkflowTemplateParameter{{Name: "blockName"}}
	if err := validateParameters(spec); err != nil {
		t.Errorf("declared parameter rejected: %v", err)
	}
}

func TestRenderTemplatedOutputs(t *testing.T) {
	run := &templateRun{
		parameters: []dinovav1alpha1.WorkflowParameter{{Name: "clientName", Value: "EXMACHINA"}, {Name: "segmentSuffix", Value: "-NET"}},
		status: &dinovav1alpha1.WorkflowTemplateStatus{Steps: []dinovav1alpha1.WorkflowStepStatus{
			{Name: "free-subnet", Outputs: map[string]string{"subnet_ip": `"178.239.21.34"`, "subnet_mask": `"31"`, "count": `3`}},
		}},
	}
	outputs := []output{
		{Name: "seg_name", Template: "P2P-{{parameters.clientName}}{{ parameters.segmentSuffix }}"},
		{Name: "nsx_segment_id", Template: "{{outputs.seg_name}}"},
		{Name: "nsx_segment_network", Template: "{{steps.free-subnet.outputs.subnet_ip}}/{{steps.free-subnet.outputs.subnet_mask}}"},
		{Name: "n", Template: "n={{steps.free-subnet.outputs.count}}"},
	}
	values, err := (&Handler{}).renderTemplatedOutputs(context.Background(), run, outputs)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"seg_name": `"P2P-EXMACHINA-NET"`, "nsx_segment_id": `"P2P-EXMACHINA-NET"`,
		"nsx_segment_network": `"178.239.21.34/31"`, "n": `"n=3"`,
	}
	for name, expected := range want {
		if string(values[name]) != expected {
			t.Errorf("%s = %s, want %s", name, values[name], expected)
		}
	}
	for name, template := range map[string]string{
		"undeclared parameter": "{{parameters.nope}}",
		"unpublished output":   "{{steps.free-subnet.outputs.nope}}",
		"later local output":   "{{outputs.later}}",
		"typo":                 "{{parameter.clientName}}",
	} {
		if _, err := (&Handler{}).renderTemplatedOutputs(context.Background(), run, []output{{Name: "x", Template: template}}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestValidateTemplatedOutputs(t *testing.T) {
	step := func(name string, outputs ...output) dinovav1alpha1.WorkflowStep { return apiStep(name, outputs...) }
	ok := []dinovav1alpha1.WorkflowStep{
		step("free-subnet", output{Name: "subnet_ip", From: "/ip"}),
		step("create",
			output{Name: "seg_name", Template: "P2P-{{parameters.clientName}}"},
			output{Name: "id", Template: "{{outputs.seg_name}}"},
			output{Name: "net", Template: "{{steps.free-subnet.outputs.subnet_ip}}/31"}),
	}
	spec := dinovav1alpha1.WorkflowTemplateSpec{Steps: ok, Parameters: []dinovav1alpha1.WorkflowTemplateParameter{{Name: "clientName"}}}
	if err := errors.Join(validateReferences(ok), validateParameters(spec)); err != nil {
		t.Errorf("valid templates rejected: %v", err)
	}
	for name, steps := range map[string][]dinovav1alpha1.WorkflowStep{
		"output declared later": {step("s", output{Name: "a", Template: "{{outputs.b}}"}, output{Name: "b", Template: "x"})},
		"step after":            {step("s", output{Name: "a", Template: "{{steps.t.outputs.x}}"}), step("t", output{Name: "x", From: "/x"})},
		"not a template output": {step("s", output{Name: "a", From: "/a"}, output{Name: "b", Template: "{{outputs.a}}"})},
	} {
		if err := validateReferences(steps); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if err := validateParameters(dinovav1alpha1.WorkflowTemplateSpec{Steps: ok}); err == nil || !strings.Contains(err.Error(), "clientName") {
		t.Errorf("an undeclared parameter in a template passed validation: %v", err)
	}
}

func TestSelectNeedsEveryCondition(t *testing.T) {
	response := []byte(`[
		{"id":5,"env_type":"vcenter","vcloud_env_name":"vcloud-r1.dinova.cloud"},
		{"id":7,"env_type":"vcloud","vcloud_env_name":"vcloud-r2.dinova.cloud"},
		{"id":6,"env_type":"vcloud","vcloud_env_name":"vcloud-r1.dinova.cloud"}]`)
	run := &templateRun{owner: &dinovav1alpha1.Workflow{}, parameters: []dinovav1alpha1.WorkflowParameter{{Name: "envName", Value: "vcloud-r1.dinova.cloud"}}}
	envID := output{Name: "env_id", From: "/id", Select: &dinovav1alpha1.WorkflowSelect{Where: []dinovav1alpha1.WorkflowCondition{
		{Field: "/env_type", EqualsJSON: `"vcloud"`},
		{Field: "/vcloud_env_name", EqualsParameter: "envName"},
	}}}
	values, err := extractOutputsWith(response, []output{envID}, (&Handler{}).conditionResolver(context.Background(), run))
	if err != nil || string(values["env_id"]) != `6` {
		t.Fatalf("env_id = %s, %v; want 6, the vcloud environment with that name", values["env_id"], err)
	}
}

func TestSelectFlexVMConfigByExactName(t *testing.T) {
	response := []byte(`[
		{"id":3914,"name":"Istanza FortiGate MEDIUM - Funzionalità UTP Bundle","product_type_id":1},
		{"id":3915,"name":"Istanza FortiGate MEDIUM - Funzionalità UTP Bundle 3 VDOM","product_type_id":1},
		{"id":25462,"name":"Fortigate_1DAY","product_type_id":4}]`)
	run := &templateRun{owner: &dinovav1alpha1.Workflow{}, parameters: []dinovav1alpha1.WorkflowParameter{
		{Name: "firewallBundle", Value: "Istanza FortiGate MEDIUM - Funzionalità UTP Bundle"},
	}}
	configID := output{Name: "config_id", From: "/id", Select: &dinovav1alpha1.WorkflowSelect{Where: []dinovav1alpha1.WorkflowCondition{
		{Field: "/name", EqualsParameter: "firewallBundle"},
	}}}
	values, err := extractOutputsWith(response, []output{configID}, (&Handler{}).conditionResolver(context.Background(), run))
	if err != nil || string(values["config_id"]) != `3914` {
		t.Fatalf("config_id = %s, %v; want 3914, not the 3 VDOM bundle that contains the same text", values["config_id"], err)
	}
}

func TestApplyInputsAsString(t *testing.T) {
	resolve := resolverFrom(map[string]string{"cfg.id": `3619`, "cfg.on": `true`, "cfg.name": `"x"`, "cfg.obj": `{"a":1}`, "cfg.none": `null`})
	body, err := applyInputs([]byte(`{}`), []input{
		{Step: "cfg", Output: "id", To: "/asText", AsString: true},
		{Step: "cfg", Output: "id", To: "/asNumber"},
		{Step: "cfg", Output: "on", To: "/flag", AsString: true},
		{Step: "cfg", Output: "name", To: "/name", AsString: true},
	}, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"asNumber":3619,"asText":"3619","flag":"true","name":"x"}`; string(body) != want {
		t.Errorf("body = %s, want %s", body, want)
	}
	for _, output := range []string{"obj", "none"} {
		if _, err := applyInputs([]byte(`{}`), []input{{Step: "cfg", Output: output, To: "/x", AsString: true}}, resolve); err == nil {
			t.Errorf("%s: a value with no text must be refused", output)
		}
	}
}

// reasonOutputMissing is the reason an output that finds nothing fails the workflow with.
const reasonOutputMissing = "OutputMissing"

func TestStorageProfileMustExist(t *testing.T) {
	response := []byte(`[{"name":"DATA4_ENT","href":"https://vcd.example/api/admin/vdcStorageProfile/1"},{"name":"DATA4_ENT-PLUS","href":"https://vcd.example/api/admin/vdcStorageProfile/2"}]`)
	check := output{Name: "storage_profile", From: "/name", Select: &dinovav1alpha1.WorkflowSelect{Where: []dinovav1alpha1.WorkflowCondition{
		{Field: "/name", EqualsParameter: "storageProfile"},
	}}}
	resolver := func(value string) conditionResolver {
		run := &templateRun{owner: &dinovav1alpha1.Workflow{}, parameters: []dinovav1alpha1.WorkflowParameter{{Name: "storageProfile", Value: value}}}
		return (&Handler{}).conditionResolver(context.Background(), run)
	}
	values, err := extractOutputsWith(response, []output{check}, resolver("DATA4_ENT"))
	if err != nil || string(values["storage_profile"]) != `"DATA4_ENT"` {
		t.Fatalf("storage_profile = %s, %v; want DATA4_ENT", values["storage_profile"], err)
	}
	for _, missing := range []string{"CHANGEME", "data4_ent", "DATA4", "DATA4_ENT-PLU"} {
		_, err := extractOutputsWith(response, []output{check}, resolver(missing))
		var failure *outputFailure
		if !errors.As(err, &failure) || failure.reason != reasonOutputMissing {
			t.Errorf("%q is not a profile of the environment and must fail the step, got %v", missing, err)
		}
	}
}

func TestApplyInputsAsNumber(t *testing.T) {
	resolve := resolverFrom(map[string]string{"cfg.n": `3619`, "cfg.text": `"4000"`, "cfg.spaced": `" 2 "`, "cfg.bad": `"many"`, "cfg.on": `true`, "cfg.nope": `"NaN"`})
	body, err := applyInputs([]byte(`{}`), []input{
		{Step: "cfg", Output: "n", To: "/number", AsNumber: true},
		{Step: "cfg", Output: "text", To: "/fromText", AsNumber: true},
		{Step: "cfg", Output: "spaced", To: "/fromSpaced", AsNumber: true},
		{Step: "cfg", Output: "text", To: "/stillText"},
	}, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"fromSpaced":2,"fromText":4000,"number":3619,"stillText":"4000"}`; string(body) != want {
		t.Errorf("body = %s, want %s", body, want)
	}
	for _, output := range []string{"bad", "on", "nope"} {
		if _, err := applyInputs([]byte(`{}`), []input{{Step: "cfg", Output: output, To: "/x", AsNumber: true}}, resolve); err == nil {
			t.Errorf("%s: a value that is not a number must be refused", output)
		}
	}
}

func TestApplyInputsAsJSON(t *testing.T) {
	resolve := resolverFrom(map[string]string{"cfg.list": `"[\"vm-1\",\"vm-2\"]"`, "cfg.object": `"{\"a\":1}"`, "cfg.bad": `"[vm-1"`, "cfg.n": `3`})
	body, err := applyInputs([]byte(`{}`), []input{
		{Step: "cfg", Output: "list", To: "/vms", AsJSON: true},
		{Step: "cfg", Output: "object", To: "/extra", AsJSON: true},
		{Step: "cfg", Output: "list", To: "/stillText"},
	}, resolve)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"extra":{"a":1},"stillText":"[\"vm-1\",\"vm-2\"]","vms":["vm-1","vm-2"]}`; string(body) != want {
		t.Errorf("body = %s, want %s", body, want)
	}
	for _, output := range []string{"bad", "n"} {
		if _, err := applyInputs([]byte(`{}`), []input{{Step: "cfg", Output: output, To: "/x", AsJSON: true}}, resolve); err == nil {
			t.Errorf("%s: a value that is not a string of JSON must be refused", output)
		}
	}
	for _, combined := range []input{
		{Step: "cfg", Output: "list", To: "/x", AsJSON: true, AsString: true},
		{Step: "cfg", Output: "list", To: "/x", AsJSON: true, AsNumber: true},
	} {
		if _, err := applyInputs([]byte(`{}`), []input{combined}, resolve); err == nil {
			t.Errorf("asJSON combined with %+v must be refused", combined)
		}
	}
}
