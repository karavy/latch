package workflow

import (
	"strings"
	"testing"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

func recoveries(progress ...string) []dinovav1alpha1.WorkflowStepRecovery {
	done := make([]dinovav1alpha1.WorkflowStepRecovery, 0, len(progress))
	for i, value := range progress {
		done = append(done, dinovav1alpha1.WorkflowStepRecovery{Number: int32(i + 1), FailedJSON: `"failed"`, ProgressJSON: value})
	}
	return done
}

func TestDecideRecovery(t *testing.T) {
	recover := &dinovav1alpha1.WorkflowPollRecover{
		WhenJSON:                  []string{`"failed"`},
		Unless:                    []dinovav1alpha1.WorkflowExpectation{{Pointer: "/manual_review_required", EqualsJSON: "true", Message: "manual review"}},
		ProgressPointer:           "/last_completed_step",
		MaxRetriesWithoutProgress: 2,
		MaxRecoveries:             5,
	}
	for name, tc := range map[string]struct {
		recover   *dinovav1alpha1.WorkflowPollRecover
		done      []dinovav1alpha1.WorkflowStepRecovery
		response  string
		progress  string
		reason    string
		inMessage string
	}{
		"first failure":                      {recover, nil, `{"last_completed_step":2}`, "2", "", ""},
		"progress since the last recovery":   {recover, recoveries("2"), `{"last_completed_step":5}`, "5", "", ""},
		"one retry at the same point":        {recover, recoveries("2", "5"), `{"last_completed_step":5}`, "5", "", ""},
		"too many retries at the same point": {recover, recoveries("2", "5", "5"), `{"last_completed_step":5}`, "5", "RecoveryExhausted", "3 times in a row"},
		"going back counts as no progress":   {recover, recoveries("5", "4"), `{"last_completed_step":3}`, "3", "RecoveryExhausted", "3 times in a row"},
		"progress resets the count":          {recover, recoveries("2", "2", "5"), `{"last_completed_step":7}`, "7", "", ""},
		"too many recoveries in all":         {recover, recoveries("1", "2", "3", "4", "5"), `{"last_completed_step":6}`, "", "RecoveryExhausted", "5 times"},
		"blocked by unless":                  {recover, nil, `{"last_completed_step":2,"manual_review_required":true}`, "", "RecoveryBlocked", "manual review"},
		"unless that does not hold":          {recover, nil, `{"last_completed_step":2,"manual_review_required":false}`, "2", "", ""},
		"progress is not a number":           {recover, nil, `{"last_completed_step":"2"}`, "", "RecoveryBlocked", "no number at /last_completed_step"},
		"progress is missing":                {recover, nil, `{}`, "", "RecoveryBlocked", "no number"},
		"invalid response":                   {recover, nil, `not json`, "", "RecoveryBlocked", "not valid JSON"},
		"without progress every one counts":  {&dinovav1alpha1.WorkflowPollRecover{MaxRetriesWithoutProgress: 2}, recoveries("", ""), `{}`, "", "RecoveryExhausted", "3 times in a row"},
		"defaults allow three in a row":      {&dinovav1alpha1.WorkflowPollRecover{}, recoveries("", ""), `{}`, "", "", ""},
	} {
		progress, reason, message := decideRecovery(tc.recover, tc.done, []byte(tc.response))
		if progress != tc.progress || reason != tc.reason || !strings.Contains(message, tc.inMessage) {
			t.Errorf("%s: got (%q, %q, %q), want progress %q, reason %q and a message with %q", name, progress, reason, message, tc.progress, tc.reason, tc.inMessage)
		}
	}
}

func TestRecoverable(t *testing.T) {
	recover := &dinovav1alpha1.WorkflowPollRecover{WhenJSON: []string{`"failed"`, `{"a": 1}`}}
	for value, want := range map[string]bool{`"failed"`: true, `{"a":1}`: true, `"waiting_src_ip"`: false, `failed`: false} {
		if got := recoverable(recover, value); got != want {
			t.Errorf("recoverable(%s) = %v, want %v", value, got, want)
		}
	}
}

func TestResponseInputsWithAdd(t *testing.T) {
	inputs := []dinovav1alpha1.WorkflowInput{
		{FromResponse: "/last_completed_step", To: "/resume_from_step", Add: 1},
		{FromResponse: "/job/id", To: "/job"},
		{FromResponse: "/text_step", To: "/text", Add: 2, AsString: true},
	}
	unused := func(input dinovav1alpha1.WorkflowInput) ([]byte, error) {
		t.Fatalf("input %v did not read the response", input)
		return nil, nil
	}
	body, err := applyInputs([]byte(`{}`), inputs, responseResolver([]byte(`{"last_completed_step":5,"job":{"id":7},"text_step":"3"}`), unused))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"job":7,"resume_from_step":6,"text":"5"}`; string(body) != want {
		t.Errorf("body = %s, want %s", body, want)
	}
	if _, err := applyInputs([]byte(`{}`), inputs[:1], responseResolver([]byte(`{}`), unused)); err == nil || !strings.Contains(err.Error(), "nothing at /last_completed_step") {
		t.Errorf("a missing response value: err = %v", err)
	}
	if _, err := applyInputs([]byte(`{}`), []dinovav1alpha1.WorkflowInput{{FromResponse: "/flag", To: "/x", Add: 1}}, responseResolver([]byte(`{"flag":true}`), unused)); err == nil {
		t.Error("add accepted a boolean")
	}
}

func TestValidateRecoveries(t *testing.T) {
	step := func(recover *dinovav1alpha1.WorkflowPollRecover, inputs ...dinovav1alpha1.WorkflowInput) []dinovav1alpha1.WorkflowStep {
		return []dinovav1alpha1.WorkflowStep{{Name: "wait", API: &dinovav1alpha1.WorkflowAPIStep{
			Inputs: inputs,
			Poll:   &dinovav1alpha1.WorkflowPoll{ConditionJSONPointer: "/status", FailWhenJSON: []string{`"failed"`, `"waiting"`}, Recover: recover},
		}}}
	}
	for name, tc := range map[string]struct {
		steps []dinovav1alpha1.WorkflowStep
		err   string
	}{
		"valid":                        {step(&dinovav1alpha1.WorkflowPollRecover{WhenJSON: []string{` "failed" `}, Inputs: []dinovav1alpha1.WorkflowInput{{FromResponse: "/x", To: "/x"}}}), ""},
		"no recover":                   {step(nil), ""},
		"value not in failWhenJSON":    {step(&dinovav1alpha1.WorkflowPollRecover{WhenJSON: []string{`"error"`}}), "not in the poll's failWhenJSON"},
		"invalid whenJSON":             {step(&dinovav1alpha1.WorkflowPollRecover{WhenJSON: []string{`failed`}}), "not valid JSON"},
		"invalid unless":               {step(&dinovav1alpha1.WorkflowPollRecover{WhenJSON: []string{`"failed"`}, Unless: []dinovav1alpha1.WorkflowExpectation{{Pointer: "/x", EqualsJSON: "yes"}}}), "unless value"},
		"fromResponse outside recover": {step(nil, dinovav1alpha1.WorkflowInput{FromResponse: "/x", To: "/x"}), "only the inputs of a poll's recover request"},
	} {
		err := validateRecoveries(tc.steps)
		if tc.err == "" && err != nil || tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.err)
		}
	}
}

func TestResponsePointers(t *testing.T) {
	recover := &dinovav1alpha1.WorkflowPollRecover{
		ProgressPointer: "/last_completed_step",
		Unless:          []dinovav1alpha1.WorkflowExpectation{{Pointer: "/manual_review_required"}},
		Inputs:          []dinovav1alpha1.WorkflowInput{{FromResponse: "/last_completed_step"}, {Step: "deliver", Output: "job_id"}},
	}
	outputs := []dinovav1alpha1.WorkflowOutput{
		{Name: "id", From: "/id"},
		{Name: "name", Template: "{{parameters.x}}"},
		{Name: "net", From: "/id", Select: &dinovav1alpha1.WorkflowSelect{Array: "/networks"}},
	}
	for name, tc := range map[string]struct {
		step        *dinovav1alpha1.WorkflowAPIStep
		pollRequest bool
		want        string
	}{
		"reads nothing": {&dinovav1alpha1.WorkflowAPIStep{Outputs: []dinovav1alpha1.WorkflowOutput{{Name: "t", Template: "x"}}}, false, ""},
		"outputs and expect": {&dinovav1alpha1.WorkflowAPIStep{Outputs: outputs, Expect: []dinovav1alpha1.WorkflowExpectation{{Pointer: "/ok"}}}, false,
			"/id /networks /ok"},
		"poll repeating the request": {&dinovav1alpha1.WorkflowAPIStep{Poll: &dinovav1alpha1.WorkflowPoll{
			ConditionJSONPointer: "/status", Outputs: []dinovav1alpha1.WorkflowOutput{{Name: "total", From: "/total_steps"}}, Recover: recover,
		}}, false, "/last_completed_step /manual_review_required /status /total_steps"},
		"initial request of a separate poll": {&dinovav1alpha1.WorkflowAPIStep{Outputs: outputs[:1], Poll: &dinovav1alpha1.WorkflowPoll{
			TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "p"}, ConditionJSONPointer: "/status", Recover: recover,
		}}, false, "/id"},
		"separate poll request": {&dinovav1alpha1.WorkflowAPIStep{Outputs: outputs[:1], Poll: &dinovav1alpha1.WorkflowPoll{
			TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "p"}, ConditionJSONPointer: "/status", Recover: recover,
			Outputs: []dinovav1alpha1.WorkflowOutput{{Name: "r", From: "/result"}},
		}}, true, "/last_completed_step /manual_review_required /result /status"},
	} {
		if got := strings.Join(responsePointers(tc.step, tc.pollRequest), " "); got != tc.want {
			t.Errorf("%s: got %q, want %q", name, got, tc.want)
		}
	}
}
