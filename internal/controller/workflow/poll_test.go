package workflow

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

func failedPollCall(reason string, statusCode int) *dinovav1alpha1.ApiCall {
	call := &dinovav1alpha1.ApiCall{}
	meta.SetStatusCondition(&call.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: "x"})
	if statusCode != 0 {
		call.Status.Response = &dinovav1alpha1.APIResponse{StatusCode: statusCode}
	}
	return call
}

func TestPollFailureTolerated(t *testing.T) {
	retryStatus := &dinovav1alpha1.WorkflowPoll{RetryOnStatusCodes: []int32{404, 503}}
	retryTransport := &dinovav1alpha1.WorkflowPoll{RetryOnTransportErrors: true}
	for name, tc := range map[string]struct {
		call *dinovav1alpha1.ApiCall
		poll *dinovav1alpha1.WorkflowPoll
		want bool
	}{
		"listed HTTP status":                   {failedPollCall("HTTPError", 503), retryStatus, true},
		"another listed HTTP status":           {failedPollCall("HTTPError", 404), retryStatus, true},
		"unlisted HTTP status":                 {failedPollCall("HTTPError", 500), retryStatus, false},
		"HTTP error without retry configured":  {failedPollCall("HTTPError", 503), &dinovav1alpha1.WorkflowPoll{}, false},
		"HTTP error without a stored response": {failedPollCall("HTTPError", 0), retryStatus, false},
		"timeout tolerated":                    {failedPollCall("Timeout", 0), retryTransport, true},
		"network failure tolerated":            {failedPollCall("RequestFailed", 0), retryTransport, true},
		"timeout without retry configured":     {failedPollCall("Timeout", 0), retryStatus, false},
		"status codes do not cover transport":  {failedPollCall("RequestFailed", 0), retryStatus, false},
		"oversized response never tolerated":   {failedPollCall("ResponseTooLarge", 200), &dinovav1alpha1.WorkflowPoll{RetryOnStatusCodes: []int32{200}, RetryOnTransportErrors: true}, false},
		"invalid request never tolerated":      {failedPollCall("InvalidRequest", 0), &dinovav1alpha1.WorkflowPoll{RetryOnStatusCodes: []int32{503}, RetryOnTransportErrors: true}, false},
		"no Ready condition":                   {&dinovav1alpha1.ApiCall{}, retryTransport, false},
	} {
		if got := pollFailureTolerated(tc.call, tc.poll); got != tc.want {
			t.Errorf("%s: tolerated = %v, want %v", name, got, tc.want)
		}
	}
}
