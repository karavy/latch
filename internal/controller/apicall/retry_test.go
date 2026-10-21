package apicall

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// retryServer answers each GET with the next of the replies (the last one repeats).
func retryServer(t *testing.T, hits *atomic.Int32, replies ...func(http.ResponseWriter)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		replies[min(int(hits.Add(1))-1, len(replies)-1)](w)
	}))
	t.Cleanup(server.Close)
	return server
}

func status(code int) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) { w.WriteHeader(code) }
}

func jobStatus(value string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) { _, _ = w.Write([]byte(`{"status":"` + value + `"}`)) }
}

// runUntilDone reconciles the ApiCall until it reaches a final phase, like the manager would.
func runUntilDone(t *testing.T, r *Handler, call *dinovav1alpha1.ApiCall) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		reconcileAPI(t, r, call)
		switch call.Status.Phase {
		case apiPhaseSucceeded, apiPhaseFailed, apiPhaseUnknown:
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the ApiCall did not finish: phase %s, attempts %d", call.Status.Phase, call.Status.Attempts)
}

func readyReason(call *dinovav1alpha1.ApiCall) string {
	if condition := meta.FindStatusCondition(call.Status.Conditions, "Ready"); condition != nil {
		return condition.Reason
	}
	return ""
}

func retrying(retry dinovav1alpha1.ApiCallRetry) *dinovav1alpha1.ApiCallRetry {
	retry.IntervalSeconds = 1
	return &retry
}

func TestApiCallWaitsInsideOneObjectUntilTheConditionHolds(t *testing.T) {
	var hits atomic.Int32
	server := retryServer(t, &hits, jobStatus("running"), jobStatus("running"), jobStatus("success"))
	call := testAPICall(server.URL, http.MethodGet)
	call.Spec.Retry = retrying(dinovav1alpha1.ApiCallRetry{
		MaxAttempts: 10, Until: &dinovav1alpha1.ApiCallCondition{JSONPointer: "/status", EqualsJSON: `"success"`},
	})
	r := apiTestReconciler(t, server, call)
	runUntilDone(t, r, call)

	if call.Status.Phase != apiPhaseSucceeded || readyReason(call) != "Completed" {
		t.Fatalf("phase %s, reason %s; want Succeeded", call.Status.Phase, readyReason(call))
	}
	if hits.Load() != 3 || call.Status.Attempts != 3 {
		t.Fatalf("%d requests and %d attempts; want 3 and 3", hits.Load(), call.Status.Attempts)
	}
	if call.Status.NextAttemptAt != nil {
		t.Error("a finished ApiCall must not wait for another attempt")
	}
	if string(call.Status.Response.Body) != `{"status":"success"}` {
		t.Errorf("the response is the last answer, got %s", call.Status.Response.Body)
	}
}

func TestApiCallRunsOutOfAttempts(t *testing.T) {
	var hits atomic.Int32
	server := retryServer(t, &hits, jobStatus("running"))
	call := testAPICall(server.URL, http.MethodGet)
	call.Spec.Retry = retrying(dinovav1alpha1.ApiCallRetry{
		MaxAttempts: 2, Until: &dinovav1alpha1.ApiCallCondition{JSONPointer: "/status", EqualsJSON: `"success"`},
	})
	r := apiTestReconciler(t, server, call)
	runUntilDone(t, r, call)

	if call.Status.Phase != apiPhaseFailed || readyReason(call) != "PollTimeout" {
		t.Fatalf("phase %s, reason %s; want Failed, PollTimeout", call.Status.Phase, readyReason(call))
	}
	if hits.Load() != 2 {
		t.Fatalf("%d requests; maxAttempts counts the first one, so want 2", hits.Load())
	}
}

func TestApiCallRetriesOnlyWhatItWasToldTo(t *testing.T) {
	for name, test := range map[string]struct {
		replies  []func(http.ResponseWriter)
		retry    dinovav1alpha1.ApiCallRetry
		phase    string
		reason   string
		requests int32
	}{
		"a listed status is retried": {
			replies: []func(http.ResponseWriter){status(503), jobStatus("ok")},
			retry:   dinovav1alpha1.ApiCallRetry{MaxAttempts: 3, OnStatusCodes: []int32{503}},
			phase:   apiPhaseSucceeded, reason: "Completed", requests: 2,
		},
		"another status fails at once": {
			replies: []func(http.ResponseWriter){status(401), jobStatus("ok")},
			retry:   dinovav1alpha1.ApiCallRetry{MaxAttempts: 3, OnStatusCodes: []int32{503}},
			phase:   apiPhaseFailed, reason: "HTTPError", requests: 1,
		},
		"a listed status that keeps failing exhausts the attempts": {
			replies: []func(http.ResponseWriter){status(503)},
			retry:   dinovav1alpha1.ApiCallRetry{MaxAttempts: 2, OnStatusCodes: []int32{503}},
			phase:   apiPhaseFailed, reason: "RetriesExhausted", requests: 2,
		},
		"a condition that cannot be read fails at once": {
			replies: []func(http.ResponseWriter){jobStatus("running")},
			retry: dinovav1alpha1.ApiCallRetry{
				MaxAttempts: 5, Until: &dinovav1alpha1.ApiCallCondition{JSONPointer: "/nothing", EqualsJSON: `"x"`},
			},
			phase: apiPhaseFailed, reason: "ConditionInvalid", requests: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var hits atomic.Int32
			server := retryServer(t, &hits, test.replies...)
			call := testAPICall(server.URL, http.MethodGet)
			call.Spec.Retry = retrying(test.retry)
			r := apiTestReconciler(t, server, call)
			runUntilDone(t, r, call)
			if call.Status.Phase != test.phase || readyReason(call) != test.reason || hits.Load() != test.requests {
				t.Fatalf("phase %s, reason %s, %d requests; want %s, %s, %d",
					call.Status.Phase, readyReason(call), hits.Load(), test.phase, test.reason, test.requests)
			}
		})
	}
}

func TestApiCallWithoutRetrySendsOnce(t *testing.T) {
	var hits atomic.Int32
	server := retryServer(t, &hits, status(503))
	call := testAPICall(server.URL, http.MethodGet)
	r := apiTestReconciler(t, server, call)
	runUntilDone(t, r, call)
	if call.Status.Phase != apiPhaseFailed || hits.Load() != 1 || call.Status.Attempts != 1 {
		t.Fatalf("phase %s, %d requests, %d attempts; want Failed after one", call.Status.Phase, hits.Load(), call.Status.Attempts)
	}
}
