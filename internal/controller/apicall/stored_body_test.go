package apicall

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

func TestStoredBody(t *testing.T) {
	large := `{"id":12345678901234567890,"status":"failed","last_completed_step":5,"log_output":"` + strings.Repeat("x", 2*apiResponseLimit) + `"}`
	for name, tc := range map[string]struct {
		body string
		keep []string
		want string
		cut  bool
	}{
		"kept values of a large body": {large, []string{"/status", "/last_completed_step", "/id"}, `{"id":12345678901234567890,"last_completed_step":5,"status":"failed"}`, false},
		"no pointers keep the body":   {`{"a":1, "b":2}`, nil, `{"a":1, "b":2}`, false},
		"large body without pointers": {large, nil, large[:apiResponseLimit], true},
		"kept values too large":       {large, []string{"/log_output"}, `{"log_output":"` + strings.Repeat("x", apiResponseLimit-len(`{"log_output":"`)), true},
		"not JSON is kept as it is":   {"plain text", []string{"/status"}, "plain text", false},
		"two documents are not JSON":  {`{"a":1}{"b":2}`, []string{"/a"}, `{"a":1}{"b":2}`, false},
		"nothing found":               {`{"a":1}`, []string{"/b"}, `null`, false},
	} {
		got, cut := storedBody([]byte(tc.body), tc.keep)
		if string(got) != tc.want || cut != tc.cut {
			t.Errorf("%s: got %.80s (cut %v), want %.80s (cut %v)", name, got, cut, tc.want, tc.cut)
		}
	}
}

func TestKeptPointers(t *testing.T) {
	until := &dinovav1alpha1.ApiCallRetry{Until: &dinovav1alpha1.ApiCallCondition{JSONPointer: "/status"}}
	if got := keptPointers(&dinovav1alpha1.ApiCallSpec{Retry: until}); got != nil {
		t.Errorf("without keepJSONPointers: %v, want the whole body", got)
	}
	spec := &dinovav1alpha1.ApiCallSpec{KeepJSONPointers: []string{"/id"}, Retry: until}
	if got := keptPointers(spec); len(got) != 2 || got[0] != "/id" || got[1] != "/status" {
		t.Errorf("with retry.until: %v", got)
	}
	if len(spec.KeepJSONPointers) != 1 {
		t.Error("the spec was changed")
	}
}

func TestApiCallKeepsOnlyTheNamedValuesOfALargeResponse(t *testing.T) {
	large := `{"status":"success","log_output":"` + strings.Repeat("x", 4*apiResponseLimit) + `"}`
	for name, tc := range map[string]struct {
		keep      []string
		body      string
		truncated bool
	}{
		"with pointers":    {[]string{"/status"}, `{"status":"success"}`, false},
		"without pointers": {nil, large[:apiResponseLimit], true},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(large))
		}))
		call := testAPICall(server.URL, http.MethodGet)
		call.Spec.KeepJSONPointers = tc.keep
		r := apiTestReconcilerReading(t, server, 1<<20, call)
		waitAPI(t, func() bool {
			reconcileAPI(t, r, call)
			return call.Status.Phase == apiPhaseFailed || call.Status.Phase == apiPhaseSucceeded
		})
		server.Close()
		// The request succeeded either way: only what the status keeps differs.
		if call.Status.Phase != apiPhaseSucceeded || string(call.Status.Response.Body) != tc.body || call.Status.Response.Truncated != tc.truncated {
			t.Errorf("%s: phase %s, truncated %v, body %.60s", name, call.Status.Phase, call.Status.Response.Truncated, call.Status.Response.Body)
		}
	}
}

// An error answer is kept whole: keepJSONPointers names what a successful answer is read for,
// and the error is what explains a failure.
func TestApiCallKeepsAnErrorAnswerWhole(t *testing.T) {
	const answer = `{"detail":[{"loc":["body","cpu_count"],"msg":"Input should be a valid integer"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(answer))
	}))
	defer server.Close()
	call := testAPICall(server.URL, http.MethodGet)
	call.Spec.KeepJSONPointers = []string{"/job_id"}
	r := apiTestReconciler(t, server, call)
	waitAPI(t, func() bool {
		reconcileAPI(t, r, call)
		return call.Status.Phase == apiPhaseFailed || call.Status.Phase == apiPhaseSucceeded
	})
	if call.Status.Phase != apiPhaseFailed || string(call.Status.Response.Body) != answer {
		t.Errorf("phase %s, body %s", call.Status.Phase, call.Status.Response.Body)
	}
}
