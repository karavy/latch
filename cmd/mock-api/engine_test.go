package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// mockServer serves the full mock with a clock the test moves.
type mockServer struct {
	*httptest.Server
	engine *engine
	now    time.Time
}

func newMockServer(t *testing.T) *mockServer {
	t.Helper()
	mux := http.NewServeMux()
	m := &mockServer{now: time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)}
	m.engine = mountSimulatedServices(mux)
	m.engine.now = func() time.Time { return m.now }
	m.Server = httptest.NewServer(mux)
	t.Cleanup(m.Close)
	return m
}

func (m *mockServer) do(t *testing.T, method, path, body string) (int, map[string]any, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, m.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.Client().Do(req)
	if err != nil {
		return 0, nil, nil
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out, resp.Header
}

func (m *mockServer) fault(t *testing.T, rule string) {
	t.Helper()
	if status, out, _ := m.do(t, http.MethodPost, "/__mock/faults", rule); status != http.StatusCreated {
		t.Fatalf("fault rule refused: %d %v", status, out)
	}
}

func TestFaultRuleAfterAndTimes(t *testing.T) {
	m := newMockServer(t)
	m.fault(t, `{"name":"img","match":{"service":"openstack","method":"GET","path":"/image/v2/images$"},"action":{"type":"status","status":503},"after":1,"times":2}`)
	got := make([]int, 0, 5)
	for range 5 {
		status, _, _ := m.do(t, http.MethodGet, "/image/v2/images", "")
		got = append(got, status)
	}
	if want := []int{200, 503, 503, 200, 200}; !equalInts(got, want) {
		t.Errorf("statuses = %v, want %v", got, want)
	}
	var rules []faultRule
	resp, _ := m.Client().Get(m.URL + "/__mock/faults")
	_ = json.NewDecoder(resp.Body).Decode(&rules)
	_ = resp.Body.Close()
	if len(rules) != 1 || rules[0].Seen != 5 || rules[0].Applied != 2 {
		t.Errorf("rule counters = %+v", rules)
	}
}

func TestFaultResetClosesTheConnection(t *testing.T) {
	m := newMockServer(t)
	m.fault(t, `{"name":"down","match":{"service":"openstack"},"action":{"type":"reset"}}`)
	if status, _, _ := m.do(t, http.MethodGet, "/image/v2/images", ""); status != 0 {
		t.Errorf("status = %d, want no answer", status)
	}
	if status, _, _ := m.do(t, http.MethodGet, "/__mock/state", ""); status != http.StatusOK {
		t.Errorf("the control API must not be affected by faults: %d", status)
	}
}

func TestFaultExpires(t *testing.T) {
	m := newMockServer(t)
	m.fault(t, `{"name":"short","match":{"service":"openstack"},"action":{"type":"status","status":500},"expiresInSeconds":20}`)
	if status, _, _ := m.do(t, http.MethodGet, "/image/v2/images", ""); status != 500 {
		t.Fatalf("status before expiry = %d", status)
	}
	m.now = m.now.Add(21 * time.Second)
	if status, _, _ := m.do(t, http.MethodGet, "/image/v2/images", ""); status != 200 {
		t.Errorf("status after expiry = %d", status)
	}
}

func TestFaultBodyAndAfterHandler(t *testing.T) {
	m := newMockServer(t)
	m.fault(t, `{"name":"cut","match":{"path":"/image/v2/images$"},"action":{"type":"body"},"times":1}`)
	resp, err := m.Client().Get(m.URL + "/image/v2/images")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if json.Valid(raw) {
		t.Errorf("a body fault must break the JSON: %s", raw)
	}
	// afterHandler: the router is created, the answer is lost.
	m.fault(t, `{"name":"lost","match":{"method":"POST","path":"/routers$"},"action":{"type":"reset","afterHandler":true},"times":1}`)
	if status, _, _ := m.do(t, http.MethodPost, "/network/v2.0/routers", `{"router":{"name":"r"}}`); status != 0 {
		t.Fatalf("status = %d, want no answer", status)
	}
	_, out, _ := m.do(t, http.MethodGet, "/network/v2.0/routers?name=r", "")
	if routers := out["routers"].([]any); len(routers) != 1 {
		t.Errorf("the router must exist although the answer was lost: %v", out)
	}
}

func TestUnhandledCallsAreRecorded(t *testing.T) {
	m := newMockServer(t)
	if status, _, _ := m.do(t, http.MethodGet, "/network/v2.0/qos/policies", ""); status != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", status)
	}
	resp, _ := m.Client().Get(m.URL + "/__mock/unhandled")
	var calls []mockRequest
	_ = json.NewDecoder(resp.Body).Decode(&calls)
	_ = resp.Body.Close()
	if len(calls) != 1 || calls[0].Path != "/network/v2.0/qos/policies" {
		t.Errorf("unhandled = %+v", calls)
	}
}

func TestInvalidFaultRulesAreRefused(t *testing.T) {
	m := newMockServer(t)
	for _, rule := range []string{`{"action":{"type":"reset"}}`, `{"name":"x","action":{"type":"boom"}}`,
		`{"name":"x","action":{"type":"status"}}`, `{"name":"x","match":{"path":"("},"action":{"type":"reset"}}`} {
		if status, _, _ := m.do(t, http.MethodPost, "/__mock/faults", rule); status != http.StatusBadRequest {
			t.Errorf("rule %s accepted with %d", rule, status)
		}
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
