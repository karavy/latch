package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestEchoCapturesAndRedactsRequest(t *testing.T) {
	store := &requestStore{}
	mux := newHandler(store)

	request := httptest.NewRequest(http.MethodPost, "/echo?cluster=dev&access_token=query-secret", strings.NewReader(`{"name":"test","client_secret":"json-secret"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer header-secret")
	request.Header.Set("X-API-Key", "api-key-secret")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("echo status = %d", response.Code)
	}
	var captured capturedRequest
	if err := json.Unmarshal(response.Body.Bytes(), &captured); err != nil {
		t.Fatal(err)
	}
	if captured.ID != 1 || captured.Method != http.MethodPost || captured.Path != "/echo" || captured.Query["cluster"][0] != "dev" {
		t.Fatalf("unexpected capture: %+v", captured)
	}
	if captured.Query["access_token"][0] != redactedValue || captured.Headers.Get("Authorization") != "Bearer "+redactedValue || captured.Headers.Get("X-API-Key") != redactedValue {
		t.Fatalf("sensitive request values were not redacted: %+v", captured)
	}
	if strings.Contains(captured.Body, "json-secret") || !strings.Contains(captured.Body, redactedValue) {
		t.Fatalf("JSON body was not redacted: %s", captured.Body)
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/__debug/requests?limit=1", nil)
	listResponse := httptest.NewRecorder()
	mux.ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), `"id":1`) {
		t.Fatalf("request history did not include echo: %s", listResponse.Body.String())
	}
}

func TestOAuthFormIsRedactedAndBodyLimitIsReported(t *testing.T) {
	store := &requestStore{}
	request := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader("grant_type=client_credentials&client_id=test-client&client_secret=very-secret"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	store.echo(response, request)
	var captured capturedRequest
	if err := json.Unmarshal(response.Body.Bytes(), &captured); err != nil {
		t.Fatal(err)
	}
	form, err := url.ParseQuery(captured.Body)
	if err != nil || form.Get("client_secret") != redactedValue || form.Get("client_id") != "test-client" {
		t.Fatalf("OAuth form fields were not sanitized: %q", captured.Body)
	}

	large := httptest.NewRequest(http.MethodPut, "/large", strings.NewReader(strings.Repeat("x", maxRequestBody+1)))
	largeResponse := httptest.NewRecorder()
	store.echo(largeResponse, large)
	if err := json.Unmarshal(largeResponse.Body.Bytes(), &captured); err != nil {
		t.Fatal(err)
	}
	if !captured.BodyTruncated || len(captured.Body) != maxRequestBody {
		t.Fatalf("oversized body was not bounded: truncated=%t size=%d", captured.BodyTruncated, len(captured.Body))
	}
}

func TestOAuthTokenEndpointIssuesDebugTokenAndCapturesRedactedCredentials(t *testing.T) {
	store := &requestStore{}
	request := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader("grant_type=client_credentials&client_id=debug-client&client_secret=do-not-store"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Authorization", "Basic c2VjcmV0")
	response := httptest.NewRecorder()
	store.token(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"access_token":"mock-debug-access-token"`) {
		t.Fatalf("unexpected token response: status=%d body=%s", response.Code, response.Body.String())
	}
	if len(store.requests) != 1 {
		t.Fatalf("expected exactly one token request, got %d", len(store.requests))
	}
	form, err := url.ParseQuery(store.requests[0].Body)
	if err != nil || form.Get("client_secret") != redactedValue || store.requests[0].Headers.Get("Authorization") != "Basic "+redactedValue {
		t.Fatalf("token request was not captured safely: %+v", store.requests)
	}
}

func TestRequestHistoryIsBoundedAndCanBeCleared(t *testing.T) {
	store := &requestStore{}
	for range maxCapturedEvents + 5 {
		store.append(&capturedRequest{Method: http.MethodGet, Path: "/request"})
	}
	if len(store.requests) != maxCapturedEvents || store.requests[0].ID != 6 || store.requests[len(store.requests)-1].ID != maxCapturedEvents+5 {
		t.Fatalf("unexpected bounded history: len=%d first=%d last=%d", len(store.requests), store.requests[0].ID, store.requests[len(store.requests)-1].ID)
	}
	response := httptest.NewRecorder()
	history := newHandler(store)
	history.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/__debug/requests", nil))
	if response.Code != http.StatusNoContent || len(store.requests) != 0 {
		t.Fatal("history clear did not empty captured requests")
	}
}

func TestDebugHistoryRouteDispatchesAndStatusEndpointWorks(t *testing.T) {
	store := &requestStore{}
	handler := newHandler(store)
	for _, test := range []struct {
		method string
		path   string
		status int
	}{
		{method: http.MethodGet, path: "/__debug/requests", status: http.StatusOK},
		{method: http.MethodDelete, path: "/__debug/requests", status: http.StatusNoContent},
		{method: http.MethodPost, path: "/__debug/requests", status: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/status/418", status: http.StatusTeapot},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		if response.Code != test.status {
			t.Errorf("%s %s status = %d, want %d (body %q)", test.method, test.path, response.Code, test.status, response.Body.String())
		}
	}
}

func postJSON(t *testing.T, handler http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestNetworkRequiresTheVDCReturnedByTheFirstCall(t *testing.T) {
	store := &requestStore{}
	mux := newHandler(store)

	early := postJSON(t, mux, "/networks", `{"vdcId":"vdc-1","name":"lan"}`)
	if early.Code != http.StatusNotFound {
		t.Fatalf("network before its VDC status = %d, want 404", early.Code)
	}

	created := postJSON(t, mux, "/vdcs", `{"name":"acme"}`)
	var vdc map[string]string
	if created.Code != http.StatusCreated || json.Unmarshal(created.Body.Bytes(), &vdc) != nil || vdc["id"] != "vdc-1" || vdc["name"] != "acme" {
		t.Fatalf("unexpected VDC response: %d %s", created.Code, created.Body.String())
	}

	network := postJSON(t, mux, "/networks", `{"vdcId":"`+vdc["id"]+`","name":"lan"}`)
	var result map[string]string
	if network.Code != http.StatusCreated || json.Unmarshal(network.Body.Bytes(), &result) != nil || result["id"] != "net-1" || result["vdcId"] != "vdc-1" {
		t.Fatalf("unexpected network response: %d %s", network.Code, network.Body.String())
	}

	history := httptest.NewRecorder()
	mux.ServeHTTP(history, httptest.NewRequest(http.MethodGet, "/__debug/requests", nil))
	var entries []capturedRequest
	if err := json.Unmarshal(history.Body.Bytes(), &entries); err != nil || len(entries) != 3 {
		t.Fatalf("history = %s (%v)", history.Body.String(), err)
	}
	if entries[0].Path != "/networks" || entries[1].Path != "/vdcs" || entries[2].Path != "/networks" {
		t.Fatalf("history order = %+v", entries)
	}
}

func TestResourceEndpointsValidateRequests(t *testing.T) {
	mux := newHandler(&requestStore{})
	for name, tc := range map[string]struct{ path, body string }{
		"vdc without name":        {"/vdcs", `{}`},
		"vdc with invalid JSON":   {"/vdcs", `not-json`},
		"network without vdcId":   {"/networks", `{"name":"lan"}`},
		"network without name":    {"/networks", `{"vdcId":"vdc-1"}`},
		"network with non-object": {"/networks", `[]`},
	} {
		if response := postJSON(t, mux, tc.path, tc.body); response.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, response.Code)
		}
	}
	for _, path := range []string{"/vdcs", "/networks"} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != http.MethodPost {
			t.Errorf("GET %s: status = %d, Allow = %q", path, response.Code, response.Header().Get("Allow"))
		}
	}
}
