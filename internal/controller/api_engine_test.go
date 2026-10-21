package controller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
)

func TestApplyAPIRequest(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			const payload = `{"name": "cluster"}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != method || req.Header.Get("Content-Type") != "application/json" || req.URL.Query().Get("cluster") != "test" {
					t.Error("request method, headers or query not preserved")
				}
				body, err := io.ReadAll(req.Body)
				if err != nil || string(body) != payload {
					t.Errorf("request body = %q, error = %v", body, err)
				}
				w.Header().Set("X-Result", "preserved")
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"conflict"}`))
			}))
			defer server.Close()

			apiClient, err := apiengine.NewClient(apiengine.Config{})
			if err != nil {
				t.Fatal(err)
			}
			reconciler := &BaseClusterReconciler{APIClient: apiClient}
			response, err := reconciler.ApplyAPIRequest(context.Background(), apiengine.Request{
				Method: method, Endpoint: server.URL,
				Headers: http.Header{"Content-Type": {"application/json"}},
				Query:   url.Values{"cluster": {"test"}},
				Body:    []byte(payload),
			})
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusConflict || response.IsSuccess() || response.Headers.Get("X-Result") != "preserved" || string(response.Body) != `{"error":"conflict"}` {
				t.Errorf("unexpected response: %+v", response)
			}
		})
	}
}

func TestApplyAPIRequestCanceledContext(t *testing.T) {
	apiClient, err := apiengine.NewClient(apiengine.Config{})
	if err != nil {
		t.Fatal(err)
	}
	reconciler := &BaseClusterReconciler{APIClient: apiClient}
	callCtx, cancelCall := context.WithCancel(context.Background())
	cancelCall()
	_, err = reconciler.ApplyAPIRequest(callCtx, apiengine.Request{
		Method: http.MethodGet, Endpoint: "http://127.0.0.1:1",
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context cancellation, got %v", err)
	}
}

func TestApplyAPIRequestUninitializedClient(t *testing.T) {
	reconciler := &BaseClusterReconciler{}
	_, err := reconciler.ApplyAPIRequest(context.Background(), apiengine.Request{})
	if err == nil {
		t.Fatal("expected an error for an uninitialized API client")
	}
}
