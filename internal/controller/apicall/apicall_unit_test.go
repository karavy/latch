package apicall

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

func apiTestReconciler(t *testing.T, server *httptest.Server, objects ...client.Object) *Handler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := dinovav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	httpClient := server.Client()
	httpClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	apiClient, err := apiengine.NewClient(apiengine.Config{HTTPClient: httpClient, Timeout: time.Hour, MaxResponseBytes: apiResponseLimit})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewAPIExecutor(apiClient, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = executor.Start(workerCtx)
	}()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("workers did not shut down")
		}
	})
	waitAPI(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return executor.ctx != nil
	})
	return &Handler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&dinovav1alpha1.ApiCall{}).WithObjects(objects...).Build(),
		Executor: executor, AllowedOrigins: []string{server.URL},
	}
}

func waitAPI(t *testing.T, check func() bool) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for !check() {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("timed out waiting for API operation")
		}
	}
}

func testAPICall(endpoint, method string) *dinovav1alpha1.ApiCall {
	return &dinovav1alpha1.ApiCall{
		ObjectMeta: metav1.ObjectMeta{
			Name: "call", Namespace: "default", UID: "test-uid", Generation: 1,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: dinovav1alpha1.GroupVersion.String(), Kind: "Workflow", Name: "owner", UID: "owner-uid",
				Controller: new(true),
			}},
		},
		Spec: dinovav1alpha1.ApiCallSpec{Endpoint: endpoint, Method: method, TimeoutSeconds: 2},
	}
}

func TestStandaloneApiCallIsNotExecuted(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	for name, owners := range map[string][]metav1.OwnerReference{
		"no owner":    nil,
		"other owner": {{APIVersion: "apps/v1", Kind: "Workflow", Name: "other", UID: "other-uid", Controller: new(true)}},
	} {
		t.Run(name, func(t *testing.T) {
			call := testAPICall(server.URL, http.MethodGet)
			call.OwnerReferences = owners
			reconciler := apiTestReconciler(t, server, call)
			reconcileAPI(t, reconciler, call)
			if call.Status.Phase != "" || requests.Load() != 0 {
				t.Errorf("standalone ApiCall ran: phase=%q requests=%d", call.Status.Phase, requests.Load())
			}
		})
	}
}

func reconcileAPI(t *testing.T, r *Handler, call *dinovav1alpha1.ApiCall) {
	t.Helper()
	name := types.NamespacedName{Name: call.Name, Namespace: call.Namespace}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: name}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), name, call); err != nil {
		t.Fatal(err)
	}
}

func TestApiCallMethodsAndAuthentication(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete} {
		for _, authType := range []string{apiAuthBearer, apiAuthBasic, apiAuthKey} {
			t.Run(method+"/"+authType, func(t *testing.T) {
				var requests atomic.Int32
				server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					requests.Add(1)
					body, err := io.ReadAll(req.Body)
					if err != nil || string(body) != `{"value":1}` || req.Method != method {
						t.Error("method or body was not preserved")
					}
					if req.Header.Get("Content-Type") != "application/json" {
						t.Error("Content-Type was not preserved")
					}
					switch authType {
					case apiAuthBearer:
						if req.Header.Get("Authorization") != "Bearer secret-token" {
							t.Error("Bearer authentication missing")
						}
					case apiAuthBasic:
						if req.Header.Get("Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("user:password")) {
							t.Error("Basic authentication missing")
						}
					case apiAuthKey:
						if req.Header.Get("X-API-Key") != "secret-token" {
							t.Error("API key missing")
						}
					}
					_, _ = w.Write([]byte(`{"value":42}`))
				}))
				defer server.Close()
				call := testAPICall(server.URL, method)
				call.Spec.Body = `{"value":1}`
				call.Spec.Headers = map[string]string{"Content-Type": "application/json"}
				call.Spec.Authentication = &dinovav1alpha1.APIAuthentication{
					Type: authType, HeaderName: "X-API-Key",
					TokenSecretRef:    &dinovav1alpha1.APISecretKeyReference{Name: "credentials", Key: "token"},
					UsernameSecretRef: &dinovav1alpha1.APISecretKeyReference{Name: "credentials", Key: "username"},
					PasswordSecretRef: &dinovav1alpha1.APISecretKeyReference{Name: "credentials", Key: "password"},
				}
				secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "default"},
					Data: map[string][]byte{"token": []byte("secret-token"), "username": []byte("user"), "password": []byte("password")}}
				r := apiTestReconciler(t, server, call, secret)
				reconcileAPI(t, r, call)
				waitAPI(t, func() bool {
					reconcileAPI(t, r, call)
					return call.Status.Phase == apiPhaseSucceeded
				})
				if string(call.Status.Response.Body) != `{"value":42}` || call.Status.Response.StatusCode != http.StatusOK {
					t.Fatalf("unexpected response: %+v", call.Status.Response)
				}
				reconcileAPI(t, r, call)
				if requests.Load() != 1 {
					t.Fatal("operation was duplicated")
				}
			})
		}
	}
}

func TestOAuth2ClientCredentialsAuthentication(t *testing.T) {
	for _, method := range []string{oauthClientSecretBasic, oauthClientSecretPost} {
		t.Run(method, func(t *testing.T) {
			var tokenRequests, resourceRequests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/oauth/token":
					tokenRequests.Add(1)
					if req.Method != http.MethodPost || req.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
						t.Error("OAuth token request method or content type is incorrect")
					}
					if err := req.ParseForm(); err != nil || req.Form.Get("grant_type") != "client_credentials" || req.Form.Get("scope") != "clusters:read clusters:write" || req.Form.Get("audience") != "cluster-api" {
						t.Error("OAuth form parameters were not sent correctly")
					}
					switch method {
					case oauthClientSecretBasic:
						want := "Basic " + base64.StdEncoding.EncodeToString([]byte("client+id:secret%2Bvalue"))
						if req.Header.Get("Authorization") != want || req.Form.Has("client_id") || req.Form.Has("client_secret") {
							t.Error("client_secret_basic credentials were not sent correctly")
						}
					case oauthClientSecretPost:
						if req.Form.Get("client_id") != "client id" || req.Form.Get("client_secret") != "secret+value" || req.Header.Get("Authorization") != "" {
							t.Error("client_secret_post credentials were not sent correctly")
						}
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"access_token":"issued-token","token_type":"Bearer","expires_in":300}`))
				case "/resource":
					resourceRequests.Add(1)
					if req.Header.Get("Authorization") != "Bearer issued-token" {
						t.Error("OAuth access token was not applied to the resource request")
					}
					_, _ = w.Write([]byte(`{"ok":true}`))
				default:
					t.Errorf("unexpected endpoint request path %q", req.URL.Path)
				}
			}))
			defer server.Close()

			call := testAPICall(server.URL+"/resource", http.MethodGet)
			call.Spec.Authentication = &dinovav1alpha1.APIAuthentication{
				Type: apiAuthOAuth2, TokenURL: server.URL + "/oauth/token",
				ClientIDSecretRef:          &dinovav1alpha1.APISecretKeyReference{Name: "oauth-client", Key: "id"},
				ClientSecretSecretRef:      &dinovav1alpha1.APISecretKeyReference{Name: "oauth-client", Key: "secret"},
				ClientAuthenticationMethod: method,
				Scopes:                     []string{"clusters:read", "clusters:write"},
				Audience:                   "cluster-api",
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "oauth-client", Namespace: "default"},
				Data:       map[string][]byte{"id": []byte("client id"), "secret": []byte("secret+value")},
			}
			r := apiTestReconciler(t, server, call, secret)
			reconcileAPI(t, r, call)
			waitAPI(t, func() bool {
				reconcileAPI(t, r, call)
				return call.Status.Phase == apiPhaseSucceeded || call.Status.Phase == apiPhaseFailed
			})
			if call.Status.Phase != apiPhaseSucceeded || tokenRequests.Load() != 1 || resourceRequests.Load() != 1 {
				t.Fatalf("OAuth call failed: phase=%s tokenRequests=%d resourceRequests=%d conditions=%+v", call.Status.Phase, tokenRequests.Load(), resourceRequests.Load(), call.Status.Conditions)
			}
		})
	}
}

func TestOAuth2TokenFailureDoesNotCallResource(t *testing.T) {
	var resourceRequests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/oauth/token" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"never persist this"}`))
			return
		}
		resourceRequests.Add(1)
	}))
	defer server.Close()
	call := testAPICall(server.URL+"/resource", http.MethodPost)
	call.Spec.Authentication = &dinovav1alpha1.APIAuthentication{
		Type: apiAuthOAuth2, TokenURL: server.URL + "/oauth/token",
		ClientIDSecretRef:     &dinovav1alpha1.APISecretKeyReference{Name: "oauth-client", Key: "id"},
		ClientSecretSecretRef: &dinovav1alpha1.APISecretKeyReference{Name: "oauth-client", Key: "secret"},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "oauth-client", Namespace: "default"}, Data: map[string][]byte{"id": []byte("client"), "secret": []byte("private")}}
	r := apiTestReconciler(t, server, call, secret)
	reconcileAPI(t, r, call)
	waitAPI(t, func() bool { reconcileAPI(t, r, call); return call.Status.Phase == apiPhaseFailed })
	if resourceRequests.Load() != 0 || call.Status.Response != nil {
		t.Fatal("resource request ran or token endpoint response was persisted after token failure")
	}
	if strings.Contains(call.Status.Conditions[0].Message, "never persist") || strings.Contains(call.Status.Conditions[0].Message, "private") {
		t.Fatal("OAuth error body or client secret leaked into status")
	}
}

func TestOAuth2TokenEndpointMustBeHTTPSAndAllowlisted(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	call := testAPICall(server.URL, http.MethodGet)
	call.Spec.Authentication = &dinovav1alpha1.APIAuthentication{
		Type: apiAuthOAuth2, TokenURL: "https://untrusted.example.com/oauth/token",
		ClientIDSecretRef:     &dinovav1alpha1.APISecretKeyReference{Name: "oauth", Key: "id"},
		ClientSecretSecretRef: &dinovav1alpha1.APISecretKeyReference{Name: "oauth", Key: "secret"},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "oauth", Namespace: "default"}, Data: map[string][]byte{"id": []byte("id"), "secret": []byte("secret")}}
	r := apiTestReconciler(t, server, call, secret)
	if _, _, err := r.buildRequest(context.Background(), call); !errors.Is(err, errInvalidRequest) {
		t.Fatal("unallowlisted token endpoint was accepted")
	}
	call.Spec.Authentication.TokenURL = "http://auth.example.com/oauth/token"
	r.AllowedOrigins = append(r.AllowedOrigins, "http://auth.example.com")
	if _, _, err := r.buildRequest(context.Background(), call); !errors.Is(err, errInvalidRequest) {
		t.Fatal("insecure OAuth token endpoint was accepted")
	}
}

func TestApiCallFailures(t *testing.T) {
	for _, test := range []struct {
		name, reason string
		status       int
		body         string
		block        bool
		store        bool
	}{
		{name: "http-error", reason: "HTTPError", status: http.StatusUnauthorized, body: "denied", store: true},
		{name: "timeout", reason: "Timeout", block: true},
		{name: "oversize", reason: "ResponseTooLarge", status: http.StatusOK, body: strings.Repeat("x", apiResponseLimit+1), store: true},
		{name: "redirect", reason: "HTTPError", status: http.StatusFound},
		{name: "no-body-storage", reason: "Completed", status: http.StatusOK, body: "sensitive"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests.Add(1)
				if test.block {
					<-req.Context().Done()
					return
				}
				w.Header().Set("Location", "/redirect")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			call := testAPICall(server.URL, http.MethodPost)
			call.Spec.TimeoutSeconds = 1
			call.Spec.StoreResponseBody = &test.store
			r := apiTestReconciler(t, server, call)
			waitAPI(t, func() bool {
				reconcileAPI(t, r, call)
				return call.Status.Phase == apiPhaseFailed || call.Status.Phase == apiPhaseSucceeded
			})
			if call.Status.Conditions[0].Reason != test.reason {
				t.Fatalf("unexpected condition: %+v", call.Status.Conditions)
			}
			if call.Status.Response != nil && !test.store && len(call.Status.Response.Body) != 0 {
				t.Fatal("sensitive response body was stored")
			}
			if test.name == "oversize" && (!call.Status.Response.Truncated || len(call.Status.Response.Body) != apiResponseLimit) {
				t.Fatal("oversized response was not bounded and marked incomplete")
			}
			reconcileAPI(t, r, call)
			if requests.Load() != 1 {
				t.Fatal("failed operation was repeated")
			}
		})
	}
}

func TestApiCallMissingCredentialsAndPolicy(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	call := testAPICall(server.URL, http.MethodGet)
	call.Spec.Authentication = &dinovav1alpha1.APIAuthentication{Type: apiAuthBearer, TokenSecretRef: &dinovav1alpha1.APISecretKeyReference{Name: "missing", Key: "token"}}
	r := apiTestReconciler(t, server, call)
	reconcileAPI(t, r, call)
	if call.Status.Phase != apiPhasePending {
		t.Fatal("missing Secret should wait without executing")
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "missing", Namespace: "default"}, Data: map[string][]byte{"token": []byte("token")}}
	if err := r.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	waitAPI(t, func() bool { reconcileAPI(t, r, call); return call.Status.Phase == apiPhaseSucceeded })

	for _, endpoint := range []string{"http://metadata.internal", "https://user:password@example.com", server.URL + "#fragment"} {
		call.Spec.Endpoint = endpoint
		if _, _, err := r.buildRequest(context.Background(), call); !errors.Is(err, errInvalidRequest) {
			t.Fatalf("unsafe endpoint accepted: %s", endpoint)
		}
	}
	call.Spec.Endpoint = server.URL
	call.Spec.Headers = map[string]string{"Authorization": "plaintext-secret"}
	if _, _, err := r.buildRequest(context.Background(), call); err == nil {
		t.Fatal("inline authorization accepted")
	}
	call.Spec.Headers = nil
	call.Spec.Endpoint = "http://example.com"
	r.AllowedOrigins = []string{call.Spec.Endpoint}
	if _, _, err := r.buildRequest(context.Background(), call); err == nil {
		t.Fatal("Secret credentials over plaintext HTTP accepted")
	}
}

func TestApiCallLostExecution(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { t.Error("lost execution must not replay") }))
	defer server.Close()
	call := testAPICall(server.URL, http.MethodPost)
	call.Status = dinovav1alpha1.ApiCallStatus{ObservedGeneration: 1, Phase: apiPhaseRunning}
	r := apiTestReconciler(t, server, call)
	reconcileAPI(t, r, call)
	if call.Status.Phase != apiPhaseUnknown {
		t.Fatalf("lost execution phase = %s", call.Status.Phase)
	}
	reconcileAPI(t, r, call)
}

func TestApiCallDeletionCancelsWorker(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		close(started)
		<-req.Context().Done()
		close(canceled)
	}))
	defer server.Close()
	call := testAPICall(server.URL, http.MethodGet)
	r := apiTestReconciler(t, server, call)
	reconcileAPI(t, r, call)
	waitAPI(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	if err := r.Delete(context.Background(), call); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: call.Name, Namespace: call.Namespace}}); err != nil {
		t.Fatal(err)
	}
	waitAPI(t, func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	})
}

func TestAPIExecutorCapacityAndCheckpoint(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	r := apiTestReconciler(t, server)
	executor := r.Executor
	request := apiengine.Request{Method: http.MethodGet, Endpoint: server.URL}
	for i, name := range []string{"one", "two", "three"} {
		key := operationKey{Name: types.NamespacedName{Name: name}, UID: types.UID(name), Generation: 1}
		reserved := executor.reserve(key, request, time.Second, true)
		if reserved != (i < 2) {
			t.Fatal("executor capacity not respected")
		}
	}
	if requests.Load() != 0 {
		t.Fatal("request ran before durable Running checkpoint")
	}
	key := operationKey{Name: types.NamespacedName{Name: "one"}, UID: "one", Generation: 1}
	executor.activate(key)
	waitAPI(t, func() bool { _, done, _ := executor.lookup(key); return done })
	if requests.Load() != 1 {
		t.Fatal("wrong number of requests")
	}
	executor.activate(key)
	executor.forget(key)
	if _, _, exists := executor.lookup(key); exists {
		t.Fatal("completed operation was not released")
	}
}

type apiFailingStatusClient struct {
	client.Client
	fail atomic.Bool
}

func (c *apiFailingStatusClient) Status() client.SubResourceWriter {
	return &apiFailingStatusWriter{SubResourceWriter: c.Client.Status(), fail: &c.fail}
}

type apiFailingStatusWriter struct {
	client.SubResourceWriter
	fail *atomic.Bool
}

func (w *apiFailingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if w.fail.Swap(false) {
		return errors.New("simulated status write failure")
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestApiCallStatusWriteFailures(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("result"))
	}))
	defer server.Close()
	call := testAPICall(server.URL, http.MethodPost)
	call.Status = dinovav1alpha1.ApiCallStatus{ObservedGeneration: 1, Phase: apiPhasePending}
	r := apiTestReconciler(t, server, call)
	failing := &apiFailingStatusClient{Client: r.Client}
	r.Client = failing
	key := operationKey{Name: types.NamespacedName{Name: call.Name, Namespace: call.Namespace}, UID: call.UID, Generation: 1, Attempt: 1}
	req := ctrl.Request{NamespacedName: key.Name}

	failing.fail.Store(true)
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("expected Running checkpoint failure")
	}
	if _, _, exists := r.Executor.lookup(key); exists || requests.Load() != 0 {
		t.Fatal("checkpoint failure did not release reservation or incorrectly sent HTTP")
	}
	reconcileAPI(t, r, call)
	waitAPI(t, func() bool { _, done, _ := r.Executor.lookup(key); return done })
	failing.fail.Store(true)
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("expected completion status failure")
	}
	if _, done, exists := r.Executor.lookup(key); !exists || !done {
		t.Fatal("result was discarded before status persisted")
	}
	reconcileAPI(t, r, call)
	if call.Status.Phase != apiPhaseSucceeded || string(call.Status.Response.Body) != "result" || requests.Load() != 1 {
		t.Fatal("status persistence retry lost the result or repeated the HTTP operation")
	}
}

type apiDeadlineReader struct {
	client.Reader
	deadline time.Time
}

func (r *apiDeadlineReader) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	r.deadline, _ = ctx.Deadline()
	return context.DeadlineExceeded
}

func TestApiCallSecretReadDeadline(t *testing.T) {
	reader := &apiDeadlineReader{}
	r := &Handler{APIReader: reader}
	ref := &dinovav1alpha1.APISecretKeyReference{Name: "credentials", Key: "token"}
	_, err := r.secretValue(context.Background(), "default", ref)
	if !errors.Is(err, errCredentialsUnavailable) || reader.deadline.IsZero() || time.Until(reader.deadline) > apiSecretReadTimeout {
		t.Fatal("Secret read did not receive a bounded deadline and sanitized retryable error")
	}
	parentCtx, cancelParent := context.WithTimeout(context.Background(), time.Second)
	defer cancelParent()
	parentDeadline, _ := parentCtx.Deadline()
	_, err = r.secretValue(parentCtx, "default", ref)
	if !errors.Is(err, errCredentialsUnavailable) || !reader.deadline.Equal(parentDeadline) {
		t.Fatal("Secret read ignored the shorter parent deadline")
	}
}

func TestApiCallGenerationPreservesConditions(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { t.Error("missing credentials must not execute") }))
	t.Cleanup(server.Close)
	call := testAPICall(server.URL, http.MethodPost)
	call.Generation = 2
	call.Spec.Authentication = &dinovav1alpha1.APIAuthentication{
		Type: apiAuthBearer, TokenSecretRef: &dinovav1alpha1.APISecretKeyReference{Name: "missing", Key: "token"},
	}
	now := metav1.Now()
	call.Status = dinovav1alpha1.ApiCallStatus{
		ObservedGeneration: 1, Phase: apiPhaseSucceeded, StartedAt: &now, CompletedAt: &now,
		Response: &dinovav1alpha1.APIResponse{StatusCode: http.StatusOK, Body: []byte("old result")},
		Conditions: []metav1.Condition{
			{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "Completed", LastTransitionTime: now},
			{Type: "Audit", Status: metav1.ConditionTrue, ObservedGeneration: 1, Reason: "Approved", LastTransitionTime: now},
		},
	}
	r := apiTestReconciler(t, server, call)
	reconcileAPI(t, r, call)
	if call.Status.Phase != apiPhasePending || call.Status.ObservedGeneration != 2 || call.Status.Response != nil || call.Status.StartedAt != nil || call.Status.CompletedAt != nil {
		t.Fatal("new generation retained stale execution status")
	}
	audit := meta.FindStatusCondition(call.Status.Conditions, "Audit")
	ready := meta.FindStatusCondition(call.Status.Conditions, "Ready")
	if audit == nil || audit.Status != metav1.ConditionTrue || ready == nil || ready.Status != metav1.ConditionFalse || ready.ObservedGeneration != 2 {
		t.Fatal("conditions were lost or not updated for the new generation")
	}
}

func TestAPIExecutorCanceledQueueEntry(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.URL.Path == "/two" {
			t.Error("canceled queued operation was executed")
		}
		if req.URL.Path == "/one" {
			close(started)
			select {
			case <-release:
			case <-req.Context().Done():
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	r := apiTestReconciler(t, server)
	executor := r.Executor
	one := operationKey{Name: types.NamespacedName{Name: "one"}, UID: "one", Generation: 1}
	two := operationKey{Name: types.NamespacedName{Name: "two"}, UID: "two", Generation: 1}
	three := operationKey{Name: types.NamespacedName{Name: "three"}, UID: "three", Generation: 1}
	if !executor.reserve(one, apiengine.Request{Method: http.MethodGet, Endpoint: server.URL + "/one"}, time.Second*4, false) {
		t.Fatal("could not reserve first operation")
	}
	executor.activate(one)
	waitAPI(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	if !executor.reserve(two, apiengine.Request{
		Method: http.MethodPost, Endpoint: server.URL + "/two", Body: []byte("private body"),
		Headers: http.Header{"Authorization": {"Bearer private-token"}},
	}, time.Second, false) {
		t.Fatal("could not reserve queued operation")
	}
	executor.activate(two)
	executor.mu.Lock()
	queued := executor.jobs[two]
	executor.mu.Unlock()
	executor.forget(two)
	executor.mu.Lock()
	credentialsCleared := queued.request.Headers == nil && queued.request.Body == nil
	executor.mu.Unlock()
	if !credentialsCleared {
		t.Fatal("canceled queue entry retained credentials")
	}
	if !executor.reserve(three, apiengine.Request{Method: http.MethodGet, Endpoint: server.URL + "/three"}, time.Second, false) {
		t.Fatal("cancellation did not release executor capacity")
	}
	executor.activate(three)
	close(release)
	waitAPI(t, func() bool { _, done, _ := executor.lookup(three); return done })
	if requests.Load() != 2 {
		t.Fatal("unexpected HTTP operations after queue cancellation")
	}
}

func TestParseOrigins(t *testing.T) {
	got := ParseOrigins("# portale\nhttps://a.example.com, https://b.example.com:8443\n\n  # off\nhttps://c.example.com\n")
	want := []string{"https://a.example.com", "https://b.example.com:8443", "https://c.example.com"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("ParseOrigins = %v, want %v", got, want)
	}
	if len(ParseOrigins("")) != 0 {
		t.Fatal("empty text must give no origins")
	}
}

func TestOriginsConfigMapExtendsTheAllowlist(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	call := testAPICall("https://portal.example.com/api", http.MethodGet)
	configMapName := types.NamespacedName{Namespace: "operator", Name: "origins"}
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "operator", Name: "origins"},
		Data:       map[string]string{OriginsConfigMapKey: "# portale\nhttps://portal.example.com\nhttps://bad.example.com/path\n"},
	}
	r := apiTestReconciler(t, server, call, configMap)

	if _, _, err := r.buildRequest(context.Background(), call); !errors.Is(err, errInvalidRequest) {
		t.Fatalf("without a ConfigMap the origin must be denied, got %v", err)
	}
	r.OriginsConfigMap = configMapName
	if _, _, err := r.buildRequest(context.Background(), call); err != nil {
		t.Fatalf("origin listed in the ConfigMap was denied: %v", err)
	}
	bad := testAPICall("https://bad.example.com/api", http.MethodGet)
	if _, _, err := r.buildRequest(context.Background(), bad); !errors.Is(err, errInvalidRequest) {
		t.Fatalf("an entry with a path must be ignored, got %v", err)
	}
	if _, _, err := r.buildRequest(context.Background(), testAPICall(server.URL, http.MethodGet)); err != nil {
		t.Fatalf("an origin from the flag must still be allowed: %v", err)
	}

	configMap.Data[OriginsConfigMapKey] = ""
	if err := r.Update(context.Background(), configMap); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.buildRequest(context.Background(), call); !errors.Is(err, errInvalidRequest) {
		t.Fatalf("an edit of the ConfigMap must apply to the next request, got %v", err)
	}

	r.OriginsConfigMap = types.NamespacedName{Namespace: "operator", Name: "missing"}
	if _, _, err := r.buildRequest(context.Background(), call); !errors.Is(err, errInvalidRequest) {
		t.Fatalf("a missing ConfigMap must deny, got %v", err)
	}
}

type failingReader struct{ client.Reader }

func (failingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("apiserver unavailable")
}

func TestOriginsConfigMapReadErrorIsRetriedNotFailed(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	call := testAPICall("https://portal.example.com/api", http.MethodGet)
	r := apiTestReconciler(t, server, call)
	r.OriginsConfigMap = types.NamespacedName{Namespace: "operator", Name: "origins"}
	r.APIReader = failingReader{}
	if _, _, err := r.buildRequest(context.Background(), call); !errors.Is(err, errOriginsUnavailable) {
		t.Fatalf("a read error must be reported as retryable, got %v", err)
	}
}

func TestDebugApiCallLogsTheRequestAndSendsNothing(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	call := testAPICall(server.URL+"/segments/create", http.MethodPost)
	call.Spec.Debug = true
	call.Spec.Body = `{"client_name":"ACME"}`
	call.Spec.Headers = map[string]string{"Content-Type": "application/json"}
	call.Spec.Authentication = &dinovav1alpha1.APIAuthentication{
		Type: apiAuthKey, HeaderName: "X-API-Key",
		TokenSecretRef: &dinovav1alpha1.APISecretKeyReference{Name: "key", Key: "api-key"},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "key", Namespace: "default"}, Data: map[string][]byte{"api-key": []byte("super-secret-key")}}
	r := apiTestReconciler(t, server, call, secret)

	var logged []string
	logger := funcr.New(func(prefix, args string) { logged = append(logged, prefix+args) }, funcr.Options{})
	ctx := logr.NewContext(context.Background(), logger)
	name := types.NamespacedName{Name: call.Name, Namespace: call.Namespace}
	for range 2 {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: name}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Get(ctx, name, call); err != nil {
		t.Fatal(err)
	}

	if call.Status.Phase != apiPhaseSucceeded || call.Status.Response != nil {
		t.Fatalf("phase = %s, response = %v; want Succeeded and no response", call.Status.Phase, call.Status.Response)
	}
	if ready := meta.FindStatusCondition(call.Status.Conditions, "Ready"); ready == nil || ready.Reason != "DebugSkipped" {
		t.Fatalf("Ready condition = %v, want reason DebugSkipped", ready)
	}
	if requests.Load() != 0 {
		t.Fatal("a debug ApiCall sent its request")
	}
	output := strings.Join(logged, "\n")
	for _, want := range []string{"Skipped ApiCall request because debug is enabled", `client_name`, "ACME", "X-Api-Key", "POST"} {
		if !strings.Contains(output, want) {
			t.Errorf("log lacks %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "super-secret-key") {
		t.Fatalf("the credential reached the log:\n%s", output)
	}
	if len(logged) != 1 {
		t.Fatalf("the request was logged %d times, want once", len(logged))
	}
}
