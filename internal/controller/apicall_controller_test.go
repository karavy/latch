/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/apicall"
)

const (
	apiCallTestResponseLimit = 64 << 10
	apiCallPhaseSucceeded    = "Succeeded"
)

var _ = Describe("ApiCall Controller", func() {
	It("watches CRs and worker completion events through a running manager", func() {
		callCtx := context.Background()
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			_, _ = w.Write([]byte(`{"managed":true}`))
		}))
		DeferCleanup(server.Close)
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: k8sClient.Scheme(), Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
		})
		Expect(err).NotTo(HaveOccurred())
		apiClient, err := apiengine.NewClient(apiengine.Config{MaxResponseBytes: apiCallTestResponseLimit})
		Expect(err).NotTo(HaveOccurred())
		executor, err := apicall.NewAPIExecutor(apiClient, 1, 1)
		Expect(err).NotTo(HaveOccurred())
		r := &ApiCallReconciler{Client: mgr.GetClient(), Executor: executor, AllowedOrigins: []string{server.URL}}
		Expect(r.SetupWithManager(mgr)).To(Succeed())
		managerCtx, stopManager := context.WithCancel(callCtx)
		managerDone := make(chan error, 1)
		go func() { managerDone <- mgr.Start(managerCtx) }()
		DeferCleanup(func() {
			stopManager()
			Eventually(managerDone, 10*time.Second).Should(Receive(BeNil()))
		})
		name := types.NamespacedName{Name: "apicall-managed", Namespace: "default"}
		call := &dinovav1alpha1.ApiCall{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec:       dinovav1alpha1.ApiCallSpec{Method: http.MethodGet, Endpoint: server.URL},
		}
		ownByWorkflow(callCtx, call)
		Expect(k8sClient.Create(callCtx, call)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(callCtx, call)).To(Succeed()) })
		Eventually(func() string {
			Expect(k8sClient.Get(callCtx, name, call)).To(Succeed())
			return call.Status.Phase
		}, 10*time.Second).Should(Equal(apiCallPhaseSucceeded))
		Expect(requests.Load()).To(Equal(int32(1)))
		Expect(string(call.Status.Response.Body)).To(Equal(`{"managed":true}`))
	})

	It("runs slow HTTP calls outside reconcile and executes each generation only once", func() {
		callCtx := context.Background()
		var requests atomic.Int32
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			requests.Add(1)
			select {
			case <-release:
			case <-req.Context().Done():
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":42}`))
		}))
		DeferCleanup(server.Close)
		apiClient, err := apiengine.NewClient(apiengine.Config{MaxResponseBytes: apiCallTestResponseLimit})
		Expect(err).NotTo(HaveOccurred())
		executor, err := apicall.NewAPIExecutor(apiClient, 1, 1)
		Expect(err).NotTo(HaveOccurred())
		workerCtx, stopWorkers := context.WithCancel(callCtx)
		workersDone := make(chan struct{})
		go func() {
			defer close(workersDone)
			_ = executor.Start(workerCtx)
		}()
		DeferCleanup(func() {
			stopWorkers()
			Eventually(workersDone).Should(BeClosed())
		})
		Eventually(executor.Ready).Should(BeTrue())

		name := types.NamespacedName{Name: "apicall-async", Namespace: "default"}
		call := &dinovav1alpha1.ApiCall{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec:       dinovav1alpha1.ApiCallSpec{Method: http.MethodPost, Endpoint: server.URL, Body: `{"name":"test"}`},
		}
		ownByWorkflow(callCtx, call)
		Expect(k8sClient.Create(callCtx, call)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(callCtx, call)).To(Succeed()) })
		reconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{server.URL}}
		req := reconcile.Request{NamespacedName: name}
		boundedCtx, cancelBounded := context.WithTimeout(callCtx, 500*time.Millisecond)
		defer cancelBounded()
		_, err = reconciler.Reconcile(boundedCtx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(callCtx, name, call)).To(Succeed())
		Expect(call.Status.Phase).To(Equal("Running"))
		Eventually(requests.Load).Should(Equal(int32(1)))
		for range 3 {
			_, err = reconciler.Reconcile(callCtx, req)
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(requests.Load()).To(Equal(int32(1)))
		close(release)
		Eventually(func() string {
			_, reconcileErr := reconciler.Reconcile(callCtx, req)
			Expect(reconcileErr).NotTo(HaveOccurred())
			Expect(k8sClient.Get(callCtx, name, call)).To(Succeed())
			return call.Status.Phase
		}).Should(Equal(apiCallPhaseSucceeded))
		Expect(string(call.Status.Response.Body)).To(Equal(`{"value":42}`))
		Expect(call.Status.Response.StatusCode).To(Equal(http.StatusOK))
		Expect(call.Status.ObservedGeneration).To(Equal(call.Generation))
		_, err = reconciler.Reconcile(callCtx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(requests.Load()).To(Equal(int32(1)))

	})

	It("never executes an ApiCall that no Workflow owns", func() {
		callCtx := context.Background()
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
		DeferCleanup(server.Close)
		apiClient, err := apiengine.NewClient(apiengine.Config{MaxResponseBytes: apiCallTestResponseLimit})
		Expect(err).NotTo(HaveOccurred())
		executor, err := apicall.NewAPIExecutor(apiClient, 1, 1)
		Expect(err).NotTo(HaveOccurred())
		name := types.NamespacedName{Name: "apicall-template", Namespace: "default"}
		call := &dinovav1alpha1.ApiCall{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec:       dinovav1alpha1.ApiCallSpec{Method: http.MethodGet, Endpoint: server.URL},
		}
		Expect(k8sClient.Create(callCtx, call)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(callCtx, call)).To(Succeed()) })
		reconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{server.URL}}
		for range 3 {
			_, err = reconciler.Reconcile(callCtx, reconcile.Request{NamespacedName: name})
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(k8sClient.Get(callCtx, name, call)).To(Succeed())
		Expect(call.Status.Phase).To(BeEmpty())
		Expect(requests.Load()).To(BeZero())
	})

	It("validates required fields and authentication in the generated CRD", func() {
		invalid := &dinovav1alpha1.ApiCall{
			ObjectMeta: metav1.ObjectMeta{Name: "apicall-invalid", Namespace: "default"},
		}
		Expect(k8sClient.Create(context.Background(), invalid)).NotTo(Succeed())
		invalid.Spec = dinovav1alpha1.ApiCallSpec{Method: "PATCH", Endpoint: "https://example.com"}
		Expect(k8sClient.Create(context.Background(), invalid)).NotTo(Succeed())
		invalid.Spec.Method = "GET"
		invalid.Spec.Authentication = &dinovav1alpha1.APIAuthentication{Type: "Bearer"}
		Expect(k8sClient.Create(context.Background(), invalid)).NotTo(Succeed())
		invalid.Spec.Authentication = &dinovav1alpha1.APIAuthentication{
			Type: "OAuth2ClientCredentials", TokenURL: "https://auth.example.com/token",
		}
		Expect(k8sClient.Create(context.Background(), invalid)).NotTo(Succeed())
	})

	It("rejects every spec change after ApiCall creation", func() {
		ctx := context.Background()
		name := types.NamespacedName{Name: "apicall-immutable-spec", Namespace: "default"}
		call := &dinovav1alpha1.ApiCall{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: dinovav1alpha1.ApiCallSpec{
				Method: http.MethodPost, Endpoint: "http://mock.example/first",
				Body: `{"value":"first"}`, Headers: map[string]string{"Content-Type": "application/json"},
				TimeoutSeconds: 30, ExecutionID: "first",
				Authentication: &dinovav1alpha1.APIAuthentication{
					Type: "OAuth2ClientCredentials", TokenURL: "https://auth.example/token",
					ClientIDSecretRef:          &dinovav1alpha1.APISecretKeyReference{Name: "oauth", Key: "client-id"},
					ClientSecretSecretRef:      &dinovav1alpha1.APISecretKeyReference{Name: "oauth", Key: "client-secret"},
					ClientAuthenticationMethod: "ClientSecretBasic",
					Scopes:                     []string{"first:scope"},
					Audience:                   "first-audience",
				},
			},
		}
		Expect(k8sClient.Create(ctx, call)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, call)).To(Succeed()) })

		updates := []struct {
			name   string
			mutate func(*dinovav1alpha1.ApiCall)
		}{
			{name: "endpoint", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Endpoint = "http://mock.example/second" }},
			{name: "method", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Method = http.MethodPut }},
			{name: "body", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Body = `{"value":"second"}` }},
			{name: "headers", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Headers["X-Test"] = "changed" }},
			{name: "authentication type", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Authentication.Type = "Bearer" }},
			{name: "authentication client ID ref", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Authentication.ClientIDSecretRef.Name = "another-secret" }},
			{name: "authentication client secret ref", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Authentication.ClientSecretSecretRef.Key = "another-key" }},
			{name: "authentication token URL", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Authentication.TokenURL = "https://other.example/token" }},
			{name: "authentication method", mutate: func(obj *dinovav1alpha1.ApiCall) {
				obj.Spec.Authentication.ClientAuthenticationMethod = "ClientSecretPost"
			}},
			{name: "authentication scopes", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Authentication.Scopes[0] = "second:scope" }},
			{name: "authentication audience", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.Authentication.Audience = "second-audience" }},
			{name: "timeout", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.TimeoutSeconds = 60 }},
			{name: "storeResponseBody", mutate: func(obj *dinovav1alpha1.ApiCall) {
				store := false
				obj.Spec.StoreResponseBody = &store
			}},
			{name: "executionID", mutate: func(obj *dinovav1alpha1.ApiCall) { obj.Spec.ExecutionID = "second" }},
		}
		for _, update := range updates {
			By("rejecting a change to " + update.name)
			current := &dinovav1alpha1.ApiCall{}
			Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
			update.mutate(current)
			Expect(k8sClient.Update(ctx, current)).NotTo(Succeed())
		}

		By("accepting metadata changes, which never repeat the call")
		current := &dinovav1alpha1.ApiCall{}
		Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
		current.Labels = map[string]string{"changed": "true"}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
	})

	It("keeps every ApiCallTemplate field editable, including authentication", func() {
		ctx := context.Background()
		name := types.NamespacedName{Name: "apicalltemplate-editable", Namespace: "default"}
		template := &dinovav1alpha1.ApiCallTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: dinovav1alpha1.ApiCallTemplateSpec{
				Method: http.MethodPost, Endpoint: "http://mock.example/first", Body: `{"value":"first"}`,
				Headers: map[string]string{"Content-Type": "application/json"}, TimeoutSeconds: 30,
				Authentication: &dinovav1alpha1.APIAuthentication{
					Type: "OAuth2ClientCredentials", TokenURL: "https://auth.example/token",
					ClientIDSecretRef:     &dinovav1alpha1.APISecretKeyReference{Name: "oauth", Key: "client-id"},
					ClientSecretSecretRef: &dinovav1alpha1.APISecretKeyReference{Name: "oauth", Key: "client-secret"},
					Scopes:                []string{"first:scope"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })

		current := &dinovav1alpha1.ApiCallTemplate{}
		Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
		current.Spec.Method = http.MethodPut
		current.Spec.Endpoint = "http://mock.example/second"
		current.Spec.Body = `{"value":"second"}`
		current.Spec.Headers["X-Test"] = "changed"
		current.Spec.TimeoutSeconds = 60
		current.Spec.Authentication.TokenURL = "https://other.example/token"
		current.Spec.Authentication.ClientIDSecretRef.Name = "another-secret"
		current.Spec.Authentication.Scopes = []string{"second:scope"}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())

		updated := &dinovav1alpha1.ApiCallTemplate{}
		Expect(k8sClient.Get(ctx, name, updated)).To(Succeed())
		Expect(updated.Spec.Authentication.TokenURL).To(Equal("https://other.example/token"))
		Expect(updated.Spec.Authentication.Scopes).To(Equal([]string{"second:scope"}))

		By("still validating the edited authentication")
		updated.Spec.Authentication = &dinovav1alpha1.APIAuthentication{Type: "Bearer"}
		Expect(k8sClient.Update(ctx, updated)).NotTo(Succeed())
	})
})

// ownByWorkflow makes call a Workflow-owned step copy, the only kind the controller executes.
func ownByWorkflow(ctx context.Context, call *dinovav1alpha1.ApiCall) {
	owner := &dinovav1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: call.Name + "-owner", Namespace: call.Namespace},
		Spec: dinovav1alpha1.WorkflowSpec{Templates: []dinovav1alpha1.WorkflowTemplateRun{{
			Name: "step", TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "template"},
		}}},
	}
	Expect(k8sClient.Create(ctx, owner)).To(Succeed())
	DeferCleanup(func() { Expect(k8sClient.Delete(ctx, owner)).To(Succeed()) })
	Expect(controllerutil.SetControllerReference(owner, call, k8sClient.Scheme())).To(Succeed())
}
