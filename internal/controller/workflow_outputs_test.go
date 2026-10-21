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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/apicall"
)

// startExecutor runs an API executor for the duration of the current spec.
func startExecutor() *apicall.APIExecutor {
	GinkgoHelper()
	apiClient, err := apiengine.NewClient(apiengine.Config{})
	Expect(err).NotTo(HaveOccurred())
	executor, err := apicall.NewAPIExecutor(apiClient, 2, 8)
	Expect(err).NotTo(HaveOccurred())
	workerCtx, stopWorkers := context.WithCancel(ctx)
	workersDone := make(chan struct{})
	go func() {
		defer close(workersDone)
		_ = executor.Start(workerCtx)
	}()
	DeferCleanup(func() {
		stopWorkers()
		Eventually(workersDone, 5*time.Second).Should(BeClosed())
	})
	Eventually(executor.Ready).Should(BeTrue())
	return executor
}

func createTemplates(templates map[string]dinovav1alpha1.ApiCallTemplateSpec) {
	GinkgoHelper()
	for name, spec := range templates {
		template := &dinovav1alpha1.ApiCallTemplate{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Spec: spec}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
	}
}

// testStage names the only use of a template in the Workflows the tests create.
const testStage = "main"

// createWorkflowFor creates template, and a Workflow of the same name that runs it as its
// only template, with the given parameter values. Both go away with the current spec.
func createWorkflowFor(template *dinovav1alpha1.WorkflowTemplate, workspaceStorage string, values ...dinovav1alpha1.WorkflowParameter) *dinovav1alpha1.Workflow {
	GinkgoHelper()
	template.Namespace = "default"
	Expect(k8sClient.Create(ctx, template)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, template))).To(Succeed()) })
	workflow := &dinovav1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: template.Name, Namespace: "default"},
		Spec: dinovav1alpha1.WorkflowSpec{
			WorkspaceStorage: workspaceStorage,
			Templates: []dinovav1alpha1.WorkflowTemplateRun{{
				Name: testStage, TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: template.Name}, Parameters: values,
			}},
		},
	}
	Expect(k8sClient.Create(ctx, workflow)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, workflow))).To(Succeed()) })
	return workflow
}

// stage returns the status of the only template the Workflow runs.
func stage(workflow *dinovav1alpha1.Workflow) dinovav1alpha1.WorkflowTemplateStatus {
	GinkgoHelper()
	Expect(workflow.Status.Templates).To(HaveLen(1))
	return workflow.Status.Templates[0]
}

// runWorkflow creates template and a Workflow that runs it, and drives the Workflow and
// ApiCall reconcilers, like a running manager would, until it reaches a final phase. It
// returns that state.
func runWorkflow(template *dinovav1alpha1.WorkflowTemplate, origin string, executor *apicall.APIExecutor, values ...dinovav1alpha1.WorkflowParameter) *dinovav1alpha1.Workflow {
	GinkgoHelper()
	workflow := createWorkflowFor(template, "", values...)
	key := client.ObjectKeyFromObject(workflow)
	workflowReconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test"}
	apiReconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{origin}}
	current := &dinovav1alpha1.Workflow{}
	Eventually(func() string {
		_, err := workflowReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		children := &dinovav1alpha1.ApiCallList{}
		Expect(k8sClient.List(ctx, children, client.InNamespace(key.Namespace), client.MatchingLabels{"dinova.one/workflow": key.Name})).To(Succeed())
		for i := range children.Items {
			_, childErr := apiReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&children.Items[i])})
			Expect(childErr).NotTo(HaveOccurred())
		}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		return current.Status.Phase
	}, 45*time.Second, 20*time.Millisecond).Should(Or(Equal("Succeeded"), Equal("Failed")))
	return current
}

func stepStatus(workflow *dinovav1alpha1.Workflow, name string) dinovav1alpha1.WorkflowStepStatus {
	GinkgoHelper()
	for _, step := range stage(workflow).Steps {
		if step.Name == name {
			return step
		}
	}
	Fail("workflow has no status for step " + name)
	return dinovav1alpha1.WorkflowStepStatus{}
}

func apiStepOf(template string, mutate func(*dinovav1alpha1.WorkflowAPIStep)) *dinovav1alpha1.WorkflowAPIStep {
	step := &dinovav1alpha1.WorkflowAPIStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: template}}
	if mutate != nil {
		mutate(step)
	}
	return step
}

var _ = Describe("Workflow step outputs and inputs", func() {
	var (
		mu       sync.Mutex
		received []map[string]any
		server   *httptest.Server
	)
	BeforeEach(func() {
		received = nil
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			raw, _ := io.ReadAll(req.Body)
			var body map[string]any
			_ = json.Unmarshal(raw, &body)
			mu.Lock()
			received = append(received, map[string]any{"path": req.URL.Path, "body": body})
			mu.Unlock()
			switch req.URL.Path {
			case "/login":
				_, _ = w.Write([]byte(`{"user":"svc","token":"s3cr3t-token","tenant":{"id":"t-9"}}`))
			default:
				_, _ = w.Write([]byte(`{"ok":true}`))
			}
		}))
		DeferCleanup(server.Close)
	})
	bodyReceived := func(path string) map[string]any {
		mu.Lock()
		defer mu.Unlock()
		for _, entry := range received {
			if entry["path"] == path {
				body, _ := entry["body"].(map[string]any)
				return body
			}
		}
		return nil
	}

	It("keeps sensitive outputs in a Secret, not in the status, and passes them to a later step", func() {
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"out-login": {Method: http.MethodPost, Endpoint: server.URL + "/login"},
			"out-noop":  {Method: http.MethodPost, Endpoint: server.URL + "/noop"},
			"out-use":   {Method: http.MethodPost, Endpoint: server.URL + "/use", Body: `{"static":"kept"}`},
		})
		workflow := runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "outputs-sensitive"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "login", API: apiStepOf("out-login", func(s *dinovav1alpha1.WorkflowAPIStep) {
					s.Outputs = []dinovav1alpha1.WorkflowOutput{
						{Name: "token", From: "/token", Sensitive: true},
						{Name: "tenantId", From: "/tenant/id"},
					}
				})},
				{Name: "unrelated", API: apiStepOf("out-noop", nil)},
				{Name: "use", API: apiStepOf("out-use", func(s *dinovav1alpha1.WorkflowAPIStep) {
					s.Inputs = []dinovav1alpha1.WorkflowInput{
						{Step: "login", Output: "token", To: "/auth/token"},
						{Step: "login", Output: "tenantId", To: "/tenant"},
					}
				})},
			}},
		}, server.URL, startExecutor())
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))

		By("sending both values to the step that uses them, from a step that is not adjacent")
		Expect(bodyReceived("/use")).To(Equal(map[string]any{"static": "kept", "auth": map[string]any{"token": "s3cr3t-token"}, "tenant": "t-9"}))

		By("keeping the sensitive value out of the Workflow status")
		login := stepStatus(workflow, "login")
		Expect(login.Outputs).To(Equal(map[string]string{"tenantId": `"t-9"`}))
		Expect(login.SensitiveOutputs).To(ConsistOf("token"))
		statusJSON, err := json.Marshal(workflow.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(statusJSON)).NotTo(ContainSubstring("s3cr3t-token"))

		By("storing it in a Secret owned by the Workflow")
		Expect(stage(workflow).OutputsSecretName).NotTo(BeEmpty())
		secret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: stage(workflow).OutputsSecretName}, secret)).To(Succeed())
		Expect(secret.Data).To(HaveKeyWithValue("login.token", []byte(`"s3cr3t-token"`)))
		Expect(secret.OwnerReferences).To(ContainElement(HaveField("Name", "outputs-sensitive")))
	})

	It("uses defaultJSON for a missing value and fails when there is no default", func() {
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"def-first":  {Method: http.MethodPost, Endpoint: server.URL + "/first"},
			"def-second": {Method: http.MethodPost, Endpoint: server.URL + "/second"},
		})
		workflow := runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "outputs-default"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "first", API: apiStepOf("def-first", func(s *dinovav1alpha1.WorkflowAPIStep) {
					s.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "region", From: "/region", DefaultJSON: `"eu-1"`}}
				})},
				{Name: "second", API: apiStepOf("def-second", func(s *dinovav1alpha1.WorkflowAPIStep) {
					s.Inputs = []dinovav1alpha1.WorkflowInput{{Step: "first", Output: "region", To: "/region"}}
				})},
			}},
		}, server.URL, startExecutor())
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(bodyReceived("/second")).To(Equal(map[string]any{"region": "eu-1"}))

		missing := runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "outputs-missing"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "first", API: apiStepOf("def-first", func(s *dinovav1alpha1.WorkflowAPIStep) {
					s.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "region", From: "/region"}}
				})},
				{Name: "second", API: apiStepOf("def-second", nil)},
			}},
		}, server.URL, startExecutor())
		Expect(missing.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(missing)).To(Equal("OutputMissing"))
		Expect(stage(missing).CurrentStep).To(Equal(int32(0)), "the workflow must stop before the next step")
	})

	It("rejects a reference to an unknown output before running any step", func() {
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"ref-first":  {Method: http.MethodPost, Endpoint: server.URL + "/first"},
			"ref-second": {Method: http.MethodPost, Endpoint: server.URL + "/second"},
		})
		workflow := runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "outputs-bad-reference"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "first", API: apiStepOf("ref-first", nil)},
				{Name: "second", API: apiStepOf("ref-second", func(s *dinovav1alpha1.WorkflowAPIStep) {
					s.Inputs = []dinovav1alpha1.WorkflowInput{{Step: "first", Output: "typo", To: "/x"}}
				})},
			}},
		}, server.URL, startExecutor())
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("InvalidTemplate"))
		mu.Lock()
		defer mu.Unlock()
		Expect(received).To(BeEmpty(), "no request may be sent for an invalid workflow")
	})
})
