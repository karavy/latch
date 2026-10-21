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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/apicall"
)

// startMockAPI builds and runs the real cmd/mock-api server and returns its base URL.
func startMockAPI() string {
	GinkgoHelper()
	binary := filepath.Join(GinkgoT().TempDir(), "mock-api")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/mock-api")
	build.Dir = filepath.Join("..", "..")
	output, err := build.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(output))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	Expect(listener.Close()).To(Succeed())

	server := exec.Command(binary)
	server.Env = append(os.Environ(), "PORT="+port, "TLS_CERT_FILE=", "TLS_KEY_FILE=")
	Expect(server.Start()).To(Succeed())
	DeferCleanup(func() {
		_ = server.Process.Kill()
		_ = server.Wait()
	})
	baseURL := "http://127.0.0.1:" + port
	Eventually(func() error {
		response, err := http.Get(baseURL + "/healthz")
		if err != nil {
			return err
		}
		return response.Body.Close()
	}, 10*time.Second, 50*time.Millisecond).Should(Succeed())
	return baseURL
}

var _ = Describe("Workflow with two sequential ApiCalls", func() {
	It("creates a VDC and then a network that needs the VDC id returned by the first call", func() {
		baseURL := startMockAPI()

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

		namespace := "default"
		templates := []*dinovav1alpha1.ApiCallTemplate{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "seq-create-vdc", Namespace: namespace},
				Spec: dinovav1alpha1.ApiCallTemplateSpec{
					Method: http.MethodPost, Endpoint: baseURL + "/vdcs",
					Headers: map[string]string{"Content-Type": "application/json"}, Body: `{"name":"acme"}`,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "seq-create-network", Namespace: namespace},
				Spec: dinovav1alpha1.ApiCallTemplateSpec{
					Method: http.MethodPost, Endpoint: baseURL + "/networks",
					Headers: map[string]string{"Content-Type": "application/json"}, Body: `{"vdcId":"","name":"lan"}`,
				},
			},
		}
		for _, template := range templates {
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		}

		name := types.NamespacedName{Name: "vdc-network-sequence", Namespace: namespace}
		createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "create-vdc", API: &dinovav1alpha1.WorkflowAPIStep{
					TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "seq-create-vdc"},
					Outputs:     []dinovav1alpha1.WorkflowOutput{{Name: "vdcId", From: "/id"}},
				}},
				{Name: "create-network", API: &dinovav1alpha1.WorkflowAPIStep{
					TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "seq-create-network"},
					Inputs:      []dinovav1alpha1.WorkflowInput{{Step: "create-vdc", Output: "vdcId", To: "/vdcId"}},
				}},
			}},
		}, "")

		workflowReconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test"}
		apiReconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{baseURL}}
		request := reconcile.Request{NamespacedName: name}
		Eventually(func() string {
			_, reconcileErr := workflowReconciler.Reconcile(ctx, request)
			Expect(reconcileErr).NotTo(HaveOccurred())
			children := &dinovav1alpha1.ApiCallList{}
			Expect(k8sClient.List(ctx, children, client.InNamespace(namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
			for i := range children.Items {
				child := &children.Items[i]
				_, childErr := apiReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: child.Namespace, Name: child.Name}})
				Expect(childErr).NotTo(HaveOccurred())
			}
			current := &dinovav1alpha1.Workflow{}
			Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
			return current.Status.Phase
		}, 30*time.Second, 20*time.Millisecond).Should(Equal("Succeeded"))

		By("running the two steps in order, each as its own owned ApiCall created from its template")
		result := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, name, result)).To(Succeed())
		Expect(stage(result).CurrentStep).To(Equal(int32(2)))
		Expect(stage(result).Steps).To(HaveLen(2))
		responseOf := func(step string) map[string]string {
			call := &dinovav1alpha1.ApiCall{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: stepCallName(result, step)}, call)).To(Succeed())
			Expect(call.Status.Phase).To(Equal("Succeeded"))
			Expect(call.Status.Response.StatusCode).To(Equal(http.StatusCreated))
			var body map[string]string
			Expect(json.Unmarshal(call.Status.Response.Body, &body)).To(Succeed())
			return body
		}
		Expect(responseOf("create-vdc")).To(HaveKeyWithValue("id", "vdc-1"))
		network := responseOf("create-network")
		Expect(network).To(HaveKeyWithValue("vdcId", "vdc-1"))
		Expect(network).To(HaveKeyWithValue("id", "net-1"))

		By("seeing exactly one request per ApiCall, in order, on the mock API")
		response, err := http.Get(baseURL + "/__debug/requests")
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = response.Body.Close() }()
		var history []struct {
			Method, Path, Body string
		}
		Expect(json.NewDecoder(response.Body).Decode(&history)).To(Succeed())
		Expect(history).To(HaveLen(2))
		Expect(history[0].Path).To(Equal("/vdcs"))
		Expect(history[1].Path).To(Equal("/networks"))
		Expect(history[1].Body).To(MatchJSON(`{"vdcId":"vdc-1","name":"lan"}`))

		By("leaving the templates untouched and never executing them")
		for _, template := range templates {
			current := &dinovav1alpha1.ApiCallTemplate{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(template), current)).To(Succeed())
			Expect(current.Spec).To(Equal(template.Spec))
		}
		calls := &dinovav1alpha1.ApiCallList{}
		Expect(k8sClient.List(ctx, calls, client.InNamespace(namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
		Expect(calls.Items).To(HaveLen(2))
		for i := range calls.Items {
			Expect(calls.Items[i].Annotations).To(HaveKey("dinova.one/api-call-template"))
		}
	})
})

// stepCallName returns the name of the child ApiCall recorded for a finished step.
func stepCallName(workflow *dinovav1alpha1.Workflow, step string) string {
	GinkgoHelper()
	children := &dinovav1alpha1.ApiCallList{}
	Expect(k8sClient.List(ctx, children, client.InNamespace(workflow.Namespace), client.MatchingLabels{
		"dinova.one/workflow": workflow.Name, "dinova.one/workflow-step": step,
	})).To(Succeed())
	Expect(children.Items).To(HaveLen(1))
	return children.Items[0].Name
}
