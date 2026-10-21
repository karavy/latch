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
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

var _ = Describe("Workflow delete steps", func() {
	var (
		mu       sync.Mutex
		requests []string
		failing  bool
		server   *httptest.Server
	)
	seen := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), requests...)
	}
	BeforeEach(func() {
		requests, failing = nil, false
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			mu.Lock()
			requests = append(requests, req.Method+" "+req.URL.RequestURI())
			fail := failing
			mu.Unlock()
			switch {
			case req.Method == http.MethodPost && fail:
				w.WriteHeader(http.StatusInternalServerError)
			case req.Method == http.MethodPost:
				_, _ = w.Write([]byte(`{"id":"seg-1"}`))
			case fail:
				w.WriteHeader(http.StatusInternalServerError)
			}
		}))
		DeferCleanup(server.Close)
	})

	// create makes a Workflow with one create step and one delete step that sends the
	// created id back, and drives the reconcilers while until holds.
	create := func(name string) (*dinovav1alpha1.Workflow, func(until func() bool)) {
		GinkgoHelper()
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			name + "-create": {Method: http.MethodPost, Endpoint: server.URL + "/segments"},
			name + "-delete": {Method: http.MethodDelete, Endpoint: server.URL + "/segments"},
		})
		workflow := createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{
				Steps: []dinovav1alpha1.WorkflowStep{{Name: "create", API: apiStepOf(name+"-create", func(step *dinovav1alpha1.WorkflowAPIStep) {
					step.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "id", From: "/id"}}
				})}},
				DeleteSteps: []dinovav1alpha1.WorkflowStep{{Name: "remove", API: apiStepOf(name+"-delete", func(step *dinovav1alpha1.WorkflowAPIStep) {
					step.Inputs = []dinovav1alpha1.WorkflowInput{{Step: "create", Output: "id", ToQuery: "id"}}
				})}},
			},
		}, "")
		DeferCleanup(func() {
			// A workflow that is still held by its finalizer must not outlive the test.
			current := &dinovav1alpha1.Workflow{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(workflow), current); err == nil {
				current.Finalizers = nil
				Expect(k8sClient.Update(ctx, current)).To(Succeed())
				_ = k8sClient.Delete(ctx, current)
			}
		})
		executor := startExecutor()
		key := client.ObjectKeyFromObject(workflow)
		workflowReconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test"}
		apiReconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{server.URL}}
		drive := func(until func() bool) {
			GinkgoHelper()
			Eventually(func() bool {
				_, err := workflowReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
				Expect(err).NotTo(HaveOccurred())
				children := &dinovav1alpha1.ApiCallList{}
				Expect(k8sClient.List(ctx, children, client.InNamespace(key.Namespace), client.MatchingLabels{"dinova.one/workflow": key.Name})).To(Succeed())
				for i := range children.Items {
					_, childErr := apiReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&children.Items[i])})
					Expect(childErr).NotTo(HaveOccurred())
				}
				return until()
			}, 45*time.Second, 20*time.Millisecond).Should(BeTrue())
		}
		return workflow, drive
	}
	get := func(workflow *dinovav1alpha1.Workflow) (*dinovav1alpha1.Workflow, error) {
		current := &dinovav1alpha1.Workflow{}
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(workflow), current)
		return current, err
	}
	phase := func(workflow *dinovav1alpha1.Workflow) string {
		current, err := get(workflow)
		if err != nil {
			return ""
		}
		return current.Status.Phase
	}

	It("runs the delete steps with the outputs of the create steps, then lets the workflow go", func() {
		workflow, drive := create("del-runs")
		drive(func() bool { return phase(workflow) == "Succeeded" })
		current, err := get(workflow)
		Expect(err).NotTo(HaveOccurred())
		Expect(current.Finalizers).To(ContainElement("dinova.one/workflow-deletion"))
		Expect(seen()).To(Equal([]string{"POST /segments"}), "delete steps must not run while the workflow lives")

		Expect(k8sClient.Delete(ctx, workflow)).To(Succeed())
		drive(func() bool { _, err := get(workflow); return apierrors.IsNotFound(err) })
		Expect(seen()).To(Equal([]string{"POST /segments", "DELETE /segments?id=seg-1"}))
	})

	It("skips a delete step whose input comes from a step that never completed", func() {
		workflow, drive := create("del-skips")
		mu.Lock()
		failing = true
		mu.Unlock()
		drive(func() bool { return phase(workflow) == "Failed" })

		Expect(k8sClient.Delete(ctx, workflow)).To(Succeed())
		drive(func() bool { _, err := get(workflow); return apierrors.IsNotFound(err) })
		Expect(seen()).To(Equal([]string{"POST /segments"}), "nothing was created, so nothing is deleted")
	})

	It("keeps the workflow when a delete step fails", func() {
		workflow, drive := create("del-fails")
		drive(func() bool { return phase(workflow) == "Succeeded" })
		mu.Lock()
		failing = true
		mu.Unlock()

		Expect(k8sClient.Delete(ctx, workflow)).To(Succeed())
		drive(func() bool {
			current, err := get(workflow)
			return err == nil && current.Status.Deletion != nil && current.Status.Deletion.Phase == "Failed"
		})
		current, err := get(workflow)
		Expect(err).NotTo(HaveOccurred(), "the workflow must stay while cleanup is undone")
		Expect(current.DeletionTimestamp.IsZero()).To(BeFalse())
		Expect(current.Finalizers).To(ContainElement("dinova.one/workflow-deletion"))
		Expect(stage(current).Deletion.Steps[0].Phase).To(Equal("Failed"))
	})
})
