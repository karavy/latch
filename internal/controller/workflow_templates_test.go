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
	"encoding/json"
	"io"
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
	"github.com/karavy/latch/internal/controller/apicall"
)

const failedPhase = "Failed"

var _ = Describe("Workflow of several WorkflowTemplates", func() {
	var (
		mu      sync.Mutex
		calls   []string
		bodies  map[string]map[string]any
		failing map[string]bool
		server  *httptest.Server
	)
	seen := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), calls...)
	}
	bodyOf := func(path string) map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return bodies[path]
	}
	BeforeEach(func() {
		calls, bodies, failing = nil, map[string]map[string]any{}, map[string]bool{}
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			raw, _ := io.ReadAll(req.Body)
			var body map[string]any
			_ = json.Unmarshal(raw, &body)
			mu.Lock()
			calls = append(calls, req.Method+" "+req.URL.Path)
			bodies[req.URL.Path] = body
			fail := failing[req.URL.Path]
			mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"id":"created"}`))
		}))
		DeferCleanup(server.Close)
	})

	// template makes a WorkflowTemplate with one POST step to path, whose body is filled from
	// the parameters, and optionally a delete step that sends DELETE to deletePath.
	template := func(name, path, deletePath string, parameters ...dinovav1alpha1.WorkflowTemplateParameter) *dinovav1alpha1.WorkflowTemplate {
		GinkgoHelper()
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			name + "-call": {Method: http.MethodPost, Endpoint: server.URL + path, Body: `{"client":"","suffix":""}`},
		})
		inputs := make([]dinovav1alpha1.WorkflowInput, 0, len(parameters))
		for _, parameter := range parameters {
			inputs = append(inputs, dinovav1alpha1.WorkflowInput{Parameter: parameter.Name, To: "/" + parameter.Name})
		}
		spec := dinovav1alpha1.WorkflowTemplateSpec{
			Parameters: parameters,
			Steps: []dinovav1alpha1.WorkflowStep{{Name: "create", API: apiStepOf(name+"-call", func(step *dinovav1alpha1.WorkflowAPIStep) {
				step.Inputs = inputs
			})}},
		}
		if deletePath != "" {
			createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{name + "-undo": {Method: http.MethodDelete, Endpoint: server.URL + deletePath}})
			spec.DeleteSteps = []dinovav1alpha1.WorkflowStep{{Name: "undo", API: apiStepOf(name+"-undo", nil)}}
		}
		return &dinovav1alpha1.WorkflowTemplate{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Spec: spec}
	}
	declared := func(name string) dinovav1alpha1.WorkflowTemplateParameter {
		return dinovav1alpha1.WorkflowTemplateParameter{Name: name}
	}
	withDefault := func(name, value string) dinovav1alpha1.WorkflowTemplateParameter {
		return dinovav1alpha1.WorkflowTemplateParameter{Name: name, Default: &value}
	}
	use := func(name, templateName string, values ...dinovav1alpha1.WorkflowParameter) dinovav1alpha1.WorkflowTemplateRun {
		return dinovav1alpha1.WorkflowTemplateRun{Name: name, TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: templateName}, Parameters: values}
	}
	value := func(name, v string) dinovav1alpha1.WorkflowParameter {
		return dinovav1alpha1.WorkflowParameter{Name: name, Value: v}
	}
	// start creates the templates (cleaned up with the spec) and a Workflow that runs the uses,
	// and returns a function that drives its reconcilers while until does not hold.
	start := func(name string, templates []*dinovav1alpha1.WorkflowTemplate, uses ...dinovav1alpha1.WorkflowTemplateRun) (*dinovav1alpha1.Workflow, func(until func() bool)) {
		GinkgoHelper()
		for _, t := range templates {
			Expect(k8sClient.Create(ctx, t)).To(Succeed())
			DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, t))).To(Succeed()) })
		}
		workflow := &dinovav1alpha1.Workflow{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       dinovav1alpha1.WorkflowSpec{Templates: uses},
		}
		Expect(k8sClient.Create(ctx, workflow)).To(Succeed())
		DeferCleanup(func() {
			current := &dinovav1alpha1.Workflow{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(workflow), current); err == nil {
				current.Finalizers = nil
				Expect(k8sClient.Update(ctx, current)).To(Succeed())
				_ = k8sClient.Delete(ctx, current)
			}
		})
		return workflow, driveWorkflow(workflow, server.URL, startExecutor())
	}
	phase := func(workflow *dinovav1alpha1.Workflow) string {
		current := &dinovav1alpha1.Workflow{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(workflow), current); err != nil {
			return ""
		}
		return current.Status.Phase
	}
	finished := func(workflow *dinovav1alpha1.Workflow) func() bool {
		return func() bool { p := phase(workflow); return p == apiCallPhaseSucceeded || p == failedPhase }
	}
	current := func(workflow *dinovav1alpha1.Workflow) *dinovav1alpha1.Workflow {
		GinkgoHelper()
		got := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(workflow), got)).To(Succeed())
		return got
	}

	It("runs the templates in order, giving each its own parameter values, and one template twice", func() {
		a := template("mt-a", "/a", "", declared("client"))
		b := template("mt-b", "/b", "", withDefault("client", "DEFAULT"), declared("suffix"))
		workflow, drive := start("mt-sequence", []*dinovav1alpha1.WorkflowTemplate{a, b},
			use("first", "mt-a", value("client", "ACME")),
			use("second", "mt-b", value("suffix", "-NET")),
			use("third", "mt-a", value("client", "OTHER")),
		)
		drive(finished(workflow))
		got := current(workflow)
		Expect(got.Status.Phase).To(Equal("Succeeded"))
		Expect(got.Status.CurrentTemplate).To(Equal(int32(3)))
		Expect(seen()).To(Equal([]string{"POST /a", "POST /b", "POST /a"}), "one template after the other")
		Expect(bodyOf("/b")).To(Equal(map[string]any{"client": "DEFAULT", "suffix": "-NET"}), "a default fills the value the workflow does not give")
		Expect(got.Status.Templates).To(HaveLen(3))
		for _, template := range got.Status.Templates {
			Expect(template.Phase).To(Equal("Succeeded"), template.Name)
		}
		By("keeping the objects of the two uses of the same template apart")
		list := &dinovav1alpha1.ApiCallList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace("default"), client.MatchingLabels{"dinova.one/workflow": workflow.Name})).To(Succeed())
		Expect(list.Items).To(HaveLen(3))
		names := map[string]bool{}
		for _, call := range list.Items {
			names[call.Name] = true
		}
		Expect(names).To(HaveLen(3))
	})

	It("refuses a parameter the template does not declare, or a missing one without a default, before sending anything", func() {
		a := template("mt-strict", "/strict", "", declared("client"))
		unknown, driveUnknown := start("mt-unknown-param", []*dinovav1alpha1.WorkflowTemplate{a}, use("only", "mt-strict", value("client", "x"), value("typo", "y")))
		driveUnknown(finished(unknown))
		Expect(readyReason(current(unknown))).To(Equal("InvalidTemplate"))

		missing, driveMissing := start("mt-missing-param", nil, use("only", "mt-strict"))
		driveMissing(finished(missing))
		got := current(missing)
		Expect(got.Status.Phase).To(Equal(failedPhase))
		Expect(readyReason(got)).To(Equal("InvalidTemplate"))
		Expect(seen()).To(BeEmpty(), "no request may be sent for a template that cannot run")
	})

	It("fails when the template does not exist", func() {
		workflow, drive := start("mt-no-template", nil, use("only", "does-not-exist"))
		drive(finished(workflow))
		Expect(readyReason(current(workflow))).To(Equal("WorkflowTemplateNotFound"))
	})

	It("stops at the first template that fails and does not start the next", func() {
		a := template("mt-fails", "/fails", "", declared("client"))
		b := template("mt-never", "/never", "", declared("client"))
		mu.Lock()
		failing["/fails"] = true
		mu.Unlock()
		workflow, drive := start("mt-stops", []*dinovav1alpha1.WorkflowTemplate{a, b},
			use("first", "mt-fails", value("client", "x")), use("second", "mt-never", value("client", "x")))
		drive(finished(workflow))
		got := current(workflow)
		Expect(got.Status.Phase).To(Equal(failedPhase))
		Expect(got.Status.Templates[0].Phase).To(Equal(failedPhase))
		Expect(got.Status.Templates[1].Phase).To(Equal("Pending"))
		Expect(seen()).To(Equal([]string{"POST /fails"}))
	})

	It("fails instead of mixing two versions when the template is edited while it runs", func() {
		a := template("mt-edited", "/edited", "", declared("client"))
		workflow, _ := start("mt-edit", []*dinovav1alpha1.WorkflowTemplate{a}, use("only", "mt-edited", value("client", "x")))
		reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(nil)}
		request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(workflow)}
		for range 2 { // initializes the status, starts the template
			_, err := reconciler.Reconcile(ctx, request)
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(current(workflow).Status.Templates[0].Phase).To(Equal("Running"))

		stored := &dinovav1alpha1.WorkflowTemplate{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(a), stored)).To(Succeed())
		stored.Spec.Steps[0].Name = "renamed"
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		got := current(workflow)
		Expect(got.Status.Phase).To(Equal(failedPhase))
		Expect(readyReason(got)).To(Equal("TemplateChanged"))
		Expect(seen()).To(BeEmpty())
	})

	It("cleans the templates up in the opposite order when the workflow is deleted", func() {
		a := template("mt-del-a", "/a/create", "/a/delete", declared("client"))
		b := template("mt-del-b", "/b/create", "/b/delete", declared("client"))
		workflow, drive := start("mt-delete", []*dinovav1alpha1.WorkflowTemplate{a, b},
			use("first", "mt-del-a", value("client", "x")), use("second", "mt-del-b", value("client", "x")))
		drive(finished(workflow))
		Expect(current(workflow).Status.Phase).To(Equal("Succeeded"))
		Expect(current(workflow).Finalizers).To(ContainElement("dinova.one/workflow-deletion"))

		Expect(k8sClient.Delete(ctx, workflow)).To(Succeed())
		drive(func() bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(workflow), &dinovav1alpha1.Workflow{})
			return apierrors.IsNotFound(err)
		})
		Expect(seen()).To(Equal([]string{"POST /a/create", "POST /b/create", "DELETE /b/delete", "DELETE /a/delete"}))
	})

	It("cleans up the templates that started, even the one that failed, and not the ones that did not", func() {
		a := template("mt-part-a", "/a/create", "/a/delete", declared("client"))
		b := template("mt-part-b", "/b/create", "/b/delete", declared("client"))
		c := template("mt-part-c", "/c/create", "/c/delete", declared("client"))
		mu.Lock()
		failing["/b/create"] = true
		mu.Unlock()
		workflow, drive := start("mt-partial", []*dinovav1alpha1.WorkflowTemplate{a, b, c},
			use("first", "mt-part-a", value("client", "x")), use("second", "mt-part-b", value("client", "x")), use("third", "mt-part-c", value("client", "x")))
		drive(finished(workflow))
		Expect(current(workflow).Status.Phase).To(Equal(failedPhase))

		Expect(k8sClient.Delete(ctx, workflow)).To(Succeed())
		drive(func() bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(workflow), &dinovav1alpha1.Workflow{})
			return apierrors.IsNotFound(err)
		})
		Expect(seen()).To(Equal([]string{"POST /a/create", "POST /b/create", "DELETE /b/delete", "DELETE /a/delete"}),
			"the failed template may have left things behind, so its delete steps run; the third never started")
	})
})

// driveWorkflow returns a function that reconciles workflow and the ApiCalls it owns, like a
// running manager would, until the condition it is given holds.
func driveWorkflow(workflow *dinovav1alpha1.Workflow, origin string, executor *apicall.APIExecutor) func(until func() bool) {
	key := client.ObjectKeyFromObject(workflow)
	workflowReconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(nil)}
	apiReconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{origin}}
	return func(until func() bool) {
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
}
