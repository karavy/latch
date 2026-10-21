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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

var _ = Describe("Workflow step expectations", func() {
	run := func(name, response string) *dinovav1alpha1.Workflow {
		GinkgoHelper()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(response)) }))
		DeferCleanup(server.Close)
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{name: {Method: http.MethodGet, Endpoint: server.URL}})
		return runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "check", API: apiStepOf(name, func(step *dinovav1alpha1.WorkflowAPIStep) {
					step.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "ok", From: "/ok"}}
					step.Expect = []dinovav1alpha1.WorkflowExpectation{{Pointer: "/ok", EqualsJSON: "true", Message: "Name not available"}}
				}),
			}}},
		}, server.URL, startExecutor())
	}

	It("succeeds when the response has the expected value", func() {
		workflow := run("expect-holds", `{"ok":true}`)
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(stepStatus(workflow, "check").Outputs).To(HaveKeyWithValue("ok", "true"))
	})

	It("fails the workflow when it does not, and does not publish the output", func() {
		workflow := run("expect-fails", `{"ok":false}`)
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("ExpectationFailed"))
		Expect(stepStatus(workflow, "check").Message).To(ContainSubstring("Name not available"))
		Expect(stepStatus(workflow, "check").Outputs).To(BeEmpty())
	})
})
