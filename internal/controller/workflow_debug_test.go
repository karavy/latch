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
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

var _ = Describe("Workflow step in debug mode", func() {
	It("builds the request from outputs, parameters and ConfigMaps, and does not send it", func() {
		var hits atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			hits.Add(1)
			if req.URL.Path == "/subnets" {
				_, _ = w.Write([]byte(`{"subnets":[{"ip":"10.0.0.0","available":false},{"ip":"10.0.0.2","mask":"31","available":true}]}`))
			}
		}))
		DeferCleanup(server.Close)
		config := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "debug-config", Namespace: "default"},
			Data:       map[string]string{"envName": "vcloud-r1.dinova.cloud"},
		}
		Expect(k8sClient.Create(ctx, config)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, config)).To(Succeed()) })
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"debug-subnets": {Method: http.MethodGet, Endpoint: server.URL + "/subnets"},
			"debug-create": {
				Method: http.MethodPost, Endpoint: server.URL + "/segments/create", Debug: true,
				Body: `{"env_name":"","client_name":"","subnet_ip":"","itop_subnet_id":null}`,
			},
		})
		workflow := runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "debug-step"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{
				Parameters: []dinovav1alpha1.WorkflowTemplateParameter{{Name: "clientName"}},
				Steps: []dinovav1alpha1.WorkflowStep{
					{Name: "subnets", API: apiStepOf("debug-subnets", func(step *dinovav1alpha1.WorkflowAPIStep) {
						step.Outputs = []dinovav1alpha1.WorkflowOutput{{
							Name: "ip", From: "/ip",
							Select: &dinovav1alpha1.WorkflowSelect{Array: "/subnets", Where: []dinovav1alpha1.WorkflowCondition{{Field: "/available", EqualsJSON: "true"}}},
						}}
					})},
					{Name: "create", API: apiStepOf("debug-create", func(step *dinovav1alpha1.WorkflowAPIStep) {
						step.Inputs = []dinovav1alpha1.WorkflowInput{
							{ConfigMapKeyRef: &dinovav1alpha1.WorkflowConfigMapKeyReference{Name: config.Name, Key: "envName"}, To: "/env_name"},
							{Parameter: "clientName", To: "/client_name"},
							{Step: "subnets", Output: "ip", To: "/subnet_ip"},
						}
					})},
				},
			},
		}, server.URL, startExecutor(), dinovav1alpha1.WorkflowParameter{Name: "clientName", Value: "ACME"})

		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(hits.Load()).To(Equal(int32(1)), "only the GET may reach the server")
		Expect(stepStatus(workflow, "create").Message).To(ContainSubstring("Debug is enabled"))
		call := &dinovav1alpha1.ApiCall{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: stepStatus(workflow, "create").APICallName}, call)).To(Succeed())
		Expect(call.Spec.Debug).To(BeTrue())
		Expect(call.Spec.Body).To(MatchJSON(`{"env_name":"vcloud-r1.dinova.cloud","client_name":"ACME","subnet_ip":"10.0.0.2","itop_subnet_id":null}`))
	})

	It("publishes the debugJSON and template outputs of a step that was not sent, and nothing else", func() {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			Fail("a debug step must not send anything")
		}))
		DeferCleanup(server.Close)
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"debug-outputs": {Method: http.MethodPost, Endpoint: server.URL + "/segments/create", Debug: true},
		})
		workflow := runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "debug-outputs"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Parameters: []dinovav1alpha1.WorkflowTemplateParameter{{Name: "clientName"}}, Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "create", API: apiStepOf("debug-outputs", func(step *dinovav1alpha1.WorkflowAPIStep) {
					step.Outputs = []dinovav1alpha1.WorkflowOutput{
						{Name: "segName", From: "/seg_name", DebugJSON: `"P2P-ACME-SEG"`},
						{Name: "other", From: "/other"},
						{Name: "built", Template: "P2P-{{parameters.clientName}}-NET"},
					}
				}),
			}}},
		}, server.URL, startExecutor(), dinovav1alpha1.WorkflowParameter{Name: "clientName", Value: "ACME"})
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(stepStatus(workflow, "create").Outputs).To(Equal(map[string]string{"segName": `"P2P-ACME-SEG"`, "built": `"P2P-ACME-NET"`}))
	})

	It("does not send a step that has debug on the step, even when its template has none", func() {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			Fail("a step in debug mode must not send anything")
		}))
		DeferCleanup(server.Close)
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"debug-on-step": {Method: http.MethodPost, Endpoint: server.URL + "/segments/create"},
		})
		workflow := runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "debug-on-step"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "create", API: apiStepOf("debug-on-step", func(step *dinovav1alpha1.WorkflowAPIStep) { step.Debug = true }),
			}}},
		}, server.URL, startExecutor())
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(stepStatus(workflow, "create").Message).To(ContainSubstring("Debug is enabled"))
		call := &dinovav1alpha1.ApiCall{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: stepStatus(workflow, "create").APICallName}, call)).To(Succeed())
		Expect(call.Spec.Debug).To(BeTrue())
	})
})
