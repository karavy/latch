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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

var _ = Describe("Waiting for a job to succeed", func() {
	// jobServer answers POST /deliver with a job id, and GET /jobs/7 with the next status of
	// the list (the last one repeats).
	jobServer := func(polls *atomic.Int32, statuses ...string) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodPost && req.URL.Path == "/deliver":
				_, _ = w.Write([]byte(`{"job_id":7,"status":"queued"}`))
			case req.Method == http.MethodGet && req.URL.Path == "/jobs/7":
				n := min(int(polls.Add(1))-1, len(statuses)-1)
				_, _ = w.Write([]byte(`{"id":7,"status":"` + statuses[n] + `"}`))
			default:
				http.NotFound(w, req)
			}
		}))
		DeferCleanup(server.Close)
		return server
	}
	run := func(name string, server *httptest.Server, maxAttempts int32) *dinovav1alpha1.Workflow {
		GinkgoHelper()
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			name + "-deliver": {Method: http.MethodPost, Endpoint: server.URL + "/deliver"},
			name + "-job":     {Method: http.MethodGet, Endpoint: server.URL + "/jobs/{job_id}"},
		})
		byID := []dinovav1alpha1.WorkflowInput{{Step: "deliver", Output: "job_id", ToPath: "job_id"}}
		return runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "deliver", API: apiStepOf(name+"-deliver", func(step *dinovav1alpha1.WorkflowAPIStep) {
					step.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "job_id", From: "/job_id"}}
				})},
				{Name: "wait", API: apiStepOf(name+"-job", func(step *dinovav1alpha1.WorkflowAPIStep) {
					step.Inputs = byID
					// The outputs are read from the answer that ends the wait.
					step.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "id", From: "/id"}, {Name: "status", From: "/status"}}
					// No templateRef: the poll sends the step's own request again, from inside its ApiCall.
					step.Poll = &dinovav1alpha1.WorkflowPoll{
						IntervalSeconds: 1, MaxAttempts: maxAttempts, ConditionJSONPointer: "/status", EqualsJSON: `"success"`,
					}
				})},
			}},
		}, server.URL, startExecutor())
	}

	// waitCalls are the ApiCalls the Workflow created for its waiting step.
	waitCalls := func(workflow *dinovav1alpha1.Workflow) []dinovav1alpha1.ApiCall {
		GinkgoHelper()
		list := &dinovav1alpha1.ApiCallList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace("default"), client.MatchingLabels{
			"dinova.one/workflow": workflow.Name, "dinova.one/workflow-step": "wait",
		})).To(Succeed())
		return list.Items
	}

	It("keeps asking until the status is success, and then lets the workflow go on", func() {
		var polls atomic.Int32
		workflow := run("job-succeeds", jobServer(&polls, "running", "running", "success"), 10)
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(polls.Load()).To(Equal(int32(3)), "three requests: running, running, success")
		Expect(waitCalls(workflow)).To(HaveLen(1), "the waiting happens inside one ApiCall, not in one ApiCall per request")
		Expect(waitCalls(workflow)[0].Status.Attempts).To(Equal(int32(3)))
		Expect(stepStatus(workflow, "wait").Outputs).To(Equal(map[string]string{"id": "7", "status": `"success"`}),
			"the outputs come from the last answer, not from the earlier ones that said running")
	})

	It("fails with PollTimeout when the job never succeeds in the attempts it is given", func() {
		var polls atomic.Int32
		workflow := run("job-never", jobServer(&polls, "running"), 2)
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("PollTimeout"))
		Expect(polls.Load()).To(Equal(int32(2)), "maxAttempts counts the requests, the first one included")
		Expect(waitCalls(workflow)).To(HaveLen(1))
	})

	It("refuses an ApiCall that would send anything but a GET again", func() {
		call := func(method string) *dinovav1alpha1.ApiCall {
			return &dinovav1alpha1.ApiCall{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "retry-", Namespace: "default"},
				Spec: dinovav1alpha1.ApiCallSpec{
					Method: method, Endpoint: "https://api.example.com/jobs/1",
					Retry: &dinovav1alpha1.ApiCallRetry{MaxAttempts: 3},
				},
			}
		}
		Expect(k8sClient.Create(ctx, call(http.MethodPost))).NotTo(Succeed(), "a POST sent again could do the same thing twice")
		get := call(http.MethodGet)
		Expect(k8sClient.Create(ctx, get)).To(Succeed())
		Expect(k8sClient.Delete(ctx, get)).To(Succeed())
	})
})
