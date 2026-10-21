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
	"slices"
	"strings"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// pollScenario runs a one-step Workflow (start call + poll) against server and
// returns the final Workflow, driving both reconcilers like a running manager.
func pollScenario(name string, server *httptest.Server, poll dinovav1alpha1.WorkflowPoll) *dinovav1alpha1.Workflow {
	GinkgoHelper()
	createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
		name + "-start": {Method: http.MethodPost, Endpoint: server.URL + "/start"},
		name + "-poll":  {Method: http.MethodPost, Endpoint: server.URL + "/poll", Body: `{"operation":""}`},
	})
	poll.TemplateRef = dinovav1alpha1.WorkflowResourceReference{Name: name + "-poll"}
	poll.Inputs = []dinovav1alpha1.WorkflowInput{{Step: "wait", Output: "operation", To: "/operation"}}
	poll.IntervalSeconds, poll.ConditionJSONPointer, poll.EqualsJSON = 1, "/ready", "true"
	return runWorkflow(&dinovav1alpha1.WorkflowTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
			Name: "wait", API: apiStepOf(name+"-start", func(step *dinovav1alpha1.WorkflowAPIStep) {
				step.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "operation", From: "/operationId"}}
				step.Poll = &poll
			}),
		}}},
	}, server.URL, startExecutor())
}

// pollCallPhases returns the phases of the step's poll ApiCalls in attempt order.
func pollCallPhases(workflow *dinovav1alpha1.Workflow) []string {
	GinkgoHelper()
	children := &dinovav1alpha1.ApiCallList{}
	Expect(k8sClient.List(ctx, children, client.InNamespace(workflow.Namespace), client.MatchingLabels{
		"dinova.one/workflow": workflow.Name, "dinova.one/workflow-step": "wait",
	})).To(Succeed())
	var polls []dinovav1alpha1.ApiCall
	for _, call := range children.Items {
		if strings.Contains(call.Name, "-poll-") {
			polls = append(polls, call)
		}
	}
	slices.SortFunc(polls, func(a, b dinovav1alpha1.ApiCall) int { return strings.Compare(a.Name, b.Name) })
	phases := make([]string, 0, len(polls))
	for _, call := range polls {
		phases = append(phases, call.Status.Phase)
	}
	return phases
}

func readyReason(workflow *dinovav1alpha1.Workflow) string {
	condition := meta.FindStatusCondition(workflow.Status.Conditions, "Ready")
	if condition == nil {
		return ""
	}
	return condition.Reason
}

var _ = Describe("Workflow poll error tolerance", func() {
	// pollServer answers /start with an operation id and /poll with replies[n] for the
	// nth poll (the last reply repeats). A nil reply drops the connection.
	pollServer := func(polls *atomic.Int32, replies ...func(http.ResponseWriter)) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/start":
				_, _ = w.Write([]byte(`{"operationId":"op-7"}`))
			case "/poll":
				n := int(polls.Add(1)) - 1
				n = min(n, len(replies)-1)
				if replies[n] == nil {
					conn, _, err := w.(http.Hijacker).Hijack()
					Expect(err).NotTo(HaveOccurred())
					_ = conn.Close()
					return
				}
				replies[n](w)
			}
		}))
		DeferCleanup(server.Close)
		return server
	}
	status := func(code int) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) { w.WriteHeader(code) }
	}
	body := func(json string) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) { _, _ = w.Write([]byte(json)) }
	}

	It("keeps polling through listed HTTP statuses and transport errors until the result is ready", func() {
		var polls atomic.Int32
		server := pollServer(&polls, status(http.StatusServiceUnavailable), nil, body(`{"ready":false}`), body(`{"ready":true}`))
		workflow := pollScenario("poll-tolerant", server, dinovav1alpha1.WorkflowPoll{
			MaxAttempts: 6, RetryOnStatusCodes: []int32{http.StatusServiceUnavailable}, RetryOnTransportErrors: true,
		})
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(polls.Load()).To(Equal(int32(4)))
		Expect(pollCallPhases(workflow)).To(Equal([]string{"Failed", "Failed", "Succeeded", "Succeeded"}))
	})

	It("fails the workflow on an HTTP status that is not listed", func() {
		var polls atomic.Int32
		server := pollServer(&polls, status(http.StatusInternalServerError), body(`{"ready":true}`))
		workflow := pollScenario("poll-unlisted", server, dinovav1alpha1.WorkflowPoll{
			MaxAttempts: 6, RetryOnStatusCodes: []int32{http.StatusServiceUnavailable}, RetryOnTransportErrors: true,
		})
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("ChildCallFailed"))
		Expect(polls.Load()).To(Equal(int32(1)))
	})

	It("does not tolerate transport errors unless asked to", func() {
		var polls atomic.Int32
		server := pollServer(&polls, nil, body(`{"ready":true}`))
		workflow := pollScenario("poll-strict", server, dinovav1alpha1.WorkflowPoll{MaxAttempts: 6})
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("ChildCallFailed"))
		Expect(polls.Load()).To(Equal(int32(1)))
	})

	It("counts tolerated failures against maxAttempts", func() {
		var polls atomic.Int32
		server := pollServer(&polls, status(http.StatusServiceUnavailable))
		workflow := pollScenario("poll-exhausted", server, dinovav1alpha1.WorkflowPoll{
			MaxAttempts: 2, RetryOnStatusCodes: []int32{http.StatusServiceUnavailable},
		})
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("PollTimeout"))
		Expect(polls.Load()).To(Equal(int32(2)))
	})
})
