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

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

var _ = Describe("Workflow step retry", func() {
	// retryServer answers every request with replies[n] for the nth one (the last reply
	// repeats). A nil reply drops the connection.
	retryServer := func(hits *atomic.Int32, replies ...func(http.ResponseWriter)) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			n := min(int(hits.Add(1))-1, len(replies)-1)
			if replies[n] == nil {
				conn, _, err := w.(http.Hijacker).Hijack()
				Expect(err).NotTo(HaveOccurred())
				_ = conn.Close()
				return
			}
			replies[n](w)
		}))
		DeferCleanup(server.Close)
		return server
	}
	status := func(code int) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) { w.WriteHeader(code) }
	}
	ok := func(w http.ResponseWriter) { _, _ = w.Write([]byte(`{"id":"b-1"}`)) }

	scenario := func(name, method string, server *httptest.Server, retry dinovav1alpha1.WorkflowRetry) *dinovav1alpha1.Workflow {
		GinkgoHelper()
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{name: {Method: method, Endpoint: server.URL + "/blocks"}})
		retry.BackoffSeconds = 1
		return runWorkflow(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "fetch", API: apiStepOf(name, func(step *dinovav1alpha1.WorkflowAPIStep) {
					step.Retry = &retry
					step.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "id", From: "/id"}}
				}),
			}}},
		}, server.URL, startExecutor())
	}

	It("sends a failed GET again until it succeeds", func() {
		var hits atomic.Int32
		server := retryServer(&hits, status(http.StatusServiceUnavailable), nil, ok)
		workflow := scenario("retry-recovers", http.MethodGet, server, dinovav1alpha1.WorkflowRetry{
			MaxAttempts: 4, StatusCodes: []int32{http.StatusServiceUnavailable},
		})
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(hits.Load()).To(Equal(int32(3)))
		Expect(stepStatus(workflow, "fetch").Outputs).To(HaveKeyWithValue("id", `"b-1"`))
	})

	It("fails once maxAttempts requests have been made", func() {
		var hits atomic.Int32
		server := retryServer(&hits, status(http.StatusServiceUnavailable))
		workflow := scenario("retry-exhausted", http.MethodGet, server, dinovav1alpha1.WorkflowRetry{
			MaxAttempts: 2, StatusCodes: []int32{http.StatusServiceUnavailable},
		})
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("RetriesExhausted"))
		Expect(hits.Load()).To(Equal(int32(2)))
	})

	It("does not retry a status that is not listed", func() {
		var hits atomic.Int32
		server := retryServer(&hits, status(http.StatusUnauthorized), ok)
		workflow := scenario("retry-unlisted", http.MethodGet, server, dinovav1alpha1.WorkflowRetry{
			MaxAttempts: 3, StatusCodes: []int32{http.StatusServiceUnavailable},
		})
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("ChildCallFailed"))
		Expect(hits.Load()).To(Equal(int32(1)))
	})

	It("never retries transport errors when switched off", func() {
		var hits atomic.Int32
		server := retryServer(&hits, nil, ok)
		off := false
		workflow := scenario("retry-no-transport", http.MethodGet, server, dinovav1alpha1.WorkflowRetry{
			MaxAttempts: 3, OnTransportErrors: &off,
		})
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(hits.Load()).To(Equal(int32(1)))
	})

	It("refuses to retry a request that is not a GET, before sending anything", func() {
		var hits atomic.Int32
		server := retryServer(&hits, ok)
		workflow := scenario("retry-post", http.MethodPost, server, dinovav1alpha1.WorkflowRetry{MaxAttempts: 3})
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("RetryNotAllowed"))
		Expect(hits.Load()).To(BeZero())
	})
})
