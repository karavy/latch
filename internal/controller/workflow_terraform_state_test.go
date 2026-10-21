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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

// fakeStateReader is an in-memory state storage: the tests write the state a Job would
// have written, and the operator reads it back.
type fakeStateReader struct {
	mu     sync.Mutex
	states map[string][]byte
	reads  []string
}

func (f *fakeStateReader) Read(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads = append(f.reads, key)
	if state, ok := f.states[key]; ok {
		return state, nil
	}
	return nil, fmt.Errorf("%w: %s", terraform.ErrStateNotFound, key)
}

func (f *fakeStateReader) put(key, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states == nil {
		f.states = map[string][]byte{}
	}
	f.states[key] = []byte(state)
}

func testStateConfig(reader terraform.StateReader) terraform.StateConfig {
	if reader == nil {
		reader = &fakeStateReader{}
	}
	return terraform.StateConfig{Endpoint: "http://minio.test:9000", Bucket: "tfstate", Region: "us-east-1", Reader: reader}
}

// succeed marks a Job as finished successfully, which the (absent) Job controller would do.
func succeed(job *batchv1.Job) {
	GinkgoHelper()
	job.Status = batchv1.JobStatus{
		Succeeded: 1, StartTime: &metav1.Time{Time: time.Now().Add(-time.Minute)}, CompletionTime: &metav1.Time{Time: time.Now()},
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		},
	}
	Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
}

var _ = Describe("Workflow Terraform state and outputs", func() {
	It("keys the state on an earlier step's output and passes Terraform outputs to the next step", func() {
		var mu sync.Mutex
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			_, _ = w.Write([]byte(`{"id":"vdc-1","name":"acme"}`))
		}))
		DeferCleanup(server.Close)
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"state-create-vdc": {Method: http.MethodPost, Endpoint: server.URL + "/vdcs"}})
		for _, name := range []string{"state-net", "state-cluster"} {
			template := &dinovav1alpha1.TerraformTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec:       dinovav1alpha1.TerraformTemplateSpec{Files: []dinovav1alpha1.TerraformFile{{Name: "main.tf", Content: `terraform {}`}}},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		}

		states := &fakeStateReader{}
		workflow := createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "terraform-state-chain", Namespace: "default"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "create-vdc", API: apiStepOf("state-create-vdc", func(s *dinovav1alpha1.WorkflowAPIStep) {
					s.Outputs = []dinovav1alpha1.WorkflowOutput{{Name: "vdcName", From: "/name"}}
				})},
				{Name: "network", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
					TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "state-net"},
					State:       dinovav1alpha1.WorkflowTerraformState{Key: "vdc/{{steps.create-vdc.outputs.vdcName}}/network"},
					Outputs: []dinovav1alpha1.WorkflowOutput{
						{Name: "networkId", From: "/network_id"},
						{Name: "region", From: "/region", DefaultJSON: `"eu-1"`},
						{Name: "adminPassword", From: "/admin_password"}, // not marked sensitive here, but Terraform marks it
					},
				}},
				{Name: "cluster", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
					TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "state-cluster"},
					State:       dinovav1alpha1.WorkflowTerraformState{Key: "vdc/{{steps.create-vdc.outputs.vdcName}}/cluster/{{steps.network.outputs.networkId}}"},
					Inputs: []dinovav1alpha1.WorkflowInput{
						{Step: "network", Output: "networkId", To: "/network_id"},
						{Step: "network", Output: "adminPassword", To: "/admin_password"},
					},
				}},
			}},
		}, "1Gi")

		name := types.NamespacedName{Name: workflow.Name, Namespace: workflow.Namespace}
		executor := startExecutor()
		workflowReconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(states)}
		apiReconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{server.URL}}
		step := func() *dinovav1alpha1.Workflow {
			_, err := workflowReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: name})
			Expect(err).NotTo(HaveOccurred())
			children := &dinovav1alpha1.ApiCallList{}
			Expect(k8sClient.List(ctx, children, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
			for i := range children.Items {
				_, err := apiReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&children.Items[i])})
				Expect(err).NotTo(HaveOccurred())
			}
			current := &dinovav1alpha1.Workflow{}
			Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
			return current
		}
		jobFor := func(stateKey string) *batchv1.Job {
			list := &batchv1.JobList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
			for i := range list.Items {
				for _, env := range list.Items[i].Spec.Template.Spec.Containers[0].Env {
					if env.Name == "TF_STATE_KEY" && env.Value == stateKey {
						return &list.Items[i]
					}
				}
			}
			return nil
		}

		By("running the API step and starting the network step with a key built from its output")
		var current *dinovav1alpha1.Workflow
		Eventually(func() int32 {
			current = step()
			return stage(current).CurrentStep
		}, 30*time.Second, 20*time.Millisecond).Should(Equal(int32(1)))
		var claim *corev1.PersistentVolumeClaim
		Eventually(func() string {
			current = step()
			return stage(current).WorkspacePVCName
		}, 10*time.Second, 20*time.Millisecond).ShouldNot(BeEmpty())
		claim = &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: name.Namespace, Name: stage(current).WorkspacePVCName}, claim)).To(Succeed())
		claim.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, claim)).To(Succeed())
		step()
		network := jobFor("default/vdc/acme/network.tfstate")
		Expect(network).NotTo(BeNil(), "the key is namespace-scoped and uses the VDC name published by the API step")

		By("publishing the network outputs read from the state once its job succeeded")
		states.put("default/vdc/acme/network.tfstate", `{"version":4,"outputs":{
			"network_id":{"value":"net-7","type":"string"},
			"admin_password":{"value":"s3cr3t","type":"string","sensitive":true}}}`)
		succeed(network)
		step()
		current = step()
		network0 := stepStatus(current, "network")
		Expect(network0.Outputs).To(Equal(map[string]string{"networkId": `"net-7"`, "region": `"eu-1"`}))
		Expect(network0.SensitiveOutputs).To(ConsistOf("adminPassword"), "an output Terraform marks sensitive goes to the Secret even if the step does not say so")
		statusJSON, err := json.Marshal(current.Status)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(statusJSON)).NotTo(ContainSubstring("s3cr3t"))

		By("starting the cluster step with its own key and the network outputs as variables")
		cluster := jobFor("default/vdc/acme/cluster/net-7.tfstate")
		Expect(cluster).NotTo(BeNil(), "the key of a Terraform step may use the outputs of an earlier Terraform step")
		variables := &corev1.SecretList{}
		Expect(k8sClient.List(ctx, variables, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
		var tfvars map[string]any
		for _, secret := range variables.Items {
			if raw, ok := secret.Data["terraform.auto.tfvars.json"]; ok {
				var decoded map[string]any
				Expect(json.Unmarshal(raw, &decoded)).To(Succeed())
				if decoded["network_id"] != nil {
					tfvars = decoded
				}
			}
		}
		Expect(tfvars).To(Equal(map[string]any{"network_id": "net-7", "admin_password": "s3cr3t"}))

		succeed(cluster)
		step()
		final := step()
		Expect(final.Status.Phase).To(Equal("Succeeded"))
		Expect(states.reads).To(ConsistOf("default/vdc/acme/network.tfstate"), "a step without declared outputs never reads its state")
	})

	It("fails clearly when the state storage is not configured or a state is missing", func() {
		template := &dinovav1alpha1.TerraformTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "state-fail-tpl", Namespace: "default"},
			Spec:       dinovav1alpha1.TerraformTemplateSpec{Files: []dinovav1alpha1.TerraformFile{{Name: "main.tf", Content: "terraform {}"}}},
		}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })

		run := func(name, stateKey string, config terraform.StateConfig, outputs []dinovav1alpha1.WorkflowOutput) (*dinovav1alpha1.Workflow, *batchv1.Job) {
			createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
					Name: "apply", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
						TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: template.Name},
						State:       dinovav1alpha1.WorkflowTerraformState{Key: stateKey}, Outputs: outputs,
					},
				}}},
			}, "1Gi")
			key := types.NamespacedName{Name: name, Namespace: "default"}
			reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: config}
			reconcileOnce := func() *dinovav1alpha1.Workflow {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
				Expect(err).NotTo(HaveOccurred())
				current := &dinovav1alpha1.Workflow{}
				Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
				return current
			}
			reconcileOnce() // initializes the status
			reconcileOnce() // starts the template
			current := reconcileOnce()
			claim := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: stage(current).WorkspacePVCName}, claim)).To(Succeed())
			claim.Status.Phase = corev1.ClaimBound
			Expect(k8sClient.Status().Update(ctx, claim)).To(Succeed())
			current = reconcileOnce()
			jobs := &batchv1.JobList{}
			Expect(k8sClient.List(ctx, jobs, client.InNamespace("default"), client.MatchingLabels{"dinova.one/workflow": name})).To(Succeed())
			if len(jobs.Items) == 1 {
				succeed(&jobs.Items[0])
				reconcileOnce()
				return reconcileOnce(), &jobs.Items[0]
			}
			return current, nil
		}

		unconfigured, job := run("state-unconfigured", "fail/unconfigured", terraform.StateConfig{}, nil)
		Expect(job).To(BeNil(), "no Job may start without a state storage: it would keep a local state that is lost")
		Expect(unconfigured.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(unconfigured)).To(Equal("StateBackendNotConfigured"))

		missing, _ := run("state-missing", "fail/missing", testStateConfig(&fakeStateReader{}), []dinovav1alpha1.WorkflowOutput{{Name: "x", From: "/x"}})
		Expect(missing.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(missing)).To(Equal("StateNotFound"))

		traversal, job := run("state-traversal", "fail/../other", testStateConfig(nil), nil)
		Expect(job).To(BeNil(), "a key that could leave its place in the storage must never reach a Job")
		Expect(traversal.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(traversal)).To(Equal("StateKeyInvalid"))
	})

	It("starts the Terraform job while the workspace claim is still Pending", func() {
		// Storage classes with WaitForFirstConsumer (Kind's default) bind a volume only when a
		// pod uses it: waiting for Bound before creating the Job would wait forever.
		template := &dinovav1alpha1.TerraformTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "pending-claim-tpl", Namespace: "default"},
			Spec:       dinovav1alpha1.TerraformTemplateSpec{Files: []dinovav1alpha1.TerraformFile{{Name: "main.tf", Content: "terraform {}"}}},
		}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		workflow := createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "terraform-pending-claim", Namespace: "default"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "apply", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
					TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: template.Name},
					State:       dinovav1alpha1.WorkflowTerraformState{Key: "pending/claim"},
				},
			}}},
		}, "1Gi")
		name := types.NamespacedName{Name: workflow.Name, Namespace: workflow.Namespace}
		reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(nil)}
		for range 4 {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: name})
			Expect(err).NotTo(HaveOccurred())
		}
		current := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
		claim := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: name.Namespace, Name: stage(current).WorkspacePVCName}, claim)).To(Succeed())
		Expect(claim.Status.Phase).NotTo(Equal(corev1.ClaimBound), "the claim stays Pending: nothing binds it in this test")
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1), "the Job must exist even though its volume is not bound yet")
		Expect(stepStatus(current, "apply").Phase).To(Equal("Running"))
	})

	It("rejects a template whose Terraform step has no state key", func() {
		workflow := &dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "no-state-", Namespace: "default"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "apply", Terraform: &dinovav1alpha1.WorkflowTerraformStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "t"}},
			}}},
		}
		Expect(k8sClient.Create(ctx, workflow)).NotTo(Succeed(), "a Terraform step without a state key would silently use a local state")
		workflow.Spec.Steps[0].Terraform.State.Key = "bad;key"
		Expect(k8sClient.Create(ctx, workflow)).NotTo(Succeed())
	})
})
