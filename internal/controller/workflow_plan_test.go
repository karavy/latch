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
	"net/http"
	"slices"
	"sync"

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

const modePlan = "Plan"

// fakePlanCleaner records the plans the operator deletes.
type fakePlanCleaner struct {
	mu       sync.Mutex
	prefixes []string
}

func (f *fakePlanCleaner) DeletePrefix(_ context.Context, prefix string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prefixes = append(f.prefixes, prefix)
	return nil
}

func (f *fakePlanCleaner) deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.prefixes)
}

// planUnderTest drives a Workflow of the two-step template plan-net-vm, whose vm step reads
// the netId output of the net step.
type planUnderTest struct {
	key        types.NamespacedName
	states     *fakeStateReader
	plans      *fakePlanCleaner
	reconciler *WorkflowReconciler
}

// createPlanTemplates creates the WorkflowTemplate plan-net-vm and its TerraformTemplates,
// whose names end with suffix: envtest has no garbage collector, so the revisions of a
// deleted TerraformTemplate stay, and a new one with the same name and files would meet them.
func createPlanTemplates(suffix string) {
	GinkgoHelper()
	createTerraformTemplate("plan-net"+suffix, map[string]string{"main.tf": "# net"})
	createTerraformTemplate("plan-vm"+suffix, map[string]string{"main.tf": "# vm"})
	template := &dinovav1alpha1.WorkflowTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "plan-net-vm", Namespace: "default"},
		Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
			{Name: "net", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
				TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "plan-net" + suffix},
				State:       dinovav1alpha1.WorkflowTerraformState{Key: "plan/net"},
				Outputs:     []dinovav1alpha1.WorkflowOutput{{Name: "netId", From: "/net_id"}},
			}},
			{Name: "vm", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
				TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "plan-vm" + suffix},
				State:       dinovav1alpha1.WorkflowTerraformState{Key: "plan/vm"},
				Inputs:      []dinovav1alpha1.WorkflowInput{{Step: "net", Output: "netId", To: "/net_id"}},
			}},
		}},
	}
	Expect(k8sClient.Create(ctx, template)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, template))).To(Succeed()) })
}

func newPlanUnderTest(name string, change func(*dinovav1alpha1.WorkflowSpec)) *planUnderTest {
	GinkgoHelper()
	workflow := &dinovav1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: dinovav1alpha1.WorkflowSpec{
			WorkspaceStorage: "1Gi",
			Templates: []dinovav1alpha1.WorkflowTemplateRun{{
				Name: testStage, TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "plan-net-vm"},
			}},
		},
	}
	change(&workflow.Spec)
	Expect(k8sClient.Create(ctx, workflow)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, workflow))).To(Succeed()) })
	states := &fakeStateReader{}
	// The net state has the output the vm step needs.
	states.put("default/plan/net.tfstate", `{"version":4,"outputs":{"net_id":{"value":"net-1","type":"string"}}}`)
	config := testStateConfig(states)
	plans := &fakePlanCleaner{}
	config.Plans = plans
	return &planUnderTest{
		key: client.ObjectKeyFromObject(workflow), states: states, plans: plans,
		reconciler: &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: config},
	}
}

func (p *planUnderTest) reconcile() *dinovav1alpha1.Workflow {
	GinkgoHelper()
	_, err := p.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: p.key})
	Expect(err).NotTo(HaveOccurred())
	workflow := &dinovav1alpha1.Workflow{}
	Expect(k8sClient.Get(ctx, p.key, workflow)).To(Succeed())
	return workflow
}

// job returns the Job of a step and its environment.
func (p *planUnderTest) job(step string) (*batchv1.Job, map[string]string) {
	GinkgoHelper()
	workflow := p.reconcile()
	name := stepStatus(workflow, step).JobName
	Expect(name).NotTo(BeEmpty(), "step %s has no Job", step)
	job := &batchv1.Job{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, job)).To(Succeed())
	env := map[string]string{}
	for _, variable := range job.Spec.Template.Spec.Containers[0].Env {
		env[variable.Name] = variable.Value
	}
	return job, env
}

// savePlan stores what the runner saves for a plan.
func (p *planUnderTest) savePlan(key, summary, text string) {
	p.states.put(key+".json", summary)
	p.states.put(key+".txt", text)
	p.states.put(key, "binary plan")
}

var _ = Describe("Workflow Terraform plan and approved plan", func() {
	It("plans step by step, and applies exactly the saved plan of a step", func() {
		createPlanTemplates("-a")
		plan := newPlanUnderTest("plan-run", func(spec *dinovav1alpha1.WorkflowSpec) { spec.TerraformMode = modePlan })
		for range 3 { // initializes the status, adds the plan finalizer, starts the template
			plan.reconcile()
		}
		current := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, plan.key, current)).To(Succeed())
		planKey := func(step string) string {
			return terraform.PlanKey("default", "plan-run", string(current.UID), testStage, step)
		}
		Expect(current.Finalizers).To(ContainElement("dinova.one/plan-cleanup"), "the saved plans are deleted with the workflow")

		By("running the net step as a plan saved in the state storage")
		job, env := plan.job("net")
		Expect(env).To(HaveKeyWithValue("TF_MODE", terraform.ModePlan))
		Expect(env).To(HaveKeyWithValue("TF_PLAN_KEY", planKey("net")))

		By("going on to the next step, with the outputs from the state, when a plan has no changes")
		plan.savePlan(planKey("net"), `{"hasChanges":false}`, "No changes.")
		succeed(job)
		workflow := plan.reconcile()
		net := stepStatus(workflow, "net")
		Expect(net.Phase).To(Equal("Succeeded"))
		Expect(net.Plan).To(HaveValue(HaveField("HasChanges", BeFalse())))
		Expect(net.Outputs).To(HaveKeyWithValue("netId", `"net-1"`))

		By("stopping at the first step with changes, with its readable plan in a Secret")
		job, env = plan.job("vm")
		Expect(env).To(HaveKeyWithValue("TF_PLAN_KEY", planKey("vm")))
		plan.savePlan(planKey("vm"), `{"hasChanges":true,"add":2,"change":1,"destroy":0}`, "+ openstack_compute_instance_v2.vm-1")
		succeed(job)
		workflow = plan.reconcile()
		vm := stepStatus(workflow, "vm")
		Expect(vm.Phase).To(Equal("Succeeded"))
		Expect(vm.Plan).To(HaveValue(And(HaveField("HasChanges", BeTrue()), HaveField("Add", BeEquivalentTo(2)), HaveField("Change", BeEquivalentTo(1)))))
		Expect(vm.Message).To(ContainSubstring("2 to add, 1 to change, 0 to destroy"))
		secret := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: vm.Plan.Secret}, secret)).To(Succeed())
		Expect(string(secret.Data["plan.txt"])).To(Equal("+ openstack_compute_instance_v2.vm-1"))
		Expect(plan.reconcile().Status.Phase).To(Equal("Succeeded"))

		By("applying that plan: the steps before it only read their outputs, no Job runs for them")
		apply := newPlanUnderTest("plan-apply", func(spec *dinovav1alpha1.WorkflowSpec) {
			spec.TerraformMode = "ApplyPlan"
			spec.ApplyPlan = &dinovav1alpha1.WorkflowPlanReference{Workflow: "plan-run", UID: string(current.UID), Step: "vm"}
		})
		for range 3 {
			apply.reconcile()
		}
		workflow = apply.reconcile()
		Expect(stepStatus(workflow, "net").JobName).To(BeEmpty())
		Expect(stepStatus(workflow, "net").Outputs).To(HaveKeyWithValue("netId", `"net-1"`))
		job, env = apply.job("vm")
		Expect(env).To(HaveKeyWithValue("TF_MODE", terraform.ModeApplyPlan))
		Expect(env).To(HaveKeyWithValue("TF_PLAN_KEY", planKey("vm")), "the plan saved by the Plan workflow")
		succeed(job)
		workflow = apply.reconcile()
		Expect(stepStatus(workflow, "vm").Message).To(Equal("Approved plan applied"))
		Expect(apply.reconcile().Status.Phase).To(Equal("Succeeded"))
		planPrefix := terraform.PlanPrefix("default", "plan-run", string(current.UID))
		Expect(apply.plans.deleted()).To(Equal([]string{planPrefix}), "an applied plan is deleted at once")
		Expect(apply.reconcile().Finalizers).NotTo(ContainElement("dinova.one/plan-cleanup"), "an ApplyPlan workflow saves no plan")

		By("deleting the plans of a Plan workflow when it is deleted")
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		_, err := plan.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: plan.key})
		Expect(err).NotTo(HaveOccurred())
		Expect(plan.plans.deleted()).To(Equal([]string{planPrefix}))
		Expect(k8sClient.Get(ctx, plan.key, &dinovav1alpha1.Workflow{})).NotTo(Succeed(), "the workflow goes once its plans are deleted")
	})

	It("skips the steps after one with changes", func() {
		createPlanTemplates("-b")
		plan := newPlanUnderTest("plan-stop", func(spec *dinovav1alpha1.WorkflowSpec) { spec.TerraformMode = modePlan })
		for range 3 {
			plan.reconcile()
		}
		current := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, plan.key, current)).To(Succeed())
		job, _ := plan.job("net")
		plan.savePlan(terraform.PlanKey("default", "plan-stop", string(current.UID), testStage, "net"), `{"hasChanges":true,"add":1}`, "+ router")
		succeed(job)
		plan.reconcile()
		workflow := plan.reconcile()
		Expect(workflow.Status.Phase).To(Equal("Succeeded"))
		Expect(stepStatus(workflow, "vm").Phase).To(Equal("Skipped"))
		Expect(stepStatus(workflow, "vm").Message).To(ContainSubstring("step net has changes"))
		Expect(stepStatus(workflow, "vm").JobName).To(BeEmpty())
	})

	It("plans Terraform steps only", func() {
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"plan-api-call": {Method: http.MethodGet, Endpoint: "https://example.com/x"}})
		template := &dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "plan-api", Namespace: "default"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "call", API: &dinovav1alpha1.WorkflowAPIStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "plan-api-call"}},
			}}},
		}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		plan := newPlanUnderTest("plan-api-run", func(spec *dinovav1alpha1.WorkflowSpec) {
			spec.TerraformMode = modePlan
			spec.Templates[0].TemplateRef.Name = "plan-api"
		})
		plan.reconcile()
		workflow := plan.reconcile()
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("InvalidTemplate"))

		By("refusing in the CRD an ApplyPlan without a plan, and a Plan of two templates")
		run := dinovav1alpha1.WorkflowTemplateRun{Name: "a", TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "plan-net-vm"}}
		for name, spec := range map[string]dinovav1alpha1.WorkflowSpec{
			"ApplyPlan without applyPlan": {TerraformMode: "ApplyPlan", Templates: []dinovav1alpha1.WorkflowTemplateRun{run}},
			"applyPlan without ApplyPlan": {Templates: []dinovav1alpha1.WorkflowTemplateRun{run}, ApplyPlan: &dinovav1alpha1.WorkflowPlanReference{Workflow: "x", UID: "y", Step: "vm"}},
			"Plan of two templates":       {TerraformMode: "Plan", Templates: []dinovav1alpha1.WorkflowTemplateRun{run, {Name: "b", TemplateRef: run.TemplateRef}}},
		} {
			workflow := &dinovav1alpha1.Workflow{ObjectMeta: metav1.ObjectMeta{GenerateName: "plan-invalid-", Namespace: "default"}, Spec: spec}
			Expect(k8sClient.Create(ctx, workflow)).NotTo(Succeed(), name)
		}
	})
})
