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
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/instance"
)

// instanceUnderTest drives a WorkflowInstance with a clock that only moves when the test says.
type instanceUnderTest struct {
	key        types.NamespacedName
	now        time.Time
	reconciler *WorkflowInstanceReconciler
}

func newInstanceUnderTest(name, template string, change func(*dinovav1alpha1.WorkflowInstance)) *instanceUnderTest {
	GinkgoHelper()
	object := &dinovav1alpha1.WorkflowInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{"dinova.one/client": "ACME"}},
		Spec: dinovav1alpha1.WorkflowInstanceSpec{
			TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: template},
			Parameters:  []dinovav1alpha1.WorkflowParameter{{Name: "clientName", Value: "ACME"}},
			Interval:    metav1.Duration{Duration: time.Hour},
		},
	}
	if change != nil {
		change(object)
	}
	Expect(k8sClient.Create(ctx, object)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, object))).To(Succeed()) })
	test := &instanceUnderTest{key: client.ObjectKeyFromObject(object), now: time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)}
	test.reconciler = &WorkflowInstanceReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Now: func() time.Time { return test.now }}
	return test
}

// reconcile runs the controller once and returns the instance and the requeue delay.
func (t *instanceUnderTest) reconcile() (*dinovav1alpha1.WorkflowInstance, time.Duration) {
	GinkgoHelper()
	result, err := t.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: t.key})
	Expect(err).NotTo(HaveOccurred())
	current := &dinovav1alpha1.WorkflowInstance{}
	Expect(k8sClient.Get(ctx, t.key, current)).To(Succeed())
	return current, result.RequeueAfter
}

// runs lists the runs of the instance, by name.
func (t *instanceUnderTest) runs() map[string]*dinovav1alpha1.Workflow {
	GinkgoHelper()
	list := &dinovav1alpha1.WorkflowList{}
	Expect(k8sClient.List(ctx, list, client.InNamespace("default"), client.MatchingLabels{instance.InstanceLabel: t.key.Name})).To(Succeed())
	runs := map[string]*dinovav1alpha1.Workflow{}
	for i := range list.Items {
		runs[list.Items[i].Name] = &list.Items[i]
	}
	return runs
}

// finish ends a run like the Workflow controller would.
func (t *instanceUnderTest) finish(name, phase string) {
	GinkgoHelper()
	run := &dinovav1alpha1.Workflow{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, run)).To(Succeed())
	run.Status.Phase = phase
	Expect(k8sClient.Status().Update(ctx, run)).To(Succeed())
}

// finishPlan ends a Plan run like the Workflow controller would: its step apply planned,
// with or without changes.
func (t *instanceUnderTest) finishPlan(name string, changes bool) {
	GinkgoHelper()
	run := &dinovav1alpha1.Workflow{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, run)).To(Succeed())
	run.Status.Phase = apiCallPhaseSucceeded
	run.Status.Templates = []dinovav1alpha1.WorkflowTemplateStatus{{Name: "run", Phase: apiCallPhaseSucceeded, Steps: []dinovav1alpha1.WorkflowStepStatus{{
		Name: "apply", Phase: apiCallPhaseSucceeded, Plan: &dinovav1alpha1.WorkflowStepPlan{HasChanges: changes, Add: 1, Secret: name + "-plan"},
	}}}}
	Expect(k8sClient.Status().Update(ctx, run)).To(Succeed())
}

// approve sets spec.approvePlan.
func (t *instanceUnderTest) approve(plan string) {
	GinkgoHelper()
	current := &dinovav1alpha1.WorkflowInstance{}
	Expect(k8sClient.Get(ctx, t.key, current)).To(Succeed())
	current.Spec.ApprovePlan = plan
	Expect(k8sClient.Update(ctx, current)).To(Succeed())
}

func instanceDrifted(current *dinovav1alpha1.WorkflowInstance) metav1.Condition {
	GinkgoHelper()
	condition := meta.FindStatusCondition(current.Status.Conditions, "Drifted")
	Expect(condition).NotTo(BeNil(), "no condition Drifted")
	return *condition
}

func instanceReady(current *dinovav1alpha1.WorkflowInstance) metav1.Condition {
	GinkgoHelper()
	ready := meta.FindStatusCondition(current.Status.Conditions, "Ready")
	Expect(ready).NotTo(BeNil())
	return *ready
}

// createTerraformWorkflowTemplate creates a WorkflowTemplate with one Terraform step that runs
// the TerraformTemplate of the same name, with a state for each client.
func createTerraformWorkflowTemplate(name string) {
	GinkgoHelper()
	template := &dinovav1alpha1.WorkflowTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: dinovav1alpha1.WorkflowTemplateSpec{
			Parameters: []dinovav1alpha1.WorkflowTemplateParameter{{Name: "clientName"}},
			Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "apply", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
					TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: name},
					State:       dinovav1alpha1.WorkflowTerraformState{Key: name + "/{{parameters.clientName}}"},
				},
			}},
		},
	}
	Expect(k8sClient.Create(ctx, template)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, template))).To(Succeed()) })
}

var _ = Describe("WorkflowInstance Controller", func() {
	It("runs at once, then again only when the code changes or the interval elapses", func() {
		createTerraformTemplate("instance-net", map[string]string{"main.tf": "# 1"})
		createTerraformWorkflowTemplate("instance-net")
		test := newInstanceUnderTest("acme-net", "instance-net", nil)

		By("starting the first run at once, as a Workflow owned by the instance")
		current, _ := test.reconcile()
		Expect(current.Status.Runs).To(BeEquivalentTo(1))
		Expect(current.Status.ActiveRun).To(Equal("acme-net-1"))
		Expect(instanceReady(current).Reason).To(Equal("RunInProgress"))
		Expect(current.Status.TerraformRevisions).To(ConsistOf(HaveField("Revision", BeEquivalentTo(1))))
		run := test.runs()["acme-net-1"]
		Expect(run).NotTo(BeNil())
		Expect(metav1.IsControlledBy(run, current)).To(BeTrue())
		Expect(run.Labels).To(HaveKeyWithValue("dinova.one/client", "ACME"), "the run carries the labels of the instance")
		Expect(run.Annotations).To(HaveKeyWithValue(instance.RevisionAnnotation, current.Status.Revision))
		Expect(run.Spec.Templates[0].TemplateRef.Name).To(Equal("instance-net"))
		Expect(run.Spec.Templates[0].Parameters).To(Equal(current.Spec.Parameters))

		By("never starting a second run while one is in progress, even past the interval")
		test.now = test.now.Add(2 * time.Hour)
		current, _ = test.reconcile()
		Expect(test.runs()).To(HaveLen(1))
		Expect(instanceReady(current).Reason).To(Equal("RunInProgress"))

		By("being ready once the run applied the current revision, until the interval elapses")
		test.now = test.now.Add(-90 * time.Minute) // back to 30 minutes after the start of the run
		test.finish("acme-net-1", "Succeeded")
		current, requeue := test.reconcile()
		Expect(instanceReady(current).Status).To(Equal(metav1.ConditionTrue))
		Expect(current.Status.LastAppliedRevision).To(Equal(current.Status.Revision))
		Expect(current.Status.ActiveRun).To(BeEmpty())
		Expect(current.Status.NextRunTime).NotTo(BeNil())
		Expect(requeue).To(BeNumerically(">", 0))
		Expect(test.runs()).To(HaveLen(1))

		By("not running again when only the interval is changed")
		current.Spec.Interval = metav1.Duration{Duration: 3 * time.Hour}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		test.reconcile()
		Expect(test.runs()).To(HaveLen(1))

		By("running again as soon as the code of a TerraformTemplate changes")
		replaceConfigMap("instance-net-files", map[string]string{"main.tf": "# 2"})
		reconcileTerraformTemplate("instance-net")
		current, _ = test.reconcile()
		Expect(test.runs()).To(HaveKey("acme-net-2"))
		Expect(current.Status.TerraformRevisions).To(ConsistOf(HaveField("Revision", BeEquivalentTo(2))))
		Expect(current.Status.LastAttemptedRevision).To(Equal(current.Status.Revision))
		Expect(current.Status.LastAppliedRevision).NotTo(Equal(current.Status.Revision))

		By("running again when the interval elapses, even if nothing changed")
		test.finish("acme-net-2", "Succeeded")
		test.reconcile()
		test.now = test.now.Add(3*time.Hour + time.Second)
		test.reconcile()
		Expect(test.runs()).To(HaveKey("acme-net-3"))
	})

	It("with manual approval, only plans, and applies a plan once it is approved", func() {
		createTerraformTemplate("instance-manual", map[string]string{"main.tf": "# 1"})
		createTerraformWorkflowTemplate("instance-manual")
		test := newInstanceUnderTest("acme-manual", "instance-manual", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.Approval = "Manual"
		})

		By("planning only")
		current, _ := test.reconcile()
		first := test.runs()["acme-manual-1"]
		Expect(first.Spec.TerraformMode).To(Equal("Plan"))
		Expect(first.Annotations).To(HaveKeyWithValue(instance.ModeAnnotation, "Plan"))
		Expect(instanceDrifted(current).Status).To(Equal(metav1.ConditionFalse))

		By("showing a plan with changes as pending, and waiting")
		test.finishPlan("acme-manual-1", true)
		current, _ = test.reconcile()
		Expect(current.Status.PendingPlan).To(HaveValue(And(
			HaveField("ID", "acme-manual-1"), HaveField("UID", string(first.UID)), HaveField("Step", "apply"),
			HaveField("Add", BeEquivalentTo(1)), HaveField("Secret", "acme-manual-1-plan"), HaveField("Revision", current.Status.Revision),
		)))
		Expect(instanceDrifted(current).Status).To(Equal(metav1.ConditionTrue))
		Expect(instanceDrifted(current).Message).To(ContainSubstring("spec.approvePlan: acme-manual-1"))
		Expect(instanceReady(current).Reason).To(Equal("PlanPending"))
		Expect(test.runs()).To(HaveLen(1))

		By("ignoring the approval of another plan")
		test.approve("acme-manual-0")
		current, _ = test.reconcile()
		Expect(test.runs()).To(HaveLen(1))
		Expect(instanceDrifted(current).Message).To(ContainSubstring("which is not the pending plan"))

		By("applying exactly the approved plan")
		test.approve("acme-manual-1")
		test.reconcile()
		apply := test.runs()["acme-manual-2"]
		Expect(apply).NotTo(BeNil())
		Expect(apply.Spec.TerraformMode).To(Equal("ApplyPlan"))
		Expect(apply.Spec.ApplyPlan).To(HaveValue(Equal(dinovav1alpha1.WorkflowPlanReference{Workflow: "acme-manual-1", UID: string(first.UID), Step: "apply"})))

		By("planning again at once after the apply, and never applying the plan twice")
		test.finish("acme-manual-2", "Succeeded")
		current, _ = test.reconcile()
		Expect(current.Status.PendingPlan).To(BeNil())
		Expect(test.runs()["acme-manual-3"].Spec.TerraformMode).To(Equal("Plan"))
		test.finishPlan("acme-manual-3", false)
		current, _ = test.reconcile()
		Expect(instanceReady(current).Status).To(Equal(metav1.ConditionTrue))
		Expect(instanceDrifted(current).Status).To(Equal(metav1.ConditionFalse))
		Expect(current.Status.LastAppliedRevision).To(Equal(current.Status.Revision))
		Expect(test.runs()).To(HaveLen(3), "spec.approvePlan still names the applied plan, which must not run again")

		By("dropping a pending plan when the code changes, and planning the new code at once")
		test.now = test.now.Add(2 * time.Hour)
		test.reconcile()
		test.finishPlan("acme-manual-4", true)
		current, _ = test.reconcile()
		Expect(current.Status.PendingPlan).To(HaveValue(HaveField("ID", "acme-manual-4")))
		replaceConfigMap("instance-manual-files", map[string]string{"main.tf": "# 2"})
		reconcileTerraformTemplate("instance-manual")
		current, _ = test.reconcile()
		Expect(current.Status.PendingPlan).To(BeNil())
		Expect(test.runs()["acme-manual-5"].Spec.TerraformMode).To(Equal("Plan"))
	})

	It("waits the retry interval after a failed run", func() {
		createTerraformTemplate("instance-retry", map[string]string{"main.tf": "# 1"})
		createTerraformWorkflowTemplate("instance-retry")
		test := newInstanceUnderTest("acme-retry", "instance-retry", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.RetryInterval = &metav1.Duration{Duration: 10 * time.Minute}
		})
		test.reconcile()
		test.finish("acme-retry-1", "Failed")
		current, requeue := test.reconcile()
		Expect(instanceReady(current).Reason).To(Equal("RunFailed"))
		Expect(requeue).To(Equal(10 * time.Minute))
		Expect(test.runs()).To(HaveLen(1), "a failed run is not repeated at once")
		test.now = test.now.Add(10 * time.Minute)
		test.reconcile()
		Expect(test.runs()).To(HaveKey("acme-retry-2"))
	})

	It("keeps only the newest runs of each outcome", func() {
		createTerraformTemplate("instance-history", map[string]string{"main.tf": "# 1"})
		createTerraformWorkflowTemplate("instance-history")
		test := newInstanceUnderTest("acme-history", "instance-history", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.SuccessfulRunsHistoryLimit = 1
			object.Spec.FailedRunsHistoryLimit = 1
		})
		for i, phase := range []string{"Failed", "Succeeded", "Failed", "Succeeded"} {
			test.reconcile()
			test.finish(runNameFor("acme-history", i+1), phase)
			test.now = test.now.Add(2 * time.Hour)
		}
		test.reconcile() // records the last run, prunes, and starts run 5
		Expect(test.runs()).To(SatisfyAll(
			HaveKey("acme-history-3"), HaveKey("acme-history-4"), HaveKey("acme-history-5"), HaveLen(3),
		))
	})

	It("does not run what it cannot run", func() {
		By("refusing a template with an API step that is not safe to repeat")
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"instance-api-call": {Method: http.MethodPost, Endpoint: "https://example.com/x"}})
		template := &dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "instance-api", Namespace: "default"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "call", API: &dinovav1alpha1.WorkflowAPIStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "instance-api-call"}},
			}}},
		}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		current, _ := newInstanceUnderTest("acme-api", "instance-api", nil).reconcile()
		Expect(instanceReady(current)).To(And(HaveField("Reason", "UnsupportedTemplate"),
			HaveField("Message", ContainSubstring(`step "call" of WorkflowTemplate "instance-api" sends a POST and is not marked idempotent`))))
		Expect(current.Status.Runs).To(BeZero())

		By("waiting for a WorkflowTemplate that does not exist")
		current, _ = newInstanceUnderTest("acme-missing", "instance-missing", nil).reconcile()
		Expect(instanceReady(current).Reason).To(Equal("WorkflowTemplateNotFound"))
		Expect(current.Status.Runs).To(BeZero())

		By("not running a TerraformTemplate that is not ready")
		createTerraformTemplate("instance-broken", map[string]string{"main.tf": "# 1"})
		createTerraformWorkflowTemplate("instance-broken")
		replaceConfigMap("instance-broken-files", map[string]string{"other.tf": "# 1"})
		reconcileTerraformTemplate("instance-broken")
		current, _ = newInstanceUnderTest("acme-broken", "instance-broken", nil).reconcile()
		Expect(instanceReady(current).Reason).To(Equal("ConfigMapKeyMissing"))
		Expect(current.Status.Runs).To(BeZero())

		By("starting nothing while suspended")
		createTerraformTemplate("instance-suspended", map[string]string{"main.tf": "# 1"})
		createTerraformWorkflowTemplate("instance-suspended")
		current, _ = newInstanceUnderTest("acme-suspended", "instance-suspended", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.Suspend = true
		}).reconcile()
		Expect(instanceReady(current).Reason).To(Equal("Suspended"))
		Expect(current.Status.Runs).To(BeZero())
	})

	It("runs a template whose API steps are safe to repeat, and again when a request changes", func() {
		createTerraformTemplate("instance-mixed", map[string]string{"main.tf": "# 1"})
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"instance-mixed-read": {Method: http.MethodGet, Endpoint: "https://example.com/read"},
			"instance-mixed-set":  {Method: http.MethodPut, Endpoint: "https://example.com/set"},
		})
		template := &dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "instance-mixed", Namespace: "default"},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{
				Parameters: []dinovav1alpha1.WorkflowTemplateParameter{{Name: "clientName"}},
				Steps: []dinovav1alpha1.WorkflowStep{
					{Name: "read", API: &dinovav1alpha1.WorkflowAPIStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "instance-mixed-read"}}},
					{Name: "apply", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
						TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "instance-mixed"},
						State:       dinovav1alpha1.WorkflowTerraformState{Key: "instance-mixed/{{parameters.clientName}}"},
					}},
					{Name: "set", API: &dinovav1alpha1.WorkflowAPIStep{
						TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "instance-mixed-set"}, Idempotent: true,
					}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })

		By("running a GET and a PUT marked idempotent next to a Terraform step")
		test := newInstanceUnderTest("acme-mixed", "instance-mixed", nil)
		current, _ := test.reconcile()
		Expect(test.runs()).To(HaveKey("acme-mixed-1"))
		Expect(current.Status.TerraformRevisions).To(ConsistOf(HaveField("Step", "apply")))
		test.finish("acme-mixed-1", "Succeeded")
		current, _ = test.reconcile()
		Expect(instanceReady(current).Status).To(Equal(metav1.ConditionTrue))
		revision := current.Status.Revision

		By("running again when a request it sends changes")
		request := &dinovav1alpha1.ApiCallTemplate{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "instance-mixed-set"}, request)).To(Succeed())
		request.Spec.Body = `{"size":2}`
		Expect(k8sClient.Update(ctx, request)).To(Succeed())
		current, _ = test.reconcile()
		Expect(current.Status.Revision).NotTo(Equal(revision))
		Expect(test.runs()).To(HaveKey("acme-mixed-2"))

		By("refusing approval Manual and deletionPolicy Destroy, which run Terraform steps only")
		manual, _ := newInstanceUnderTest("acme-mixed-manual", "instance-mixed", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.Approval = "Manual"
		}).reconcile()
		Expect(instanceReady(manual)).To(And(HaveField("Reason", "UnsupportedTemplate"), HaveField("Message", ContainSubstring("approval Manual"))))
		destroyTest := newInstanceUnderTest("acme-mixed-destroy", "instance-mixed", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.DeletionPolicy = modeDestroy
		})
		destroyTest.reconcile() // adds the finalizer of deletionPolicy Destroy
		destroy, _ := destroyTest.reconcile()
		Expect(instanceReady(destroy)).To(And(HaveField("Reason", "UnsupportedTemplate"), HaveField("Message", ContainSubstring("deletionPolicy Retain"))))
	})
})

func runNameFor(instanceName string, number int) string {
	return instanceName + "-" + strconv.Itoa(number)
}
