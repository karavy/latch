package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// holdRun ends a run like the Workflow controller does when its step would apply to a state
// whose lock was removed.
func (t *instanceUnderTest) holdRun(name, lockID string) {
	GinkgoHelper()
	run := &dinovav1alpha1.Workflow{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, run)).To(Succeed())
	run.Status.Phase = failedPhase
	run.Status.Templates = []dinovav1alpha1.WorkflowTemplateStatus{{Name: "run", Phase: failedPhase, Steps: []dinovav1alpha1.WorkflowStepStatus{{
		Name: "apply", Phase: failedPhase, StateKey: "default/instance-lock/ACME.tfstate",
		ReleasedStateLock: &dinovav1alpha1.WorkflowReleasedStateLock{
			ID: lockID, Who: "runner@old-pod-abcde", Created: metav1.NewTime(t.now.Add(-time.Hour)),
			ReleasedAt: metav1.NewTime(t.now.Add(-time.Minute)), ReleasedBy: "default/acme-lock-1",
		},
	}}}}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "StateLockReleased", Message: "Not applied"})
	Expect(k8sClient.Status().Update(ctx, run)).To(Succeed())
}

var _ = Describe("WorkflowInstance removed state locks", func() {
	It("stops running while a removed lock waits, and runs at once when it is accepted", func() {
		createTerraformTemplate("instance-lock", map[string]string{"main.tf": "# 1"})
		createTerraformWorkflowTemplate("instance-lock")
		test := newInstanceUnderTest("acme-lock", "instance-lock", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.RetryInterval = &metav1.Duration{Duration: time.Minute}
		})
		test.reconcile()
		test.holdRun("acme-lock-1", "lock-9")
		current, requeue := test.reconcile()
		Expect(current.Status.StateHold).To(HaveValue(And(HaveField("Reason", "StateLockReleased"), HaveField("Run", "acme-lock-1"), HaveField("StateKey", "default/instance-lock/ACME.tfstate"))))
		Expect(current.Status.StateHold.ID).To(Equal("lock-9"))
		held := meta.FindStatusCondition(current.Status.Conditions, "StateHold")
		Expect(held).To(HaveValue(HaveField("Status", metav1.ConditionTrue)))
		Expect(held.Message).To(And(ContainSubstring("lock-9"), ContainSubstring("spec.acceptStateHolds")))
		Expect(instanceReady(current).Reason).To(Equal("StateLockReleased"))
		Expect(requeue).To(BeZero())

		By("not retrying: every apply would fail the same way")
		test.now = test.now.Add(time.Hour)
		test.reconcile()
		Expect(test.runs()).To(HaveLen(1))

		By("running at once, with the accepted lock, when the instance accepts it")
		Expect(k8sClient.Get(ctx, test.key, current)).To(Succeed())
		current.Spec.AcceptStateHolds = []string{"lock-9"}
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		test.reconcile()
		Expect(test.runs()).To(HaveKey("acme-lock-2"))
		Expect(test.runs()["acme-lock-2"].Spec.AcceptStateHolds).To(Equal([]string{"lock-9"}))

		By("forgetting the hold once a run applies")
		test.finish("acme-lock-2", "Succeeded")
		current, _ = test.reconcile()
		Expect(current.Status.StateHold).To(BeNil())
		Expect(meta.FindStatusCondition(current.Status.Conditions, "StateHold")).To(BeNil())
	})

	It("stops on resources a failed apply did not save, like on a removed lock", func() {
		createTerraformTemplate("instance-unsaved", map[string]string{"main.tf": "# 1"})
		createTerraformWorkflowTemplate("instance-unsaved")
		test := newInstanceUnderTest("acme-unsaved", "instance-unsaved", nil)
		test.reconcile()
		run := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "acme-unsaved-1"}, run)).To(Succeed())
		run.Status.Phase = failedPhase
		run.Status.Templates = []dinovav1alpha1.WorkflowTemplateStatus{{Name: "run", Phase: failedPhase, Steps: []dinovav1alpha1.WorkflowStepStatus{{
			Name: "apply", Phase: failedPhase, StateKey: "default/instance-unsaved/ACME.tfstate",
			UnsavedResources: &dinovav1alpha1.WorkflowUnsavedResources{
				ID: "u-1", Resources: []string{"openstack_networking_router_v2.r"}, Who: "runner@pod-abcde", DetectedAt: metav1.NewTime(test.now),
			},
		}}}}
		meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "UnsavedResources", Message: "unsaved"})
		Expect(k8sClient.Status().Update(ctx, run)).To(Succeed())
		current, _ := test.reconcile()
		Expect(current.Status.StateHold).To(HaveValue(And(HaveField("Reason", "UnsavedResources"), HaveField("ID", "u-1"))))
		Expect(instanceReady(current).Reason).To(Equal("UnsavedResources"))
		Expect(instanceReady(current).Message).To(And(ContainSubstring("openstack_networking_router_v2.r"), ContainSubstring(`"u-1"`)))
		test.now = test.now.Add(2 * time.Hour)
		test.reconcile()
		Expect(test.runs()).To(HaveLen(1), "no run while the hold waits")
	})
})
