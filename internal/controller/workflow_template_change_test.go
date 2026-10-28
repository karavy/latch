package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

var _ = Describe("Workflow whose template is edited during a Terraform Job", func() {
	It("waits for the Job to finish before it fails, so that no second Job works on the same state", func() {
		createPlanTemplates("-g")
		run := newPlanUnderTest("edited-during-job", func(*dinovav1alpha1.WorkflowSpec) {})
		job := run.reconcileToJob("net")

		By("editing the template while the Job of net runs")
		template := &dinovav1alpha1.WorkflowTemplate{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: "plan-net-vm"}, template)).To(Succeed())
		template.Spec.Steps[1].Terraform.TimeoutSeconds = 61
		Expect(k8sClient.Update(ctx, template)).To(Succeed())
		run.reconcile()
		workflow := run.reconcile()
		Expect(workflow.Status.Phase).To(Equal("Running"), "a run that has a Job working on a state has not ended")
		Expect(readyReason(workflow)).To(Equal("TemplateChanged"))
		Expect(readyMessage(workflow)).To(ContainSubstring("once Terraform Job " + job.Name + " has finished"))
		Expect(stepStatus(workflow, "vm").JobName).To(BeEmpty(), "the next step does not start")

		By("failing once the Job has finished")
		succeed(job)
		workflow = run.reconcile()
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(workflow)).To(Equal("TemplateChanged"))
		Expect(readyMessage(workflow)).NotTo(ContainSubstring("Terraform Job"))
		net := stepStatus(workflow, "net")
		Expect(net.Phase).To(Equal("Succeeded"), "the step ends with its Job's outcome")
		Expect(net.Message).To(ContainSubstring("outputs were not published"))
		Expect(stepStatus(workflow, "vm").JobName).To(BeEmpty())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, template))).To(Succeed()) })
	})
})
