package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

const modeDestroy = "Destroy"

// A state with one resource and the output the vm step reads, and one with only a resource.
const (
	netState = `{"version":4,"outputs":{"net_id":{"value":"net-1","type":"string"}},"resources":[{"type":"openstack_networking_network_v2","name":"net"}]}`
	vmState  = `{"version":4,"outputs":{},"resources":[{"type":"openstack_compute_instance_v2","name":"vm"}]}`
)

// reconcileToJob reconciles until the step has a Job, and returns it.
func (p *planUnderTest) reconcileToJob(step string) *batchv1.Job {
	GinkgoHelper()
	for range 10 {
		workflow := p.reconcile()
		if len(workflow.Status.Templates) == 0 || len(workflow.Status.Templates[0].Steps) == 0 {
			continue // the template has not started yet
		}
		if stepStatus(workflow, step).JobName != "" {
			job, _ := p.job(step)
			return job
		}
	}
	Fail("step " + step + " has no Job")
	return nil
}

var _ = Describe("Workflow Terraform destroy", func() {
	It("reads the outputs of the steps in order, then destroys them from the last to the first", func() {
		createPlanTemplates("-d")
		destroy := newPlanUnderTest("destroy-run", func(spec *dinovav1alpha1.WorkflowSpec) { spec.TerraformMode = modeDestroy })
		destroy.states.put("default/plan/net.tfstate", netState)
		destroy.states.put("default/plan/vm.tfstate", vmState)

		By("destroying vm first, with the variables it needs from the state of net")
		job := destroy.reconcileToJob("vm")
		workflow := destroy.reconcile()
		net := stepStatus(workflow, "net")
		Expect(net.JobName).To(BeEmpty(), "net is destroyed after vm")
		Expect(net.Phase).To(Equal("Pending"))
		Expect(net.Outputs).To(HaveKeyWithValue("netId", `"net-1"`))
		_, env := destroy.job("vm")
		Expect(env).To(HaveKeyWithValue("TF_MODE", terraform.ModeDestroy))
		Expect(env).To(HaveKeyWithValue("TF_STATE_KEY", "default/plan/vm.tfstate"))
		Expect(env).To(HaveKeyWithValue("TF_PLAN_KEY", ""), "a destroy saves no plan")
		variables := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: job.Spec.Template.Spec.Volumes[2].Secret.SecretName}, variables)).To(Succeed())
		Expect(string(variables.Data[terraform.VariablesKey])).To(MatchJSON(`{"net_id":"net-1"}`))

		By("destroying net once vm is destroyed")
		succeed(job)
		workflow = destroy.reconcile()
		Expect(stepStatus(workflow, "vm").Phase).To(Equal(apiCallPhaseSucceeded))
		Expect(stepStatus(workflow, "vm").Message).To(Equal("Terraform destroy completed"))
		job = destroy.reconcileToJob("net")
		_, env = destroy.job("net")
		Expect(env).To(HaveKeyWithValue("TF_MODE", terraform.ModeDestroy))
		succeed(job)
		destroy.reconcile()
		workflow = destroy.reconcile()
		Expect(stepStatus(workflow, "net").Phase).To(Equal(apiCallPhaseSucceeded))
		Expect(workflow.Status.Phase).To(Equal(apiCallPhaseSucceeded))
	})

	It("skips the steps that have nothing to destroy", func() {
		createPlanTemplates("-e")
		destroy := newPlanUnderTest("destroy-nothing", func(spec *dinovav1alpha1.WorkflowSpec) { spec.TerraformMode = modeDestroy })
		// net was destroyed before: its state is left with no resources. vm was never applied.
		destroy.states.put("default/plan/net.tfstate", `{"version":4,"outputs":{},"resources":[]}`)
		var workflow *dinovav1alpha1.Workflow
		for range 10 {
			if workflow = destroy.reconcile(); workflow.Status.Phase == apiCallPhaseSucceeded {
				break
			}
		}
		Expect(workflow.Status.Phase).To(Equal(apiCallPhaseSucceeded))
		Expect(stepStatus(workflow, "net").Phase).To(Equal("Skipped"))
		Expect(stepStatus(workflow, "net").Message).To(ContainSubstring("its state has no resources"))
		Expect(stepStatus(workflow, "vm").Phase).To(Equal("Skipped"))
		Expect(stepStatus(workflow, "vm").Message).To(ContainSubstring("the step has no state"))
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace("default"), client.MatchingLabels{"dinova.one/workflow": "destroy-nothing"})).To(Succeed())
		Expect(jobs.Items).To(BeEmpty())
	})

	It("destroys a step whose state lost its outputs in an interrupted destroy", func() {
		createPlanTemplates("-f")
		destroy := newPlanUnderTest("destroy-lost-outputs", func(spec *dinovav1alpha1.WorkflowSpec) { spec.TerraformMode = modeDestroy })
		// An interrupted destroy left a resource of net, and removed its outputs. vm is gone.
		destroy.states.put("default/plan/net.tfstate", `{"version":4,"outputs":{},"resources":[{"type":"openstack_networking_network_v2","name":"net"}]}`)
		job := destroy.reconcileToJob("net")
		workflow := destroy.reconcile()
		Expect(stepStatus(workflow, "net").Outputs).NotTo(HaveKey("netId"), "an output the state lost is left out, not invented")
		Expect(stepStatus(workflow, "vm").Phase).To(Equal("Skipped"))
		succeed(job)
		destroy.reconcile()
		Expect(destroy.reconcile().Status.Phase).To(Equal(apiCallPhaseSucceeded))
	})
})
