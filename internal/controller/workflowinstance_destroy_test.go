package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/instance"
)

var _ = Describe("WorkflowInstance deletion", func() {
	It("with deletionPolicy Destroy, destroys the resources before letting the instance go", func() {
		createTerraformTemplate("instance-gone", map[string]string{"main.tf": "# gone"})
		createTerraformWorkflowTemplate("instance-gone")
		test := newInstanceUnderTest("acme-gone", "instance-gone", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.DeletionPolicy = "Destroy"
		})
		current, _ := test.reconcile()
		Expect(current.Finalizers).To(ContainElement("dinova.one/instance-destroy"))
		test.reconcile()
		Expect(test.runs()).To(HaveKey("acme-gone-1"))

		By("waiting for the run in progress before destroying")
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		current, _ = test.reconcile()
		Expect(instanceReady(current).Reason).To(Equal("WaitingToDestroy"))
		Expect(test.runs()).To(HaveLen(1))

		By("running the template as a Destroy Workflow that the instance does not own")
		test.finish("acme-gone-1", apiCallPhaseSucceeded)
		current, _ = test.reconcile()
		name := "acme-gone-destroy-" + string(current.UID)[:8]
		Expect(current.Status.DestroyRun).To(Equal(name))
		Expect(instanceReady(current).Reason).To(Equal("Destroying"))
		destroy := test.runs()[name]
		Expect(destroy).NotTo(BeNil())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, destroy))).To(Succeed()) })
		Expect(destroy.Spec.TerraformMode).To(Equal("Destroy"))
		Expect(destroy.Spec.Templates[0].Parameters).To(Equal(current.Spec.Parameters))
		Expect(destroy.Labels).To(HaveKeyWithValue("dinova.one/client", "ACME"))
		Expect(metav1.IsControlledBy(destroy, current)).To(BeFalse(), "the record of the destruction outlives the instance")
		Expect(instance.DestroyRunInstance(destroy)).To(Equal("acme-gone"))

		By("keeping the instance when the destroy fails, and trying again once that run is deleted")
		test.finish(name, "Failed")
		current, _ = test.reconcile()
		Expect(instanceReady(current).Reason).To(Equal("DestroyFailed"))
		Expect(k8sClient.Delete(ctx, destroy)).To(Succeed())
		current, _ = test.reconcile()
		Expect(instanceReady(current).Reason).To(Equal("Destroying"))
		Expect(test.runs()).To(HaveKey(name))

		By("letting the instance go once the destroy succeeded")
		test.finish(name, apiCallPhaseSucceeded)
		_, err := test.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: test.key})
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, test.key, &dinovav1alpha1.WorkflowInstance{}))).To(BeTrue())
		Expect(test.runs()).To(HaveKey(name), "the Destroy Workflow stays")
	})

	It("with deletionPolicy Retain, the default, leaves the resources", func() {
		createTerraformTemplate("instance-kept", map[string]string{"main.tf": "# kept"})
		createTerraformWorkflowTemplate("instance-kept")
		test := newInstanceUnderTest("acme-kept", "instance-kept", nil)
		current, _ := test.reconcile()
		Expect(current.Finalizers).To(BeEmpty())
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, test.key, &dinovav1alpha1.WorkflowInstance{}))).To(BeTrue())
	})

	It("gives up destroying when deletionPolicy is set back to Retain", func() {
		createTerraformTemplate("instance-giveup", map[string]string{"main.tf": "# give up"})
		createTerraformWorkflowTemplate("instance-giveup")
		test := newInstanceUnderTest("acme-giveup", "instance-giveup", func(object *dinovav1alpha1.WorkflowInstance) {
			object.Spec.DeletionPolicy = "Destroy"
		})
		current, _ := test.reconcile()
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		current, _ = test.reconcile()
		DeferCleanup(func() {
			for _, run := range test.runs() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, run))).To(Succeed())
			}
		})
		test.finish(current.Status.DestroyRun, "Failed")
		current, _ = test.reconcile()
		Expect(instanceReady(current).Reason).To(Equal("DestroyFailed"))
		current.Spec.DeletionPolicy = "Retain"
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		_, err := test.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: test.key})
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, test.key, &dinovav1alpha1.WorkflowInstance{}))).To(BeTrue())
	})
})
