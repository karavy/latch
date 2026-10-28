package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

// fakeLockStore keeps state locks, the records of the removed ones and of the unsaved
// resources, in memory.
type fakeLockStore struct {
	mu       sync.Mutex
	locks    map[string]*terraform.StateLock
	released map[string]*terraform.ReleasedLock
	unsaved  map[string]*terraform.UnsavedResources
	removed  []string
}

func (f *fakeLockStore) ReadUnsaved(_ context.Context, key string) (*terraform.UnsavedResources, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unsaved[key], nil
}

func (f *fakeLockStore) ClearUnsaved(_ context.Context, key, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if unsaved := f.unsaved[key]; unsaved != nil && unsaved.ID == id {
		delete(f.unsaved, key)
	}
	return nil
}

// recordUnsaved writes what the runner writes when an apply did not save what it began to create.
func (f *fakeLockStore) recordUnsaved(key, id string, created time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unsaved == nil {
		f.unsaved = map[string]*terraform.UnsavedResources{}
	}
	f.unsaved[key] = &terraform.UnsavedResources{
		ID: id, Who: "runner@pod", Workflow: "default/earlier", Step: "net", Created: created,
		Resources: []terraform.UnsavedResource{{Address: "openstack_networking_router_v2.r", Error: "Error waiting for router to become available"}},
	}
}

func (f *fakeLockStore) ReadLock(_ context.Context, key string) (*terraform.StateLock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.locks[key], nil
}

func (f *fakeLockStore) RemoveLock(_ context.Context, key, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if lock := f.locks[key]; lock != nil && lock.ID == id {
		delete(f.locks, key)
		f.removed = append(f.removed, key)
	}
	return nil
}

func (f *fakeLockStore) MarkReleased(_ context.Context, key string, released terraform.ReleasedLock) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.released == nil {
		f.released = map[string]*terraform.ReleasedLock{}
	}
	f.released[key] = &released
	return nil
}

func (f *fakeLockStore) ReadReleased(_ context.Context, key string) (*terraform.ReleasedLock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.released[key], nil
}

func (f *fakeLockStore) ClearReleased(_ context.Context, key, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if released := f.released[key]; released != nil && released.ID == id {
		delete(f.released, key)
	}
	return nil
}

// held reports whether the state of the net step is locked.
func (f *fakeLockStore) held() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.locks[lockedNetState] != nil
}

// releasedRecord returns the record of the lock removed from the state of the net step.
func (f *fakeLockStore) releasedRecord() *terraform.ReleasedLock {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.released[lockedNetState]
}

const lockedNetState = "default/plan/net.tfstate"

func lockedBy(who string) *fakeLockStore {
	return &fakeLockStore{locks: map[string]*terraform.StateLock{
		lockedNetState: {ID: "lock-1", Who: who, Operation: "OperationTypeApply", Created: time.Now().Add(-10 * time.Minute)},
	}}
}

// runnerPod creates a pod of a Terraform Job in the given phase.
func runnerPod(name string, phase corev1.PodPhase) {
	GinkgoHelper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "terraform", Image: "runner:test"}}},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pod))).To(Succeed()) })
	pod.Status.Phase = phase
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// deadRunner runs a Workflow to the Job of its net step, and fails the Job: it returns the
// host name of its pod, which the lock a dead runner left names.
func deadRunner(name string) string {
	GinkgoHelper()
	old := newPlanUnderTest(name, func(*dinovav1alpha1.WorkflowSpec) {})
	old.reconciler.TerraformState.Locks = &fakeLockStore{}
	job := old.reconcileToJob("net")
	job.Status = batchv1.JobStatus{
		Failed: 1, StartTime: &metav1.Time{Time: time.Now().Add(-5 * time.Minute)},
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonPodFailurePolicy},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonPodFailurePolicy},
		},
	}
	Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	return job.Name + "-abcde"
}

// reconcileToFailedStep reconciles until a step fails, and returns the Workflow.
func (p *planUnderTest) reconcileToFailedStep(step string) *dinovav1alpha1.Workflow {
	GinkgoHelper()
	for range 10 {
		workflow := p.reconcile()
		if len(workflow.Status.Templates) > 0 && len(workflow.Status.Templates[0].Steps) > 0 && stepStatus(workflow, step).Phase == failedPhase {
			return workflow
		}
	}
	Fail("step " + step + " did not fail")
	return nil
}

var _ = Describe("Workflow orphaned Terraform state locks", func() {
	It("removes the lock a dead runner left, records it, and does not apply until it is accepted", func() {
		createPlanTemplates("-h")
		host := deadRunner("lock-dead")
		runnerPod(host, corev1.PodFailed)
		run := newPlanUnderTest("lock-orphan", func(*dinovav1alpha1.WorkflowSpec) {})
		locks := lockedBy("runner@" + host)
		run.reconciler.TerraformState.Locks = locks
		workflow := run.reconcileToFailedStep("net")
		Expect(locks.held()).To(BeFalse())
		Expect(locks.releasedRecord()).To(HaveValue(And(HaveField("ID", "lock-1"), HaveField("Workflow", "default/lock-orphan"))))
		net := stepStatus(workflow, "net")
		Expect(net.JobName).To(BeEmpty(), "nothing is applied to the state")
		Expect(net.ReleasedStateLock).To(HaveValue(And(HaveField("ID", "lock-1"), HaveField("Who", "runner@"+host))))
		ready := meta.FindStatusCondition(workflow.Status.Conditions, "Ready")
		Expect(ready.Reason).To(Equal("StateLockReleased"))
		Expect(ready.Message).To(And(ContainSubstring("lock-1"), ContainSubstring("acceptStateHolds")))

		By("still refusing a later run that does not accept the lock")
		again := newPlanUnderTest("lock-again", func(*dinovav1alpha1.WorkflowSpec) {})
		again.reconciler.TerraformState.Locks = locks
		Expect(stepStatus(again.reconcileToFailedStep("net"), "net").ReleasedStateLock).To(HaveValue(HaveField("ReleasedBy", "default/lock-orphan")))

		By("applying once a run accepts it, and forgetting the record")
		accepted := newPlanUnderTest("lock-accepted", func(spec *dinovav1alpha1.WorkflowSpec) {
			spec.AcceptStateHolds = []string{"lock-1"}
		})
		accepted.reconciler.TerraformState.Locks = locks
		accepted.reconcileToJob("net")
		Expect(locks.releasedRecord()).To(BeNil())
	})

	It("plans on a state whose lock was removed", func() {
		createPlanTemplates("-l")
		host := deadRunner("lock-dead-plan")
		plan := newPlanUnderTest("lock-plan", func(spec *dinovav1alpha1.WorkflowSpec) { spec.TerraformMode = modePlan })
		locks := lockedBy("runner@" + host)
		plan.reconciler.TerraformState.Locks = locks
		plan.reconcileToJob("net")
		Expect(locks.held()).To(BeFalse())
		Expect(locks.releasedRecord()).NotTo(BeNil(), "the next apply still waits for the lock to be accepted")
	})

	It("leaves the lock of a runner that still runs", func() {
		createPlanTemplates("-i")
		host := deadRunner("lock-dead-running")
		runnerPod(host, corev1.PodRunning)
		running := newPlanUnderTest("lock-running", func(*dinovav1alpha1.WorkflowSpec) {})
		locks := lockedBy("runner@" + host)
		running.reconciler.TerraformState.Locks = locks
		running.reconcileToJob("net")
		Expect(locks.held()).To(BeTrue(), "the pod still works on the state although its Job failed")
		Expect(locks.releasedRecord()).To(BeNil())
	})

	It("leaves a lock it cannot trace back to a finished Job of a Workflow", func() {
		createPlanTemplates("-j")
		for _, who := range []string{
			"alice@laptop",                                 // not the latch runner
			"runner@no-such-job-abcde",                     // its Job is gone
			"runner@" + strings.Repeat("x", 57) + "-abcde", // cut to 63 characters
		} {
			manual := newPlanUnderTest("lock-manual-"+fmt.Sprint(len(who)), func(*dinovav1alpha1.WorkflowSpec) {})
			locks := lockedBy(who)
			manual.reconciler.TerraformState.Locks = locks
			manual.reconcileToJob("net")
			Expect(locks.held()).To(BeTrue(), who)
		}
	})

	It("says that a Job ran out of time, and removes the lock it left", func() {
		createPlanTemplates("-k")
		run := newPlanUnderTest("lock-timeout", func(*dinovav1alpha1.WorkflowSpec) {})
		locks := &fakeLockStore{locks: map[string]*terraform.StateLock{}}
		run.reconciler.TerraformState.Locks = locks
		job := run.reconcileToJob("net")
		// The runner took the lock, then was stopped at the deadline while a request hung.
		locks.locks[lockedNetState] = &terraform.StateLock{ID: "lock-2", Who: "runner@" + job.Name + "-xyz12", Created: time.Now()}
		job.Status = batchv1.JobStatus{
			Failed: 1, StartTime: &metav1.Time{Time: time.Now().Add(-2 * time.Minute)},
			// As the Job controller leaves it until the pod is gone: FailureTarget, not yet Failed.
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonDeadlineExceeded},
			},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
		workflow := run.reconcile()
		Expect(workflow.Status.Phase).To(Equal("Failed"))
		Expect(readyMessage(workflow)).To(And(ContainSubstring("exceeded its timeout"), ContainSubstring("the state lock it left was removed")))
		Expect(locks.held()).To(BeFalse())
		Expect(locks.releasedRecord()).To(HaveValue(HaveField("ID", "lock-2")))
	})

	It("fails a Job that left resources unsaved, and applies nothing more to that state until they are accepted", func() {
		createPlanTemplates("-m")
		run := newPlanUnderTest("unsaved-run", func(*dinovav1alpha1.WorkflowSpec) {})
		locks := &fakeLockStore{locks: map[string]*terraform.StateLock{}}
		run.reconciler.TerraformState.Locks = locks
		job := run.reconcileToJob("net")
		locks.recordUnsaved(lockedNetState, "u-1", time.Now())
		job.Status = batchv1.JobStatus{
			Failed: 1, StartTime: &metav1.Time{Time: time.Now().Add(-time.Minute)},
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded},
			},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
		workflow := run.reconcile()
		Expect(readyReason(workflow)).To(Equal("UnsavedResources"))
		Expect(readyMessage(workflow)).To(And(ContainSubstring("openstack_networking_router_v2.r"), ContainSubstring(`"u-1"`)))
		Expect(stepStatus(workflow, "net").UnsavedResources).To(HaveValue(HaveField("Resources", ConsistOf("openstack_networking_router_v2.r"))))

		By("holding the next apply, without a Job")
		next := newPlanUnderTest("unsaved-next", func(*dinovav1alpha1.WorkflowSpec) {})
		next.reconciler.TerraformState.Locks = locks
		net := stepStatus(next.reconcileToFailedStep("net"), "net")
		Expect(net.JobName).To(BeEmpty())
		Expect(net.UnsavedResources).To(HaveValue(HaveField("ID", "u-1")))

		By("planning anyway")
		plan := newPlanUnderTest("unsaved-plan", func(spec *dinovav1alpha1.WorkflowSpec) { spec.TerraformMode = modePlan })
		plan.reconciler.TerraformState.Locks = locks
		plan.reconcileToJob("net")

		By("applying once accepted")
		accepted := newPlanUnderTest("unsaved-accepted", func(spec *dinovav1alpha1.WorkflowSpec) { spec.AcceptStateHolds = []string{"u-1"} })
		accepted.reconciler.TerraformState.Locks = locks
		accepted.reconcileToJob("net")
		Expect(locks.unsaved).To(BeEmpty())
	})

	It("ignores a record older than the Job", func() {
		createPlanTemplates("-n")
		run := newPlanUnderTest("unsaved-old", func(*dinovav1alpha1.WorkflowSpec) {})
		locks := &fakeLockStore{locks: map[string]*terraform.StateLock{}}
		run.reconciler.TerraformState.Locks = locks
		job := run.reconcileToJob("net")
		locks.recordUnsaved(lockedNetState, "u-old", time.Now().Add(-time.Hour))
		job.Status = batchv1.JobStatus{Failed: 1, StartTime: &metav1.Time{Time: time.Now().Add(-time.Minute)}}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
		Expect(readyReason(run.reconcile())).To(Equal("TerraformJobFailed"))
	})

	It("says that the runner stopped at its time limit", func() {
		createPlanTemplates("-o")
		run := newPlanUnderTest("time-limit", func(*dinovav1alpha1.WorkflowSpec) {})
		run.reconciler.TerraformState.Locks = &fakeLockStore{}
		job := run.reconcileToJob("net")
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-abcde", Namespace: "default", Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name}},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "terraform", Image: "runner:test"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pod))).To(Succeed()) })
		pod.Status.Phase = corev1.PodFailed
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "terraform", Image: "runner:test", ImageID: "x", State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "Stopped at its time limit"},
		}}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		job.Status = batchv1.JobStatus{Failed: 1, StartTime: &metav1.Time{Time: time.Now().Add(-time.Minute)}}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
		Expect(readyMessage(run.reconcile())).To(ContainSubstring("reached its time limit"))
	})
})
