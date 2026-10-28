package workflow

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

const (
	// runnerLockOwner prefixes the Who of a lock written by the latch runner: its user, then the
	// host name of its pod.
	runnerLockOwner = "runner@"
	// podNameSuffix is the length of the "-xxxxx" the Job controller appends to the Job name to
	// name a pod; maxHostName is the length a host name is cut to.
	podNameSuffix = 6
	maxHostName   = 63

	reasonStateLockReleased = "StateLockReleased"
	reasonUnsavedResources  = "UnsavedResources"
	reasonStateHoldAccepted = "StateHoldAccepted"
	// runnerTimeLimitMessage begins the termination message of a runner that stopped OpenTofu
	// at its time limit (cmd/terraform-runner).
	runnerTimeLimitMessage = "Stopped at its time limit"
)

// releaseOrphanedLock removes the lock that a Terraform runner left on the step's state when it
// died without releasing it. It removes only a lock written by the latch runner whose pod
// belongs to a Job that a Workflow created and that has finished, and when no pod of that name
// runs: a lock of anyone else, of a runner still running, or of a pod it cannot trace back to
// its Job, is left in place. Before removing it, it records the removal next to the state (see
// holdReleasedLock); the step status and an Event say what was removed.
func (r *Handler) releaseOrphanedLock(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus) error {
	locks := r.TerraformState.Locks
	if locks == nil || state.StateKey == "" {
		return nil
	}
	lock, err := locks.ReadLock(ctx, state.StateKey)
	if err != nil || lock == nil {
		return err
	}
	host, ours := strings.CutPrefix(lock.Who, runnerLockOwner)
	if !ours || host == "" {
		return nil
	}
	orphaned, err := r.runnerFinished(ctx, run.owner.Namespace, host)
	if err != nil || !orphaned {
		return err
	}
	released := terraform.ReleasedLock{
		StateLock: *lock, ReleasedAt: time.Now().UTC(),
		Workflow: run.owner.Namespace + "/" + run.owner.Name, Step: state.Name,
	}
	if err := locks.MarkReleased(ctx, state.StateKey, released); err != nil {
		return err
	}
	if err := locks.RemoveLock(ctx, state.StateKey, lock.ID); err != nil {
		// The lock is still there (or another run took it): nothing was released.
		if clearErr := locks.ClearReleased(ctx, state.StateKey, lock.ID); clearErr != nil {
			logf.FromContext(ctx).Error(clearErr, "Could not delete the record of a Terraform state lock", "state", state.StateKey)
		}
		return err
	}
	state.ReleasedStateLock = releasedStatus(released)
	logf.FromContext(ctx).Info("Removed orphaned Terraform state lock", "state", state.StateKey, "lock", lock.ID, "who", lock.Who)
	r.event(run.owner, corev1.EventTypeWarning, reasonStateLockReleased, "ReleaseStateLock",
		"Removed the lock %s that %s left on state %s (step %s): applying to this state waits until the lock ID is listed in acceptStateHolds",
		lock.ID, lock.Who, state.StateKey, state.Name)
	return nil
}

// runnerFinished reports whether the runner pod with this host name has certainly stopped: its
// Job exists, a Workflow created it, it has finished, and no pod of that name still runs. The
// pod name is the Job name and a 5-character suffix; a host name cut to 63 characters cannot be
// traced back to its Job, and neither can a pod whose Job is gone, so their locks stay.
func (r *Handler) runnerFinished(ctx context.Context, namespace, host string) (bool, error) {
	if len(host) >= maxHostName || len(host) <= podNameSuffix || host[len(host)-podNameSuffix] != '-' {
		return false, nil
	}
	job := &batchv1.Job{}
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: namespace, Name: host[:len(host)-podNameSuffix]}, job)
	switch {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, err
	}
	owner := metav1.GetControllerOf(job)
	if owner == nil || owner.Kind != "Workflow" || owner.APIVersion != dinovav1alpha1.GroupVersion.String() || !jobFinished(job) {
		return false, nil
	}
	pod := &corev1.Pod{}
	err = r.reader().Get(ctx, types.NamespacedName{Namespace: namespace, Name: host}, pod)
	switch {
	case apierrors.IsNotFound(err):
		return true, nil
	case err != nil:
		return false, err
	}
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed, nil
}

// holdState returns the failure of a step that would apply to a held state: its lock was
// removed after its runner died, or an apply that failed began to create resources it did not
// save. Resources missing from the state may exist in the cloud, and applying would create them
// again. A hold the Workflow accepts (acceptStateHolds) is deleted, so the later runs apply
// without accepting it again.
func (r *Handler) holdState(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus) (*outputFailure, error) {
	locks := r.TerraformState.Locks
	if locks == nil || state.StateKey == "" {
		return nil, nil
	}
	released, err := locks.ReadReleased(ctx, state.StateKey)
	if err != nil {
		return nil, err
	}
	if released != nil {
		if !r.acceptHold(ctx, run, state, released.ID, locks.ClearReleased) {
			state.ReleasedStateLock = releasedStatus(*released)
			return &outputFailure{reasonStateLockReleased, fmt.Sprintf(
				"Not applied: the lock %s of state %s was removed at %s after its runner (%s) died, by step %s of Workflow %s. "+
					"Resources it created after its last save of the state may be missing from the state, and applying would create them again. "+
					"Look for them (a plan shows what the state lacks), import or delete them, then list %q in acceptStateHolds",
				released.ID, state.StateKey, released.ReleasedAt.Format(time.RFC3339), released.Who, released.Step, released.Workflow, released.ID)}, nil
		}
	}
	unsaved, err := locks.ReadUnsaved(ctx, state.StateKey)
	if err != nil || unsaved == nil {
		return nil, err
	}
	if r.acceptHold(ctx, run, state, unsaved.ID, locks.ClearUnsaved) {
		return nil, nil
	}
	state.UnsavedResources = unsavedStatus(*unsaved)
	return &outputFailure{reasonUnsavedResources, fmt.Sprintf(
		"Not applied: an apply on state %s (step %s of Workflow %s, %s) did not save in the state resources it began to create (%s). "+
			"They may exist in the cloud, and applying would create them again. Look for them, import or delete them, then list %q in acceptStateHolds",
		state.StateKey, unsaved.Step, unsaved.Workflow, unsaved.Created.Format(time.RFC3339), resourceList(unsaved), unsaved.ID)}, nil
}

// acceptHold deletes a hold the Workflow accepts, and reports whether it did.
func (r *Handler) acceptHold(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, id string,
	clear func(ctx context.Context, stateKey, id string) error) bool {
	if !slices.Contains(run.owner.Spec.AcceptStateHolds, id) {
		return false
	}
	if err := clear(ctx, state.StateKey, id); err != nil {
		logf.FromContext(ctx).Error(err, "Could not delete an accepted Terraform state hold", "state", state.StateKey, "hold", id)
		return false
	}
	logf.FromContext(ctx).Info("Accepted Terraform state hold", "state", state.StateKey, "hold", id)
	r.event(run.owner, corev1.EventTypeNormal, reasonStateHoldAccepted, "AcceptStateHold",
		"Accepted the hold %s of state %s: step %s applies again", id, state.StateKey, state.Name)
	return true
}

// unsavedOf returns the record of the resources that the failed Job of the step began to create
// without saving them, if it wrote one.
func (r *Handler) unsavedOf(ctx context.Context, state *dinovav1alpha1.WorkflowStepStatus, job *batchv1.Job) *terraform.UnsavedResources {
	locks := r.TerraformState.Locks
	if locks == nil || state.StateKey == "" {
		return nil
	}
	unsaved, err := locks.ReadUnsaved(ctx, state.StateKey)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Could not read the unsaved resources of a Terraform state", "state", state.StateKey)
		return nil
	}
	if unsaved == nil || unsaved.Created.Before(job.CreationTimestamp.Time) {
		return nil
	}
	return unsaved
}

// resourceList names the resources of a record, at most ten.
func resourceList(unsaved *terraform.UnsavedResources) string {
	addresses := make([]string, 0, len(unsaved.Resources))
	for _, resource := range unsaved.Resources {
		addresses = append(addresses, resource.Address)
	}
	if len(addresses) > 10 {
		return strings.Join(addresses[:10], ", ") + fmt.Sprintf(" and %d more", len(addresses)-10)
	}
	return strings.Join(addresses, ", ")
}

func unsavedStatus(unsaved terraform.UnsavedResources) *dinovav1alpha1.WorkflowUnsavedResources {
	status := &dinovav1alpha1.WorkflowUnsavedResources{
		ID: unsaved.ID, Who: unsaved.Who, DetectedAt: metav1.NewTime(unsaved.Created), RecordedBy: unsaved.Workflow,
	}
	for _, resource := range unsaved.Resources {
		if len(status.Resources) < 64 {
			status.Resources = append(status.Resources, resource.Address)
		}
	}
	return status
}

// stoppedAtTimeLimit reports whether the runner of a failed Job stopped OpenTofu at its own time
// limit, before the Job's deadline (its pod's termination message says so).
func (r *Handler) stoppedAtTimeLimit(ctx context.Context, job *batchv1.Job) bool {
	pods := &corev1.PodList{}
	if err := r.reader().List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name}); err != nil {
		logf.FromContext(ctx).Error(err, "Could not list the pods of a Terraform Job", "job", job.Name)
		return false
	}
	for _, pod := range pods.Items {
		for _, container := range pod.Status.ContainerStatuses {
			if terminated := container.State.Terminated; terminated != nil && strings.HasPrefix(terminated.Message, runnerTimeLimitMessage) {
				return true
			}
		}
	}
	return false
}

func releasedStatus(released terraform.ReleasedLock) *dinovav1alpha1.WorkflowReleasedStateLock {
	return &dinovav1alpha1.WorkflowReleasedStateLock{
		ID: released.ID, Who: released.Who, Created: metav1.NewTime(released.Created),
		ReleasedAt: metav1.NewTime(released.ReleasedAt), ReleasedBy: released.Workflow,
	}
}

// event records an Event about the Workflow, related to the instance that runs it, if any.
func (r *Handler) event(workflow *dinovav1alpha1.Workflow, eventType, reason, action, note string, args ...any) {
	if r.Recorder == nil {
		return
	}
	var related runtime.Object
	if owner := metav1.GetControllerOf(workflow); owner != nil && owner.Kind == "WorkflowInstance" {
		related = &corev1.ObjectReference{
			APIVersion: owner.APIVersion, Kind: owner.Kind, Namespace: workflow.Namespace, Name: owner.Name, UID: owner.UID,
		}
	}
	r.Recorder.Eventf(workflow, related, eventType, reason, action, note, args...)
}

// holdsApply reports whether a mode writes the state, and so must wait for a hold to be
// accepted: plans only read it, and destroying removes what the state knows.
func holdsApply(mode string) bool {
	return mode == terraform.ModeApply || mode == terraform.ModeApplyPlan
}
