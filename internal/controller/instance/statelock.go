package instance

import (
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

const (
	conditionStateHold = "StateHold"
	// The reasons a Workflow step fails with when it would apply to a held state (see the
	// Workflow's acceptStateHolds), or when its own Job left resources unsaved.
	reasonStateLockReleased = "StateLockReleased"
	reasonUnsavedResources  = "UnsavedResources"
)

// stateHold returns the hold that stopped the newest run that applies, if it failed for that.
// Plan runs are passed over: they run while a hold waits to be accepted.
func stateHold(runs []dinovav1alpha1.Workflow) *dinovav1alpha1.WorkflowInstanceStateHold {
	for i := range runs {
		run := &runs[i]
		if !finished(run) || runMode(run) != modeApply && runMode(run) != modeApplyPlan {
			continue
		}
		ready := meta.FindStatusCondition(run.Status.Conditions, conditionReady)
		if run.Status.Phase != phaseFailed || ready == nil || ready.Reason != reasonStateLockReleased && ready.Reason != reasonUnsavedResources {
			return nil
		}
		for _, template := range run.Status.Templates {
			for _, step := range template.Steps {
				if step.Phase != phaseFailed {
					continue
				}
				hold := &dinovav1alpha1.WorkflowInstanceStateHold{Reason: ready.Reason, Run: run.Name, Step: step.Name, StateKey: step.StateKey}
				switch {
				case ready.Reason == reasonStateLockReleased && step.ReleasedStateLock != nil:
					hold.ID, hold.ReleasedStateLock = step.ReleasedStateLock.ID, step.ReleasedStateLock
				case ready.Reason == reasonUnsavedResources && step.UnsavedResources != nil:
					hold.ID, hold.UnsavedResources = step.UnsavedResources.ID, step.UnsavedResources
				default:
					continue
				}
				return hold
			}
		}
		return nil
	}
	return nil
}

// holdAccepted reports whether the instance accepts the hold that stops it.
func holdAccepted(instance *dinovav1alpha1.WorkflowInstance) bool {
	hold := instance.Status.StateHold
	return hold != nil && slices.Contains(instance.Spec.AcceptStateHolds, hold.ID)
}

// holdMessage says what holds the instance and how to go on.
func holdMessage(instance *dinovav1alpha1.WorkflowInstance) string {
	hold := instance.Status.StateHold
	if holdAccepted(instance) {
		return fmt.Sprintf("Hold %s of state %s accepted: the next run applies again", hold.ID, hold.StateKey)
	}
	what := "resources may be missing from it"
	switch {
	case hold.ReleasedStateLock != nil:
		what = fmt.Sprintf("its lock was removed at %s after its runner (%s) died, and resources it created after its last save may be missing from it",
			hold.ReleasedStateLock.ReleasedAt.UTC().Format("2006-01-02T15:04:05Z"), hold.ReleasedStateLock.Who)
	case hold.UnsavedResources != nil:
		what = fmt.Sprintf("an apply did not save in it resources it began to create: %s", strings.Join(hold.UnsavedResources.Resources, ", "))
	}
	return fmt.Sprintf("Run %s did not apply step %s: state %s is held (%s): %s. "+
		"Look for those resources in the cloud, import or delete them, then list %q in spec.acceptStateHolds",
		hold.Run, hold.Step, hold.StateKey, hold.Reason, what, hold.ID)
}

// setStateHold sets StateHold: True while a hold stops the instance.
func setStateHold(instance *dinovav1alpha1.WorkflowInstance) {
	if instance.Status.StateHold == nil {
		meta.RemoveStatusCondition(&instance.Status.Conditions, conditionStateHold)
		return
	}
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type: conditionStateHold, Status: metav1.ConditionTrue, Reason: instance.Status.StateHold.Reason,
		Message: holdMessage(instance), ObservedGeneration: instance.Generation,
	})
}

// recordHold sets the hold of the newest runs in the status, with an Event when a new hold
// stops the instance.
func (h *Handler) recordHold(instance *dinovav1alpha1.WorkflowInstance, runs []dinovav1alpha1.Workflow) {
	previous := instance.Status.StateHold
	instance.Status.StateHold = stateHold(runs)
	if hold := instance.Status.StateHold; hold != nil && (previous == nil || previous.ID != hold.ID) && h.Recorder != nil {
		h.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, hold.Reason, "HoldApply", "%s", holdMessage(instance))
	}
	setStateHold(instance)
}
