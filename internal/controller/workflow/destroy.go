package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

// startDestroyStep handles a step of a Destroy workflow that has no Job running. In the first
// pass it reads the step's outputs from its state, for the steps after it, or skips the step
// when it has nothing to destroy; in the second pass it starts the Job that destroys it.
func (r *Handler) startDestroyStep(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	tf := step.Terraform
	if state.Phase == stepPhaseSkipped { // second pass: skipped when it was read
		run.status.CurrentStep++
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
	}
	if !run.reading() {
		return r.startTerraformJob(ctx, run, step, state, r.TerraformState.Backend(state.StateKey))
	}
	if !r.TerraformState.Configured() {
		return r.failWorkflow(ctx, run, state, "StateBackendNotConfigured", "The operator has no Terraform state storage configured (--tfstate-s3-* flags)")
	}
	if _, err := renderStateKey(tf.State.Key, r.outputResolver(ctx, run), run.parameterValues()); errors.Is(err, errOutputNotPublished) {
		return r.skipDestroy(ctx, run, state, "Nothing to destroy: its state key needs an output of a step that has nothing to destroy")
	}
	backend, failed := r.stepBackend(ctx, run, tf, state)
	if failed != nil {
		return r.failWorkflow(ctx, run, state, failed.reason, failed.message)
	}
	raw, err := r.TerraformState.Reader.Read(ctx, backend.Key)
	switch {
	case errors.Is(err, terraform.ErrStateNotFound):
		return r.skipDestroy(ctx, run, state, "Nothing to destroy: the step has no state")
	case err != nil:
		return ctrl.Result{}, err // transient: the reconciler retries
	}
	hasResources, err := terraform.StateHasResources(raw)
	switch {
	case err != nil:
		return r.failWorkflow(ctx, run, state, "StateInvalid", "The Terraform state could not be read")
	case !hasResources:
		return r.skipDestroy(ctx, run, state, "Nothing to destroy: its state has no resources")
	}
	missing, err := r.captureStateOutputs(ctx, run, state, tf, raw)
	if err != nil {
		return r.outputError(ctx, run, state, err)
	}
	state.Message = "Outputs read from the state; destroyed after the steps that follow it"
	if len(missing) > 0 {
		state.Message += fmt.Sprintf(" (not in the state: %s)", strings.Join(missing, ", "))
	}
	run.status.CurrentStep++
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
}

// skipDestroy ends, in the first pass of a Destroy workflow, a step that has nothing to destroy.
func (r *Handler) skipDestroy(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, message string) (ctrl.Result, error) {
	state.Phase, state.Message = stepPhaseSkipped, message
	run.status.CurrentStep++
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
}

// captureStateOutputs publishes the outputs of a step that its state has, for the steps after
// it, and returns the names of those it does not have. A state left by an interrupted destroy
// may have lost some outputs; that must not stop the destroy, so they are only left out, and a
// later step that needs one fails when it reads it.
func (r *Handler) captureStateOutputs(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, tf *dinovav1alpha1.WorkflowTerraformStep, raw []byte) ([]string, error) {
	if len(tf.Outputs) == 0 {
		return nil, nil
	}
	document, sensitive, err := terraform.StateOutputs(raw)
	if err != nil {
		return nil, &outputFailure{"StateInvalid", "The Terraform state could not be read for its outputs"}
	}
	var parsed any
	if err := json.Unmarshal(document, &parsed); err != nil {
		return nil, &outputFailure{"StateInvalid", "The Terraform state could not be read for its outputs"}
	}
	lookup := r.conditionResolver(ctx, run)
	var present []dinovav1alpha1.WorkflowOutput
	var missing []string
	for _, output := range tf.Outputs {
		if _, err := readOutput(parsed, output, lookup); errors.Is(err, errOutputNotFound) && output.DefaultJSON == "" {
			missing = append(missing, output.Name)
			continue
		}
		present = append(present, output)
	}
	return missing, r.captureOutputs(ctx, run, state, present, document, sensitiveDeclared(present, sensitive)...)
}
