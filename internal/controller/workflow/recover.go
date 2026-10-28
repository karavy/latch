package workflow

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

const (
	reasonRecoveryBlocked   = "RecoveryBlocked"
	reasonRecoveryExhausted = "RecoveryExhausted"
)

// recoverOrFail handles a poll response that has one of the poll's failWhenJSON values: it
// sends the poll's recover request when the value is worth one and the limits allow it, and
// fails the workflow with PollFailed, RecoveryBlocked or RecoveryExhausted otherwise.
func (r *Handler) recoverOrFail(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus, response []byte, value string) (ctrl.Result, error) {
	poll := step.API.Poll
	recover := poll.Recover
	if recover == nil || !recoverable(recover, value) {
		return r.failWorkflow(ctx, run, state, "PollFailed", fmt.Sprintf("API step %q: the polling response has %s at %s", step.Name, value, poll.ConditionJSONPointer))
	}
	progress, reason, message := decideRecovery(recover, state.Recoveries, response)
	if reason != "" {
		return r.failWorkflow(ctx, run, state, reason, fmt.Sprintf("API step %q: the polling response has %s at %s; %s", step.Name, value, poll.ConditionJSONPointer, message))
	}
	number := int32(len(state.Recoveries)) + 1
	call, err := r.buildAPICall(ctx, run, step.Name, fmt.Sprintf("recover-%02d", number), recover.TemplateRef.Name, recover.Inputs, false, nil, response)
	if errors.Is(err, errTemplateUnavailable) {
		return r.failWorkflow(ctx, run, state, "ApiCallTemplateUnavailable", fmt.Sprintf("Referenced ApiCallTemplate %q was not found", recover.TemplateRef.Name))
	}
	if err != nil {
		return r.failBuild(ctx, run, state, err, "RecoveryBuildFailed", "Could not build the recover request from the failed poll response, the step outputs, parameters and ConfigMaps")
	}
	if call.Spec.Debug {
		return r.failWorkflow(ctx, run, state, "RecoveryNotSent", fmt.Sprintf("API step %q: ApiCallTemplate %q has debug enabled, so the recover request would not be sent and the operation would not resume", step.Name, recover.TemplateRef.Name))
	}
	if err := r.createOwned(ctx, run.owner, call); err != nil {
		return ctrl.Result{}, err
	}
	state.Recoveries = append(state.Recoveries, dinovav1alpha1.WorkflowStepRecovery{
		Number: number, FailedJSON: value, ProgressJSON: progress, APICallName: call.Name, StartedAt: metav1.Now(),
	})
	state.Phase = stepPhaseRecovering
	state.APICallName = call.Name
	state.Message = fmt.Sprintf("Recovery %d submitted: the polling response had %s at %s", number, value, poll.ConditionJSONPointer)
	logf.FromContext(ctx).Info("Submitted recover request", "step", step.Name, "recovery", number, "progress", progress, "apiCall", call.Name)
	return ctrl.Result{RequeueAfter: workflowPollInterval}, r.Status().Update(ctx, run.owner)
}

// recoverable reports whether a failWhenJSON value is one of the recover's whenJSON values.
func recoverable(recover *dinovav1alpha1.WorkflowPollRecover, value string) bool {
	var actual any
	if json.Unmarshal([]byte(value), &actual) != nil {
		return false
	}
	for _, candidate := range recover.WhenJSON {
		var wanted any
		if json.Unmarshal([]byte(candidate), &wanted) == nil && jsonEqual(actual, wanted) {
			return true
		}
	}
	return false
}

// decideRecovery decides whether a failed operation may be recovered once more, after the
// recoveries done so far. It returns the progress the failed response shows, as JSON, and a
// reason and message when the recovery is refused. A recovery counts as "without progress"
// when the operation fails again at the same point or further back than an earlier one.
func decideRecovery(recover *dinovav1alpha1.WorkflowPollRecover, done []dinovav1alpha1.WorkflowStepRecovery, response []byte) (progress, reason, message string) {
	var document any
	if err := json.Unmarshal(response, &document); err != nil {
		return "", reasonRecoveryBlocked, "the failed polling response is not valid JSON"
	}
	for _, condition := range recover.Unless {
		var expected any
		actual, err := getPointer(document, condition.Pointer)
		if err == nil && json.Unmarshal([]byte(condition.EqualsJSON), &expected) == nil && jsonEqual(actual, expected) {
			return "", reasonRecoveryBlocked, cmp.Or(condition.Message, fmt.Sprintf("it also has %s at %s, which rules a recovery out", condition.EqualsJSON, condition.Pointer))
		}
	}
	if maxRecoveries := cmp.Or(recover.MaxRecoveries, 10); int32(len(done)) >= maxRecoveries {
		return "", reasonRecoveryExhausted, fmt.Sprintf("the operation was already recovered %d times, the most recover.maxRecoveries allows", len(done))
	}
	stalled := len(done)
	if recover.ProgressPointer != "" {
		actual, err := getPointer(document, recover.ProgressPointer)
		current, ok := actual.(float64)
		if err != nil || !ok {
			return "", reasonRecoveryBlocked, fmt.Sprintf("it has no number at %s to tell how far the operation went", recover.ProgressPointer)
		}
		encoded, _ := json.Marshal(current)
		progress = string(encoded)
		stalled = 0
		for i := len(done) - 1; i >= 0; i-- {
			var previous float64
			if json.Unmarshal([]byte(done[i].ProgressJSON), &previous) != nil || previous < current {
				break
			}
			stalled++
		}
	}
	if maxStalled := cmp.Or(recover.MaxRetriesWithoutProgress, 3); int32(stalled) >= maxStalled {
		where := ""
		if progress != "" {
			where = fmt.Sprintf(" (%s at %s)", progress, recover.ProgressPointer)
		}
		return progress, reasonRecoveryExhausted, fmt.Sprintf("the operation failed %d times in a row at the same point%s", stalled+1, where)
	}
	return progress, "", ""
}

// pollAgain starts a new round of polls once the recover request succeeded. It waits one poll
// interval after the recovery first, so that the polled operation has time to leave the state
// that started it.
func (r *Handler) pollAgain(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	apiStep := step.API
	recovery := state.Recoveries[len(state.Recoveries)-1]
	interval := time.Duration(cmp.Or(apiStep.Poll.IntervalSeconds, 5)) * time.Second
	if wait := time.Until(recovery.StartedAt.Add(interval)); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	if separatePoll(apiStep) {
		state.Phase = stepPhasePolling
		state.APICallName = ""
		state.Attempts = 0
		state.Message = fmt.Sprintf("Recovery %d accepted; polling again", recovery.Number)
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
	}
	// The poll is the step's own request repeated by its ApiCall: a new ApiCall starts it over.
	call, err := r.buildAPICall(ctx, run, step.Name, fmt.Sprintf("r%02d", recovery.Number), apiStep.TemplateRef.Name, apiStep.Inputs, apiStep.Debug, callRetry(apiStep), nil)
	if errors.Is(err, errTemplateUnavailable) {
		return r.failWorkflow(ctx, run, state, "ApiCallTemplateUnavailable", fmt.Sprintf("Referenced ApiCallTemplate %q was not found", apiStep.TemplateRef.Name))
	}
	if err != nil {
		return r.failBuild(ctx, run, state, err, "RequestBuildFailed", "Could not build the API request from the step outputs, parameters and ConfigMaps")
	}
	call.Spec.KeepJSONPointers = responsePointers(apiStep, false)
	if err := r.createOwned(ctx, run.owner, call); err != nil {
		return ctrl.Result{}, err
	}
	state.Phase = stepPhaseRunning
	state.APICallName = call.Name
	state.Attempts = 1
	state.Message = fmt.Sprintf("Recovery %d accepted; polling again", recovery.Number)
	return ctrl.Result{RequeueAfter: workflowPollInterval}, r.Status().Update(ctx, run.owner)
}
