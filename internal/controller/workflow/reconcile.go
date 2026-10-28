package workflow

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
	tfengine "github.com/karavy/terraform-engine"
)

const (
	workflowPhasePending   = "Pending"
	workflowPhaseRunning   = "Running"
	workflowPhaseSucceeded = "Succeeded"
	callPhaseSucceeded     = "Succeeded"
	workflowPhaseFailed    = "Failed"
	workflowPhaseUnknown   = "Unknown"
	stepPhasePending       = "Pending"
	stepPhaseRunning       = "Running"
	stepPhasePolling       = "Polling"
	stepPhaseRecovering    = "Recovering"
	stepPhaseSucceeded     = "Succeeded"
	stepPhaseFailed        = "Failed"
	stepPhaseUnknown       = "Unknown"
	stepPhaseSkipped       = "Skipped"
	workflowPollInterval   = 2 * time.Second
	// deletionFinalizer keeps a Workflow whose templates have deleteSteps until they have run.
	deletionFinalizer = "dinova.one/workflow-deletion"
	// planCleanupFinalizer keeps a Plan workflow until its saved plans are deleted.
	planCleanupFinalizer = "dinova.one/plan-cleanup"
)

var errTemplateUnavailable = errors.New("referenced ApiCallTemplate was not found")

var dnsSanitizer = regexp.MustCompile(`[^a-z0-9-]+`)

// Handler sequences child ApiCall resources and Terraform Jobs without waiting in Reconcile.
type Handler struct {
	client.Client
	Scheme               *runtime.Scheme
	TerraformRunnerImage string
	// APIReader reads Secrets and ConfigMaps without a cluster-wide cache; Client is used when nil.
	APIReader client.Reader
	// TerraformState is the storage of every Terraform state; Terraform steps need it.
	TerraformState terraform.StateConfig
	// Recorder records Events about the Workflows; none are recorded when nil.
	Recorder events.EventRecorder
}

func initialStepStatuses(steps []dinovav1alpha1.WorkflowStep) []dinovav1alpha1.WorkflowStepStatus {
	statuses := make([]dinovav1alpha1.WorkflowStepStatus, 0, len(steps))
	for _, step := range steps {
		statuses = append(statuses, dinovav1alpha1.WorkflowStepStatus{Name: step.Name, Phase: stepPhasePending})
	}
	return statuses
}

// skipStep completes a delete step that has nothing to undo.
func (r *Handler) skipStep(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	state.Phase = stepPhaseSkipped
	state.Message = "Skipped: an input of this step comes from a step that did not complete, so there is nothing to undo"
	run.advance()
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
}

func (r *Handler) reconcileAPIStep(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	apiStep := step.API
	if state.Phase == stepPhasePending {
		call, err := r.buildAPICall(ctx, run, step.Name, "initial", apiStep.TemplateRef.Name, apiStep.Inputs, apiStep.Debug, callRetry(apiStep), nil)
		if errors.Is(err, errTemplateUnavailable) {
			return r.failWorkflow(ctx, run, state, "ApiCallTemplateUnavailable", fmt.Sprintf("Referenced ApiCallTemplate %q was not found", apiStep.TemplateRef.Name))
		}
		if errors.Is(err, errOutputNotPublished) && run.deleting() {
			return r.skipStep(ctx, run, state)
		}
		if err != nil {
			return r.failBuild(ctx, run, state, err, "RequestBuildFailed", "Could not build the API request from the step outputs, parameters and ConfigMaps")
		}
		if call.Spec.Retry != nil && call.Spec.Method != http.MethodGet {
			return r.failWorkflow(ctx, run, state, "RetryNotAllowed", fmt.Sprintf("Step %q repeats its request, but only GET requests can be repeated", step.Name))
		}
		call.Spec.KeepJSONPointers = responsePointers(apiStep, false)
		if err := r.createOwned(ctx, run.owner, call); err != nil {
			return ctrl.Result{}, err
		}
		state.Phase = stepPhaseRunning
		state.APICallName = call.Name
		state.Attempts = 1
		state.Message = "Initial API request submitted"
		return ctrl.Result{RequeueAfter: workflowPollInterval}, r.Status().Update(ctx, run.owner)
	}
	if state.APICallName == "" {
		return r.createPollCall(ctx, run, step, state)
	}
	call, err := r.getChildCall(ctx, types.NamespacedName{Namespace: run.owner.Namespace, Name: state.APICallName})
	if apierrors.IsNotFound(err) {
		return r.failWorkflow(ctx, run, state, "ChildCallLost", "Workflow child ApiCall was not found")
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if call.Spec.Retry != nil {
		state.Attempts = call.Status.Attempts
	}
	switch call.Status.Phase {
	case "Pending", "Running", "":
		return ctrl.Result{RequeueAfter: workflowPollInterval}, nil
	case "Unknown":
		return r.failWorkflow(ctx, run, state, "ChildCallUnknown", "Child ApiCall execution is unknown; workflow will not replay it")
	case "Failed":
		return r.reconcileFailedCall(ctx, run, step, state, call)
	case callPhaseSucceeded:
	default:
		return r.failWorkflow(ctx, run, state, "InvalidChildPhase", "Child ApiCall reported an invalid phase")
	}
	if state.Phase == stepPhaseRecovering {
		return r.pollAgain(ctx, run, step, state)
	}
	if call.Spec.Debug {
		return r.completeDebugStep(ctx, run, apiStep, state)
	}
	// A step that reads nothing from the response leaves keepJSONPointers empty, and does not
	// mind a body that did not fit.
	if call.Status.Response == nil || call.Status.Response.Truncated && len(call.Spec.KeepJSONPointers) > 0 {
		return r.failWorkflow(ctx, run, state, "ResponseUnavailable", "API response is missing, or the values the step reads exceed 64 KiB")
	}
	if state.Phase == stepPhaseRunning {
		if err := checkExpectations(call.Status.Response.Body, apiStep.Expect); err != nil {
			return r.outputError(ctx, run, state, err)
		}
	}
	if state.Phase == stepPhaseRunning && separatePoll(apiStep) {
		if err := r.captureOutputs(ctx, run, state, apiStep.Outputs, call.Status.Response.Body); err != nil {
			return r.outputError(ctx, run, state, err)
		}
		state.Phase = stepPhasePolling
		state.APICallName = ""
		state.Attempts = 0
		state.Message = "Initial request completed; polling for readiness"
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
	}
	if state.Phase == stepPhasePolling {
		return r.reconcilePollResponse(ctx, run, step, state, call)
	}
	if err := r.captureOutputs(ctx, run, state, finalOutputs(apiStep), call.Status.Response.Body); err != nil {
		return r.outputError(ctx, run, state, err)
	}
	state.Phase = stepPhaseSucceeded
	state.Message = fmt.Sprintf("API step completed with HTTP %d", call.Status.Response.StatusCode)
	run.advance()
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
}

// reconcilePollResponse handles the response of a poll request of another template: the step
// succeeds, recovers or fails, or polls again.
func (r *Handler) reconcilePollResponse(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus, call *dinovav1alpha1.ApiCall) (ctrl.Result, error) {
	apiStep := step.API
	ready, err := pollCondition(call.Status.Response.Body, apiStep.Poll)
	if err != nil {
		return r.failWorkflow(ctx, run, state, "PollResponseInvalid", "Polling response did not contain valid JSON at the configured pointer")
	}
	if ready {
		if err := r.captureOutputs(ctx, run, state, apiStep.Poll.Outputs, call.Status.Response.Body); err != nil {
			return r.outputError(ctx, run, state, err)
		}
		state.Phase = stepPhaseSucceeded
		state.Message = "Polling condition satisfied"
		run.advance()
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
	}
	if value, failed := pollFailed(call.Status.Response.Body, apiStep.Poll); failed {
		return r.recoverOrFail(ctx, run, step, state, call.Status.Response.Body, value)
	}
	return r.scheduleNextPoll(ctx, run, apiStep.Poll, state, "Polling condition not satisfied; another poll will be scheduled")
}

// getChildCall reads the ApiCall of a step. The cache may not have seen one created a moment
// ago, most of all when the API server is slow, so a miss is checked against the server itself
// before the call is declared lost.
func (r *Handler) getChildCall(ctx context.Context, key types.NamespacedName) (*dinovav1alpha1.ApiCall, error) {
	call := &dinovav1alpha1.ApiCall{}
	err := r.Get(ctx, key, call)
	if apierrors.IsNotFound(err) {
		err = r.reader().Get(ctx, key, call)
	}
	return call, err
}

// reconcileFailedCall decides what a failed child ApiCall means for its step: a poll that
// tolerates the failure goes on, anything else fails the workflow. An ApiCall that repeats its
// own request has already tried as often as it was allowed to: its reason becomes the
// workflow's.
func (r *Handler) reconcileFailedCall(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus, call *dinovav1alpha1.ApiCall) (ctrl.Result, error) {
	apiStep := step.API
	if state.Phase == stepPhaseRecovering {
		return r.failWorkflow(ctx, run, state, "RecoveryFailed", fmt.Sprintf("API step %q: the recover request failed, so the operation was not resumed%s", step.Name, callFailure(call)))
	}
	if state.Phase == stepPhasePolling && pollFailureTolerated(call, apiStep.Poll) {
		return r.scheduleNextPoll(ctx, run, apiStep.Poll, state, "Poll request failed with a tolerated error; another poll will be scheduled")
	}
	if ready := meta.FindStatusCondition(call.Status.Conditions, "Ready"); call.Spec.Retry != nil && ready != nil {
		// A poll without templateRef ends inside the ApiCall; its last response says why.
		if ready.Reason == "PollFailed" && apiStep.Poll != nil && call.Status.Response != nil && !call.Status.Response.Truncated {
			if value, failed := pollFailed(call.Status.Response.Body, apiStep.Poll); failed {
				return r.recoverOrFail(ctx, run, step, state, call.Status.Response.Body, value)
			}
		}
		switch ready.Reason {
		case "PollTimeout", "PollFailed", "RetriesExhausted", "ConditionInvalid":
			return r.failWorkflow(ctx, run, state, ready.Reason, fmt.Sprintf("API step %q: %s", step.Name, ready.Message))
		}
	}
	return r.failWorkflow(ctx, run, state, "ChildCallFailed", fmt.Sprintf("API step %q failed%s", step.Name, callFailure(call)))
}

// callFailure is ": <why>" for a failed ApiCall, from the message the operator wrote in its Ready
// condition (for example "Remote API returned HTTP 422"); the response itself is never copied.
func callFailure(call *dinovav1alpha1.ApiCall) string {
	if ready := meta.FindStatusCondition(call.Status.Conditions, "Ready"); ready != nil && ready.Message != "" {
		return ": " + ready.Message
	}
	return ""
}

// callRetry returns how the ApiCall of a step repeats its own request: its retry, or its poll
// when the poll names no template and so sends the same request again. Nil when it does not.
func callRetry(apiStep *dinovav1alpha1.WorkflowAPIStep) *dinovav1alpha1.ApiCallRetry {
	switch {
	case apiStep.Poll != nil && apiStep.Poll.TemplateRef.Name == "":
		poll := apiStep.Poll
		transport := poll.RetryOnTransportErrors
		return &dinovav1alpha1.ApiCallRetry{
			MaxAttempts:       cmp.Or(poll.MaxAttempts, 20),
			IntervalSeconds:   cmp.Or(poll.IntervalSeconds, 5),
			OnStatusCodes:     poll.RetryOnStatusCodes,
			OnTransportErrors: &transport,
			Until:             &dinovav1alpha1.ApiCallCondition{JSONPointer: poll.ConditionJSONPointer, EqualsJSON: poll.EqualsJSON, FailWhenJSON: poll.FailWhenJSON},
		}
	case apiStep.Retry != nil:
		retry := apiStep.Retry
		return &dinovav1alpha1.ApiCallRetry{
			MaxAttempts: retry.MaxAttempts, IntervalSeconds: retry.BackoffSeconds,
			OnStatusCodes: retry.StatusCodes, OnTransportErrors: retry.OnTransportErrors,
		}
	}
	return nil
}

// separatePoll is true when the step polls with requests of another template, one ApiCall each.
func separatePoll(apiStep *dinovav1alpha1.WorkflowAPIStep) bool {
	return apiStep.Poll != nil && apiStep.Poll.TemplateRef.Name != ""
}

// finalOutputs are the outputs read from the response that ends the step: the step's own, and
// the poll's when the poll is the step's own request repeated.
func finalOutputs(apiStep *dinovav1alpha1.WorkflowAPIStep) []dinovav1alpha1.WorkflowOutput {
	if apiStep.Poll != nil && !separatePoll(apiStep) {
		return append(slices.Clone(apiStep.Outputs), apiStep.Poll.Outputs...)
	}
	return apiStep.Outputs
}

// completeDebugStep ends a step whose request was logged and not sent. There is no response,
// so the outputs that declare a debugJSON are published with that value, and the template
// outputs are built as usual; the others are not published.
func (r *Handler) completeDebugStep(ctx context.Context, run *templateRun, apiStep *dinovav1alpha1.WorkflowAPIStep, state *dinovav1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	var outputs []dinovav1alpha1.WorkflowOutput
	for _, output := range apiStep.Outputs {
		if output.Template != "" {
			outputs = append(outputs, output) // built from known values, so there is no response to miss
		} else if output.DebugJSON != "" {
			outputs = append(outputs, dinovav1alpha1.WorkflowOutput{Name: output.Name, From: "/debug", DefaultJSON: output.DebugJSON, Sensitive: output.Sensitive})
		}
	}
	if err := r.captureOutputs(ctx, run, state, outputs, []byte(`{}`)); err != nil {
		return r.outputError(ctx, run, state, err)
	}
	state.Phase = stepPhaseSucceeded
	state.Message = "Debug is enabled: the request was logged by the operator and not sent"
	run.advance()
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
}

// scheduleNextPoll ends the current poll attempt as "not ready": it fails the workflow
// once maxAttempts is used up, otherwise it requeues after the poll interval.
func (r *Handler) scheduleNextPoll(ctx context.Context, run *templateRun, poll *dinovav1alpha1.WorkflowPoll, state *dinovav1alpha1.WorkflowStepStatus, message string) (ctrl.Result, error) {
	maxAttempts := poll.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 20
	}
	if state.Attempts >= maxAttempts {
		return r.failWorkflow(ctx, run, state, "PollTimeout", "Polling condition was not satisfied within maxAttempts")
	}
	state.APICallName = ""
	state.Message = message
	interval := poll.IntervalSeconds
	if interval == 0 {
		interval = 5
	}
	return ctrl.Result{RequeueAfter: time.Duration(interval) * time.Second}, r.Status().Update(ctx, run.owner)
}

// pollFailureTolerated reports whether a failed poll ApiCall is one the poll was
// configured to treat as "not ready yet". Invalid requests, oversized responses and
// lost executions are never tolerated.
func pollFailureTolerated(call *dinovav1alpha1.ApiCall, poll *dinovav1alpha1.WorkflowPoll) bool {
	return failureTolerated(call, poll.RetryOnStatusCodes, poll.RetryOnTransportErrors)
}

// failureTolerated reports whether a failed ApiCall failed with one of the listed HTTP
// statuses, or at the network level when transport errors are accepted.
func failureTolerated(call *dinovav1alpha1.ApiCall, statusCodes []int32, transportErrors bool) bool {
	ready := meta.FindStatusCondition(call.Status.Conditions, "Ready")
	if ready == nil {
		return false
	}
	switch ready.Reason {
	case "HTTPError":
		return call.Status.Response != nil && slices.Contains(statusCodes, int32(call.Status.Response.StatusCode))
	case "Timeout", "RequestFailed":
		return transportErrors
	}
	return false
}

func (r *Handler) createPollCall(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	poll := step.API.Poll
	if poll == nil {
		return r.failWorkflow(ctx, run, state, "PollMissing", "Polling state has no poll configuration")
	}
	state.Attempts++
	suffix := fmt.Sprintf("poll-%02d", state.Attempts)
	if recoveries := len(state.Recoveries); recoveries > 0 {
		suffix = fmt.Sprintf("r%02d-%s", recoveries, suffix) // a new round of polls after each recovery
	}
	call, err := r.buildAPICall(ctx, run, step.Name, suffix, poll.TemplateRef.Name, poll.Inputs, false, nil, nil)
	if errors.Is(err, errTemplateUnavailable) {
		state.Attempts--
		return r.failWorkflow(ctx, run, state, "ApiCallTemplateUnavailable", fmt.Sprintf("Referenced ApiCallTemplate %q was not found", poll.TemplateRef.Name))
	}
	if err != nil {
		return r.failBuild(ctx, run, state, err, "PollBuildFailed", "Could not build the polling request from the step outputs, parameters and ConfigMaps")
	}
	call.Spec.KeepJSONPointers = responsePointers(step.API, true)
	if err := r.createOwned(ctx, run.owner, call); err != nil {
		state.Attempts--
		return ctrl.Result{}, err
	}
	state.APICallName = call.Name
	state.Message = fmt.Sprintf("Polling attempt %d submitted", state.Attempts)
	return ctrl.Result{RequeueAfter: workflowPollInterval}, r.Status().Update(ctx, run.owner)
}

// failBuild fails the workflow with the reason of an input that cannot be resolved when it
// says why (a missing ConfigMap or parameter), and with the generic one otherwise.
func (r *Handler) failBuild(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, err error, reason, message string) (ctrl.Result, error) {
	var failure *outputFailure
	if errors.As(err, &failure) {
		return r.failWorkflow(ctx, run, state, failure.reason, failure.message)
	}
	return r.failWorkflow(ctx, run, state, reason, message)
}

// buildAPICall returns a Workflow-owned ApiCall created from the named ApiCallTemplate.
// Templates are never executed; the ApiCall controller only runs Workflow-owned calls.
// response is the failed poll response a recover request reads with fromResponse, or nil.
func (r *Handler) buildAPICall(ctx context.Context, run *templateRun, stepName, suffix, templateName string, inputs []dinovav1alpha1.WorkflowInput, debug bool, retry *dinovav1alpha1.ApiCallRetry, response []byte) (*dinovav1alpha1.ApiCall, error) {
	template := &dinovav1alpha1.ApiCallTemplate{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.owner.Namespace, Name: templateName}, template); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errTemplateUnavailable
		}
		return nil, err
	}
	spec := &dinovav1alpha1.ApiCallSpec{
		Method: template.Spec.Method, Endpoint: template.Spec.Endpoint, Body: template.Spec.Body,
		Headers: maps.Clone(template.Spec.Headers), Authentication: template.Spec.Authentication.DeepCopy(),
		TimeoutSeconds: template.Spec.TimeoutSeconds, Debug: template.Spec.Debug || debug, Retry: retry,
	}
	if len(inputs) > 0 {
		resolve := r.inputResolver(ctx, run)
		if response != nil {
			resolve = responseResolver(response, resolve)
		}
		endpoint, err := applyPathInputs(spec.Endpoint, inputs, resolve)
		if err != nil {
			return nil, err
		}
		endpoint, err = applyQueryInputs(endpoint, inputs, resolve)
		if err != nil {
			return nil, err
		}
		spec.Endpoint = endpoint
		body, err := applyInputs([]byte(spec.Body), inputs, resolve)
		if err != nil {
			return nil, err
		}
		spec.Body = string(body)
	}
	// The Workflow reads each response to map it into later steps.
	spec.StoreResponseBody = new(true)
	if spec.TimeoutSeconds == 0 {
		spec.TimeoutSeconds = 120
	}
	call := &dinovav1alpha1.ApiCall{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: run.owner.Namespace,
			Name:      run.childName(stepName, suffix),
			Labels: map[string]string{
				"dinova.one/workflow":          run.owner.Name,
				"dinova.one/workflow-template": run.name,
				"dinova.one/workflow-step":     stepName,
			},
			Annotations: map[string]string{"dinova.one/api-call-template": templateName},
		},
		Spec: *spec,
	}
	return call, controllerutil.SetControllerReference(run.owner, call, r.Scheme)
}

// terraformFiles returns the copy of the files a Terraform step runs, owned by the Workflow so
// that later revisions of the template never change a running step: the copy an earlier
// attempt of this step made, or else a new one of the current revision of the template. It
// records the revision in the step status. done is true when the step stops here, waiting for
// the revision of a template that just changed, or failed.
func (r *Handler) terraformFiles(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus) (*corev1.ConfigMap, ctrl.Result, bool, error) {
	name := types.NamespacedName{Namespace: run.owner.Namespace, Name: run.childName(step.Name, "tf-template")}
	existing := &corev1.ConfigMap{}
	switch err := r.reader().Get(ctx, name, existing); {
	case err == nil:
		state.TerraformDigest = existing.Annotations[terraform.DigestAnnotation]
		state.TerraformRevision, _ = strconv.ParseInt(existing.Annotations[terraform.RevisionAnnotation], 10, 64)
		return existing, ctrl.Result{}, false, nil
	case !apierrors.IsNotFound(err):
		return nil, ctrl.Result{}, true, err
	}
	template := &dinovav1alpha1.TerraformTemplate{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.owner.Namespace, Name: step.Terraform.TemplateRef.Name}, template); err != nil {
		if apierrors.IsNotFound(err) {
			result, err := r.failWorkflow(ctx, run, state, "TerraformTemplateUnavailable", fmt.Sprintf("Referenced TerraformTemplate %q was not found", step.Terraform.TemplateRef.Name))
			return nil, result, true, err
		}
		return nil, ctrl.Result{}, true, err
	}
	revisionName, err := terraform.CurrentRevision(template)
	var sourceError *terraform.SourceError
	switch {
	case errors.Is(err, terraform.ErrRevisionPending):
		return nil, ctrl.Result{RequeueAfter: workflowPollInterval}, true, nil
	case errors.As(err, &sourceError):
		result, err := r.failWorkflow(ctx, run, state, sourceError.Reason, sourceError.Message)
		return nil, result, true, err
	}
	revision := &appsv1.ControllerRevision{}
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: run.owner.Namespace, Name: revisionName}, revision); err != nil {
		if apierrors.IsNotFound(err) { // pruned or replaced in the meantime: the template has a newer one
			return nil, ctrl.Result{RequeueAfter: workflowPollInterval}, true, nil
		}
		return nil, ctrl.Result{}, true, err
	}
	files, err := terraform.RevisionFiles(revision)
	if err != nil {
		result, err := r.failWorkflow(ctx, run, state, "TerraformRevisionInvalid", err.Error())
		return nil, result, true, err
	}
	state.TerraformRevision = revision.Revision
	state.TerraformDigest = revision.Annotations[terraform.DigestAnnotation]
	copied := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: name.Namespace, Name: name.Name,
			Labels: map[string]string{"dinova.one/workflow": run.owner.Name},
			Annotations: map[string]string{
				"dinova.one/terraform-template": template.Name,
				terraform.RevisionAnnotation:    strconv.FormatInt(revision.Revision, 10),
				terraform.DigestAnnotation:      state.TerraformDigest,
			},
		},
		Data:      files,
		Immutable: new(true),
	}
	if err := r.createOwned(ctx, run.owner, copied); err != nil {
		return nil, ctrl.Result{}, true, err
	}
	return copied, ctrl.Result{}, false, nil
}

func (r *Handler) reconcileTerraformStep(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	tf := step.Terraform
	mode := terraformMode(run.owner)
	if mode == terraform.ModeDestroy && (state.Phase == stepPhasePending || state.Phase == stepPhaseSkipped) {
		return r.startDestroyStep(ctx, run, step, state)
	}
	if state.Phase == stepPhasePending {
		if !r.TerraformState.Configured() {
			return r.failWorkflow(ctx, run, state, "StateBackendNotConfigured", "The operator has no Terraform state storage configured (--tfstate-s3-* flags)")
		}
		backend, failed := r.stepBackend(ctx, run, tf, state)
		if failed != nil {
			return r.failWorkflow(ctx, run, state, failed.reason, failed.message)
		}
		if mode == terraform.ModeApplyPlan && step.Name != run.owner.Spec.ApplyPlan.Step {
			// Steps before the approved one were up to date when it was planned.
			return r.publishFromState(ctx, run, state, tf, "Up to date when the plan was made: outputs read from the state")
		}
		return r.startTerraformJob(ctx, run, step, state, backend)
	}
	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: run.owner.Namespace, Name: state.JobName}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failWorkflow(ctx, run, state, "TerraformJobLost", "Terraform Job was not found")
		}
		return ctrl.Result{}, err
	}
	if job.Status.Succeeded > 0 {
		switch mode {
		case terraform.ModePlan:
			return r.finishPlanStep(ctx, run, step, state)
		case terraform.ModeApplyPlan:
			if err := r.captureTerraformOutputs(ctx, run, state, tf); err != nil {
				return r.outputError(ctx, run, state, err)
			}
			state.Phase, state.Message = stepPhaseSucceeded, "Approved plan applied"
			skipFollowingSteps(run, step.Name, "Not applied: plan again to apply this step")
			r.deleteAppliedPlans(ctx, run.owner)
			return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
		case terraform.ModeDestroy:
			state.Phase, state.Message = stepPhaseSucceeded, "Terraform destroy completed"
			run.status.CurrentStep++
			return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
		}
		if err := r.captureTerraformOutputs(ctx, run, state, tf); err != nil {
			return r.outputError(ctx, run, state, err)
		}
		state.Phase = stepPhaseSucceeded
		state.Message = "Terraform apply completed"
		run.status.CurrentStep++
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
	}
	if job.Status.Failed > 0 || job.Status.Active == 0 && job.Status.StartTime != nil && time.Since(job.Status.StartTime.Time) > 2*time.Minute {
		return r.failTerraformJob(ctx, run, state, job)
	}
	return ctrl.Result{RequeueAfter: workflowPollInterval}, nil
}

// failTerraformJob fails the workflow at a Terraform Job that failed, saying whether it ran out
// of time, and removes the state lock it may have left (see releaseOrphanedLock).
func (r *Handler) failTerraformJob(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, job *batchv1.Job) (ctrl.Result, error) {
	message := "Terraform Job failed; inspect its pod logs"
	// The Job controller sets FailureTarget first, and Failed only once the pod is gone.
	switch {
	case deadlineExceeded(job):
		message = fmt.Sprintf("Terraform Job exceeded its timeout (%ds); inspect its pod logs", *job.Spec.ActiveDeadlineSeconds)
	case r.stoppedAtTimeLimit(ctx, job):
		message = fmt.Sprintf("Terraform Job reached its time limit, before its timeout (%ds), and stopped OpenTofu; inspect its pod logs", *job.Spec.ActiveDeadlineSeconds)
	}
	released := state.ReleasedStateLock
	if err := r.releaseOrphanedLock(ctx, run, state); err != nil {
		logf.FromContext(ctx).Error(err, "Could not check the Terraform state lock", "state", state.StateKey)
	}
	if state.ReleasedStateLock != released {
		message += "; the state lock it left was removed"
	}
	if unsaved := r.unsavedOf(ctx, state, job); unsaved != nil {
		state.UnsavedResources = unsavedStatus(*unsaved)
		r.event(run.owner, corev1.EventTypeWarning, reasonUnsavedResources, "RecordUnsavedResources",
			"Step %s did not save in state %s resources it began to create (%s): applying to this state waits until %s is listed in acceptStateHolds",
			state.Name, state.StateKey, resourceList(unsaved), unsaved.ID)
		return r.failWorkflow(ctx, run, state, reasonUnsavedResources, fmt.Sprintf(
			"%s. It did not save in the state resources it began to create (%s): they may exist in the cloud, and applying would create them again. "+
				"Look for them, import or delete them, then list %q in acceptStateHolds", message, resourceList(unsaved), unsaved.ID))
	}
	return r.failWorkflow(ctx, run, state, "TerraformJobFailed", message)
}

// deadlineExceeded reports whether a Job was stopped at its activeDeadlineSeconds.
func deadlineExceeded(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if (condition.Type == batchv1.JobFailed || condition.Type == batchv1.JobFailureTarget) &&
			condition.Status == corev1.ConditionTrue && condition.Reason == batchv1.JobReasonDeadlineExceeded {
			return true
		}
	}
	return false
}

// terraformMode is the runner mode of the Terraform steps of a Workflow.
func terraformMode(workflow *dinovav1alpha1.Workflow) string {
	switch workflow.Spec.TerraformMode {
	case "Plan":
		return terraform.ModePlan
	case "ApplyPlan":
		return terraform.ModeApplyPlan
	case "Destroy":
		return terraform.ModeDestroy
	}
	return terraform.ModeApply
}

// stepBackend renders the state key of a Terraform step, records it in the step status and
// returns the backend of the state.
func (r *Handler) stepBackend(ctx context.Context, run *templateRun, tf *dinovav1alpha1.WorkflowTerraformStep, state *dinovav1alpha1.WorkflowStepStatus) (tfengine.S3Backend, *outputFailure) {
	stateKey, err := renderStateKey(tf.State.Key, r.outputResolver(ctx, run), run.parameterValues())
	if err != nil {
		return tfengine.S3Backend{}, &outputFailure{"StateKeyInvalid", err.Error()}
	}
	backend := r.TerraformState.Backend(terraform.ObjectKey(run.owner.Namespace, stateKey))
	if err := backend.Validate(); err != nil {
		return tfengine.S3Backend{}, &outputFailure{"StateKeyInvalid", err.Error()}
	}
	state.StateKey = backend.Key
	return backend, nil
}

// planKey is where the plan of a step is saved: by this workflow in Plan mode, by the Plan
// workflow it names in ApplyPlan mode.
func planKey(run *templateRun, step string) string {
	owner := run.owner
	if reference := owner.Spec.ApplyPlan; reference != nil {
		return terraform.PlanKey(owner.Namespace, reference.Workflow, reference.UID, run.name, step)
	}
	return terraform.PlanKey(owner.Namespace, owner.Name, string(owner.UID), run.name, step)
}

// startTerraformJob creates the Job of a Terraform step, with its files and its variables.
func (r *Handler) startTerraformJob(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus, backend tfengine.S3Backend) (ctrl.Result, error) {
	tf := step.Terraform
	// The Job is created even if the claim is still Pending: storage classes with
	// WaitForFirstConsumer (Kind's default) bind a volume only once a pod uses it, so
	// waiting for Bound here would wait forever. The Job's deadline bounds a claim that
	// never binds.
	workspace, err := r.ensureWorkspacePVC(ctx, run)
	if err != nil {
		return r.failWorkflow(ctx, run, state, "WorkspaceFailed", "Could not create Terraform workspace PVC")
	}
	templateFiles, result, done, err := r.terraformFiles(ctx, run, step, state)
	if done || err != nil {
		return result, err
	}
	variables, err := applyInputs([]byte(`{}`), tf.Inputs, r.inputResolver(ctx, run))
	if err != nil {
		return r.failBuild(ctx, run, state, err, "VariablesMappingFailed", "Could not build Terraform variables from the step outputs, parameters and ConfigMaps")
	}
	inputSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: run.owner.Namespace, Name: run.childName(step.Name, "tf-input"),
			Labels: map[string]string{"dinova.one/workflow": run.owner.Name},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{terraform.VariablesKey: variables},
	}
	if err := controllerutil.SetControllerReference(run.owner, inputSecret, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.createOwned(ctx, run.owner, inputSecret); err != nil {
		return ctrl.Result{}, err
	}
	config := terraform.JobConfig{
		Namespace: run.owner.Namespace, Name: run.childName(step.Name, "terraform"), StepName: step.Name,
		RunnerImage: r.TerraformRunnerImage, WorkspacePVC: workspace.Name,
		TemplateConfigMap: templateFiles.Name, InputSecret: inputSecret.Name,
		TimeoutSeconds: tf.TimeoutSeconds, EnvFromSecrets: tf.EnvFromSecrets, State: backend,
		Labels: map[string]string{"dinova.one/workflow": run.owner.Name},
		Mode:   terraformMode(run.owner),
	}
	if config.Mode == terraform.ModePlan || config.Mode == terraform.ModeApplyPlan {
		config.PlanKey = planKey(run, step.Name)
	}
	config.RenderedPrefix = terraform.RenderedPrefix(run.owner.Namespace, run.owner.Name, string(run.owner.UID), run.name, step.Name)
	metadata, err := json.Marshal(map[string]any{
		"namespace": run.owner.Namespace, "workflow": run.owner.Name, "workflowUID": run.owner.UID,
		"template": run.name, "step": step.Name, "terraformTemplate": tf.TemplateRef.Name,
		"terraformRevision": state.TerraformRevision, "terraformDigest": state.TerraformDigest,
		"stateKey": backend.Key, "mode": cmp.Or(config.Mode, terraform.ModeApply),
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	config.RenderedMetadata = string(metadata)
	if err := r.releaseOrphanedLock(ctx, run, state); err != nil {
		// The Job meets the lock, and its error says so: nothing is lost by going on.
		logf.FromContext(ctx).Error(err, "Could not check the Terraform state lock", "state", state.StateKey)
	}
	if holdsApply(config.Mode) {
		held, err := r.holdState(ctx, run, state)
		if err != nil {
			return ctrl.Result{}, err
		}
		if held != nil {
			return r.failWorkflow(ctx, run, state, held.reason, held.message)
		}
	}
	job, err := terraform.NewJob(config)
	if err != nil {
		return r.failWorkflow(ctx, run, state, "TerraformJobInvalid", "Could not prepare Terraform Job")
	}
	if err := r.createOwned(ctx, run.owner, job); err != nil {
		return ctrl.Result{}, err
	}
	state.Phase = stepPhaseRunning
	state.JobName = job.Name
	state.RenderedFiles = config.RenderedPrefix
	state.Message = "Terraform Job submitted"
	return ctrl.Result{RequeueAfter: workflowPollInterval}, r.Status().Update(ctx, run.owner)
}

// publishFromState completes a Terraform step without a Job, publishing its outputs from its
// current state, for a step that has nothing to apply.
func (r *Handler) publishFromState(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, tf *dinovav1alpha1.WorkflowTerraformStep, message string) (ctrl.Result, error) {
	if err := r.captureTerraformOutputs(ctx, run, state, tf); err != nil {
		return r.outputError(ctx, run, state, err)
	}
	state.Phase, state.Message = stepPhaseSucceeded, message
	run.status.CurrentStep++
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
}

// finishPlanStep records the plan a Plan Job saved: its summary in the step status, its text
// in a Secret. A plan with changes ends the template, since the next steps may depend on what
// it would change; a plan without changes publishes the outputs from the state, so that the
// next step can be planned.
func (r *Handler) finishPlanStep(ctx context.Context, run *templateRun, step dinovav1alpha1.WorkflowStep, state *dinovav1alpha1.WorkflowStepStatus) (ctrl.Result, error) {
	key := planKey(run, step.Name)
	raw, err := r.TerraformState.Reader.Read(ctx, key+".json")
	if errors.Is(err, terraform.ErrStateNotFound) {
		return r.failWorkflow(ctx, run, state, "PlanNotFound", "The Terraform plan was not found after the Job succeeded")
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	var summary tfengine.PlanSummary
	if err := json.Unmarshal(raw, &summary); err != nil {
		return r.failWorkflow(ctx, run, state, "PlanInvalid", "The summary of the Terraform plan could not be read")
	}
	text, err := r.TerraformState.Reader.Read(ctx, key+".txt")
	if err != nil && !errors.Is(err, terraform.ErrStateNotFound) {
		return ctrl.Result{}, err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: run.owner.Namespace, Name: run.childName(step.Name, "plan"),
			Labels: map[string]string{"dinova.one/workflow": run.owner.Name},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"plan.txt": text},
	}
	if err := r.createOwned(ctx, run.owner, secret); err != nil {
		return ctrl.Result{}, err
	}
	state.Plan = &dinovav1alpha1.WorkflowStepPlan{
		HasChanges: summary.HasChanges, Add: int32(summary.Add), Change: int32(summary.Change), Destroy: int32(summary.Destroy),
		Secret: secret.Name,
	}
	if !summary.HasChanges {
		return r.publishFromState(ctx, run, state, step.Terraform, "Plan has no changes")
	}
	state.Phase = stepPhaseSucceeded
	state.Message = fmt.Sprintf("Plan has changes: %d to add, %d to change, %d to destroy", summary.Add, summary.Change, summary.Destroy)
	skipFollowingSteps(run, step.Name, fmt.Sprintf("Not planned: step %s has changes to apply first", step.Name))
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
}

// deleteAppliedPlans deletes the plans of the Plan workflow an ApplyPlan workflow applied: an
// applied plan is never applied again. A failure is only logged: the plans go anyway when the
// Plan workflow is deleted.
func (r *Handler) deleteAppliedPlans(ctx context.Context, workflow *dinovav1alpha1.Workflow) {
	reference := workflow.Spec.ApplyPlan
	if reference == nil || r.TerraformState.Plans == nil {
		return
	}
	if err := r.TerraformState.Plans.DeletePrefix(ctx, terraform.PlanPrefix(workflow.Namespace, reference.Workflow, reference.UID)); err != nil {
		logf.FromContext(ctx).Error(err, "Could not delete applied Terraform plans", "planWorkflow", reference.Workflow)
	}
}

// skipFollowingSteps marks the steps after step as skipped and ends the template.
func skipFollowingSteps(run *templateRun, step, message string) {
	index := slices.IndexFunc(run.steps, func(candidate dinovav1alpha1.WorkflowStep) bool { return candidate.Name == step })
	for _, following := range run.steps[index+1:] {
		if status := run.findStep(following.Name); status != nil {
			status.Phase, status.Message = stepPhaseSkipped, message
		}
	}
	run.status.CurrentStep = int32(len(run.steps))
}

// captureTerraformOutputs publishes the step's declared outputs from the Terraform state
// the Job has just written. Without declared outputs the state is not even read.
func (r *Handler) captureTerraformOutputs(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, tf *dinovav1alpha1.WorkflowTerraformStep) error {
	if len(tf.Outputs) == 0 {
		return nil
	}
	raw, err := r.TerraformState.Reader.Read(ctx, state.StateKey)
	switch {
	case errors.Is(err, terraform.ErrStateNotFound):
		return &outputFailure{"StateNotFound", "The Terraform state was not found after the Job succeeded"}
	case err != nil:
		return err // transient: the reconciler retries
	}
	document, sensitive, err := terraform.StateOutputs(raw)
	if err != nil {
		return &outputFailure{"StateInvalid", "The Terraform state could not be read for its outputs"}
	}
	return r.captureOutputs(ctx, run, state, tf.Outputs, document, sensitiveDeclared(tf.Outputs, sensitive)...)
}

// sensitiveDeclared returns the names of the declared outputs that read from a Terraform
// output marked sensitive. An output reads from the Terraform output named by the first
// segment of its pointer, so "/db/password" is sensitive when the "db" output is.
func sensitiveDeclared(declared []dinovav1alpha1.WorkflowOutput, sensitiveTerraformOutputs []string) []string {
	var names []string
	for _, output := range declared {
		segments, err := pointerSegments(output.From)
		if err == nil && len(segments) > 0 && slices.Contains(sensitiveTerraformOutputs, segments[0]) {
			names = append(names, output.Name)
		}
	}
	return names
}

func (r *Handler) ensureWorkspacePVC(ctx context.Context, run *templateRun) (*corev1.PersistentVolumeClaim, error) {
	name := run.childName("workspace", "pvc")
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: run.owner.Namespace, Name: name}, pvc)
	if err == nil {
		run.status.WorkspacePVCName = name
		return pvc, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	storage := run.owner.Spec.WorkspaceStorage
	if storage == "" {
		storage = "2Gi"
	}
	quantity, err := resource.ParseQuantity(storage)
	if err != nil {
		return nil, err
	}
	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: run.owner.Namespace, Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: quantity}},
		},
	}
	if err := controllerutil.SetControllerReference(run.owner, pvc, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	run.status.WorkspacePVCName = name
	return pvc, nil
}

func pollCondition(response []byte, poll *dinovav1alpha1.WorkflowPoll) (bool, error) {
	var value any
	if err := json.Unmarshal(response, &value); err != nil {
		return false, err
	}
	actual, err := getPointer(value, poll.ConditionJSONPointer)
	if err != nil {
		return false, err
	}
	if poll.EqualsJSON == "" {
		return true, nil
	}
	var expected any
	if err := json.Unmarshal([]byte(poll.EqualsJSON), &expected); err != nil {
		return false, err
	}
	return jsonEqual(actual, expected), nil
}

// pollFailed reports whether the polling response has one of the poll's failWhenJSON values,
// and which one.
func pollFailed(response []byte, poll *dinovav1alpha1.WorkflowPoll) (string, bool) {
	var value any
	if len(poll.FailWhenJSON) == 0 || json.Unmarshal(response, &value) != nil {
		return "", false
	}
	actual, err := getPointer(value, poll.ConditionJSONPointer)
	if err != nil {
		return "", false
	}
	for _, candidate := range poll.FailWhenJSON {
		var failed any
		if json.Unmarshal([]byte(candidate), &failed) == nil && jsonEqual(actual, failed) {
			return candidate, true
		}
	}
	return "", false
}

func jsonEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func (r *Handler) createOwned(ctx context.Context, owner, object client.Object) error {
	if err := controllerutil.SetControllerReference(owner, object, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, object); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// outputError fails the workflow for output problems that retrying cannot fix and
// returns any other error so that the reconciler retries.
func (r *Handler) outputError(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, err error) (ctrl.Result, error) {
	var failure *outputFailure
	if errors.As(err, &failure) {
		return r.failWorkflow(ctx, run, state, failure.reason, failure.message)
	}
	return ctrl.Result{}, err
}

func (r *Handler) failWorkflow(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, reason, message string) (ctrl.Result, error) {
	state.Phase, state.Message = stepPhaseFailed, message
	if run.deleting() && run.status.Deletion != nil {
		run.status.Deletion.Phase = workflowPhaseFailed
		return r.failDeletion(ctx, run.owner, reason, message)
	}
	return r.failStage(ctx, run.owner, run.status, reason, message)
}
