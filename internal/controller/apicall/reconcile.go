package apicall

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// Handler implements the ApiCall state machine used by the Kubebuilder controller.
// It never starts goroutines: HTTP execution belongs to the shared APIExecutor.
type Handler struct {
	client.Client
	APIReader      client.Reader
	Executor       *APIExecutor
	AllowedOrigins []string
	// OriginsConfigMap names a ConfigMap whose "origins" key adds to AllowedOrigins.
	// Its zero value means none.
	OriginsConfigMap types.NamespacedName
}

const (
	apiPollInterval   = 2 * time.Second
	apiPhasePending   = "Pending"
	apiPhaseRunning   = "Running"
	apiPhaseSucceeded = "Succeeded"
	apiPhaseFailed    = "Failed"
	apiPhaseUnknown   = "Unknown"
)

// Reconcile prepares work and persists checkpoints/results without waiting for HTTP.
func (r *Handler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Executor == nil {
		return ctrl.Result{}, errors.New("API executor is not configured")
	}
	call := &dinovav1alpha1.ApiCall{}
	if err := r.reader().Get(ctx, req.NamespacedName, call); err != nil {
		if apierrors.IsNotFound(err) {
			r.Executor.cancelObsolete(req.NamespacedName, nil)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	key := operationKey{Name: req.NamespacedName, UID: call.UID, Generation: call.Generation, Attempt: call.Status.Attempts}
	if !call.DeletionTimestamp.IsZero() {
		r.Executor.cancelObsolete(req.NamespacedName, nil)
		return ctrl.Result{}, nil
	}
	if !ownedByWorkflow(call) {
		// Standalone ApiCalls are templates referenced by Workflow steps, never executed.
		r.Executor.cancelObsolete(req.NamespacedName, nil)
		return ctrl.Result{}, nil
	}
	r.Executor.cancelObsolete(req.NamespacedName, &key)
	if call.Status.ObservedGeneration != call.Generation {
		call.Status = dinovav1alpha1.ApiCallStatus{
			ObservedGeneration: call.Generation, Phase: apiPhasePending,
			Conditions: call.Status.Conditions,
		}
		setAPIConditions(call, apiPhasePending, "Queued", "Waiting to execute the API request")
		if err := r.Status().Update(ctx, call); err != nil {
			return ctrl.Result{}, err
		}
	}
	switch call.Status.Phase {
	case apiPhaseSucceeded, apiPhaseFailed, apiPhaseUnknown:
		r.Executor.forget(key)
		return ctrl.Result{}, nil
	case apiPhaseRunning:
		if call.Status.NextAttemptAt != nil {
			return r.nextAttempt(ctx, call, key)
		}
		return r.collectResult(ctx, call, key)
	default:
		return r.scheduleRequest(ctx, call, key)
	}
}

func (r *Handler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *Handler) scheduleRequest(ctx context.Context, call *dinovav1alpha1.ApiCall, key operationKey) (ctrl.Result, error) {
	request, oauth, err := r.buildRequest(ctx, call)
	if err != nil {
		reason, message := "CredentialsUnavailable", "Waiting for the authentication Secret and key"
		if errors.Is(err, errOriginsUnavailable) {
			reason, message = "OriginsUnavailable", "Waiting for the origin allowlist ConfigMap to be readable"
		}
		if errors.Is(err, errCredentialsUnavailable) || errors.Is(err, errOriginsUnavailable) {
			condition := meta.FindStatusCondition(call.Status.Conditions, "Ready")
			if condition != nil && condition.Reason == reason && condition.ObservedGeneration == call.Generation {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			setAPIConditions(call, apiPhasePending, reason, message)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, r.Status().Update(ctx, call)
		}
		call.Status.Phase = apiPhaseFailed
		completed := metav1.Now()
		call.Status.CompletedAt = &completed
		setAPIConditions(call, apiPhaseFailed, "InvalidRequest", "Invalid method, endpoint, headers or authentication; check the operator origin allowlist")
		return ctrl.Result{}, r.Status().Update(ctx, call)
	}
	if call.Spec.Debug {
		return r.skipForDebug(ctx, call, request)
	}
	timeout := call.Spec.TimeoutSeconds
	if timeout == 0 {
		timeout = 120
	}
	store := call.Spec.StoreResponseBody == nil || *call.Spec.StoreResponseBody
	key.Attempt = call.Status.Attempts + 1
	if !r.Executor.reserveWithOAuth(key, request, oauth, time.Duration(timeout)*time.Second, store) {
		return ctrl.Result{RequeueAfter: apiPollInterval}, nil
	}
	// Write before sending: after a crash an ambiguous operation is not replayed.
	call.Status.Phase = apiPhaseRunning
	call.Status.Attempts = key.Attempt
	call.Status.NextAttemptAt = nil
	if call.Status.StartedAt == nil {
		started := metav1.Now()
		call.Status.StartedAt = &started
	}
	setAPIConditions(call, apiPhaseRunning, "Executing", "API request queued or executing in a background worker")
	if err := r.Status().Update(ctx, call); err != nil {
		r.Executor.forget(key)
		return ctrl.Result{}, err
	}
	r.Executor.activate(key)
	return ctrl.Result{RequeueAfter: apiPollInterval}, nil
}

// skipForDebug logs the request that would have been sent and completes the ApiCall
// without sending it. Header values are never logged, because they carry credentials.
func (r *Handler) skipForDebug(ctx context.Context, call *dinovav1alpha1.ApiCall, request apiengine.Request) (ctrl.Result, error) {
	headers := make([]string, 0, len(request.Headers))
	for name := range request.Headers {
		headers = append(headers, name)
	}
	slices.Sort(headers)
	log.FromContext(ctx).Info("Skipped ApiCall request because debug is enabled",
		"apiCall", client.ObjectKeyFromObject(call).String(), "method", request.Method,
		"endpoint", request.Endpoint, "headers", headers, "body", string(request.Body))
	call.Status.Phase = apiPhaseSucceeded
	completed := metav1.Now()
	call.Status.CompletedAt = &completed
	setAPIConditions(call, apiPhaseSucceeded, "DebugSkipped", "Debug is enabled: the request was logged and not sent")
	return ctrl.Result{}, r.Status().Update(ctx, call)
}

func (r *Handler) collectResult(ctx context.Context, call *dinovav1alpha1.ApiCall, key operationKey) (ctrl.Result, error) {
	result, done, exists := r.Executor.lookup(key)
	if !exists {
		// A request that is sent again is a GET, so one lost with the worker can simply be
		// replaced by the next attempt, while there are attempts left.
		if retry := call.Spec.Retry; retry != nil && call.Status.Attempts < retryMaxAttempts(retry) {
			return r.waitForNextAttempt(ctx, call, key, retry, "ExecutionLost",
				fmt.Sprintf("Attempt %d was lost with its worker; it is replaced by the next one", call.Status.Attempts))
		}
		call.Status.Phase = apiPhaseUnknown
		completed := metav1.Now()
		call.Status.CompletedAt = &completed
		setAPIConditions(call, apiPhaseUnknown, "ExecutionLost", "Worker state was lost; the request may have taken effect and will not be automatically repeated")
		return ctrl.Result{}, r.Status().Update(ctx, call)
	}
	if !done {
		r.Executor.activate(key)
		return ctrl.Result{RequeueAfter: apiPollInterval}, nil
	}
	call.Status.Response = result.Response
	if retry := call.Spec.Retry; retry != nil {
		verdict := judge(retry, call.Status.Attempts, result)
		if verdict.again {
			return r.waitForNextAttempt(ctx, call, key, retry, verdict.reason, verdict.message)
		}
		result.Success, result.Reason, result.Message = verdict.success, verdict.reason, verdict.message
	}
	call.Status.Phase = apiPhaseFailed
	if result.Success {
		call.Status.Phase = apiPhaseSucceeded
	}
	call.Status.CompletedAt = &result.CompletedAt
	setAPIConditions(call, call.Status.Phase, result.Reason, result.Message)
	if err := r.Status().Update(ctx, call); err != nil {
		return ctrl.Result{}, err // Keep the result until a durable status update succeeds.
	}
	r.Executor.forget(key)
	return ctrl.Result{}, nil
}

// waitForNextAttempt records when the next request is due. The ApiCall stays Running, with the
// last response kept in its status, so whoever watches it sees one object doing the waiting.
func (r *Handler) waitForNextAttempt(ctx context.Context, call *dinovav1alpha1.ApiCall, key operationKey, retry *dinovav1alpha1.ApiCallRetry, reason, message string) (ctrl.Result, error) {
	due := metav1.NewTime(time.Now().Add(retryInterval(retry)))
	call.Status.NextAttemptAt = &due
	setAPIConditions(call, apiPhaseRunning, reason, message)
	if err := r.Status().Update(ctx, call); err != nil {
		return ctrl.Result{}, err // Keep the result until a durable status update succeeds.
	}
	r.Executor.forget(key)
	return ctrl.Result{RequeueAfter: retryInterval(retry)}, nil
}

// nextAttempt sends the next request once its time has come. A reconcile that arrives earlier,
// for example after a status update, waits for the rest of the interval.
func (r *Handler) nextAttempt(ctx context.Context, call *dinovav1alpha1.ApiCall, key operationKey) (ctrl.Result, error) {
	if wait := time.Until(call.Status.NextAttemptAt.Time); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	return r.scheduleRequest(ctx, call, key)
}

func setAPIConditions(call *dinovav1alpha1.ApiCall, phase, reason, message string) {
	ready, progressing := metav1.ConditionFalse, metav1.ConditionFalse
	switch phase {
	case apiPhaseSucceeded:
		ready = metav1.ConditionTrue
	case apiPhaseUnknown:
		ready = metav1.ConditionUnknown
	}
	if phase == apiPhasePending || phase == apiPhaseRunning {
		progressing = metav1.ConditionTrue
	}
	for conditionType, status := range map[string]metav1.ConditionStatus{"Ready": ready, "Progressing": progressing} {
		meta.SetStatusCondition(&call.Status.Conditions, metav1.Condition{
			Type: conditionType, Status: status, ObservedGeneration: call.Generation,
			Reason: reason, Message: message,
		})
	}
}

// ownedByWorkflow reports whether a Workflow created this ApiCall as a step copy.
func ownedByWorkflow(call *dinovav1alpha1.ApiCall) bool {
	owner := metav1.GetControllerOf(call)
	if owner == nil || owner.Kind != "Workflow" {
		return false
	}
	group, _, _ := strings.Cut(owner.APIVersion, "/")
	return group == dinovav1alpha1.GroupVersion.Group
}
