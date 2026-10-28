// Package instance keeps WorkflowInstances applied: it runs the WorkflowTemplate of an instance
// as a new Workflow every time what it runs changes, and every interval in between.
package instance

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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
	"github.com/karavy/latch/internal/controller/workflow"
)

const (
	// InstanceLabel names, on a Workflow, the WorkflowInstance it is a run of.
	InstanceLabel = "dinova.one/workflow-instance"
	// RunAnnotation is the number of a run, RevisionAnnotation the instance revision it runs.
	RunAnnotation      = "dinova.one/instance-run"
	RevisionAnnotation = "dinova.one/instance-revision"
	// ModeAnnotation is what a run does: the Terraform mode of its Workflow (Apply, Plan,
	// ApplyPlan, or Destroy for the run that destroys the resources of a deleted instance).
	ModeAnnotation = "dinova.one/instance-run-mode"
	// destroyFinalizer keeps an instance with deletionPolicy Destroy until its resources are
	// destroyed.
	destroyFinalizer = "dinova.one/instance-destroy"
	// TemplateIndex indexes WorkflowInstances by the WorkflowTemplate they run.
	TemplateIndex = "spec.templateRef.name"
	// TerraformTemplateIndex indexes WorkflowTemplates by the TerraformTemplates their steps run,
	// and APICallTemplateIndex by the ApiCallTemplates they send.
	TerraformTemplateIndex = "spec.steps.terraform.templateRef.name"
	APICallTemplateIndex   = "spec.steps.api.templateRef.name"

	// templateRunName is the name of the only template use in the Workflow of a run.
	templateRunName  = "run"
	conditionReady   = "Ready"
	conditionDrifted = "Drifted"
	approvalManual   = "Manual"
	modeApply        = "Apply"
	modePlan         = "Plan"
	modeApplyPlan    = "ApplyPlan"
	modeDestroy      = "Destroy"
	policyDestroy    = "Destroy"
	phaseSucceeded   = "Succeeded"
	phaseFailed      = "Failed"
	// revisionWait is how long to wait for the operator to make the revision of a
	// TerraformTemplate that just changed.
	revisionWait = 2 * time.Second
)

// notReady is why an instance cannot run now. A nil error with a notReady stops the reconcile
// until something changes; pending ones are checked again after revisionWait.
type notReady struct {
	reason, message string
	pending         bool
}

func (n *notReady) Error() string { return n.message }

// Handler reconciles WorkflowInstances.
type Handler struct {
	Client client.Client
	Scheme *runtime.Scheme
	// Now is the clock; time.Now when nil.
	Now func() time.Time
	// Recorder records Events about the instances; none are recorded when nil.
	Recorder events.EventRecorder
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// Reconcile starts a run of an instance when what it runs changed or its interval elapsed,
// one run at a time, and keeps its status and its history of runs.
func (h *Handler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &dinovav1alpha1.WorkflowInstance{}
	if err := h.Client.Get(ctx, req.NamespacedName, instance); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !instance.DeletionTimestamp.IsZero() {
		return h.reconcileDeletion(ctx, instance)
	}
	if syncDestroyFinalizer(instance) {
		return ctrl.Result{Requeue: true}, h.Client.Update(ctx, instance)
	}
	runs, err := h.runs(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	active, lastMode := recordRuns(instance, runs)
	h.recordHold(instance, runs)
	if err := h.prune(ctx, instance, runs); err != nil {
		return ctrl.Result{}, err
	}
	instance.Status.ObservedGeneration = instance.Generation
	revision, err := h.revision(ctx, instance)
	var blocked *notReady
	switch {
	case errors.As(err, &blocked):
		instance.Status.NextRunTime = nil
		setReady(instance, metav1.ConditionFalse, blocked.reason, blocked.message)
		result := ctrl.Result{}
		if blocked.pending {
			result.RequeueAfter = revisionWait
		}
		return result, h.Client.Status().Update(ctx, instance)
	case err != nil:
		return ctrl.Result{}, err
	}
	instance.Status.Revision = revision
	if pending := instance.Status.PendingPlan; pending != nil && pending.Revision != revision {
		instance.Status.PendingPlan = nil // made for code that changed: a new plan comes
	}
	setDrifted(instance)
	switch {
	case active != nil:
		instance.Status.NextRunTime = nil
		setReady(instance, metav1.ConditionFalse, "RunInProgress", fmt.Sprintf("Run %s is in progress", active.Name))
		return ctrl.Result{}, h.Client.Status().Update(ctx, instance)
	case instance.Spec.Suspend:
		instance.Status.NextRunTime = nil
		setReady(instance, metav1.ConditionFalse, "Suspended", "The instance is suspended: no run starts")
		return ctrl.Result{}, h.Client.Status().Update(ctx, instance)
	case instance.Status.StateHold != nil && !holdAccepted(instance) && !manual(instance):
		// Every apply would fail the same way: wait for the hold to be accepted. With approval
		// Manual the plans go on, since they show what the state lacks.
		instance.Status.NextRunTime = nil
		setReady(instance, metav1.ConditionFalse, instance.Status.StateHold.Reason, holdMessage(instance))
		return ctrl.Result{}, h.Client.Status().Update(ctx, instance)
	}
	if pending := instance.Status.PendingPlan; manual(instance) && pending != nil && instance.Spec.ApprovePlan == pending.ID {
		return h.startRun(ctx, instance, modeApplyPlan)
	}
	if next := h.nextRun(instance, lastMode); next.After(h.now()) {
		instance.Status.NextRunTime = &metav1.Time{Time: next}
		setIdleReady(instance)
		return ctrl.Result{RequeueAfter: next.Sub(h.now())}, h.Client.Status().Update(ctx, instance)
	}
	if manual(instance) {
		return h.startRun(ctx, instance, modePlan)
	}
	return h.startRun(ctx, instance, modeApply)
}

func manual(instance *dinovav1alpha1.WorkflowInstance) bool {
	return instance.Spec.Approval == approvalManual
}

func runMode(run *dinovav1alpha1.Workflow) string {
	return cmp.Or(run.Annotations[ModeAnnotation], modeApply)
}

// changedStep returns the status of the step of a Plan run whose plan has changes, if any.
func changedStep(run *dinovav1alpha1.Workflow) *dinovav1alpha1.WorkflowStepStatus {
	for i := range run.Status.Templates {
		for j := range run.Status.Templates[i].Steps {
			if step := &run.Status.Templates[i].Steps[j]; step.Plan != nil && step.Plan.HasChanges {
				return step
			}
		}
	}
	return nil
}

// inSync reports whether a run left the resources as the code of its revision says: an Apply
// run that succeeded, or a Plan run that found nothing to change.
func inSync(run *dinovav1alpha1.Workflow) bool {
	if run.Status.Phase != phaseSucceeded {
		return false
	}
	switch runMode(run) {
	case modeApply:
		return true
	case modePlan:
		return changedStep(run) == nil
	}
	return false
}

// pendingPlan returns the plan waiting for approval: that of the newest finished run, when it
// is a Plan run with changes. A failed run says nothing and older ones are looked at; any
// newer outcome (an apply, a plan without changes, an approved plan applied or refused)
// leaves nothing pending, so an approved plan is never applied twice.
func pendingPlan(runs []dinovav1alpha1.Workflow) *dinovav1alpha1.WorkflowInstancePendingPlan {
	for i := range runs {
		run := &runs[i]
		switch {
		case !finished(run):
			continue
		case runMode(run) == modeApplyPlan:
			return nil
		case run.Status.Phase != phaseSucceeded:
			continue
		}
		step := changedStep(run)
		if runMode(run) != modePlan || step == nil {
			return nil
		}
		return &dinovav1alpha1.WorkflowInstancePendingPlan{
			ID: run.Name, UID: string(run.UID), Revision: run.Annotations[RevisionAnnotation], Step: step.Name,
			Add: step.Plan.Add, Change: step.Plan.Change, Destroy: step.Plan.Destroy, Secret: step.Plan.Secret,
			Time: run.CreationTimestamp,
		}
	}
	return nil
}

// setDrifted sets Drifted, for approval Manual only: True while a plan with changes waits.
func setDrifted(instance *dinovav1alpha1.WorkflowInstance) {
	if !manual(instance) {
		meta.RemoveStatusCondition(&instance.Status.Conditions, conditionDrifted)
		return
	}
	pending := instance.Status.PendingPlan
	condition := metav1.Condition{
		Type: conditionDrifted, Status: metav1.ConditionFalse, Reason: "NoChanges",
		Message: "No plan with changes is waiting for approval", ObservedGeneration: instance.Generation,
	}
	if pending != nil {
		condition.Status, condition.Reason = metav1.ConditionTrue, "PlanPending"
		condition.Message = fmt.Sprintf("Plan %s changes step %s: %d to add, %d to change, %d to destroy; read it in Secret %s and approve it with spec.approvePlan: %s",
			pending.ID, pending.Step, pending.Add, pending.Change, pending.Destroy, pending.Secret, pending.ID)
		if approved := instance.Spec.ApprovePlan; approved != "" && approved != pending.ID {
			condition.Message += fmt.Sprintf(" (spec.approvePlan names %s, which is not the pending plan)", approved)
		}
	}
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
}

// runs lists the runs of an instance, newest first.
func (h *Handler) runs(ctx context.Context, instance *dinovav1alpha1.WorkflowInstance) ([]dinovav1alpha1.Workflow, error) {
	list := &dinovav1alpha1.WorkflowList{}
	if err := h.Client.List(ctx, list, client.InNamespace(instance.Namespace), client.MatchingLabels{InstanceLabel: instance.Name}); err != nil {
		return nil, err
	}
	runs := make([]dinovav1alpha1.Workflow, 0, len(list.Items))
	for _, run := range list.Items {
		if metav1.IsControlledBy(&run, instance) {
			runs = append(runs, run)
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runNumber(&runs[i]) > runNumber(&runs[j]) })
	return runs, nil
}

func runNumber(run *dinovav1alpha1.Workflow) int64 {
	number, _ := strconv.ParseInt(run.Annotations[RunAnnotation], 10, 64)
	return number
}

func finished(run *dinovav1alpha1.Workflow) bool {
	return run.Status.Phase == phaseSucceeded || run.Status.Phase == phaseFailed
}

// recordRuns sets in the status what the runs did, and returns the run still in progress and
// the mode of the newest run.
func recordRuns(instance *dinovav1alpha1.WorkflowInstance, runs []dinovav1alpha1.Workflow) (*dinovav1alpha1.Workflow, string) {
	instance.Status.ActiveRun = ""
	var active *dinovav1alpha1.Workflow
	for i := range runs {
		run := &runs[i]
		if !finished(run) {
			active = run
			instance.Status.ActiveRun = run.Name
			break
		}
	}
	// The newest run tells what was last attempted, even if a status update was lost.
	if len(runs) > 0 {
		newest := &runs[0]
		instance.Status.Runs = max(instance.Status.Runs, runNumber(newest))
		instance.Status.LastRun = newest.Name
		instance.Status.LastAttemptedRevision = newest.Annotations[RevisionAnnotation]
		if instance.Status.LastRunTime == nil {
			instance.Status.LastRunTime = &newest.CreationTimestamp
		}
		instance.Status.LastRunPhase = ""
		if finished(newest) {
			instance.Status.LastRunPhase = newest.Status.Phase
		}
	}
	for i := range runs {
		if inSync(&runs[i]) {
			instance.Status.LastAppliedRevision = runs[i].Annotations[RevisionAnnotation]
			break
		}
	}
	instance.Status.PendingPlan = pendingPlan(runs)
	if len(runs) == 0 {
		return active, ""
	}
	return active, runMode(&runs[0])
}

// prune deletes the oldest finished runs beyond the history limits.
func (h *Handler) prune(ctx context.Context, instance *dinovav1alpha1.WorkflowInstance, runs []dinovav1alpha1.Workflow) error {
	kept := map[string]int{}
	limits := map[string]int{
		phaseSucceeded: limitOr(instance.Spec.SuccessfulRunsHistoryLimit, 3),
		phaseFailed:    limitOr(instance.Spec.FailedRunsHistoryLimit, 3),
	}
	for i := range runs {
		run := &runs[i]
		if !finished(run) {
			continue
		}
		if kept[run.Status.Phase] < limits[run.Status.Phase] {
			kept[run.Status.Phase]++
			continue
		}
		if err := h.Client.Delete(ctx, run, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func limitOr(limit int32, fallback int) int {
	if limit < 1 {
		return fallback
	}
	return int(limit)
}

// revision returns the SHA-256 of what a run would run now: the parts of the spec that change
// a run, the WorkflowTemplate, and the current revision of each TerraformTemplate. A
// *notReady error says why the instance cannot run.
func (h *Handler) revision(ctx context.Context, instance *dinovav1alpha1.WorkflowInstance) (string, error) {
	template := &dinovav1alpha1.WorkflowTemplate{}
	err := h.Client.Get(ctx, types.NamespacedName{Namespace: instance.Namespace, Name: instance.Spec.TemplateRef.Name}, template)
	if apierrors.IsNotFound(err) {
		return "", &notReady{reason: "WorkflowTemplateNotFound", message: fmt.Sprintf("WorkflowTemplate %q was not found", instance.Spec.TemplateRef.Name)}
	}
	if err != nil {
		return "", err
	}
	if err := h.supportedTemplate(ctx, instance, template); err != nil {
		return "", err
	}
	revisions := make([]dinovav1alpha1.WorkflowInstanceTerraformRevision, 0, len(template.Spec.Steps))
	for _, step := range template.Spec.Steps {
		if step.Terraform == nil {
			continue
		}
		revision, err := h.terraformRevision(ctx, instance.Namespace, step)
		if err != nil {
			return "", err
		}
		revisions = append(revisions, revision)
	}
	instance.Status.TerraformRevisions = revisions
	requests, err := h.apiRequests(ctx, instance.Namespace, template)
	if err != nil {
		return "", err
	}
	// Requests is left out for a template with Terraform steps only, so that its revision stays
	// what it was before instances ran API steps.
	raw, err := json.Marshal(struct {
		TemplateRef      string                                             `json:"templateRef"`
		Parameters       []dinovav1alpha1.WorkflowParameter                 `json:"parameters"`
		WorkspaceStorage string                                             `json:"workspaceStorage"`
		Template         dinovav1alpha1.WorkflowTemplateSpec                `json:"template"`
		Terraform        []dinovav1alpha1.WorkflowInstanceTerraformRevision `json:"terraform"`
		Requests         []apiRequest                                       `json:"requests,omitempty"`
	}{instance.Spec.TemplateRef.Name, instance.Spec.Parameters, instance.Spec.WorkspaceStorage, template.Spec, revisions, requests})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// supportedTemplate refuses a template an instance cannot run again and again. Terraform steps
// converge to a state; an API step is accepted only when its request is safe to repeat at every
// interval (a GET, or a step marked idempotent). A template with API steps also needs approval
// Auto, since API steps have no plan, and deletionPolicy Retain, since a Destroy Workflow runs
// Terraform steps only. deleteSteps are refused: an instance deletes its old runs, which would
// run them.
func (h *Handler) supportedTemplate(ctx context.Context, instance *dinovav1alpha1.WorkflowInstance, template *dinovav1alpha1.WorkflowTemplate) error {
	unsupported := func(format string, args ...any) error {
		return &notReady{reason: "UnsupportedTemplate", message: fmt.Sprintf(format, args...)}
	}
	if len(template.Spec.DeleteSteps) > 0 {
		return unsupported("WorkflowTemplate %q has deleteSteps: an instance deletes its old runs, which would run them", template.Name)
	}
	if !slices.ContainsFunc(template.Spec.Steps, func(step dinovav1alpha1.WorkflowStep) bool { return step.API != nil }) {
		return nil
	}
	reason, err := workflow.UnrepeatableStep(ctx, h.Client, instance.Namespace, template)
	switch {
	case err != nil:
		return err
	case reason != "":
		return unsupported("An instance runs its template again at every interval, so every API step must be safe to repeat (a GET, or marked idempotent), and %s", reason)
	case manual(instance):
		return unsupported("WorkflowTemplate %q has API steps, which have no plan: an instance with approval Manual runs Terraform steps only", template.Name)
	case instance.Spec.DeletionPolicy == policyDestroy:
		return unsupported("WorkflowTemplate %q has API steps, and a Destroy Workflow runs Terraform steps only: use deletionPolicy Retain", template.Name)
	}
	return nil
}

// apiRequest is an ApiCallTemplate a template sends, in the revision: changing a request makes
// the instance run again, as changing Terraform code does.
type apiRequest struct {
	Name string                             `json:"name"`
	Spec dinovav1alpha1.ApiCallTemplateSpec `json:"spec"`
}

// apiRequests returns the ApiCallTemplates the API steps of a template send.
func (h *Handler) apiRequests(ctx context.Context, namespace string, template *dinovav1alpha1.WorkflowTemplate) ([]apiRequest, error) {
	var requests []apiRequest
	for _, name := range workflow.APICallTemplateNames(template) {
		request := &dinovav1alpha1.ApiCallTemplate{}
		err := h.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, request)
		if apierrors.IsNotFound(err) {
			return nil, &notReady{reason: "ApiCallTemplateNotFound", message: fmt.Sprintf("ApiCallTemplate %q was not found", name)}
		}
		if err != nil {
			return nil, err
		}
		requests = append(requests, apiRequest{Name: name, Spec: request.Spec})
	}
	return requests, nil
}

// APICallTemplateNames returns the ApiCallTemplates the steps of a WorkflowTemplate send, for
// APICallTemplateIndex.
func APICallTemplateNames(template *dinovav1alpha1.WorkflowTemplate) []string {
	return workflow.APICallTemplateNames(template)
}

func (h *Handler) terraformRevision(ctx context.Context, namespace string, step dinovav1alpha1.WorkflowStep) (dinovav1alpha1.WorkflowInstanceTerraformRevision, error) {
	name := step.Terraform.TemplateRef.Name
	template := &dinovav1alpha1.TerraformTemplate{}
	err := h.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, template)
	if apierrors.IsNotFound(err) {
		return dinovav1alpha1.WorkflowInstanceTerraformRevision{}, &notReady{reason: "TerraformTemplateUnavailable", message: fmt.Sprintf("TerraformTemplate %q of step %q was not found", name, step.Name)}
	}
	if err != nil {
		return dinovav1alpha1.WorkflowInstanceTerraformRevision{}, err
	}
	_, err = terraform.CurrentRevision(template)
	var sourceError *terraform.SourceError
	switch {
	case errors.Is(err, terraform.ErrRevisionPending):
		return dinovav1alpha1.WorkflowInstanceTerraformRevision{}, &notReady{reason: "WaitingForRevision", message: fmt.Sprintf("Waiting for the revision of TerraformTemplate %q", name), pending: true}
	case errors.As(err, &sourceError):
		return dinovav1alpha1.WorkflowInstanceTerraformRevision{}, &notReady{reason: sourceError.Reason, message: sourceError.Message}
	case err != nil:
		return dinovav1alpha1.WorkflowInstanceTerraformRevision{}, err
	}
	return dinovav1alpha1.WorkflowInstanceTerraformRevision{
		Step: step.Name, Template: name, Revision: template.Status.Revision, Digest: template.Status.Digest,
	}, nil
}

// nextRun is when the next run is due: now when the revision was never run or an approved
// plan was just applied (to plan the steps after it, and check the result), else the start of
// the last run plus the interval, or the retry interval after a failure.
func (h *Handler) nextRun(instance *dinovav1alpha1.WorkflowInstance, lastMode string) time.Time {
	status := instance.Status
	if status.LastAttemptedRevision != status.Revision || status.LastRunTime == nil || lastMode == modeApplyPlan ||
		holdAccepted(instance) && !manual(instance) {
		return h.now()
	}
	wait := instance.Spec.Interval.Duration
	if status.LastRunPhase == phaseFailed && instance.Spec.RetryInterval != nil {
		wait = instance.Spec.RetryInterval.Duration
	}
	return status.LastRunTime.Add(wait)
}

// setIdleReady sets Ready while no run is in progress and none is due.
func setIdleReady(instance *dinovav1alpha1.WorkflowInstance) {
	status := instance.Status
	switch {
	case status.StateHold != nil && !holdAccepted(instance):
		setReady(instance, metav1.ConditionFalse, status.StateHold.Reason, holdMessage(instance))
	case status.LastRunPhase == phaseFailed:
		setReady(instance, metav1.ConditionFalse, "RunFailed", fmt.Sprintf("Run %s failed", status.LastRun))
	case status.PendingPlan != nil:
		setReady(instance, metav1.ConditionFalse, "PlanPending", fmt.Sprintf("Plan %s waits for approval (spec.approvePlan)", status.PendingPlan.ID))
	case status.LastAppliedRevision == status.Revision:
		setReady(instance, metav1.ConditionTrue, "Applied", fmt.Sprintf("The resources match the current revision (run %s)", status.LastRun))
	default:
		setReady(instance, metav1.ConditionFalse, "NotApplied", "The current revision has not been applied yet")
	}
}

// startRun creates the next run of an instance: a Workflow owned by it, with its labels, that
// applies (mode Apply), only plans (Plan), or applies the pending plan (ApplyPlan).
func (h *Handler) startRun(ctx context.Context, instance *dinovav1alpha1.WorkflowInstance, mode string) (ctrl.Result, error) {
	number := instance.Status.Runs + 1
	run := &dinovav1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: instance.Namespace, Name: runName(instance.Name, number),
			Labels: map[string]string{},
			Annotations: map[string]string{
				RunAnnotation: strconv.FormatInt(number, 10), RevisionAnnotation: instance.Status.Revision, ModeAnnotation: mode,
			},
		},
		Spec: dinovav1alpha1.WorkflowSpec{
			WorkspaceStorage: instance.Spec.WorkspaceStorage,
			Templates: []dinovav1alpha1.WorkflowTemplateRun{{
				Name: templateRunName, TemplateRef: instance.Spec.TemplateRef, Parameters: instance.Spec.Parameters,
			}},
			AcceptStateHolds: instance.Spec.AcceptStateHolds,
		},
	}
	if mode != modeApply {
		run.Spec.TerraformMode = mode
	}
	if pending := instance.Status.PendingPlan; mode == modeApplyPlan {
		run.Spec.ApplyPlan = &dinovav1alpha1.WorkflowPlanReference{Workflow: pending.ID, UID: pending.UID, Step: pending.Step}
	}
	maps.Copy(run.Labels, instance.Labels)
	run.Labels[InstanceLabel] = instance.Name
	if err := controllerutil.SetControllerReference(instance, run, h.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	// A run left by an attempt whose status update failed has this same name: it is this run.
	if err := h.Client.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, err
	}
	logf.FromContext(ctx).Info("Started WorkflowInstance run", "workflow", run.Name, "mode", mode, "revision", instance.Status.Revision)
	now := metav1.NewTime(h.now())
	instance.Status.Runs = number
	instance.Status.LastRun = run.Name
	instance.Status.ActiveRun = run.Name
	instance.Status.LastAttemptedRevision = instance.Status.Revision
	instance.Status.LastRunTime = &now
	instance.Status.LastRunPhase = ""
	instance.Status.NextRunTime = nil
	setReady(instance, metav1.ConditionFalse, "RunInProgress", fmt.Sprintf("Run %s is in progress", run.Name))
	return ctrl.Result{}, h.Client.Status().Update(ctx, instance)
}

// syncDestroyFinalizer adds the finalizer of deletionPolicy Destroy, or removes it with Retain,
// and reports whether the instance changed.
func syncDestroyFinalizer(instance *dinovav1alpha1.WorkflowInstance) bool {
	if instance.Spec.DeletionPolicy == policyDestroy {
		return controllerutil.AddFinalizer(instance, destroyFinalizer)
	}
	return controllerutil.RemoveFinalizer(instance, destroyFinalizer)
}

// reconcileDeletion destroys the resources of an instance deleted with deletionPolicy Destroy:
// it waits for the run in progress, runs the template as a Destroy Workflow, and lets the
// instance go once that has succeeded. With Retain, set to give up after a failure, it lets the
// instance go at once.
func (h *Handler) reconcileDeletion(ctx context.Context, instance *dinovav1alpha1.WorkflowInstance) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(instance, destroyFinalizer) {
		return ctrl.Result{}, nil
	}
	if instance.Spec.DeletionPolicy != policyDestroy {
		controllerutil.RemoveFinalizer(instance, destroyFinalizer)
		return ctrl.Result{}, h.Client.Update(ctx, instance)
	}
	runs, err := h.runs(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if active, _ := recordRuns(instance, runs); active != nil {
		setReady(instance, metav1.ConditionFalse, "WaitingToDestroy",
			fmt.Sprintf("Run %s is in progress: the resources are destroyed once it has finished", active.Name))
		return ctrl.Result{}, h.Client.Status().Update(ctx, instance)
	}
	name := destroyRunName(instance)
	run := &dinovav1alpha1.Workflow{}
	err = h.Client.Get(ctx, types.NamespacedName{Namespace: instance.Namespace, Name: name}, run)
	switch {
	case apierrors.IsNotFound(err):
		return h.startDestroy(ctx, instance, name)
	case err != nil:
		return ctrl.Result{}, err
	}
	instance.Status.DestroyRun = name
	switch run.Status.Phase {
	case phaseSucceeded:
		logf.FromContext(ctx).Info("Destroyed WorkflowInstance resources", "workflow", name)
		controllerutil.RemoveFinalizer(instance, destroyFinalizer)
		return ctrl.Result{}, h.Client.Update(ctx, instance)
	case phaseFailed:
		setReady(instance, metav1.ConditionFalse, "DestroyFailed", fmt.Sprintf(
			"Destroy run %s failed: delete it to try again, or set deletionPolicy Retain to keep the resources and let the instance go", name))
	default:
		setReady(instance, metav1.ConditionFalse, "Destroying", fmt.Sprintf("Destroy run %s is in progress", name))
	}
	return ctrl.Result{}, h.Client.Status().Update(ctx, instance)
}

// startDestroy creates the Destroy Workflow of a deleted instance. It is not owned by the
// instance, so that it outlives it as the record of the destruction; its labels still name the
// instance.
func (h *Handler) startDestroy(ctx context.Context, instance *dinovav1alpha1.WorkflowInstance, name string) (ctrl.Result, error) {
	run := &dinovav1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: instance.Namespace, Name: name,
			Labels:      map[string]string{},
			Annotations: map[string]string{RevisionAnnotation: instance.Status.Revision, ModeAnnotation: modeDestroy},
		},
		Spec: dinovav1alpha1.WorkflowSpec{
			WorkspaceStorage: instance.Spec.WorkspaceStorage,
			TerraformMode:    modeDestroy,
			Templates: []dinovav1alpha1.WorkflowTemplateRun{{
				Name: templateRunName, TemplateRef: instance.Spec.TemplateRef, Parameters: instance.Spec.Parameters,
			}},
		},
	}
	maps.Copy(run.Labels, instance.Labels)
	run.Labels[InstanceLabel] = instance.Name
	if err := h.Client.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, err
	}
	logf.FromContext(ctx).Info("Started WorkflowInstance destroy run", "workflow", name)
	instance.Status.DestroyRun = name
	setReady(instance, metav1.ConditionFalse, "Destroying", fmt.Sprintf("Destroy run %s is in progress", name))
	return ctrl.Result{}, h.Client.Status().Update(ctx, instance)
}

// destroyRunName is "<instance>-destroy-<start of its UID>": the UID keeps apart the instances
// that had the same name, whose Destroy Workflows outlive them.
func destroyRunName(instance *dinovav1alpha1.WorkflowInstance) string {
	suffix := "-destroy-" + string(instance.UID)[:min(8, len(instance.UID))]
	name := instance.Name
	if len(name) > 253-len(suffix) {
		name = name[:253-len(suffix)]
	}
	return name + suffix
}

// DestroyRunInstance returns the instance a Destroy Workflow destroys the resources of, for a
// watch: it is not owned by the instance. Empty for any other Workflow.
func DestroyRunInstance(run *dinovav1alpha1.Workflow) string {
	if run.Annotations[ModeAnnotation] != modeDestroy {
		return ""
	}
	return run.Labels[InstanceLabel]
}

// runName is "<instance>-<number>", short enough for any instance name.
func runName(instance string, number int64) string {
	suffix := "-" + strconv.FormatInt(number, 10)
	if len(instance) > 253-len(suffix) {
		instance = instance[:253-len(suffix)]
	}
	return instance + suffix
}

func setReady(instance *dinovav1alpha1.WorkflowInstance, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: status, Reason: reason, Message: message, ObservedGeneration: instance.Generation,
	})
}

// TerraformTemplateNames returns the TerraformTemplates the steps of a WorkflowTemplate run,
// for TerraformTemplateIndex.
func TerraformTemplateNames(template *dinovav1alpha1.WorkflowTemplate) []string {
	var names []string
	for _, step := range template.Spec.Steps {
		if step.Terraform != nil {
			names = append(names, step.Terraform.TemplateRef.Name)
		}
	}
	return names
}
