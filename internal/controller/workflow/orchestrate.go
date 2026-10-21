package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// Reconcile runs the templates of a Workflow one after the other, and, once the Workflow is
// deleted, cleans them up in the opposite order.
func (r *Handler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	workflow := &dinovav1alpha1.Workflow{}
	if err := r.Get(ctx, req.NamespacedName, workflow); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !workflow.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, workflow)
	}
	if workflow.Status.ObservedGeneration != workflow.Generation {
		workflow.Status = dinovav1alpha1.WorkflowStatus{
			ObservedGeneration: workflow.Generation,
			Phase:              workflowPhasePending,
			Templates:          initialTemplateStatuses(workflow.Spec.Templates),
		}
		setWorkflowCondition(workflow, metav1.ConditionTrue, "Progressing", "Started", "Workflow execution initialized")
		if err := r.Status().Update(ctx, workflow); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if workflow.Status.Phase == workflowPhaseSucceeded || workflow.Status.Phase == workflowPhaseFailed || workflow.Status.Phase == workflowPhaseUnknown {
		return ctrl.Result{}, nil
	}
	if int(workflow.Status.CurrentTemplate) >= len(workflow.Spec.Templates) {
		workflow.Status.Phase = workflowPhaseSucceeded
		setWorkflowCondition(workflow, metav1.ConditionTrue, "Ready", "Completed", "All workflow templates completed")
		return ctrl.Result{}, r.Status().Update(ctx, workflow)
	}
	stage := workflow.Spec.Templates[workflow.Status.CurrentTemplate]
	status := findTemplateStatus(workflow, stage.Name)
	if status == nil {
		return ctrl.Result{}, fmt.Errorf("workflow status has no entry for template %q", stage.Name)
	}
	template := &dinovav1alpha1.WorkflowTemplate{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: workflow.Namespace, Name: stage.TemplateRef.Name}, template); err != nil {
		if apierrors.IsNotFound(err) {
			return r.failStage(ctx, workflow, status, "WorkflowTemplateNotFound", fmt.Sprintf("Referenced WorkflowTemplate %q was not found", stage.TemplateRef.Name))
		}
		return ctrl.Result{}, err
	}
	parameters, err := effectiveParameters(template, stage)
	if err == nil && status.Phase == stepPhasePending {
		err = validateTemplate(template)
	}
	if err != nil {
		return r.failStage(ctx, workflow, status, "InvalidTemplate", err.Error())
	}
	if status.Phase == stepPhasePending {
		return r.startTemplate(ctx, workflow, status, template)
	}
	if template.Generation != status.TemplateGeneration {
		return r.failStage(ctx, workflow, status, "TemplateChanged",
			fmt.Sprintf("WorkflowTemplate %q was edited while the workflow was running it", template.Name))
	}
	run := newTemplateRun(workflow, stage.Name, template, parameters, status)
	if int(status.CurrentStep) >= len(run.steps) {
		status.Phase = workflowPhaseSucceeded
		workflow.Status.CurrentTemplate++
		if int(workflow.Status.CurrentTemplate) >= len(workflow.Spec.Templates) {
			workflow.Status.Phase = workflowPhaseSucceeded
			setWorkflowCondition(workflow, metav1.ConditionTrue, "Ready", "Completed", "All workflow templates completed")
		}
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, workflow)
	}
	workflow.Status.Phase = workflowPhaseRunning
	step := run.steps[status.CurrentStep]
	stepStatus := run.findStep(step.Name)
	if stepStatus == nil {
		return ctrl.Result{}, fmt.Errorf("workflow status has no entry for step %q", step.Name)
	}
	if step.API != nil {
		return r.reconcileAPIStep(ctx, run, step, stepStatus)
	}
	if step.Terraform != nil {
		return r.reconcileTerraformStep(ctx, run, step, stepStatus)
	}
	return r.failWorkflow(ctx, run, stepStatus, "InvalidStep", "Workflow step has no operation")
}

// startTemplate begins the template the Workflow has reached. The finalizer goes in first,
// when the template has delete steps: it is what lets them run before the object disappears.
func (r *Handler) startTemplate(ctx context.Context, workflow *dinovav1alpha1.Workflow, status *dinovav1alpha1.WorkflowTemplateStatus, template *dinovav1alpha1.WorkflowTemplate) (ctrl.Result, error) {
	if len(template.Spec.DeleteSteps) > 0 && !controllerutil.ContainsFinalizer(workflow, deletionFinalizer) {
		controllerutil.AddFinalizer(workflow, deletionFinalizer)
		if err := r.Update(ctx, workflow); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	status.Phase = workflowPhaseRunning
	status.TemplateGeneration = template.Generation
	status.Steps = initialStepStatuses(template.Spec.Steps)
	workflow.Status.Phase = workflowPhaseRunning
	return ctrl.Result{Requeue: true}, r.Status().Update(ctx, workflow)
}

// validateTemplate checks, before any step of a template runs, that its references hold.
func validateTemplate(template *dinovav1alpha1.WorkflowTemplate) error {
	allSteps := append(slices.Clone(template.Spec.Steps), template.Spec.DeleteSteps...)
	return errors.Join(validateReferences(allSteps), validateParameters(template.Spec), validateRepeats(allSteps))
}

func initialTemplateStatuses(templates []dinovav1alpha1.WorkflowTemplateRun) []dinovav1alpha1.WorkflowTemplateStatus {
	statuses := make([]dinovav1alpha1.WorkflowTemplateStatus, 0, len(templates))
	for _, template := range templates {
		statuses = append(statuses, dinovav1alpha1.WorkflowTemplateStatus{Name: template.Name, Phase: stepPhasePending})
	}
	return statuses
}

func findTemplateStatus(workflow *dinovav1alpha1.Workflow, name string) *dinovav1alpha1.WorkflowTemplateStatus {
	for i := range workflow.Status.Templates {
		if workflow.Status.Templates[i].Name == name {
			return &workflow.Status.Templates[i]
		}
	}
	return nil
}

// failStage ends the Workflow as failed, at a template that cannot run.
func (r *Handler) failStage(ctx context.Context, workflow *dinovav1alpha1.Workflow, status *dinovav1alpha1.WorkflowTemplateStatus, reason, message string) (ctrl.Result, error) {
	status.Phase = workflowPhaseFailed
	workflow.Status.Phase = workflowPhaseFailed
	setWorkflowCondition(workflow, metav1.ConditionFalse, "Ready", reason, message)
	return ctrl.Result{}, r.Status().Update(ctx, workflow)
}

// failDeletion stops the cleanup. The Workflow stays in Kubernetes with its finalizer, so
// cleanup that did not happen is never forgotten; removing the finalizer by hand abandons it.
func (r *Handler) failDeletion(ctx context.Context, workflow *dinovav1alpha1.Workflow, reason, message string) (ctrl.Result, error) {
	workflow.Status.Deletion.Phase = workflowPhaseFailed
	setWorkflowCondition(workflow, metav1.ConditionFalse, "Deletion", reason, message)
	return ctrl.Result{}, r.Status().Update(ctx, workflow)
}

// lastStartedTemplate is the index of the last template that began, or -1.
func lastStartedTemplate(workflow *dinovav1alpha1.Workflow) int32 {
	last := int32(-1)
	for i, template := range workflow.Spec.Templates {
		if status := findTemplateStatus(workflow, template.Name); status != nil && status.Phase != stepPhasePending {
			last = int32(i)
		}
	}
	return last
}

// reconcileDeletion runs the delete steps of the templates that ran, from the last one to the
// first, then lets the Workflow go. A failed delete step leaves the Workflow in place.
func (r *Handler) reconcileDeletion(ctx context.Context, workflow *dinovav1alpha1.Workflow) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(workflow, deletionFinalizer) {
		return ctrl.Result{}, nil
	}
	if workflow.Status.Deletion == nil {
		workflow.Status.Deletion = &dinovav1alpha1.WorkflowDeletionStatus{
			Phase: workflowPhaseRunning, CurrentTemplate: lastStartedTemplate(workflow),
		}
		setWorkflowCondition(workflow, metav1.ConditionTrue, "Deletion", "Started", "Running the delete steps")
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, workflow)
	}
	deletion := workflow.Status.Deletion
	if deletion.Phase == workflowPhaseFailed {
		return ctrl.Result{}, nil
	}
	for deletion.CurrentTemplate >= 0 {
		stage := workflow.Spec.Templates[deletion.CurrentTemplate]
		status := findTemplateStatus(workflow, stage.Name)
		if status == nil {
			return ctrl.Result{}, fmt.Errorf("workflow status has no entry for template %q", stage.Name)
		}
		template := &dinovav1alpha1.WorkflowTemplate{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: workflow.Namespace, Name: stage.TemplateRef.Name}, template); err != nil {
			if apierrors.IsNotFound(err) {
				return r.failDeletion(ctx, workflow, "WorkflowTemplateNotFound",
					fmt.Sprintf("WorkflowTemplate %q, needed to clean up %q, was not found", stage.TemplateRef.Name, stage.Name))
			}
			return ctrl.Result{}, err
		}
		if len(template.Spec.DeleteSteps) == 0 {
			deletion.CurrentTemplate--
			continue
		}
		parameters, err := effectiveParameters(template, stage)
		if err != nil {
			return r.failDeletion(ctx, workflow, "InvalidTemplate", err.Error())
		}
		run := newTemplateRun(workflow, stage.Name, template, parameters, status)
		return r.cleanUpTemplate(ctx, run)
	}
	deletion.Phase = workflowPhaseSucceeded
	setWorkflowCondition(workflow, metav1.ConditionTrue, "Deletion", "Completed", "All delete steps completed")
	if err := r.Status().Update(ctx, workflow); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(workflow, deletionFinalizer)
	return ctrl.Result{}, r.Update(ctx, workflow)
}

// cleanUpTemplate runs the next delete step of one template, and moves on to the template
// before it when they are all done.
func (r *Handler) cleanUpTemplate(ctx context.Context, run *templateRun) (ctrl.Result, error) {
	deletion := run.status.Deletion
	if deletion == nil {
		run.status.Deletion = &dinovav1alpha1.WorkflowTemplateDeletionStatus{
			Phase: workflowPhaseRunning, Steps: initialStepStatuses(run.deleteSteps),
		}
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
	}
	if int(deletion.CurrentStep) >= len(run.deleteSteps) {
		deletion.Phase = workflowPhaseSucceeded
		run.owner.Status.Deletion.CurrentTemplate--
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, run.owner)
	}
	step := run.deleteSteps[deletion.CurrentStep]
	state := run.findStep(step.Name)
	if state == nil || step.API == nil {
		return ctrl.Result{}, fmt.Errorf("workflow status has no entry for delete step %q", step.Name)
	}
	return r.reconcileAPIStep(ctx, run, step, state)
}

// validateRepeats checks the steps that repeat their own request from inside their ApiCall: a
// poll without a template has no other request to build, and it brings its own retry rules, so
// the step's retry cannot be added to them.
func validateRepeats(steps []dinovav1alpha1.WorkflowStep) error {
	for _, step := range steps {
		if step.API == nil || step.API.Poll == nil || separatePoll(step.API) {
			continue
		}
		switch {
		case step.API.Retry != nil:
			return fmt.Errorf("step %q has retry and a poll without templateRef: the poll already says which failures to tolerate", step.Name)
		case len(step.API.Poll.Inputs) > 0:
			return fmt.Errorf("step %q has a poll without templateRef, which sends the step's own request again: its inputs belong to the step", step.Name)
		}
	}
	return nil
}
