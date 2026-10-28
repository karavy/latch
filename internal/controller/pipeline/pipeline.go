// Package pipeline runs WorkflowPipelines: it creates the WorkflowInstances and Workflows of
// their stages in the order of their dependencies, and passes outputs from one stage to the
// parameters of the next.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/instance"
	"github.com/karavy/latch/internal/controller/workflow"
)

const (
	// PipelineLabel names, on the objects of a stage, the pipeline, and StageLabel the stage.
	PipelineLabel = "dinova.one/workflow-pipeline"
	StageLabel    = "dinova.one/pipeline-stage"
	// RunAnnotation is the number of a run of a Workflow stage.
	RunAnnotation = "dinova.one/pipeline-run"
	// TemplateIndex indexes WorkflowPipelines by the WorkflowTemplates their stages run.
	TemplateIndex = "spec.stages.templateRef.name"
	// finalizer keeps a pipeline until the objects of its stages are deleted, in the opposite
	// order of their dependencies.
	finalizer = "dinova.one/pipeline-deletion"

	// templateRunName is the name of the only template use in the Workflow of a stage, as in
	// the runs of a WorkflowInstance.
	templateRunName = "run"
	conditionReady  = "Ready"

	phasePending   = "Pending"
	phaseRunning   = "Running"
	phaseReady     = "Ready"
	phaseOutdated  = "Outdated"
	phaseFailed    = "Failed"
	phaseDeleting  = "Deleting"
	phaseSucceeded = "Succeeded"
)

// problem is why a stage cannot get its values or start now: pending until something changes
// upstream, or failed until the spec or a template changes.
type problem struct {
	reason, message string
	failed          bool
}

func (p *problem) apply(status *dinovav1alpha1.WorkflowPipelineStageStatus) {
	status.Phase = phasePending
	if p.failed {
		status.Phase = phaseFailed
	}
	status.Message = p.message
}

// Handler reconciles WorkflowPipelines.
type Handler struct {
	Client client.Client
	Scheme *runtime.Scheme
}

// children are the objects of the stages of a pipeline.
type children struct {
	instances map[string]*dinovav1alpha1.WorkflowInstance
	// runs are the Workflows of each Workflow stage, newest first.
	runs map[string][]dinovav1alpha1.Workflow
}

func (c *children) present(stage string) bool {
	return c.instances[stage] != nil || len(c.runs[stage]) > 0
}

// Reconcile starts the stages whose dependencies are ready, keeps the instances of the stages
// in line with the spec and the outputs they read, and sets the status of each stage.
func (h *Handler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pipeline := &dinovav1alpha1.WorkflowPipeline{}
	if err := h.Client.Get(ctx, req.NamespacedName, pipeline); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !pipeline.DeletionTimestamp.IsZero() {
		return h.reconcileDeletion(ctx, pipeline)
	}
	if controllerutil.AddFinalizer(pipeline, finalizer) {
		return ctrl.Result{Requeue: true}, h.Client.Update(ctx, pipeline)
	}
	pipeline.Status.ObservedGeneration = pipeline.Generation
	order, err := stageOrder(pipeline)
	if err != nil {
		setReady(pipeline, metav1.ConditionFalse, "InvalidPipeline", err.Error())
		return ctrl.Result{}, h.Client.Status().Update(ctx, pipeline)
	}
	kids, err := h.children(ctx, pipeline)
	if err != nil {
		return ctrl.Result{}, err
	}
	previous := map[string]dinovav1alpha1.WorkflowPipelineStageStatus{}
	for _, status := range pipeline.Status.Stages {
		previous[status.Name] = status
	}
	statuses := map[string]*dinovav1alpha1.WorkflowPipelineStageStatus{}
	ready := map[string]bool{}
	for _, stage := range order {
		status := &dinovav1alpha1.WorkflowPipelineStageStatus{Name: stage.Name, Runs: previous[stage.Name].Runs}
		statuses[stage.Name] = status
		if stage.Instance != nil {
			err = h.instanceStage(ctx, pipeline, stage, kids, ready, status)
		} else {
			err = h.workflowStage(ctx, pipeline, stage, kids, ready, status)
		}
		if err != nil {
			return ctrl.Result{}, err
		}
		ready[stage.Name] = status.Phase == phaseReady || status.Phase == phaseOutdated
	}
	pipeline.Status.Stages = pipeline.Status.Stages[:0]
	for _, stage := range pipeline.Spec.Stages {
		pipeline.Status.Stages = append(pipeline.Status.Stages, *statuses[stage.Name])
	}
	setPipelineReady(pipeline)
	return ctrl.Result{}, h.Client.Status().Update(ctx, pipeline)
}

// stageOrder checks the dependencies of the stages and returns the stages so that each comes
// after those it depends on, otherwise in the order of the spec.
func stageOrder(pipeline *dinovav1alpha1.WorkflowPipeline) ([]dinovav1alpha1.WorkflowPipelineStage, error) {
	stages := map[string]dinovav1alpha1.WorkflowPipelineStage{}
	for _, stage := range pipeline.Spec.Stages {
		stages[stage.Name] = stage
	}
	for _, stage := range pipeline.Spec.Stages {
		for _, dependency := range stage.DependsOn {
			if _, ok := stages[dependency]; !ok || dependency == stage.Name {
				return nil, fmt.Errorf("stage %q depends on %q, which is not another stage of the pipeline", stage.Name, dependency)
			}
		}
	}
	var order []dinovav1alpha1.WorkflowPipelineStage
	placed := map[string]bool{}
	for len(order) < len(pipeline.Spec.Stages) {
		progress := false
		for _, stage := range pipeline.Spec.Stages {
			if !placed[stage.Name] && allPlaced(stage.DependsOn, placed) {
				order = append(order, stage)
				placed[stage.Name] = true
				progress = true
			}
		}
		if !progress {
			var cycle []string
			for _, stage := range pipeline.Spec.Stages {
				if !placed[stage.Name] {
					cycle = append(cycle, stage.Name)
				}
			}
			return nil, fmt.Errorf("stages %s depend on each other", strings.Join(cycle, ", "))
		}
	}
	for _, stage := range pipeline.Spec.Stages {
		upstream := ancestors(stages, stage.Name)
		for _, parameter := range stage.Parameters {
			if from := parameter.ValueFrom; from != nil && !upstream[from.Stage] {
				return nil, fmt.Errorf("parameter %q of stage %q reads an output of stage %q, which the stage does not depend on", parameter.Name, stage.Name, from.Stage)
			}
		}
	}
	return order, nil
}

func allPlaced(names []string, placed map[string]bool) bool {
	for _, name := range names {
		if !placed[name] {
			return false
		}
	}
	return true
}

// ancestors returns the stages a stage depends on, directly or through other stages.
func ancestors(stages map[string]dinovav1alpha1.WorkflowPipelineStage, name string) map[string]bool {
	found := map[string]bool{}
	pending := slices.Clone(stages[name].DependsOn)
	for len(pending) > 0 {
		next := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if !found[next] {
			found[next] = true
			pending = append(pending, stages[next].DependsOn...)
		}
	}
	return found
}

// children lists the objects of the stages of a pipeline: those it controls, by the stage label.
// The runs of a stage's instance carry the labels of the instance, but the instance controls them.
func (h *Handler) children(ctx context.Context, pipeline *dinovav1alpha1.WorkflowPipeline) (*children, error) {
	kids := &children{instances: map[string]*dinovav1alpha1.WorkflowInstance{}, runs: map[string][]dinovav1alpha1.Workflow{}}
	selector := []client.ListOption{client.InNamespace(pipeline.Namespace), client.MatchingLabels{PipelineLabel: pipeline.Name}}
	instances := &dinovav1alpha1.WorkflowInstanceList{}
	if err := h.Client.List(ctx, instances, selector...); err != nil {
		return nil, err
	}
	for i := range instances.Items {
		if item := &instances.Items[i]; metav1.IsControlledBy(item, pipeline) {
			kids.instances[item.Labels[StageLabel]] = item
		}
	}
	runs := &dinovav1alpha1.WorkflowList{}
	if err := h.Client.List(ctx, runs, selector...); err != nil {
		return nil, err
	}
	for _, run := range runs.Items {
		if metav1.IsControlledBy(&run, pipeline) {
			kids.runs[run.Labels[StageLabel]] = append(kids.runs[run.Labels[StageLabel]], run)
		}
	}
	for stage := range kids.runs {
		sort.Slice(kids.runs[stage], func(i, j int) bool {
			return number(&kids.runs[stage][i], RunAnnotation) > number(&kids.runs[stage][j], RunAnnotation)
		})
	}
	return kids, nil
}

func number(run *dinovav1alpha1.Workflow, annotation string) int64 {
	value, _ := strconv.ParseInt(run.Annotations[annotation], 10, 64)
	return value
}

func finished(run *dinovav1alpha1.Workflow) bool {
	return run.Status.Phase == phaseSucceeded || run.Status.Phase == phaseFailed
}

// waitingFor returns the stages a stage depends on that are not ready.
func waitingFor(stage dinovav1alpha1.WorkflowPipelineStage, ready map[string]bool) []string {
	var waiting []string
	for _, dependency := range stage.DependsOn {
		if !ready[dependency] {
			waiting = append(waiting, dependency)
		}
	}
	return waiting
}

func waitingProblem(waiting []string) *problem {
	return &problem{reason: "WaitingForStages", message: "Waiting for stages " + strings.Join(waiting, ", ")}
}

// template reads the WorkflowTemplate of a stage.
func (h *Handler) template(ctx context.Context, namespace, name string) (*dinovav1alpha1.WorkflowTemplate, *problem, error) {
	template := &dinovav1alpha1.WorkflowTemplate{}
	err := h.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, template)
	if apierrors.IsNotFound(err) {
		return nil, &problem{reason: "WorkflowTemplateNotFound", message: fmt.Sprintf("WorkflowTemplate %q was not found", name), failed: true}, nil
	}
	if err != nil {
		return nil, nil, err
	}
	return template, nil, nil
}

// values returns the parameters of a stage's run: those of the pipeline that its template
// declares, then its own, with the outputs they read from earlier stages, sorted by name.
func (h *Handler) values(ctx context.Context, pipeline *dinovav1alpha1.WorkflowPipeline, stage dinovav1alpha1.WorkflowPipelineStage,
	template *dinovav1alpha1.WorkflowTemplate, kids *children) ([]dinovav1alpha1.WorkflowParameter, *problem, error) {
	declared := map[string]bool{}
	for _, parameter := range template.Spec.Parameters {
		declared[parameter.Name] = true
	}
	values := map[string]string{}
	for _, parameter := range pipeline.Spec.Parameters {
		if declared[parameter.Name] {
			values[parameter.Name] = parameter.Value
		}
	}
	for _, parameter := range stage.Parameters {
		if !declared[parameter.Name] {
			return nil, &problem{reason: "UndeclaredParameter", failed: true, message: fmt.Sprintf(
				"Parameter %q is not declared by WorkflowTemplate %q", parameter.Name, template.Name)}, nil
		}
		if parameter.Value != nil {
			values[parameter.Name] = *parameter.Value
			continue
		}
		value, blocked, err := h.output(ctx, pipeline, *parameter.ValueFrom, kids)
		if err != nil || blocked != nil {
			return nil, blocked, err
		}
		values[parameter.Name] = value
	}
	parameters := make([]dinovav1alpha1.WorkflowParameter, 0, len(values))
	for _, name := range slices.Sorted(maps.Keys(values)) {
		parameters = append(parameters, dinovav1alpha1.WorkflowParameter{Name: name, Value: values[name]})
	}
	return parameters, nil, nil
}

// output reads an output of an earlier stage, as a parameter value: the text of a string, the
// JSON of anything else. It reads what the Workflow engine published (workflow.StepOutput):
// from the current run of a Workflow stage once it succeeded, from the newest succeeded run
// of an instance that published it.
func (h *Handler) output(ctx context.Context, pipeline *dinovav1alpha1.WorkflowPipeline, from dinovav1alpha1.WorkflowPipelineOutputReference,
	kids *children) (string, *problem, error) {
	var candidates []dinovav1alpha1.Workflow
	upstream := stageNamed(pipeline, from.Stage)
	if upstream.Workflow != nil {
		runs := kids.runs[from.Stage]
		if len(runs) == 0 || runs[0].Status.Phase != phaseSucceeded {
			return "", &problem{reason: "WaitingForOutput", message: fmt.Sprintf("Waiting for stage %s to succeed, for output %s of step %s", from.Stage, from.Output, from.Step)}, nil
		}
		candidates = runs[:1]
	} else if item := kids.instances[from.Stage]; item != nil {
		runs, err := h.instanceRuns(ctx, item)
		if err != nil {
			return "", nil, err
		}
		candidates = runs
	}
	for i := range candidates {
		run := &candidates[i]
		if run.Status.Phase != phaseSucceeded {
			continue
		}
		raw, sensitive, found := workflow.StepOutput(run, templateRunName, from.Step, from.Output)
		switch {
		case sensitive:
			return "", &problem{reason: "SensitiveOutput", failed: true, message: fmt.Sprintf(
				"Output %s of step %s of stage %s is sensitive: a pipeline does not pass it, since it would be written in the spec of the stage", from.Output, from.Step, from.Stage)}, nil
		case !found:
			continue
		}
		value := string(raw)
		var text string
		if json.Unmarshal(raw, &text) == nil {
			value = text
		}
		if len(value) > 1024 {
			return "", &problem{reason: "OutputTooLong", failed: true, message: fmt.Sprintf(
				"Output %s of step %s of stage %s is longer than a parameter can be (1024 characters)", from.Output, from.Step, from.Stage)}, nil
		}
		return value, nil, nil
	}
	if upstream.Workflow != nil {
		return "", &problem{reason: "OutputMissing", failed: true, message: fmt.Sprintf(
			"Step %s of stage %s did not publish output %s", from.Step, from.Stage, from.Output)}, nil
	}
	return "", &problem{reason: "WaitingForOutput", message: fmt.Sprintf(
		"Waiting for a run of stage %s that publishes output %s of step %s", from.Stage, from.Output, from.Step)}, nil
}

func stageNamed(pipeline *dinovav1alpha1.WorkflowPipeline, name string) dinovav1alpha1.WorkflowPipelineStage {
	for _, stage := range pipeline.Spec.Stages {
		if stage.Name == name {
			return stage
		}
	}
	return dinovav1alpha1.WorkflowPipelineStage{}
}

// instanceRuns lists the runs of an instance, newest first.
func (h *Handler) instanceRuns(ctx context.Context, item *dinovav1alpha1.WorkflowInstance) ([]dinovav1alpha1.Workflow, error) {
	list := &dinovav1alpha1.WorkflowList{}
	if err := h.Client.List(ctx, list, client.InNamespace(item.Namespace), client.MatchingLabels{instance.InstanceLabel: item.Name}); err != nil {
		return nil, err
	}
	runs := make([]dinovav1alpha1.Workflow, 0, len(list.Items))
	for _, run := range list.Items {
		if metav1.IsControlledBy(&run, item) {
			runs = append(runs, run)
		}
	}
	sort.Slice(runs, func(i, j int) bool {
		return number(&runs[i], instance.RunAnnotation) > number(&runs[j], instance.RunAnnotation)
	})
	return runs, nil
}

// labels are those of the objects of a stage: the pipeline's, and the pipeline and the stage.
func labels(pipeline *dinovav1alpha1.WorkflowPipeline, stage string) map[string]string {
	result := maps.Clone(pipeline.Labels)
	if result == nil {
		result = map[string]string{}
	}
	result[PipelineLabel] = pipeline.Name
	result[StageLabel] = stage
	return result
}

// instanceStage creates the WorkflowInstance of a stage once its dependencies are ready, then
// keeps its spec in line with the stage and with the outputs it reads: a changed output changes
// the instance's revision, and the instance applies again by itself.
func (h *Handler) instanceStage(ctx context.Context, pipeline *dinovav1alpha1.WorkflowPipeline, stage dinovav1alpha1.WorkflowPipelineStage,
	kids *children, ready map[string]bool, status *dinovav1alpha1.WorkflowPipelineStageStatus) error {
	status.Instance = childName(pipeline.Name, stage.Name)
	current := kids.instances[stage.Name]
	if current == nil {
		if waiting := waitingFor(stage, ready); len(waiting) > 0 {
			waitingProblem(waiting).apply(status)
			return nil
		}
	}
	template, blocked, err := h.template(ctx, pipeline.Namespace, stage.Instance.TemplateRef.Name)
	if err != nil {
		return err
	}
	var parameters []dinovav1alpha1.WorkflowParameter
	if blocked == nil {
		parameters, blocked, err = h.values(ctx, pipeline, stage, template, kids)
		if err != nil {
			return err
		}
	}
	if current == nil {
		if blocked != nil {
			blocked.apply(status)
			return nil
		}
		created := &dinovav1alpha1.WorkflowInstance{
			ObjectMeta: metav1.ObjectMeta{Namespace: pipeline.Namespace, Name: status.Instance, Labels: labels(pipeline, stage.Name)},
		}
		setInstanceSpec(&created.Spec, stage.Instance, parameters)
		if err := controllerutil.SetControllerReference(pipeline, created, h.Scheme); err != nil {
			return err
		}
		if err := h.Client.Create(ctx, created); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		logf.FromContext(ctx).Info("Created WorkflowInstance of pipeline stage", "stage", stage.Name, "instance", created.Name)
		status.Phase, status.Message = phaseRunning, fmt.Sprintf("Created WorkflowInstance %s", created.Name)
		return nil
	}
	// Values that cannot be read now (an upstream stage running again) leave those of the
	// instance as they are.
	if blocked != nil {
		parameters = current.Spec.Parameters
	}
	desired := current.Spec.DeepCopy()
	setInstanceSpec(desired, stage.Instance, parameters)
	if !equality.Semantic.DeepEqual(desired, &current.Spec) {
		current.Spec = *desired
		if err := h.Client.Update(ctx, current); err != nil {
			return err
		}
		logf.FromContext(ctx).Info("Updated WorkflowInstance of pipeline stage", "stage", stage.Name, "instance", current.Name)
	}
	instancePhase(current, status)
	if blocked != nil && blocked.failed {
		blocked.apply(status)
	}
	return nil
}

// setInstanceSpec sets the fields of an instance spec that the stage manages. The others (the
// history limits) keep their values, so that defaults do not make the spec differ.
func setInstanceSpec(spec *dinovav1alpha1.WorkflowInstanceSpec, stage *dinovav1alpha1.WorkflowPipelineInstanceStage, parameters []dinovav1alpha1.WorkflowParameter) {
	spec.TemplateRef = stage.TemplateRef
	spec.Parameters = parameters
	spec.Interval = stage.Interval
	spec.RetryInterval = stage.RetryInterval
	spec.Approval = stage.Approval
	spec.ApprovePlan = stage.ApprovePlan
	spec.Suspend = stage.Suspend
	spec.WorkspaceStorage = stage.WorkspaceStorage
	spec.DeletionPolicy = stage.DeletionPolicy
	spec.AcceptStateHolds = stage.AcceptStateHolds
}

// instancePhase sets the phase of an instance stage from the Ready condition of the instance.
func instancePhase(item *dinovav1alpha1.WorkflowInstance, status *dinovav1alpha1.WorkflowPipelineStageStatus) {
	condition := meta.FindStatusCondition(item.Status.Conditions, conditionReady)
	if condition == nil || condition.ObservedGeneration != item.Generation {
		status.Phase, status.Message = phaseRunning, fmt.Sprintf("Waiting for WorkflowInstance %s to take its spec", item.Name)
		return
	}
	status.Message = fmt.Sprintf("WorkflowInstance %s: %s", item.Name, condition.Message)
	switch {
	case condition.Status == metav1.ConditionTrue:
		status.Phase = phaseReady
	case item.Status.ActiveRun != "", slices.Contains([]string{"RunInProgress", "NotApplied", "WaitingForRevision"}, condition.Reason):
		status.Phase = phaseRunning
	case slices.Contains([]string{"PlanPending", "Suspended"}, condition.Reason):
		status.Phase = phasePending
	default:
		status.Phase = phaseFailed
	}
}

// workflowStage runs a Workflow stage once its dependencies are ready. A run that succeeded
// is not run again by itself: when the values it used change, the stage is Outdated, unless
// rerunOnChange runs it again. A run listed in spec.rerun is run again.
func (h *Handler) workflowStage(ctx context.Context, pipeline *dinovav1alpha1.WorkflowPipeline, stage dinovav1alpha1.WorkflowPipelineStage,
	kids *children, ready map[string]bool, status *dinovav1alpha1.WorkflowPipelineStageStatus) error {
	runs := kids.runs[stage.Name]
	var current *dinovav1alpha1.Workflow
	if len(runs) > 0 {
		current = &runs[0]
		status.Workflow = current.Name
		status.Runs = max(status.Runs, number(current, RunAnnotation))
	}
	template, blocked, err := h.template(ctx, pipeline.Namespace, stage.Workflow.TemplateRef.Name)
	if err != nil {
		return err
	}
	if blocked == nil && stage.Workflow.RerunOnChange {
		reason, err := workflow.UnrepeatableStep(ctx, h.Client, pipeline.Namespace, template)
		if err != nil {
			return err
		}
		if reason != "" {
			blocked = &problem{reason: "NotIdempotent", failed: true, message: "rerunOnChange needs every API step to be safe to repeat, and " + reason}
		}
	}
	waiting := waitingFor(stage, ready)
	start := func() error {
		if len(waiting) > 0 {
			waitingProblem(waiting).apply(status)
			return nil
		}
		parameters, blocked, err := h.values(ctx, pipeline, stage, template, kids)
		if err != nil || blocked != nil {
			if blocked != nil {
				blocked.apply(status)
			}
			return err
		}
		return h.startRun(ctx, pipeline, stage, parameters, status)
	}
	switch {
	case current == nil && blocked != nil:
		blocked.apply(status)
		return nil
	case current == nil:
		return start()
	case !finished(current):
		status.Phase, status.Message = phaseRunning, fmt.Sprintf("Workflow %s is running", current.Name)
		return nil
	}
	rerun := slices.Contains(pipeline.Spec.Rerun, current.Name)
	if current.Status.Phase == phaseFailed {
		if rerun && blocked == nil {
			return start()
		}
		status.Phase = phaseFailed
		status.Message = fmt.Sprintf("Workflow %s failed: %s. List %s in spec.rerun to run the stage again", current.Name, readyMessage(current), current.Name)
		if blocked != nil {
			status.Message = blocked.message
		}
		return nil
	}
	status.Phase, status.Message = phaseReady, fmt.Sprintf("Workflow %s succeeded", current.Name)
	if blocked != nil {
		if blocked.failed {
			blocked.apply(status)
		}
		return nil
	}
	parameters, unresolved, err := h.values(ctx, pipeline, stage, template, kids)
	if err != nil || unresolved != nil {
		// The values cannot be compared now: the run stays as it is.
		return err
	}
	status.Changed = changedParameters(current.Spec.Templates[0].Parameters, parameters)
	if rerun || len(status.Changed) > 0 && stage.Workflow.RerunOnChange {
		status.Changed = nil
		return start()
	}
	if len(status.Changed) > 0 {
		names := make([]string, 0, len(status.Changed))
		for _, change := range status.Changed {
			names = append(names, change.Name)
		}
		status.Phase = phaseOutdated
		status.Message = fmt.Sprintf("Workflow %s succeeded, but values it used changed since (%s): list %s in spec.rerun to run the stage again",
			current.Name, strings.Join(names, ", "), current.Name)
	}
	return nil
}

func readyMessage(run *dinovav1alpha1.Workflow) string {
	if condition := meta.FindStatusCondition(run.Status.Conditions, conditionReady); condition != nil && condition.Message != "" {
		return strings.TrimSuffix(condition.Message, ".")
	}
	return "see its status"
}

// changedParameters lists the values that differ between two sets of parameters.
func changedParameters(old, current []dinovav1alpha1.WorkflowParameter) []dinovav1alpha1.WorkflowPipelineChangedParameter {
	before, after := map[string]string{}, map[string]string{}
	for _, parameter := range old {
		before[parameter.Name] = parameter.Value
	}
	for _, parameter := range current {
		after[parameter.Name] = parameter.Value
	}
	names := maps.Clone(before)
	maps.Copy(names, after)
	var changed []dinovav1alpha1.WorkflowPipelineChangedParameter
	for _, name := range slices.Sorted(maps.Keys(names)) {
		if before[name] != after[name] {
			changed = append(changed, dinovav1alpha1.WorkflowPipelineChangedParameter{Name: name, Old: before[name], New: after[name]})
		}
	}
	return changed
}

// startRun creates the next run of a Workflow stage: a Workflow owned by the pipeline.
func (h *Handler) startRun(ctx context.Context, pipeline *dinovav1alpha1.WorkflowPipeline, stage dinovav1alpha1.WorkflowPipelineStage,
	parameters []dinovav1alpha1.WorkflowParameter, status *dinovav1alpha1.WorkflowPipelineStageStatus) error {
	next := status.Runs + 1
	run := &dinovav1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pipeline.Namespace, Name: childName(pipeline.Name, stage.Name) + "-" + strconv.FormatInt(next, 10),
			Labels: labels(pipeline, stage.Name), Annotations: map[string]string{RunAnnotation: strconv.FormatInt(next, 10)},
		},
		Spec: dinovav1alpha1.WorkflowSpec{
			Templates: []dinovav1alpha1.WorkflowTemplateRun{{
				Name: templateRunName, TemplateRef: stage.Workflow.TemplateRef, Parameters: parameters,
			}},
			WorkspaceStorage: stage.Workflow.WorkspaceStorage,
			AcceptStateHolds: stage.Workflow.AcceptStateHolds,
		},
	}
	if err := controllerutil.SetControllerReference(pipeline, run, h.Scheme); err != nil {
		return err
	}
	// A run left by an attempt whose status update failed has this same name: it is this run.
	if err := h.Client.Create(ctx, run); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	logf.FromContext(ctx).Info("Started Workflow of pipeline stage", "stage", stage.Name, "workflow", run.Name)
	status.Runs, status.Workflow = next, run.Name
	status.Phase, status.Message = phaseRunning, fmt.Sprintf("Workflow %s is running", run.Name)
	return nil
}

// childName is "<pipeline>-<stage>"; the spec keeps both short enough for labels.
func childName(pipeline, stage string) string {
	return pipeline + "-" + stage
}

// setPipelineReady sets Ready from the stages: the first failed stage, else the first outdated,
// else the first not ready.
func setPipelineReady(pipeline *dinovav1alpha1.WorkflowPipeline) {
	for _, check := range []struct {
		phases []string
		reason string
	}{
		{[]string{phaseFailed}, "StageFailed"},
		{[]string{phaseOutdated}, "StageOutdated"},
		{[]string{phasePending, phaseRunning}, "InProgress"},
	} {
		for _, stage := range pipeline.Status.Stages {
			if slices.Contains(check.phases, stage.Phase) {
				setReady(pipeline, metav1.ConditionFalse, check.reason, fmt.Sprintf("Stage %s: %s", stage.Name, stage.Message))
				return
			}
		}
	}
	setReady(pipeline, metav1.ConditionTrue, "StagesReady", fmt.Sprintf("All %d stages are ready", len(pipeline.Status.Stages)))
}

func setReady(pipeline *dinovav1alpha1.WorkflowPipeline, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&pipeline.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: status, Reason: reason, Message: message, ObservedGeneration: pipeline.Generation,
	})
}

// reconcileDeletion deletes the objects of the stages in the opposite order of their
// dependencies: a stage's objects go once no stage that depends on it has objects left. An
// instance with deletionPolicy Destroy destroys its resources first, and a Workflow runs its
// deleteSteps, so the pipeline waits for each to be gone. If the dependencies are not valid,
// everything is deleted at once.
func (h *Handler) reconcileDeletion(ctx context.Context, pipeline *dinovav1alpha1.WorkflowPipeline) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pipeline, finalizer) {
		return ctrl.Result{}, nil
	}
	kids, err := h.children(ctx, pipeline)
	if err != nil {
		return ctrl.Result{}, err
	}
	var remaining, deletable []string
	for _, stage := range pipeline.Spec.Stages {
		if kids.present(stage.Name) {
			remaining = append(remaining, stage.Name)
		}
	}
	if len(remaining) == 0 {
		controllerutil.RemoveFinalizer(pipeline, finalizer)
		return ctrl.Result{}, h.Client.Update(ctx, pipeline)
	}
	for _, name := range remaining {
		if !dependentPresent(pipeline, name, kids) {
			deletable = append(deletable, name)
		}
	}
	if _, err := stageOrder(pipeline); err != nil || len(deletable) == 0 {
		deletable = remaining
	}
	for _, name := range deletable {
		if err := h.deleteStage(ctx, name, kids); err != nil {
			return ctrl.Result{}, err
		}
	}
	for i := range pipeline.Status.Stages {
		stage := &pipeline.Status.Stages[i]
		switch {
		case slices.Contains(deletable, stage.Name):
			stage.Phase, stage.Message = phaseDeleting, "Deleting the objects of the stage"
		case slices.Contains(remaining, stage.Name):
			stage.Phase, stage.Message = phaseDeleting, "Waiting for the stages that depend on it to be deleted"
		}
	}
	setReady(pipeline, metav1.ConditionFalse, "Deleting", "Deleting stages "+strings.Join(remaining, ", "))
	return ctrl.Result{}, h.Client.Status().Update(ctx, pipeline)
}

// dependentPresent reports whether a stage that depends on this one still has objects.
func dependentPresent(pipeline *dinovav1alpha1.WorkflowPipeline, name string, kids *children) bool {
	for _, stage := range pipeline.Spec.Stages {
		if slices.Contains(stage.DependsOn, name) && kids.present(stage.Name) {
			return true
		}
	}
	return false
}

func (h *Handler) deleteStage(ctx context.Context, stage string, kids *children) error {
	var objects []client.Object
	if item := kids.instances[stage]; item != nil {
		objects = append(objects, item)
	}
	for i := range kids.runs[stage] {
		objects = append(objects, &kids.runs[stage][i])
	}
	for _, object := range objects {
		if !object.GetDeletionTimestamp().IsZero() {
			continue
		}
		if err := h.Client.Delete(ctx, object, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			return err
		}
		logf.FromContext(ctx).Info("Deleted object of pipeline stage", "stage", stage, "name", object.GetName())
	}
	return nil
}

// TemplateNames returns the WorkflowTemplates the stages of a pipeline run, for TemplateIndex.
func TemplateNames(pipeline *dinovav1alpha1.WorkflowPipeline) []string {
	var names []string
	for _, stage := range pipeline.Spec.Stages {
		switch {
		case stage.Instance != nil:
			names = append(names, stage.Instance.TemplateRef.Name)
		case stage.Workflow != nil:
			names = append(names, stage.Workflow.TemplateRef.Name)
		}
	}
	return names
}
