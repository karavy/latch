package workflow

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// templateRun is one WorkflowTemplate of a Workflow, being run or cleaned up. It is the unit
// the step machinery works on: it holds the Workflow that owns everything created, the steps
// of the template, the parameter values to use, and this template's part of the status.
type templateRun struct {
	owner       *dinovav1alpha1.Workflow
	name        string // the use of the template, spec.templates[].name
	steps       []dinovav1alpha1.WorkflowStep
	deleteSteps []dinovav1alpha1.WorkflowStep
	parameters  []dinovav1alpha1.WorkflowParameter
	status      *dinovav1alpha1.WorkflowTemplateStatus
}

func newTemplateRun(owner *dinovav1alpha1.Workflow, name string, template *dinovav1alpha1.WorkflowTemplate, parameters []dinovav1alpha1.WorkflowParameter, status *dinovav1alpha1.WorkflowTemplateStatus) *templateRun {
	return &templateRun{
		owner: owner, name: name, parameters: parameters, status: status,
		steps: template.Spec.Steps, deleteSteps: template.Spec.DeleteSteps,
	}
}

func (run *templateRun) deleting() bool { return !run.owner.DeletionTimestamp.IsZero() }

// advance moves on to the next step of the section that is running.
func (run *templateRun) advance() {
	if run.deleting() && run.status.Deletion != nil {
		run.status.Deletion.CurrentStep++
		return
	}
	run.status.CurrentStep++
}

// findStep looks a step up by name among the steps and, once the Workflow is being
// deleted, the delete steps. Names are unique across both.
func (run *templateRun) findStep(name string) *dinovav1alpha1.WorkflowStepStatus {
	for i := range run.status.Steps {
		if run.status.Steps[i].Name == name {
			return &run.status.Steps[i]
		}
	}
	if run.status.Deletion != nil {
		for i := range run.status.Deletion.Steps {
			if run.status.Deletion.Steps[i].Name == name {
				return &run.status.Deletion.Steps[i]
			}
		}
	}
	return nil
}

// childName names an object created for a step. The name of the template use is part of it,
// so two templates can have steps with the same name.
func (run *templateRun) childName(step, suffix string) string {
	return workflowChildName(run.owner, run.name+"-"+step, suffix)
}

// effectiveParameters returns the values a template runs with: the ones the Workflow gives,
// or the defaults. A value for a parameter the template does not declare is refused, and so
// is a missing one without a default, so a typo cannot silently run with the wrong value.
func effectiveParameters(template *dinovav1alpha1.WorkflowTemplate, stage dinovav1alpha1.WorkflowTemplateRun) ([]dinovav1alpha1.WorkflowParameter, error) {
	declared := make(map[string]dinovav1alpha1.WorkflowTemplateParameter, len(template.Spec.Parameters))
	for _, parameter := range template.Spec.Parameters {
		declared[parameter.Name] = parameter
	}
	given := make(map[string]string, len(stage.Parameters))
	for _, parameter := range stage.Parameters {
		if _, ok := declared[parameter.Name]; !ok {
			return nil, fmt.Errorf("template %q has no parameter %q", template.Name, parameter.Name)
		}
		given[parameter.Name] = parameter.Value
	}
	values := make([]dinovav1alpha1.WorkflowParameter, 0, len(declared))
	for _, parameter := range template.Spec.Parameters {
		switch value, ok := given[parameter.Name]; {
		case ok:
			values = append(values, dinovav1alpha1.WorkflowParameter{Name: parameter.Name, Value: value})
		case parameter.Default != nil:
			values = append(values, dinovav1alpha1.WorkflowParameter{Name: parameter.Name, Value: *parameter.Default})
		default:
			return nil, fmt.Errorf("parameter %q of template %q has no value and no default", parameter.Name, template.Name)
		}
	}
	return values, nil
}

func setWorkflowCondition(workflow *dinovav1alpha1.Workflow, status metav1.ConditionStatus, conditionType, reason, message string) {
	meta.SetStatusCondition(&workflow.Status.Conditions, metav1.Condition{
		Type: conditionType, Status: status, ObservedGeneration: workflow.Generation,
		Reason: reason, Message: message,
	})
}

func workflowChildName(workflow *dinovav1alpha1.Workflow, step, suffix string) string {
	uid := string(workflow.UID)
	if len(uid) > 6 {
		uid = uid[:6]
	}
	base := strings.Trim(dnsSanitizer.ReplaceAllString(strings.ToLower(workflow.Name+"-"+step), "-"), "-")
	tail := "-" + uid + "-" + suffix
	maxBase := 63 - len(tail)
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return base + tail
}
