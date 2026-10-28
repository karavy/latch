/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkflowPipelineSpec is a whole process, for example the activation of a client, made of
// stages that run in the order of their dependencies. A stage is either a WorkflowInstance,
// for Terraform resources kept applied over time, or a Workflow, for operations that run once,
// such as API requests. The operator creates the stage objects, named
// "<pipeline>-<stage>" (a Workflow stage adds the number of its run), when the stages they
// depend on are ready, and passes outputs from one stage to the parameters of the next.
// +kubebuilder:validation:XValidation:rule="self.stages.all(s, !has(s.dependsOn) || s.dependsOn.all(d, d != s.name && self.stages.exists(t, t.name == d)))",message="dependsOn must name other stages of the pipeline"
type WorkflowPipelineSpec struct {
	// Parameters are given to every stage whose WorkflowTemplate declares a parameter of that
	// name, for example the client name. A stage's own parameters take precedence.
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +optional
	Parameters []WorkflowParameter `json:"parameters,omitempty"`
	// Stages are the steps of the process. A stage starts once all the stages it depends on
	// are ready; stages that do not depend on each other run at the same time.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	// +listType=map
	// +listMapKey=name
	Stages []WorkflowPipelineStage `json:"stages"`
	// Rerun lists Workflows of Workflow stages to run again (status.stages[].workflow): a stage
	// whose run failed, or that is Outdated because a value it used changed. The stage starts a
	// new run with the current values. An entry that is not the current run of a stage is
	// ignored, so a stage runs again only once for each entry.
	// +kubebuilder:validation:MaxItems=20
	// +kubebuilder:validation:items:MaxLength=253
	// +listType=set
	// +optional
	Rerun []string `json:"rerun,omitempty"`
}

// WorkflowPipelineStage is one stage of a pipeline: a WorkflowInstance or a Workflow.
// +kubebuilder:validation:XValidation:rule="has(self.instance) != has(self.workflow)",message="exactly one of instance or workflow must be set"
type WorkflowPipelineStage struct {
	// Name identifies the stage, and names its objects "<pipeline>-<name>".
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Name string `json:"name"`
	// DependsOn names the stages that must be ready before this one starts: a WorkflowInstance
	// whose Ready condition is True, or a Workflow that succeeded. A stage that reads the
	// outputs of another (valueFrom) must depend on it, directly or through other stages.
	// +kubebuilder:validation:MaxItems=20
	// +kubebuilder:validation:items:MaxLength=40
	// +listType=set
	// +optional
	DependsOn []string `json:"dependsOn,omitempty"`
	// Parameters give a value to the parameters the stage's WorkflowTemplate declares: a fixed
	// value, or an output of an earlier stage.
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +optional
	Parameters []WorkflowPipelineParameter `json:"parameters,omitempty"`
	// Instance makes the stage a WorkflowInstance: Terraform steps only, applied again when
	// their code or values change and every interval. When an output it reads changes, its
	// revision changes, and it runs again by itself.
	// +optional
	Instance *WorkflowPipelineInstanceStage `json:"instance,omitempty"`
	// Workflow makes the stage a Workflow that runs once: API and Terraform steps. When a
	// value it used changes, it does not run again by itself: it becomes Outdated, unless
	// rerunOnChange says otherwise.
	// +optional
	Workflow *WorkflowPipelineWorkflowStage `json:"workflow,omitempty"`
}

// WorkflowPipelineParameter gives a value to a parameter of a stage's WorkflowTemplate.
// +kubebuilder:validation:XValidation:rule="has(self.value) != has(self.valueFrom)",message="exactly one of value or valueFrom must be set"
type WorkflowPipelineParameter struct {
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// Value is a fixed value.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Value *string `json:"value,omitempty"`
	// ValueFrom reads an output of an earlier stage.
	// +optional
	ValueFrom *WorkflowPipelineOutputReference `json:"valueFrom,omitempty"`
}

// WorkflowPipelineOutputReference names an output published by a step of an earlier stage. A
// string output is passed as it is; any other value (a number, a list, an object) as its JSON
// text, which the template can read with asNumber or asJSON. Sensitive outputs cannot be
// passed: the value would be written in the spec of the stage's object.
type WorkflowPipelineOutputReference struct {
	// Stage is the stage that publishes the output. The stage that reads it must depend on it.
	// For a Workflow stage the output is read from its current run, once it succeeded; for a
	// WorkflowInstance stage, from its newest run that succeeded and published it.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Stage string `json:"stage"`
	// Step is the step of the stage's WorkflowTemplate, and Output the output it publishes.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Step string `json:"step"`
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	Output string `json:"output"`
}

// WorkflowPipelineInstanceStage is a stage run as a WorkflowInstance. Its fields are those of
// the WorkflowInstance spec (see there); the pipeline keeps them on the instance, so set them
// here, not on the instance.
type WorkflowPipelineInstanceStage struct {
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// +kubebuilder:validation:Format=duration
	Interval metav1.Duration `json:"interval"`
	// +kubebuilder:validation:Format=duration
	// +optional
	RetryInterval *metav1.Duration `json:"retryInterval,omitempty"`
	// +kubebuilder:validation:Enum=Auto;Manual
	// +kubebuilder:default=Auto
	// +optional
	Approval string `json:"approval,omitempty"`
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ApprovePlan string `json:"approvePlan,omitempty"`
	// +optional
	Suspend bool `json:"suspend,omitempty"`
	// +kubebuilder:default="2Gi"
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]*(Mi|Gi|Ti)$`
	// +optional
	WorkspaceStorage string `json:"workspaceStorage,omitempty"`
	// +kubebuilder:validation:Enum=Retain;Destroy
	// +kubebuilder:default=Retain
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=128
	// +listType=set
	// +optional
	AcceptStateHolds []string `json:"acceptStateHolds,omitempty"`
}

// WorkflowPipelineWorkflowStage is a stage run as a Workflow, once.
type WorkflowPipelineWorkflowStage struct {
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// +kubebuilder:default="2Gi"
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]*(Mi|Gi|Ti)$`
	// +optional
	WorkspaceStorage string `json:"workspaceStorage,omitempty"`
	// RerunOnChange runs the stage again, as a new Workflow, when a value it used changes,
	// instead of marking it Outdated. It is refused unless every API step of the template is
	// safe to repeat: a GET, or a step marked idempotent.
	// +optional
	RerunOnChange bool `json:"rerunOnChange,omitempty"`
	// AcceptStateHolds is passed to the runs (see the Workflow's acceptStateHolds).
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=128
	// +listType=set
	// +optional
	AcceptStateHolds []string `json:"acceptStateHolds,omitempty"`
}

// WorkflowPipelineChangedParameter is a value a Workflow stage used that changed since.
type WorkflowPipelineChangedParameter struct {
	Name string `json:"name"`
	// Old is the value of the current run, New the value a new run would get.
	Old string `json:"old"`
	New string `json:"new"`
}

// WorkflowPipelineStageStatus is where one stage is.
type WorkflowPipelineStageStatus struct {
	Name string `json:"name"`
	// Phase is Pending (waiting for the stages it depends on, or for an output), Running,
	// Ready (the instance is Ready, or the Workflow succeeded), Outdated (the Workflow
	// succeeded, but a value it used changed since), Failed, or Deleting.
	// +kubebuilder:validation:Enum=Pending;Running;Ready;Outdated;Failed;Deleting
	Phase string `json:"phase"`
	// Instance is the WorkflowInstance of an instance stage.
	// +optional
	Instance string `json:"instance,omitempty"`
	// Workflow is the current run of a Workflow stage, and Runs how many it started.
	// +optional
	Workflow string `json:"workflow,omitempty"`
	// +optional
	Runs int64 `json:"runs,omitempty"`
	// Changed lists the values that changed since the current run of an Outdated stage.
	// +listType=atomic
	// +optional
	Changed []WorkflowPipelineChangedParameter `json:"changed,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// WorkflowPipelineStatus is where the stages of the pipeline are.
type WorkflowPipelineStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Stages has one entry per stage, in the order of the spec.
	// +listType=map
	// +listMapKey=name
	// +optional
	Stages []WorkflowPipelineStageStatus `json:"stages,omitempty"`
	// Conditions has Ready: True when every stage is Ready, False with the first stage that is
	// not, otherwise.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +kubebuilder:validation:XValidation:rule="self.spec.stages.all(s, size(self.metadata.name) + size(s.name) <= 55)",message="the pipeline name and each stage name are at most 55 characters together: they name the stage objects, which are used in labels"

// WorkflowPipeline runs a process made of WorkflowInstances and Workflows, in the order of
// their dependencies, passing outputs from one stage to the next.
type WorkflowPipeline struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WorkflowPipeline
	// +required
	Spec WorkflowPipelineSpec `json:"spec"`

	// status defines the observed state of WorkflowPipeline
	// +optional
	Status WorkflowPipelineStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkflowPipelineList contains a list of WorkflowPipeline
type WorkflowPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkflowPipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkflowPipeline{}, &WorkflowPipelineList{})
}
