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

// WorkflowSpec runs WorkflowTemplates one after the other, giving values to their parameters.
// It is immutable: a Workflow is the record of one run, so a different run needs a new Workflow.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Workflow spec is immutable; create a new workflow to change what it runs"
type WorkflowSpec struct {
	// Templates run strictly in order. A template starts only after the one before it has
	// completed, and the first one that fails stops the Workflow. When the Workflow is deleted,
	// the deleteSteps of the templates that ran are executed in the opposite order.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	// +listType=map
	// +listMapKey=name
	Templates []WorkflowTemplateRun `json:"templates"`
	// WorkspaceStorage requests, for each template that has a Terraform step, a PVC for the
	// Terraform working files (rendered templates, providers). Terraform state does not live
	// here: see WorkflowTerraformState.
	// +kubebuilder:default="2Gi"
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]*(Mi|Gi|Ti)$`
	// +optional
	WorkspaceStorage string `json:"workspaceStorage,omitempty"`
}

// WorkflowTemplateRun is one use of a WorkflowTemplate by a Workflow.
type WorkflowTemplateRun struct {
	// Name identifies this use inside the Workflow, and prefixes the names of the objects it
	// creates. The same template can be used twice under two names.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Name string `json:"name"`
	// TemplateRef names the WorkflowTemplate to run, in the Workflow's namespace.
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// Parameters give a value to the parameters the template declares. A parameter that
	// the template does not declare is refused, and so is a missing one that has no default.
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +optional
	Parameters []WorkflowParameter `json:"parameters,omitempty"`
}

// WorkflowParameter is a named string value.
type WorkflowParameter struct {
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// +kubebuilder:validation:MaxLength=1024
	Value string `json:"value"`
}

// WorkflowStepStatus records execution progress. Besides the declared non-sensitive
// outputs, no API response data is copied into the Workflow status.
type WorkflowStepStatus struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=Pending;Running;Polling;Succeeded;Failed;Unknown;Skipped
	Phase string `json:"phase"`
	// +optional
	APICallName string `json:"apiCallName,omitempty"`
	// +optional
	JobName string `json:"jobName,omitempty"`
	// +optional
	Attempts int32 `json:"attempts,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// Outputs holds the step's non-sensitive output values, JSON-encoded, by name.
	// +kubebuilder:validation:MaxProperties=32
	// +optional
	Outputs map[string]string `json:"outputs,omitempty"`
	// StateKey is the object key of the step's Terraform state in the state storage.
	// +optional
	StateKey string `json:"stateKey,omitempty"`
	// SensitiveOutputs lists the output names whose values live in the workflow's
	// outputs Secret, under the key "<step>.<output>". Values are never shown here.
	// +kubebuilder:validation:MaxItems=32
	// +listType=set
	// +optional
	SensitiveOutputs []string `json:"sensitiveOutputs,omitempty"`
}

// WorkflowTemplateStatus records the progress of one template of the Workflow. Besides the
// declared non-sensitive outputs, no API response data is copied into the status.
type WorkflowTemplateStatus struct {
	// Name is the name of the template use, from spec.templates.
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Unknown
	Phase string `json:"phase"`
	// TemplateGeneration is the generation of the WorkflowTemplate when it started. If the
	// template is edited while it runs, the Workflow fails instead of mixing two versions.
	// +optional
	TemplateGeneration int64 `json:"templateGeneration,omitempty"`
	// Index of the next step of the template; zero-based.
	// +optional
	CurrentStep int32 `json:"currentStep,omitempty"`
	// Name of the Secret holding this template's sensitive outputs, if any.
	// +optional
	OutputsSecretName string `json:"outputsSecretName,omitempty"`
	// PVC for the Terraform files of this template.
	// +optional
	WorkspacePVCName string `json:"workspacePVCName,omitempty"`
	// +listType=map
	// +listMapKey=name
	// +optional
	Steps []WorkflowStepStatus `json:"steps,omitempty"`
	// Deletion records the template's deleteSteps once the Workflow is being deleted.
	// +optional
	Deletion *WorkflowTemplateDeletionStatus `json:"deletion,omitempty"`
}

// WorkflowTemplateDeletionStatus records the progress of the deleteSteps of one template.
type WorkflowTemplateDeletionStatus struct {
	// +kubebuilder:validation:Enum=Running;Succeeded;Failed
	// +optional
	Phase string `json:"phase,omitempty"`
	// Index of the next delete step; zero-based.
	// +optional
	CurrentStep int32 `json:"currentStep,omitempty"`
	// +listType=map
	// +listMapKey=name
	// +optional
	Steps []WorkflowStepStatus `json:"steps,omitempty"`
}

// WorkflowDeletionStatus records the progress of the cleanup once the Workflow is deleted.
type WorkflowDeletionStatus struct {
	// +kubebuilder:validation:Enum=Running;Succeeded;Failed
	// +optional
	Phase string `json:"phase,omitempty"`
	// Index of the template being cleaned up; zero-based. It goes down, from the last
	// template that ran to the first.
	// +optional
	CurrentTemplate int32 `json:"currentTemplate,omitempty"`
}

// WorkflowStatus defines the observed state of Workflow.
type WorkflowStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Unknown
	// +optional
	Phase string `json:"phase,omitempty"`
	// Index of the template that runs next; zero-based.
	// +optional
	CurrentTemplate int32 `json:"currentTemplate,omitempty"`
	// +listType=map
	// +listMapKey=name
	// +optional
	Templates []WorkflowTemplateStatus `json:"templates,omitempty"`
	// Deletion is set once the Workflow is being deleted and a template that ran has deleteSteps.
	// +optional
	Deletion *WorkflowDeletionStatus `json:"deletion,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Template",type=integer,JSONPath=".status.currentTemplate"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// Workflow is the Schema for the workflows API
type Workflow struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Workflow
	// +required
	Spec WorkflowSpec `json:"spec"`

	// status defines the observed state of Workflow
	// +optional
	Status WorkflowStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkflowList contains a list of Workflow
type WorkflowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Workflow `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Workflow{}, &WorkflowList{})
}
