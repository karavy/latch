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
// +kubebuilder:validation:XValidation:rule="(has(self.terraformMode) && self.terraformMode == 'ApplyPlan') == has(self.applyPlan)",message="applyPlan is set exactly when terraformMode is ApplyPlan"
// +kubebuilder:validation:XValidation:rule="!has(self.terraformMode) || self.terraformMode == 'Apply' || size(self.templates) == 1",message="a Plan, ApplyPlan or Destroy workflow runs exactly one template"
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
	// TerraformMode says what the Terraform steps do. Apply (the default) plans and applies
	// each step. Plan only plans: it saves the plan of each step, and stops at the first step
	// with changes, whose plan an ApplyPlan workflow can then apply; a step without changes
	// publishes its outputs from its state, so that the next step can be planned. ApplyPlan
	// applies exactly the saved plan named by applyPlan, and nothing else. Destroy destroys
	// what the steps manage: it first reads, in order, the outputs of each step from its state
	// (the later steps may need them for their variables or their state key), then destroys the
	// steps from the last to the first; a step without a state, or with an empty one, is
	// skipped, since it has nothing to destroy. Plan, ApplyPlan and Destroy run exactly one
	// template, with Terraform steps only.
	// +kubebuilder:validation:Enum=Apply;Plan;ApplyPlan;Destroy
	// +optional
	TerraformMode string `json:"terraformMode,omitempty"`
	// ApplyPlan names the saved plan an ApplyPlan workflow applies.
	// +optional
	ApplyPlan *WorkflowPlanReference `json:"applyPlan,omitempty"`
	// AcceptStateHolds lists the IDs of the holds on Terraform states that someone has checked.
	// A state is held when resources may exist in the cloud without being in it: its lock was
	// removed after its runner died (WorkflowReleasedStateLock), or an apply that failed began
	// to create resources it did not save (WorkflowUnsavedResources). A Terraform step that
	// applies to a held state fails, with reason StateLockReleased or UnsavedResources, until
	// the ID of the hold is listed here; plans and destroys still run. Before listing it, find
	// those resources and import or delete them.
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=128
	// +listType=set
	// +optional
	AcceptStateHolds []string `json:"acceptStateHolds,omitempty"`
}

// WorkflowPlanReference names the saved plan of a Terraform step of a Plan workflow.
type WorkflowPlanReference struct {
	// Workflow and UID identify the Plan workflow that saved the plan. The plan stays in the
	// state storage after that workflow is deleted.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Workflow string `json:"workflow"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	UID string `json:"uid"`
	// Step is the Terraform step whose plan is applied. The steps before it publish their
	// outputs from their states; the steps after it are skipped.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Step string `json:"step"`
}

// WorkflowStepPlan summarizes the plan of a Terraform step, as terraform plan does. A replaced
// resource counts in Add and in Destroy.
type WorkflowStepPlan struct {
	// HasChanges is true when the plan changes something, even only outputs.
	HasChanges bool  `json:"hasChanges"`
	Add        int32 `json:"add"`
	Change     int32 `json:"change"`
	Destroy    int32 `json:"destroy"`
	// Secret holds the readable plan, under the key plan.txt. It is a Secret because a plan
	// shows the values of the resources.
	// +optional
	Secret string `json:"secret,omitempty"`
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
	// +kubebuilder:validation:Enum=Pending;Running;Polling;Recovering;Succeeded;Failed;Unknown;Skipped
	Phase string `json:"phase"`
	// +optional
	APICallName string `json:"apiCallName,omitempty"`
	// TerraformRevision is the revision of the TerraformTemplate a Terraform step ran, and
	// TerraformDigest the SHA-256 of its files (see TerraformTemplate status).
	// +optional
	TerraformRevision int64 `json:"terraformRevision,omitempty"`
	// +optional
	TerraformDigest string `json:"terraformDigest,omitempty"`
	// Plan is what the Terraform step of a Plan workflow found.
	// +optional
	Plan *WorkflowStepPlan `json:"plan,omitempty"`
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
	// RenderedFiles is the prefix of the object keys, in the bucket of the state storage, under
	// which the Job of a Terraform step stored the files it ran: the .tf files rendered from the
	// template, terraform.auto.tfvars.json and a manifest.json that says which run they belong
	// to. They are stored before init, so a run that fails keeps them too, and they are never
	// deleted by the operator.
	// +optional
	RenderedFiles string `json:"renderedFiles,omitempty"`
	// ReleasedStateLock is the lock that a Terraform Job had left on the step's state when it
	// died without releasing it, and that the operator removed (see WorkflowReleasedStateLock):
	// by this step, or by an earlier run, when the step failed with reason StateLockReleased.
	// +optional
	ReleasedStateLock *WorkflowReleasedStateLock `json:"releasedStateLock,omitempty"`
	// UnsavedResources are the resources the step's apply began to create without saving them
	// in the state, or, when the step failed with reason UnsavedResources before its Job, those
	// an earlier apply left (see WorkflowUnsavedResources).
	// +optional
	UnsavedResources *WorkflowUnsavedResources `json:"unsavedResources,omitempty"`
	// SensitiveOutputs lists the output names whose values live in the workflow's
	// outputs Secret, under the key "<step>.<output>". Values are never shown here.
	// +kubebuilder:validation:MaxItems=32
	// +listType=set
	// +optional
	SensitiveOutputs []string `json:"sensitiveOutputs,omitempty"`
	// Recoveries lists, in order, every recover request the step's poll sent (see
	// WorkflowPollRecover).
	// +kubebuilder:validation:MaxItems=50
	// +listType=atomic
	// +optional
	Recoveries []WorkflowStepRecovery `json:"recoveries,omitempty"`
}

// WorkflowReleasedStateLock records a state lock the operator removed. It removes a lock only
// when the latch runner wrote it ("runner@<pod>"), the pod belongs to a Job of a Workflow that
// has finished, and no pod of that name runs: the runner was killed, ran out of memory, lost its
// node, or was stopped at the Job's deadline while a request hung. Removing the lock does not
// repair the state: resources created after the last save of the state are not in it, and the
// next apply may create them again. So the operator records the removal next to the state
// ("<state key>.released-lock"), and no step applies to that state until the lock's ID is
// listed in acceptStateHolds; plans and destroys still run.
type WorkflowReleasedStateLock struct {
	// ID, Who and Created are those of the lock file.
	ID      string      `json:"id"`
	Who     string      `json:"who"`
	Created metav1.Time `json:"created"`
	// ReleasedAt is when the operator removed it.
	ReleasedAt metav1.Time `json:"releasedAt"`
	// ReleasedBy is the Workflow ("<namespace>/<name>") whose step removed it.
	// +optional
	ReleasedBy string `json:"releasedBy,omitempty"`
}

// WorkflowUnsavedResources records the resources an apply began to create and that are not in
// the state when it ended, failed or interrupted: the API may have created them (a create
// accepted whose wait failed, an interruption at the Job's time limit), so applying again may
// create them twice. The runner records them next to the state ("<state key>.unsaved-resources")
// and no step applies to that state until the ID is listed in acceptStateHolds. A create the API
// refused outright (a 4xx answer to the create request) is not recorded.
type WorkflowUnsavedResources struct {
	// ID identifies the record, for acceptStateHolds.
	ID string `json:"id"`
	// Resources are the resource addresses, as in the Terraform code.
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	Resources []string `json:"resources"`
	// Who is the runner that wrote the record ("runner@<pod>"), DetectedAt when.
	Who        string      `json:"who"`
	DetectedAt metav1.Time `json:"detectedAt"`
	// RecordedBy is the Workflow ("<namespace>/<name>") whose step wrote it.
	// +optional
	RecordedBy string `json:"recordedBy,omitempty"`
}

// WorkflowStepRecovery records one recovery of a polled operation that failed.
type WorkflowStepRecovery struct {
	// Number counts the recoveries of the step, from 1.
	Number int32 `json:"number"`
	// FailedJSON is the value of the poll's condition field that started the recovery, as
	// JSON, for example "failed".
	FailedJSON string `json:"failedJSON"`
	// ProgressJSON is the value at recover.progressPointer in the failed poll response, as
	// JSON, for example 5.
	// +optional
	ProgressJSON string `json:"progressJSON,omitempty"`
	// APICallName is the ApiCall of the recover request.
	APICallName string `json:"apiCallName"`
	// StartedAt is when the recover request was created.
	StartedAt metav1.Time `json:"startedAt"`
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
