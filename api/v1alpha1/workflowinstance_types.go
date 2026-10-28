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

// WorkflowInstanceSpec is the desired state of the resources of one client or project: a
// WorkflowTemplate run with fixed parameters, kept applied over time. The operator runs it as a
// new Workflow every time its code changes (a new revision of one of its TerraformTemplates,
// or an edit of the WorkflowTemplate or of this spec), and every interval in between, so that
// changes made outside it are corrected. The Workflows are its runs: one at a time, the newest
// kept as history.
type WorkflowInstanceSpec struct {
	// TemplateRef names the WorkflowTemplate to run, in the instance's namespace. Its steps run
	// again at every run, so an API step must be safe to repeat: a GET, or a step marked
	// idempotent. A template with API steps needs approval Auto (API steps have no plan) and
	// deletionPolicy Retain (a Destroy Workflow runs Terraform steps only), and no template may
	// have deleteSteps.
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// Parameters give a value to the parameters the template declares, as in a Workflow.
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +optional
	Parameters []WorkflowParameter `json:"parameters,omitempty"`
	// Interval is the time between two runs when nothing changes, for example "6h". A change
	// of the code starts a run at once, without waiting for it.
	// +kubebuilder:validation:Format=duration
	Interval metav1.Duration `json:"interval"`
	// RetryInterval is the time before running again after a failed run, when nothing changed.
	// It defaults to Interval.
	// +kubebuilder:validation:Format=duration
	// +optional
	RetryInterval *metav1.Duration `json:"retryInterval,omitempty"`
	// Approval says how a change is applied. Auto (the default): every run plans and applies.
	// Manual: every run only plans; when the plan has changes, the instance shows it as its
	// pending plan (status.pendingPlan, condition Drifted) and applies exactly that plan once
	// approvePlan names it. Runs plan one step at a time: the plan stops at the first step with
	// changes, and the next run, started at once after the apply, plans the following ones.
	// +kubebuilder:validation:Enum=Auto;Manual
	// +kubebuilder:default=Auto
	// +optional
	Approval string `json:"approval,omitempty"`
	// ApprovePlan approves the pending plan with this ID (status.pendingPlan.id). An approval
	// of another plan is ignored: a newer plan, or a change of what the instance runs, needs a
	// new approval.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ApprovePlan string `json:"approvePlan,omitempty"`
	// Suspend stops starting runs; a run already started goes on.
	// +optional
	Suspend bool `json:"suspend,omitempty"`
	// WorkspaceStorage is the workspace size of each run (see Workflow spec.workspaceStorage).
	// +kubebuilder:default="2Gi"
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]*(Mi|Gi|Ti)$`
	// +optional
	WorkspaceStorage string `json:"workspaceStorage,omitempty"`
	// SuccessfulRunsHistoryLimit is how many succeeded runs are kept.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=3
	// +optional
	SuccessfulRunsHistoryLimit int32 `json:"successfulRunsHistoryLimit,omitempty"`
	// FailedRunsHistoryLimit is how many failed runs are kept.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=3
	// +optional
	FailedRunsHistoryLimit int32 `json:"failedRunsHistoryLimit,omitempty"`
	// DeletionPolicy says what happens to the resources when the instance is deleted. Retain
	// (the default) leaves them, and their Terraform states, as they are. Destroy destroys them
	// first: once a run in progress has finished, the operator runs the template as a Destroy
	// Workflow (status.destroyRun), and keeps the instance until it succeeds. If it fails, the
	// instance stays, with the reason in its Ready condition: delete the Destroy Workflow to try
	// again, or set Retain to give up. The Destroy Workflow is not deleted with the instance: it
	// is the record that the resources were destroyed.
	// +kubebuilder:validation:Enum=Retain;Destroy
	// +kubebuilder:default=Retain
	// +optional
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
	// AcceptStateHolds is passed to every run (see the Workflow's acceptStateHolds): list the ID
	// of status.stateHold once the resources of its state have been checked, and the next run
	// starts at once.
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=128
	// +listType=set
	// +optional
	AcceptStateHolds []string `json:"acceptStateHolds,omitempty"`
}

// WorkflowInstanceStateHold is a hold on a Terraform state that waits to be accepted.
type WorkflowInstanceStateHold struct {
	// Reason is StateLockReleased or UnsavedResources, ID what to list in acceptStateHolds.
	Reason string `json:"reason"`
	ID     string `json:"id"`
	// Run and Step are where the hold stopped the instance, StateKey the state held.
	Run      string `json:"run"`
	Step     string `json:"step"`
	StateKey string `json:"stateKey"`
	// ReleasedStateLock is the removed lock (reason StateLockReleased), UnsavedResources the
	// resources not saved (reason UnsavedResources).
	// +optional
	ReleasedStateLock *WorkflowReleasedStateLock `json:"releasedStateLock,omitempty"`
	// +optional
	UnsavedResources *WorkflowUnsavedResources `json:"unsavedResources,omitempty"`
}

// WorkflowInstanceStatus is what the instance runs and what its last runs did.
type WorkflowInstanceStatus struct {
	// ObservedGeneration is the generation of the spec this status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Revision is the SHA-256 of what a run would run now: this spec, the WorkflowTemplate and
	// the current revision of each TerraformTemplate, as "sha256:<hex>". It changes when any of
	// them changes, and that starts a run.
	// +optional
	Revision string `json:"revision,omitempty"`
	// TerraformRevisions are the current revisions of the TerraformTemplates, by step.
	// +listType=map
	// +listMapKey=step
	// +optional
	TerraformRevisions []WorkflowInstanceTerraformRevision `json:"terraformRevisions,omitempty"`
	// LastAttemptedRevision is the revision of the last run that started, and
	// LastAppliedRevision that of the last run that succeeded.
	// +optional
	LastAttemptedRevision string `json:"lastAttemptedRevision,omitempty"`
	// +optional
	LastAppliedRevision string `json:"lastAppliedRevision,omitempty"`
	// Runs counts the runs started; the next one is named <instance>-<runs+1>.
	// +optional
	Runs int64 `json:"runs,omitempty"`
	// LastRun is the newest run, ActiveRun the one still running, if any.
	// +optional
	LastRun string `json:"lastRun,omitempty"`
	// +optional
	ActiveRun string `json:"activeRun,omitempty"`
	// LastRunTime is when the newest run started, LastRunPhase how it ended (Succeeded or
	// Failed) once it did.
	// +optional
	LastRunTime *metav1.Time `json:"lastRunTime,omitempty"`
	// +optional
	LastRunPhase string `json:"lastRunPhase,omitempty"`
	// PendingPlan is the plan waiting for approval (approval Manual): the newest plan with
	// changes of the current revision. Approve it by setting spec.approvePlan to its ID.
	// +optional
	PendingPlan *WorkflowInstancePendingPlan `json:"pendingPlan,omitempty"`
	// NextRunTime is when the next run starts if nothing changes before.
	// +optional
	NextRunTime *metav1.Time `json:"nextRunTime,omitempty"`
	// DestroyRun is the Destroy Workflow that destroys the resources of an instance being
	// deleted with deletionPolicy Destroy.
	// +optional
	DestroyRun string `json:"destroyRun,omitempty"`
	// StateHold is what stops the runs from applying: the newest run that applies failed
	// because one of its states is held (see the Workflow's acceptStateHolds). Condition
	// StateHold is True while it is set.
	// +optional
	StateHold *WorkflowInstanceStateHold `json:"stateHold,omitempty"`
	// Conditions has Ready: True when the last run applied the current revision, or (approval
	// Manual) found nothing to change; False with the reason otherwise (a run is in progress or
	// failed, a plan waits for approval, the template is not supported or not ready, the
	// instance is suspended). With approval Manual it also has Drifted: True while a plan with
	// changes waits for approval. It has StateHold: True while status.stateHold is set.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// WorkflowInstancePendingPlan is a saved plan with changes, waiting for approval.
type WorkflowInstancePendingPlan struct {
	// ID is the name of the Plan run that saved the plan: the value of spec.approvePlan that
	// approves it.
	ID string `json:"id"`
	// UID is the UID of that run.
	UID string `json:"uid"`
	// Revision is the instance revision the plan was made for.
	Revision string `json:"revision"`
	// Step is the Terraform step with changes; the steps after it are planned once it is
	// applied.
	Step string `json:"step"`
	// Add, Change and Destroy summarize the plan.
	Add     int32 `json:"add"`
	Change  int32 `json:"change"`
	Destroy int32 `json:"destroy"`
	// Secret holds the readable plan, under the key plan.txt.
	Secret string `json:"secret"`
	// Time is when the plan was made.
	Time metav1.Time `json:"time"`
}

// WorkflowInstanceTerraformRevision is the revision of the TerraformTemplate a step runs.
type WorkflowInstanceTerraformRevision struct {
	// Step is the name of the Terraform step.
	Step string `json:"step"`
	// Template is the TerraformTemplate, Revision and Digest its current revision.
	Template string `json:"template"`
	Revision int64  `json:"revision"`
	Digest   string `json:"digest"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Template",type=string,JSONPath=".spec.templateRef.name"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Drifted",type=string,JSONPath=".status.conditions[?(@.type=='Drifted')].status"
// +kubebuilder:printcolumn:name="Pending plan",type=string,JSONPath=".status.pendingPlan.id"
// +kubebuilder:printcolumn:name="Last run",type=string,JSONPath=".status.lastRun"
// +kubebuilder:printcolumn:name="Next run",type=string,JSONPath=".status.nextRunTime"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// WorkflowInstance keeps a WorkflowTemplate applied for one client or project, running it
// again when its code changes and every interval.
type WorkflowInstance struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WorkflowInstance
	// +required
	Spec WorkflowInstanceSpec `json:"spec"`

	// status defines the observed state of WorkflowInstance
	// +optional
	Status WorkflowInstanceStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkflowInstanceList contains a list of WorkflowInstance
type WorkflowInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkflowInstance `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkflowInstance{}, &WorkflowInstanceList{})
}
