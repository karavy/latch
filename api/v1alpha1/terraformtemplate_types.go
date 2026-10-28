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

// TerraformTemplateSpec is the reusable definition of the Terraform/OpenTofu files of
// one workflow step. A Workflow step references it by name and runs its current revision
// (see TerraformTemplateStatus): the operator snapshots the files every time they change, so
// each run records exactly which code it ran.
type TerraformTemplateSpec struct {
	// Files are the Terraform files. Names ending in .tf.tmpl are rendered by
	// terraform-engine (<FGT_XXX> placeholders and <FGT_FOR_STR> loops) against the
	// step's input variables; plain .tf files are used as they are.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	Files []TerraformFile `json:"files"`
	// RevisionHistoryLimit is how many revisions are kept, the current one included. Older
	// revisions are deleted; a run that used one keeps its own copy of the files.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=10
	// +optional
	RevisionHistoryLimit int32 `json:"revisionHistoryLimit,omitempty"`
}

// TerraformFile is one file of a TerraformTemplate, read from a key of an immutable ConfigMap.
type TerraformFile struct {
	// Name is a plain file name (no directories) ending in .tf or .tf.tmpl.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*\.tf(\.tmpl)?$`
	// +kubebuilder:validation:MaxLength=128
	Name string `json:"name"`
	// ConfigMapKeyRef reads the file body from a key of a ConfigMap in the template's
	// namespace. The ConfigMap must be immutable (immutable: true), so that the code of a
	// revision can never change: to change it, create a new ConfigMap (or delete and recreate
	// this one), and the operator makes a new revision.
	// +required
	ConfigMapKeyRef WorkflowConfigMapKeyReference `json:"configMapKeyRef"`
}

// TerraformTemplateStatus is the current revision of the files, made by the operator.
type TerraformTemplateStatus struct {
	// ObservedGeneration is the generation of the spec this status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Revision is the number of the current revision. The operator gives a new number every
	// time the content of the files changes; files with the content of an older revision that
	// is still kept bring that revision back.
	// +optional
	Revision int64 `json:"revision,omitempty"`
	// RevisionName names the ControllerRevision that holds the files of the current revision.
	// +optional
	RevisionName string `json:"revisionName,omitempty"`
	// Digest is the SHA-256 of the files of the current revision, as "sha256:<hex>".
	// +optional
	Digest string `json:"digest,omitempty"`
	// Sources are the ConfigMaps the current revision was read from.
	// +listType=atomic
	// +optional
	Sources []TerraformTemplateSource `json:"sources,omitempty"`
	// Conditions has Ready: True when the current revision matches the spec and its
	// ConfigMaps, False with the reason when the files cannot be read (a ConfigMap or a key is
	// missing, or a ConfigMap is not immutable). A Workflow runs a template only when it is
	// Ready.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// TerraformTemplateSource is a ConfigMap a revision was read from.
type TerraformTemplateSource struct {
	// Name of the ConfigMap.
	Name string `json:"name"`
	// UID of the ConfigMap: a ConfigMap deleted and created again has another one.
	UID string `json:"uid"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Revision",type=integer,JSONPath=".status.revision"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// TerraformTemplate is the Schema for the terraformtemplates API
type TerraformTemplate struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the template
	// +required
	Spec TerraformTemplateSpec `json:"spec"`

	// status is the current revision of the files
	// +optional
	Status TerraformTemplateStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TerraformTemplateList contains a list of TerraformTemplate
type TerraformTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TerraformTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TerraformTemplate{}, &TerraformTemplateList{})
}
