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
// +kubebuilder:validation:XValidation:rule="has(self.git) || (has(self.files) && size(self.files) > 0)",message="set git, files or both"
type TerraformTemplateSpec struct {
	// Files are the Terraform files read from ConfigMaps. Names ending in .tf.tmpl are
	// rendered by terraform-engine (<FGT_XXX> placeholders and <FGT_FOR_STR> loops) against
	// the step's input variables; plain .tf files are used as they are. They can be used
	// together with Git, for example to add a file that only Latch needs to a module read
	// from git; a name may not come from both.
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +optional
	Files []TerraformFile `json:"files,omitempty"`
	// Git reads the Terraform files from one directory of a git repository.
	// +optional
	Git *TerraformGitSource `json:"git,omitempty"`
	// RevisionHistoryLimit is how many revisions are kept, the current one included. Older
	// revisions are deleted; a run that used one keeps its own copy of the files.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=10
	// +optional
	RevisionHistoryLimit int32 `json:"revisionHistoryLimit,omitempty"`
	// AllowMutableSources accepts ConfigMaps without immutable: true, so that the files can be
	// edited in place while debugging. Every edit still makes a new revision, and a run still
	// records the revision it ran; but an edit reaches the next run of every Workflow and
	// WorkflowInstance that use the template, without anyone creating a new ConfigMap. The
	// operator honours it only when started with --allow-mutable-terraform-sources: leave that
	// off in production.
	// +optional
	AllowMutableSources bool `json:"allowMutableSources,omitempty"`
}

// TerraformFile is one file of a TerraformTemplate, read from a key of an immutable ConfigMap
// (or of any ConfigMap, see AllowMutableSources).
type TerraformFile struct {
	// Name is a plain file name (no directories) ending in .tf or .tf.tmpl.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*\.tf(\.tmpl)?$`
	// +kubebuilder:validation:MaxLength=128
	Name string `json:"name"`
	// ConfigMapKeyRef reads the file body from a key of a ConfigMap in the template's
	// namespace. The ConfigMap must be immutable (immutable: true), so that the code of a
	// revision can never change: to change it, create a new ConfigMap (or delete and recreate
	// this one), and the operator makes a new revision. AllowMutableSources lifts the rule
	// for debugging.
	// +required
	ConfigMapKeyRef WorkflowConfigMapKeyReference `json:"configMapKeyRef"`
}

// TerraformGitSource is a directory of a git repository that holds Terraform files. The
// operator reads it when the template is created and every time url, ref or path change, not
// periodically: a new commit on a branch reaches the template only when someone changes the
// ref or the annotation dinova.one/git-refresh (any new value). Only the files directly in
// the directory with a name valid for Files are read; subdirectories and other files are
// ignored.
type TerraformGitSource struct {
	// URL of the repository, over HTTPS.
	// +kubebuilder:validation:Pattern=`^https://[^\s]+$`
	// +kubebuilder:validation:MaxLength=2048
	URL string `json:"url"`
	// Ref is the branch, tag or commit to read.
	Ref TerraformGitReference `json:"ref"`
	// Path is the directory to read, relative to the root of the repository. Empty or "."
	// is the root. Every segment starts with a letter, a digit or "_", so ".." is refused.
	// +kubebuilder:validation:Pattern=`^(\.|[A-Za-z0-9_][A-Za-z0-9._-]*(/[A-Za-z0-9_][A-Za-z0-9._-]*)*)?$`
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Path string `json:"path,omitempty"`
	// SecretRef names a Secret in the template's namespace with the keys username and
	// password, sent with HTTP basic authentication: for example a GitLab deploy token with
	// read_repository. Omit it for a public repository.
	// +optional
	SecretRef *TerraformGitSecretReference `json:"secretRef,omitempty"`
}

// TerraformGitReference selects what to read. Commit pins an exact commit; with a branch or a
// tag it must be reachable from it, alone it must be reachable from the default branch.
// +kubebuilder:validation:XValidation:rule="!(has(self.branch) && has(self.tag))",message="branch and tag cannot be used together"
// +kubebuilder:validation:XValidation:rule="has(self.branch) || has(self.tag) || has(self.commit)",message="set branch, tag or commit"
type TerraformGitReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +optional
	Branch string `json:"branch,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +optional
	Tag string `json:"tag,omitempty"`
	// Commit is a full SHA-1, 40 hexadecimal digits.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{40}$`
	// +optional
	Commit string `json:"commit,omitempty"`
}

// TerraformGitSecretReference names a Secret in the template's namespace.
type TerraformGitSecretReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// TerraformTemplateGitStatus is what the operator read from git for the current revision.
type TerraformTemplateGitStatus struct {
	// URL, Ref and Path are the source that was read; Ref is "branch/<name>", "tag/<name>"
	// or "commit".
	URL  string `json:"url"`
	Ref  string `json:"ref"`
	Path string `json:"path,omitempty"`
	// Commit is the SHA-1 of the commit that was read.
	Commit string `json:"commit"`
	// Files are the names of the files read from git.
	// +listType=atomic
	Files []string `json:"files"`
	// Refresh is the value of the annotation dinova.one/git-refresh when the source was read.
	// +optional
	Refresh string `json:"refresh,omitempty"`
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
	// Git is the commit the files of spec.git were read from.
	// +optional
	Git *TerraformTemplateGitStatus `json:"git,omitempty"`
	// Conditions has Ready: True when the current revision matches the spec and its
	// ConfigMaps, False with the reason when the files cannot be read (a ConfigMap or a key is
	// missing, a ConfigMap is not immutable and mutable sources are not allowed, or the git
	// repository cannot be read). A Workflow runs a template only when it is Ready.
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
	// Mutable is true when the ConfigMap is not immutable (see AllowMutableSources): its
	// content can change, and each change makes a new revision.
	// +optional
	Mutable bool `json:"mutable,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Revision",type=integer,JSONPath=".status.revision"
// +kubebuilder:printcolumn:name="Commit",type=string,JSONPath=".status.git.commit",priority=1
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
