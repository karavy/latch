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
// one workflow step. A Workflow step references it by name; the Workflow copies its
// files when the step starts, and terraform-engine renders and runs them.
type TerraformTemplateSpec struct {
	// Files are the Terraform files. Names ending in .tf.tmpl are rendered by
	// terraform-engine (<FGT_XXX> placeholders and <FGT_FOR_STR> loops) against the
	// step's input variables; plain .tf files are used as they are.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	Files []TerraformFile `json:"files"`
}

// TerraformFile is one file of a TerraformTemplate.
type TerraformFile struct {
	// Name is a plain file name (no directories) ending in .tf or .tf.tmpl.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*\.tf(\.tmpl)?$`
	// +kubebuilder:validation:MaxLength=128
	Name string `json:"name"`
	// Content is the file body. Use a YAML block scalar.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=262144
	Content string `json:"content"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// TerraformTemplate is the Schema for the terraformtemplates API
type TerraformTemplate struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the template; it has no status because nothing executes it
	// +required
	Spec TerraformTemplateSpec `json:"spec"`
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
