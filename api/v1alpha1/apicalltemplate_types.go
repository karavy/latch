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

// ApiCallTemplateSpec is the reusable definition of one HTTP operation.
// A Workflow step references it by name and runs an ApiCall copy of it; the
// template itself is never executed.
type ApiCallTemplateSpec struct {
	// Method is the HTTP verb.
	// +kubebuilder:validation:Enum=GET;PUT;POST;DELETE
	Method string `json:"method"`
	// Endpoint must also be permitted by the operator's origin allowlist.
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^https?://[^\s]+$`
	Endpoint string `json:"endpoint"`
	// Body is sent verbatim; a step body mapping requires a JSON object body.
	// +kubebuilder:validation:MaxLength=65536
	// +optional
	Body string `json:"body,omitempty"`
	// Headers contains non-secret request headers. Use authentication for credentials.
	// +kubebuilder:validation:MaxProperties=32
	// +optional
	Headers map[string]string `json:"headers,omitempty"`
	// Authentication references a Secret in the template's namespace. Unlike an
	// ApiCall, a template stays editable; Workflows read it when a step starts.
	// +optional
	Authentication *APIAuthentication `json:"authentication,omitempty"`
	// Debug makes the ApiCalls created from this template validate and log their request
	// without sending it. See ApiCallSpec.Debug.
	// +optional
	Debug bool `json:"debug,omitempty"`
	// TimeoutSeconds bounds the whole request, including response body reading.
	// +kubebuilder:default=120
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Method",type=string,JSONPath=".spec.method"
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=".spec.endpoint"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// ApiCallTemplate is the Schema for the apicalltemplates API
type ApiCallTemplate struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the template; it has no status because nothing executes it
	// +required
	Spec ApiCallTemplateSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// ApiCallTemplateList contains a list of ApiCallTemplate
type ApiCallTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ApiCallTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ApiCallTemplate{}, &ApiCallTemplateList{})
}
