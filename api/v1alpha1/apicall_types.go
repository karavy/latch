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

// ApiCallSpec describes a single HTTP operation. It is immutable: an ApiCall is the
// record of one execution, so a different request needs a new ApiCall (normally
// created by a Workflow from an ApiCallTemplate).
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ApiCall spec is immutable; create a new ApiCall (or run a new Workflow) to change the request"
// +kubebuilder:validation:XValidation:rule="!has(self.retry) || self.method == 'GET'",message="retry only applies to GET requests: sending any other method again could do the same operation twice"
type ApiCallSpec struct {
	// Method is the HTTP verb. A request is sent once, unless retry says otherwise.
	// +kubebuilder:validation:Enum=GET;PUT;POST;DELETE
	Method string `json:"method"`
	// Endpoint must also be permitted by the operator's origin allowlist.
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^https?://[^\s]+$`
	Endpoint string `json:"endpoint"`
	// Body is sent verbatim; use a YAML block scalar for JSON or other text.
	// +kubebuilder:validation:MaxLength=65536
	// +optional
	Body string `json:"body,omitempty"`
	// Headers contains non-secret request headers. Use authentication for credentials.
	// +kubebuilder:validation:MaxProperties=32
	// +optional
	Headers map[string]string `json:"headers,omitempty"`
	// Authentication references a Secret in the ApiCall's namespace.
	// +optional
	Authentication *APIAuthentication `json:"authentication,omitempty"`
	// TimeoutSeconds bounds the whole request, including response body reading.
	// +kubebuilder:default=120
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
	// StoreResponseBody defaults to true. Disable for sensitive responses.
	// +kubebuilder:default=true
	// +optional
	StoreResponseBody *bool `json:"storeResponseBody,omitempty"`
	// KeepJSONPointers makes status.response keep only the values these JSON Pointers name in
	// a JSON response, each at its place (see status.response.body), instead of the whole body.
	// A large response, such as a job with its log, then fits in the status as long as the kept
	// values do. The pointer of retry.until is always kept. The Workflow sets it to what its
	// step reads. Empty keeps the whole body, as does a response that is not JSON.
	// +kubebuilder:validation:MaxItems=128
	// +kubebuilder:validation:items:MaxLength=512
	// +kubebuilder:validation:items:Pattern=`^(/.*)?$`
	// +listType=atomic
	// +optional
	KeepJSONPointers []string `json:"keepJSONPointers,omitempty"`
	// Retry sends the same request again, from inside this ApiCall, when it fails with a
	// retryable error and, with until, while the response does not yet say that the work is
	// done. Only a GET can be repeated.
	// +optional
	Retry *ApiCallRetry `json:"retry,omitempty"`
	// Debug validates the request and logs it, without sending it. The ApiCall then
	// succeeds with no response, so a Workflow step cannot publish outputs from it. The log
	// has the method, endpoint, header names (never their values) and the body.
	// +optional
	Debug bool `json:"debug,omitempty"`
	// ExecutionID distinguishes otherwise identical ApiCalls. The spec is immutable,
	// so it cannot be used to re-run an existing ApiCall.
	// +kubebuilder:validation:MaxLength=128
	// +optional
	ExecutionID string `json:"executionID,omitempty"`
}

// ApiCallRetry says when the request of an ApiCall is sent again. The ApiCall stays one object:
// status.attempts counts the requests, and status.response is the last answer.
type ApiCallRetry struct {
	// MaxAttempts is the total number of requests, the first one included.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=3
	// +optional
	MaxAttempts int32 `json:"maxAttempts,omitempty"`
	// IntervalSeconds is the wait between an attempt and the next one.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=5
	// +optional
	IntervalSeconds int32 `json:"intervalSeconds,omitempty"`
	// OnStatusCodes lists the HTTP statuses of the response that are worth another try. Any
	// other non-2xx status fails the ApiCall at once.
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Minimum=100
	// +kubebuilder:validation:items:Maximum=599
	// +listType=set
	// +optional
	OnStatusCodes []int32 `json:"onStatusCodes,omitempty"`
	// OnTransportErrors also retries a request that timed out or failed at the network level.
	// Invalid requests and oversized responses are never retried.
	// +kubebuilder:default=true
	// +optional
	OnTransportErrors *bool `json:"onTransportErrors,omitempty"`
	// Until makes a successful response count only when it satisfies the condition: the
	// request is sent again, up to maxAttempts, while it does not. This is how to wait for a
	// job to finish.
	// +optional
	Until *ApiCallCondition `json:"until,omitempty"`
}

// ApiCallCondition is satisfied when a field of a JSON response has a given value.
type ApiCallCondition struct {
	// JSONPointer points into the response, for example /status.
	// +kubebuilder:validation:Pattern=`^/.*$`
	// +kubebuilder:validation:MaxLength=512
	JSONPointer string `json:"jsonPointer"`
	// EqualsJSON is the expected value as JSON (for example true, "success" or 3). Without it,
	// the field only has to exist.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	EqualsJSON string `json:"equalsJSON,omitempty"`
	// FailWhenJSON lists the values of the same field that mean the wait can never end well, as
	// JSON (for example "failed"). The first response that has one of them fails the ApiCall at
	// once, instead of waiting for maxAttempts. It needs jsonPointer to exist in the response.
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=1024
	// +listType=set
	// +optional
	FailWhenJSON []string `json:"failWhenJSON,omitempty"`
}

// APISecretKeyReference selects a key from a same-namespace Secret.
type APISecretKeyReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Key string `json:"key"`
}

// APIAuthentication selects credentials without embedding them in the CR. It is
// shared by ApiCall (immutable, enforced on ApiCallSpec) and ApiCallTemplate (editable).
// +kubebuilder:validation:XValidation:rule="self.type != 'Bearer' || has(self.tokenSecretRef)",message="Bearer requires tokenSecretRef"
// +kubebuilder:validation:XValidation:rule="self.type != 'Basic' || (has(self.usernameSecretRef) && has(self.passwordSecretRef))",message="Basic requires usernameSecretRef and passwordSecretRef"
// +kubebuilder:validation:XValidation:rule="self.type != 'APIKey' || (has(self.tokenSecretRef) && has(self.headerName))",message="APIKey requires tokenSecretRef and headerName"
// +kubebuilder:validation:XValidation:rule="self.type != 'OAuth2ClientCredentials' || (has(self.tokenURL) && has(self.clientIDSecretRef) && has(self.clientSecretSecretRef))",message="OAuth2ClientCredentials requires tokenURL, clientIDSecretRef and clientSecretSecretRef"
type APIAuthentication struct {
	// +kubebuilder:validation:Enum=Bearer;Basic;APIKey;OAuth2ClientCredentials
	Type string `json:"type"`
	// +optional
	TokenSecretRef *APISecretKeyReference `json:"tokenSecretRef,omitempty"`
	// +optional
	UsernameSecretRef *APISecretKeyReference `json:"usernameSecretRef,omitempty"`
	// +optional
	PasswordSecretRef *APISecretKeyReference `json:"passwordSecretRef,omitempty"`
	// HeaderName is the header containing an API key.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9-]+$`
	// +kubebuilder:validation:MaxLength=128
	// +optional
	HeaderName string `json:"headerName,omitempty"`
	// TokenURL is the OAuth 2.0 token endpoint. Its HTTPS origin must be allowlisted.
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^https://[^\s]+$`
	// +optional
	TokenURL string `json:"tokenURL,omitempty"`
	// ClientIDSecretRef selects the OAuth client ID from a same-namespace Secret.
	// +optional
	ClientIDSecretRef *APISecretKeyReference `json:"clientIDSecretRef,omitempty"`
	// ClientSecretSecretRef selects the OAuth client secret from a same-namespace Secret.
	// +optional
	ClientSecretSecretRef *APISecretKeyReference `json:"clientSecretSecretRef,omitempty"`
	// ClientAuthenticationMethod determines how client credentials are sent to the token endpoint.
	// +kubebuilder:validation:Enum=ClientSecretBasic;ClientSecretPost
	// +kubebuilder:default=ClientSecretBasic
	// +optional
	ClientAuthenticationMethod string `json:"clientAuthenticationMethod,omitempty"`
	// Scopes requested from the OAuth authorization server.
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Scopes []string `json:"scopes,omitempty"`
	// Audience is an optional provider-specific token audience parameter.
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	Audience string `json:"audience,omitempty"`
}

// APIResponse preserves the bounded remote result, including HTTP error statuses.
type APIResponse struct {
	StatusCode int `json:"statusCode"`
	// +optional
	ContentType string `json:"contentType,omitempty"`
	// Body is base64-encoded by JSON serialization, preserving arbitrary bytes. With
	// spec.keepJSONPointers it is a JSON document with only the kept values. It holds at most
	// 64 KiB; a response is read up to the operator's --apicall-max-response-bytes.
	// +optional
	Body []byte `json:"body,omitempty"`
	// Truncated indicates that the body is incomplete: the response exceeded the operator's
	// read limit (the ApiCall fails with ResponseTooLarge), or what was to be kept exceeded
	// 64 KiB (the outcome of the request does not change).
	// +optional
	Truncated bool `json:"truncated,omitempty"`
}

// ApiCallStatus defines the observed state of ApiCall.
type ApiCallStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Unknown
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// Attempts counts the requests sent so far, retries included.
	// +optional
	Attempts int32 `json:"attempts,omitempty"`
	// NextAttemptAt is when the next request is due, while the ApiCall waits between attempts.
	// +optional
	NextAttemptAt *metav1.Time `json:"nextAttemptAt,omitempty"`
	// Response is the last answer received.
	// +optional
	Response *APIResponse `json:"response,omitempty"`
	// Conditions include Ready and Progressing. Messages never contain credentials,
	// remote bodies or raw transport errors.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Method",type=string,JSONPath=".spec.method"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="HTTP",type=integer,JSONPath=".status.response.statusCode"
// +kubebuilder:printcolumn:name="Attempts",type=integer,JSONPath=".status.attempts",priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// ApiCall is the Schema for the apicalls API
type ApiCall struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ApiCall
	// +required
	Spec ApiCallSpec `json:"spec"`

	// status defines the observed state of ApiCall
	// +optional
	Status ApiCallStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ApiCallList contains a list of ApiCall
type ApiCallList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ApiCall `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ApiCall{}, &ApiCallList{})
}
