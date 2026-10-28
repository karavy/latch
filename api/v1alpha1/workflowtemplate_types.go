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

// WorkflowTemplateSpec is a reusable sequence of API and Terraform steps. Nothing runs a
// template by itself: a Workflow names it, gives a value to each of its parameters and runs
// its steps. A template stays editable; a Workflow that is running one fails if the template
// changes under it, rather than mixing two versions.
// +kubebuilder:validation:XValidation:rule="!has(self.deleteSteps) || self.deleteSteps.all(s, has(s.api))",message="deleteSteps can only contain api steps"
type WorkflowTemplateSpec struct {
	// Parameters are the values a Workflow gives when it runs the template, such as a client
	// name. A step reads one with an input that sets "parameter", or with a template output.
	// A parameter without a default must be given by the Workflow.
	// +kubebuilder:validation:MaxItems=32
	// +listType=map
	// +listMapKey=name
	// +optional
	Parameters []WorkflowTemplateParameter `json:"parameters,omitempty"`
	// Steps run strictly in order, when the Workflow reaches the template. A step starts only
	// after its predecessor succeeds.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	// +listType=atomic
	Steps []WorkflowStep `json:"steps"`
	// DeleteSteps run, strictly in order, when the Workflow that ran the template is deleted
	// from Kubernetes, and only api steps are allowed. They can read the parameters and the
	// outputs of the steps above; a step that needs an output of a step that never completed
	// is skipped, because there is nothing to undo. A failed delete step keeps the Workflow in
	// Kubernetes (its finalizer stays), so that cleanup is never dropped silently. Step names
	// are unique across steps and deleteSteps.
	// +kubebuilder:validation:MaxItems=20
	// +listType=atomic
	// +optional
	DeleteSteps []WorkflowStep `json:"deleteSteps,omitempty"`
}

// WorkflowTemplateParameter declares a parameter of a WorkflowTemplate.
type WorkflowTemplateParameter struct {
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// Description says what the parameter is for.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Description string `json:"description,omitempty"`
	// Default is used when the Workflow gives no value. Without it the parameter is
	// required.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Default *string `json:"default,omitempty"`
}

// WorkflowStep is exactly one API or Terraform operation.
// +kubebuilder:validation:XValidation:rule="has(self.api) != has(self.terraform)",message="exactly one of api or terraform must be set"
type WorkflowStep struct {
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Name string `json:"name"`
	// +optional
	API *WorkflowAPIStep `json:"api,omitempty"`
	// +optional
	Terraform *WorkflowTerraformStep `json:"terraform,omitempty"`
}

// WorkflowResourceReference names an object in the Workflow's namespace.
type WorkflowResourceReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// WorkflowAPIStep runs an ApiCall created from an existing ApiCallTemplate and
// may poll until ready.
type WorkflowAPIStep struct {
	// TemplateRef names the ApiCallTemplate that defines this step's request.
	// The Workflow runs a child ApiCall created from it, owned by the Workflow.
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// Inputs set values published by earlier steps (see Outputs) into the copied body.
	// The body must be a JSON object.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Inputs []WorkflowInput `json:"inputs,omitempty"`
	// Outputs publish values from this request's response, by name, for later steps.
	// With a poll, they are read from the initial response; add poll.outputs to
	// publish values from the final poll response.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Outputs []WorkflowOutput `json:"outputs,omitempty"`
	// Debug makes the step validate and log its request without sending it, like debug on the
	// ApiCallTemplate: either one is enough. The step completes with no response, so it
	// publishes only the outputs that need none (see WorkflowOutput.Template and DebugJSON).
	// The requests of a poll are not affected.
	// +optional
	Debug bool `json:"debug,omitempty"`
	// Idempotent declares that sending the step's requests again gives the same result, for
	// example a PUT that sets a configuration, or a create that first checks whether the object
	// exists. It changes nothing in a Workflow; a WorkflowPipeline runs a Workflow stage again
	// when its values change (rerunOnChange) only if all its API steps are GETs or idempotent.
	// The operator cannot check it: a step marked idempotent that is not may create duplicates.
	// +optional
	Idempotent bool `json:"idempotent,omitempty"`
	// Expect lists conditions on the response of the initial request. If one does not hold,
	// the workflow fails, and the request is not retried. They are checked before the outputs
	// are published, and not on the responses of a poll.
	// +kubebuilder:validation:MaxItems=8
	// +listType=atomic
	// +optional
	Expect []WorkflowExpectation `json:"expect,omitempty"`
	// Retry sends the step's request again, from inside its ApiCall (one object, with
	// status.attempts), when it fails with a retryable error. Only GET requests can be
	// retried, because repeating any other method could do the same operation twice; a step
	// that sets retry on a template with another method fails before sending anything. It
	// cannot be combined with a poll that has no templateRef, which has its own tolerance.
	// +optional
	Retry *WorkflowRetry `json:"retry,omitempty"`
	// Poll repeats a read request until the JSON response satisfies the condition.
	// +optional
	Poll *WorkflowPoll `json:"poll,omitempty"`
}

// WorkflowExpectation requires a field of a JSON response to have a given value.
type WorkflowExpectation struct {
	// Pointer is a JSON Pointer into the response, for example /ok.
	// +kubebuilder:validation:Pattern=`^/.*$`
	// +kubebuilder:validation:MaxLength=512
	Pointer string `json:"pointer"`
	// EqualsJSON is the expected value as JSON (for example true, "ready" or 3).
	// +kubebuilder:validation:MaxLength=1024
	EqualsJSON string `json:"equalsJSON"`
	// Message explains a failure in the workflow status. The response itself is never copied
	// there.
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Message string `json:"message,omitempty"`
}

// WorkflowRetry defines when and how often a failed GET request is sent again. The
// poll requests of a step have their own tolerance (see WorkflowPoll).
type WorkflowRetry struct {
	// MaxAttempts is the total number of requests, the first one included.
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=3
	// +optional
	MaxAttempts int32 `json:"maxAttempts,omitempty"`
	// BackoffSeconds is the wait between a failure and the next request.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=5
	// +optional
	BackoffSeconds int32 `json:"backoffSeconds,omitempty"`
	// StatusCodes lists the HTTP statuses of the response that are worth another try.
	// Any other non-2xx status fails the workflow at once.
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Minimum=100
	// +kubebuilder:validation:items:Maximum=599
	// +kubebuilder:default={429,502,503,504}
	// +listType=set
	// +optional
	StatusCodes []int32 `json:"statusCodes,omitempty"`
	// OnTransportErrors also retries a request that timed out or failed at the network
	// level. Invalid requests and oversized responses are never retried.
	// +kubebuilder:default=true
	// +optional
	OnTransportErrors *bool `json:"onTransportErrors,omitempty"`
}

// WorkflowPoll defines a bounded polling loop after the initial API call.
type WorkflowPoll struct {
	// TemplateRef names the ApiCallTemplate used for each polling request, which creates an
	// ApiCall for every poll. Leave it out when the poll just sends the step's own request
	// again: then the step's ApiCall repeats it by itself, as one object, up to maxAttempts
	// requests in all (the first included), and only a GET can be repeated that way. Without
	// templateRef, inputs must be empty, and outputs are read from the final response.
	// +optional
	TemplateRef WorkflowResourceReference `json:"templateRef,omitzero"`
	// Inputs set published values into each poll request body. They may use outputs
	// of earlier steps and of this step's initial request. They need a templateRef.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Inputs []WorkflowInput `json:"inputs,omitempty"`
	// Outputs publish values from the final poll response, the one that satisfies
	// the condition. Names must differ from the step's own outputs.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Outputs []WorkflowOutput `json:"outputs,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=5
	// +optional
	IntervalSeconds int32 `json:"intervalSeconds,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=20
	// +optional
	MaxAttempts int32 `json:"maxAttempts,omitempty"`
	// JSON Pointer into the polling response.
	// +kubebuilder:validation:Pattern=`^/.*$`
	ConditionJSONPointer string `json:"conditionJSONPointer"`
	// Expected value as JSON (for example true, "ready", or 3).
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	EqualsJSON string `json:"equalsJSON,omitempty"`
	// FailWhenJSON lists values of the condition's field, as JSON (for example "failed"), that
	// mean the polled operation ended badly or needs somebody to act. The first poll response
	// that has one of them fails the workflow with PollFailed, instead of waiting for
	// maxAttempts.
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=1024
	// +listType=set
	// +optional
	FailWhenJSON []string `json:"failWhenJSON,omitempty"`
	// RetryOnStatusCodes lists HTTP statuses of a poll response (for example 404 or 503)
	// that mean "not ready yet" instead of a failure. Such a poll consumes one of
	// maxAttempts. Any other non-2xx status fails the workflow. It never applies to
	// the step's initial request.
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Minimum=100
	// +kubebuilder:validation:items:Maximum=599
	// +listType=set
	// +optional
	RetryOnStatusCodes []int32 `json:"retryOnStatusCodes,omitempty"`
	// RetryOnTransportErrors treats a poll that timed out or failed at the network
	// level as "not ready yet". Invalid requests and oversized responses still fail.
	// +optional
	RetryOnTransportErrors bool `json:"retryOnTransportErrors,omitempty"`
	// Recover resumes the polled operation instead of failing the step, when a poll response
	// has one of the values in recover.whenJSON: it sends the recover request, then polls
	// again from the start, with a full maxAttempts. It may happen again and again, as long as
	// the operation makes progress, for example a job that fails at phase 3 of 10, resumes, fails
	// at phase 6, resumes again, and so on until it succeeds.
	// +optional
	Recover *WorkflowPollRecover `json:"recover,omitempty"`
}

// WorkflowPollRecover defines the request that resumes a polled operation that failed, and
// how many times it may be sent.
type WorkflowPollRecover struct {
	// WhenJSON lists the values of the poll's condition field, as JSON (for example "failed"),
	// that are worth a recovery. Each one must also be in failWhenJSON; the other values there
	// still fail the step at once.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=1024
	WhenJSON []string `json:"whenJSON"`
	// Unless lists conditions on the failed poll response that rule a recovery out, for
	// example a job that asks for a manual review. The first one that holds fails the step
	// with RecoveryBlocked and its message.
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Unless []WorkflowExpectation `json:"unless,omitempty"`
	// TemplateRef names the ApiCallTemplate of the recover request. It is sent once per
	// recovery, never repeated, because it usually restarts work (a POST).
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// Inputs set values into the recover request. Besides parameters, ConfigMaps and outputs
	// of earlier steps (and of this step's initial request), they may read the failed poll
	// response with fromResponse, for example the last phase the operation completed.
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Inputs []WorkflowInput `json:"inputs,omitempty"`
	// ProgressPointer is a JSON Pointer to a number in the poll response that grows as the
	// operation goes on, for example /last_completed_step. A recovery after which it has grown
	// starts a new count of maxRetriesWithoutProgress. Without it, every recovery counts.
	// +kubebuilder:validation:Pattern=`^/.*$`
	// +kubebuilder:validation:MaxLength=512
	// +optional
	ProgressPointer string `json:"progressPointer,omitempty"`
	// MaxRetriesWithoutProgress is how many recoveries in a row may find the operation failed
	// at the same point. One more failure there fails the step with RecoveryExhausted.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	// +optional
	MaxRetriesWithoutProgress int32 `json:"maxRetriesWithoutProgress,omitempty"`
	// MaxRecoveries bounds the recoveries of the step in all, progress or not.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=50
	// +kubebuilder:default=10
	// +optional
	MaxRecoveries int32 `json:"maxRecoveries,omitempty"`
}

// WorkflowTerraformState identifies the Terraform state of a step.
type WorkflowTerraformState struct {
	// Key names the resource this step manages, for example "vdc/acme/network". Workflows
	// that use the same key in the same namespace share one state, even at different times,
	// so a later run with changed inputs modifies the existing resources. Concurrent runs on
	// the same key do not overlap: the second one fails on the state lock.
	//
	// Segments are separated by "/" and use letters, digits, ".", "_" and "-". A segment can
	// contain placeholders {{parameters.<name>}}, replaced by the value the Workflow gives to
	// that parameter (so that every Workflow of one client or project works on the same state),
	// and {{steps.<step>.outputs.<name>}}, replaced by the output of an earlier step, which must
	// be a string, number or boolean. Both must be made of the same characters as a segment.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9._{}/ -]+$`
	Key string `json:"key"`
}

// WorkflowOutput publishes one named value from a JSON response so that later steps
// can use it, instead of depending on the shape of the previous response.
// +kubebuilder:validation:XValidation:rule="has(self.template) ? (!has(self.select) && (!has(self.from) || size(self.from) == 0)) : ((has(self.from) && size(self.from) > 0) || has(self.select))",message="a template output has no from or select; any other needs from, which may be empty only together with select"
type WorkflowOutput struct {
	// Name identifies the output within its step.
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// From is a JSON Pointer into the response, for example /id. With select it is
	// relative to the selected array element, and empty publishes the whole element. A
	// template output has no from.
	// +kubebuilder:validation:Pattern=`^(/.*)?$`
	// +kubebuilder:validation:MaxLength=512
	// +optional
	From string `json:"from,omitempty"`
	// Select picks one element of an array in the response, so that From does not depend
	// on its position.
	// +optional
	Select *WorkflowSelect `json:"select,omitempty"`
	// Template builds the value as a string from the response-independent values it names,
	// instead of reading the response: {{parameters.<name>}}, {{steps.<step>.outputs.<name>}}
	// for an earlier step, and {{outputs.<name>}} for a template output declared above it in
	// the same step. For example "P2P-{{parameters.clientName}}{{parameters.segmentSuffix}}".
	// Values must be strings, numbers or booleans. The output is published when the step
	// completes, and in debug mode too.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Template string `json:"template,omitempty"`
	// DefaultJSON is the JSON value used when From does not exist in the response, or
	// when select matches no element (for example "eu-1", 3 or null). Without it, a
	// missing value fails the workflow.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	DefaultJSON string `json:"defaultJSON,omitempty"`
	// DebugJSON is the value published when the step runs in debug mode, where nothing is
	// sent and there is no response to read. It is never used for a real response, so a
	// missing field cannot silently turn into it. Without it, a debug step publishes nothing.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	DebugJSON string `json:"debugJSON,omitempty"`
	// Sensitive stores the value in a per-workflow Secret instead of the Workflow status.
	// +optional
	Sensitive bool `json:"sensitive,omitempty"`
}

// WorkflowSelect chooses the first element of a JSON array that satisfies every condition.
type WorkflowSelect struct {
	// Array is a JSON Pointer to the array in the response. Empty means that the response
	// itself is the array.
	// +kubebuilder:validation:Pattern=`^(/.*)?$`
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Array string `json:"array,omitempty"`
	// Where lists the conditions an element must all satisfy. The first element in
	// array order that does is selected.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +listType=atomic
	Where []WorkflowCondition `json:"where"`
}

// WorkflowCondition compares one field of an array element with an expected value.
// +kubebuilder:validation:XValidation:rule="(has(self.equalsJSON) ? 1 : 0) + (has(self.equalsFrom) ? 1 : 0) + (has(self.equalsOutput) ? 1 : 0) + (has(self.equalsParameter) ? 1 : 0) == 1",message="exactly one of equalsJSON, equalsFrom, equalsOutput or equalsParameter must be set"
type WorkflowCondition struct {
	// Field is a JSON Pointer relative to the array element, for example /name.
	// +kubebuilder:validation:Pattern=`^/.+$`
	// +kubebuilder:validation:MaxLength=512
	Field string `json:"field"`
	// EqualsJSON is the expected value as JSON (for example true, "ready" or 3).
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	EqualsJSON string `json:"equalsJSON,omitempty"`
	// EqualsFrom reads the expected value, a string, from a ConfigMap in the Workflow's
	// namespace when the step's output is read.
	// +optional
	EqualsFrom *WorkflowConfigMapKeyReference `json:"equalsFrom,omitempty"`
	// EqualsParameter compares with a parameter of the Workflow's spec.parameters. The
	// comparison is exact, on the whole value.
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	EqualsParameter string `json:"equalsParameter,omitempty"`
	// EqualsOutput compares with an output published by an earlier step. The comparison is
	// exact, on the whole value: it is never a substring match.
	// +optional
	EqualsOutput *WorkflowOutputReference `json:"equalsOutput,omitempty"`
}

// WorkflowOutputReference names an output published by an earlier step.
type WorkflowOutputReference struct {
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	Step string `json:"step"`
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	Output string `json:"output"`
}

// WorkflowConfigMapKeyReference selects a key of a ConfigMap in the Workflow's namespace.
type WorkflowConfigMapKeyReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Key string `json:"key"`
}

// WorkflowInput sets a value into the request of a step: into a JSON object body (To) or
// into the query string of the endpoint (ToQuery) or into its path (ToPath). The value comes from exactly one source:
// an output of an earlier step (Step and Output), a Workflow parameter (Parameter), a
// key of a ConfigMap (ConfigMapKeyRef), or the failed poll response of a recovery
// (FromResponse).
// +kubebuilder:validation:XValidation:rule="(has(self.to) ? 1 : 0) + (has(self.toQuery) ? 1 : 0) + (has(self.toPath) ? 1 : 0) == 1",message="exactly one of to, toQuery or toPath must be set"
// +kubebuilder:validation:XValidation:rule="!has(self.asString) || has(self.to)",message="asString only applies to an input set into the body with to"
// +kubebuilder:validation:XValidation:rule="!has(self.asNumber) || has(self.to)",message="asNumber only applies to an input set into the body with to"
// +kubebuilder:validation:XValidation:rule="!has(self.asString) || !has(self.asNumber)",message="asString and asNumber cannot be used together"
// +kubebuilder:validation:XValidation:rule="has(self.step) == has(self.output)",message="step and output must be set together"
// +kubebuilder:validation:XValidation:rule="(has(self.output) ? 1 : 0) + (has(self.parameter) ? 1 : 0) + (has(self.configMapKeyRef) ? 1 : 0) + (has(self.fromResponse) ? 1 : 0) == 1",message="exactly one of step and output, parameter, configMapKeyRef or fromResponse must be set"
// +kubebuilder:validation:XValidation:rule="!has(self.add) || has(self.to)",message="add only applies to an input set into the body with to"
type WorkflowInput struct {
	// Step is the name of the step that publishes the output. It must come before the
	// current step (a poll may also use outputs of its own step's initial request).
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=40
	// +optional
	Step string `json:"step,omitempty"`
	// Output is the name of the output declared by that step.
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Output string `json:"output,omitempty"`
	// Parameter is the name of a parameter declared in the Workflow's spec.parameters.
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9_-]*$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Parameter string `json:"parameter,omitempty"`
	// ConfigMapKeyRef reads a string from a ConfigMap in the Workflow's namespace when the
	// step starts.
	// +optional
	ConfigMapKeyRef *WorkflowConfigMapKeyReference `json:"configMapKeyRef,omitempty"`
	// FromResponse is a JSON Pointer into the poll response that failed, for example
	// /last_completed_step. Only the inputs of a poll's recover request can use it.
	// +kubebuilder:validation:Pattern=`^/.*$`
	// +kubebuilder:validation:MaxLength=512
	// +optional
	FromResponse string `json:"fromResponse,omitempty"`
	// Add adds a whole number to a numeric value before it is set into the body, for example
	// 1 to turn the last completed phase into the first one to run. The value must be a number,
	// or a string that holds one.
	// +optional
	Add int32 `json:"add,omitempty"`
	// To is a non-root JSON Pointer into the target object, for example /vdcId.
	// +kubebuilder:validation:Pattern=`^/.+$`
	// +kubebuilder:validation:MaxLength=512
	// +optional
	To string `json:"to,omitempty"`
	// AsString sets a number or a boolean into the body as its text, for example 3619 as "3619",
	// for an API that wants a string. Without it the value keeps its JSON type. A string is set
	// as it is, and any other value (an object, an array, null) is refused.
	// +optional
	AsString bool `json:"asString,omitempty"`
	// AsNumber sets a value into the body as a JSON number, for example the string "4000" of a
	// parameter as 4000, for an API that wants a number. A number passes as it is, and a string
	// must hold one; anything else is refused. Without it the value keeps its JSON type.
	// +optional
	AsNumber bool `json:"asNumber,omitempty"`
	// AsJSON reads a string value as JSON and sets what it holds into the body, for example the
	// parameter '["vm-1","vm-2"]' as an array, for a body that wants a list or an object, which a
	// parameter, always a string, cannot be. The string must be valid JSON, and any other value is
	// refused. It cannot be combined with asString or asNumber. Without it the value keeps its
	// JSON type.
	// +optional
	AsJSON bool `json:"asJSON,omitempty"`
	// ToQuery is the name of a query parameter of the endpoint to set, for example
	// block_id. It replaces a parameter of the same name in the template's endpoint, and
	// the value must be a string, number or boolean. Only API steps support it.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.-]+$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	ToQuery string `json:"toQuery,omitempty"`
	// ToPath is the name of a {placeholder} in the path of the endpoint, for example env_id
	// for /envs/{env_id}/segments. The value is escaped as a path segment and must be a
	// string, number or boolean. The endpoint must contain the placeholder. Only API steps
	// support it.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.-]+$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	ToPath string `json:"toPath,omitempty"`
}

// WorkflowTerraformStep provisions infrastructure with terraform-engine. Its state is kept
// in S3-compatible storage under State.Key, independently of the workflow: any later
// workflow that uses the same key works on the same state and so on the same resources.
// Every Terraform step works in its own folder of the workflow's workspace, named after
// the step, so a workflow can have any number of them.
type WorkflowTerraformStep struct {
	// TemplateRef names the TerraformTemplate whose files this step runs. Its files are
	// copied when the step starts; later edits of the template do not affect it.
	TemplateRef WorkflowResourceReference `json:"templateRef"`
	// Inputs set values published by earlier steps into terraform.auto.tfvars.json;
	// To is a JSON pointer such as /cluster_id naming the Terraform variable.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Inputs []WorkflowInput `json:"inputs,omitempty"`
	// State says where the Terraform state of this step lives.
	State WorkflowTerraformState `json:"state"`
	// Outputs publish values from the Terraform outputs (the "output" blocks of the
	// template) for later steps. They are read from the state once the Job succeeds,
	// as one JSON object {"<output name>": value}, so From is a JSON Pointer such as
	// /vpc_id or /ips/0. Terraform outputs marked sensitive are always kept in the Secret.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	Outputs []WorkflowOutput `json:"outputs,omitempty"`
	// Maximum wall time for the Terraform Job.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=7200
	// +kubebuilder:default=1800
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
	// Secret names in the workflow namespace whose keys are exposed as environment variables to the Terraform Job.
	// The storage credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) must come from one of them.
	// +kubebuilder:validation:MaxItems=8
	// +optional
	EnvFromSecrets []string `json:"envFromSecrets,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

// WorkflowTemplate is the Schema for the workflowtemplates API
type WorkflowTemplate struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the template; it has no status because nothing executes it
	// +required
	Spec WorkflowTemplateSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// WorkflowTemplateList contains a list of WorkflowTemplate
type WorkflowTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkflowTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkflowTemplate{}, &WorkflowTemplateList{})
}
