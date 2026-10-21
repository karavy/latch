package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

const (
	// maxOutputValueBytes bounds one JSON-encoded output value kept in the Workflow status.
	maxOutputValueBytes = 4096
	maxOutputsPerStep   = 32
)

// outputFailure is an error that must fail the workflow with a fixed reason; any
// other error returned by output handling is transient and retried by the reconciler.
type outputFailure struct{ reason, message string }

func (e *outputFailure) Error() string { return e.message }

// inputResolver returns the JSON-encoded value of an input, whatever its source.
type inputResolver func(input dinovav1alpha1.WorkflowInput) ([]byte, error)

// configMapLookup returns the string stored under a ConfigMap key.
type configMapLookup func(ref dinovav1alpha1.WorkflowConfigMapKeyReference) (string, error)

// conditionResolver returns the expected value of a select condition that does not carry it
// inline: the one in a ConfigMap, or the output of an earlier step.
type conditionResolver func(condition dinovav1alpha1.WorkflowCondition) (any, error)

// errOutputNotPublished marks an input that reads an output no step has published, for
// example because the step that makes it never completed.
var errOutputNotPublished = errors.New("output has not been published")

// errOutputNotFound marks a pointer or a select that finds nothing in the response.
var errOutputNotFound = errors.New("value not found in the response")

// extractOutputs reads every declared output from a JSON response and returns the
// JSON-encoded values by output name. A missing pointer uses DefaultJSON, or fails.
// Outputs with a select that compares against a ConfigMap need extractOutputsWith.
func extractOutputs(response []byte, outputs []dinovav1alpha1.WorkflowOutput) (map[string][]byte, error) {
	return extractOutputsWith(response, outputs, nil)
}

// extractOutputsWith is extractOutputs with a lookup for select conditions that read
// their expected value from a ConfigMap.
func extractOutputsWith(response []byte, outputs []dinovav1alpha1.WorkflowOutput, lookup conditionResolver) (map[string][]byte, error) {
	if len(outputs) == 0 {
		return nil, nil
	}
	var document any
	if err := json.Unmarshal(response, &document); err != nil {
		return nil, &outputFailure{"OutputExtractionFailed", "Response is not valid JSON, so step outputs cannot be read"}
	}
	values := make(map[string][]byte, len(outputs))
	for _, output := range outputs {
		value, err := readOutput(document, output, lookup)
		if errors.Is(err, errOutputNotFound) {
			if output.DefaultJSON == "" {
				return nil, &outputFailure{"OutputMissing", fmt.Sprintf("Output %q is missing from the response at %s and has no defaultJSON", output.Name, describeSource(output))}
			}
			if err := json.Unmarshal([]byte(output.DefaultJSON), &value); err != nil {
				return nil, &outputFailure{"OutputExtractionFailed", fmt.Sprintf("Output %q has an invalid defaultJSON", output.Name)}
			}
		} else if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, &outputFailure{"OutputExtractionFailed", fmt.Sprintf("Output %q could not be encoded", output.Name)}
		}
		if len(encoded) > maxOutputValueBytes {
			return nil, &outputFailure{"OutputTooLarge", fmt.Sprintf("Output %q exceeds %d bytes", output.Name, maxOutputValueBytes)}
		}
		values[output.Name] = encoded
	}
	return values, nil
}

func describeSource(output dinovav1alpha1.WorkflowOutput) string {
	if output.Select != nil {
		return fmt.Sprintf("the selected element of %q at %q", output.Select.Array, output.From)
	}
	return output.From
}

// readOutput returns the value of one output: errOutputNotFound when nothing is there,
// an *outputFailure when the output cannot be evaluated.
func readOutput(document any, output dinovav1alpha1.WorkflowOutput, lookup conditionResolver) (any, error) {
	source := document
	if output.Select != nil {
		element, err := selectElement(document, output.Select, lookup)
		if err != nil {
			return nil, err
		}
		source = element
	}
	value, err := getPointer(source, output.From)
	if err != nil {
		return nil, errOutputNotFound
	}
	return value, nil
}

// selectElement returns the first element of the array that satisfies every condition.
func selectElement(document any, selector *dinovav1alpha1.WorkflowSelect, lookup conditionResolver) (any, error) {
	array, err := getPointer(document, selector.Array)
	if err != nil {
		return nil, errOutputNotFound
	}
	items, ok := array.([]any)
	if !ok {
		return nil, &outputFailure{"OutputExtractionFailed", fmt.Sprintf("Select expects a JSON array at %q", selector.Array)}
	}
	expected := make([]any, len(selector.Where))
	for i, condition := range selector.Where {
		value, err := conditionValue(condition, lookup)
		if err != nil {
			return nil, err
		}
		expected[i] = value
	}
	for _, item := range items {
		matches := true
		for i, condition := range selector.Where {
			actual, err := getPointer(item, condition.Field)
			if err != nil || !reflect.DeepEqual(actual, expected[i]) {
				matches = false
				break
			}
		}
		if matches {
			return item, nil
		}
	}
	return nil, errOutputNotFound
}

func conditionValue(condition dinovav1alpha1.WorkflowCondition, resolve conditionResolver) (any, error) {
	if condition.EqualsJSON == "" {
		if resolve == nil {
			return nil, &outputFailure{"OutputExtractionFailed", "Select compares with a ConfigMap or an output, but none can be read here"}
		}
		return resolve(condition)
	}
	var value any
	if err := json.Unmarshal([]byte(condition.EqualsJSON), &value); err != nil {
		return nil, &outputFailure{"OutputExtractionFailed", fmt.Sprintf("Select condition on %q has an invalid equalsJSON", condition.Field)}
	}
	return value, nil
}

// checkExpectations verifies that the response has the expected value at every pointer. A
// failure carries the pointer and the optional message, never the value received.
func checkExpectations(response []byte, expectations []dinovav1alpha1.WorkflowExpectation) error {
	if len(expectations) == 0 {
		return nil
	}
	var document any
	if err := json.Unmarshal(response, &document); err != nil {
		return &outputFailure{"ExpectationFailed", "Response is not valid JSON, so its expected values cannot be checked"}
	}
	for _, expectation := range expectations {
		var expected any
		if err := json.Unmarshal([]byte(expectation.EqualsJSON), &expected); err != nil {
			return &outputFailure{"ExpectationFailed", fmt.Sprintf("Expectation on %s has an invalid equalsJSON", expectation.Pointer)}
		}
		actual, err := getPointer(document, expectation.Pointer)
		if err == nil && reflect.DeepEqual(actual, expected) {
			continue
		}
		message := fmt.Sprintf("Response field %s did not have the expected value %s", expectation.Pointer, expectation.EqualsJSON)
		if err != nil {
			message = fmt.Sprintf("Response has no field %s, expected %s", expectation.Pointer, expectation.EqualsJSON)
		}
		if expectation.Message != "" {
			message = expectation.Message + ": " + message
		}
		return &outputFailure{"ExpectationFailed", message}
	}
	return nil
}

// applyInputs sets each body input (To) into destination, which must be empty or a
// JSON object, and returns the resulting JSON. Query inputs (ToQuery) are skipped: see
// applyQueryInputs.
func applyInputs(destination []byte, inputs []dinovav1alpha1.WorkflowInput, resolve inputResolver) ([]byte, error) {
	var bodyInputs []dinovav1alpha1.WorkflowInput
	for _, input := range inputs {
		if input.ToQuery != "" || input.ToPath != "" {
			continue
		}
		bodyInputs = append(bodyInputs, input)
	}
	if len(bodyInputs) == 0 {
		return append([]byte(nil), destination...), nil
	}
	var target any = map[string]any{}
	if len(destination) > 0 {
		if err := json.Unmarshal(destination, &target); err != nil {
			return nil, fmt.Errorf("target body must be valid JSON: %w", err)
		}
	}
	object, ok := target.(map[string]any)
	if !ok {
		return nil, errors.New("target body must be a JSON object")
	}
	seen := make(map[string]struct{}, len(bodyInputs))
	for _, input := range bodyInputs {
		if _, duplicate := seen[input.To]; duplicate {
			return nil, fmt.Errorf("duplicate input target %q", input.To)
		}
		seen[input.To] = struct{}{}
		raw, err := resolve(input)
		if err != nil {
			return nil, err
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("%s is not valid JSON", describeInput(input))
		}
		if input.AsNumber {
			number, err := asNumber(value)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", describeInput(input), err)
			}
			value = number
		}
		if input.AsString {
			text, err := asText(value)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", describeInput(input), err)
			}
			value = text
		}
		if err := setObjectPointer(object, input.To, value); err != nil {
			return nil, fmt.Errorf("set input target %q: %w", input.To, err)
		}
	}
	return json.Marshal(object)
}

// applyQueryInputs sets each query input (ToQuery) into the endpoint's query string,
// replacing a parameter of the same name, and returns the resulting endpoint. Only the
// query changes, so the origin checked against the allowlist stays the same.
func applyQueryInputs(endpoint string, inputs []dinovav1alpha1.WorkflowInput, resolve inputResolver) (string, error) {
	var queryInputs []dinovav1alpha1.WorkflowInput
	for _, input := range inputs {
		if input.ToQuery != "" {
			queryInputs = append(queryInputs, input)
		}
	}
	if len(queryInputs) == 0 {
		return endpoint, nil
	}
	target, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("endpoint is not a valid URL: %w", err)
	}
	query := target.Query()
	seen := make(map[string]struct{}, len(queryInputs))
	for _, input := range queryInputs {
		if _, duplicate := seen[input.ToQuery]; duplicate {
			return "", fmt.Errorf("duplicate query parameter %q", input.ToQuery)
		}
		seen[input.ToQuery] = struct{}{}
		raw, err := resolve(input)
		if err != nil {
			return "", err
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", fmt.Errorf("%s is not valid JSON", describeInput(input))
		}
		switch typed := value.(type) {
		case string:
			query.Set(input.ToQuery, typed)
		case bool:
			query.Set(input.ToQuery, strconv.FormatBool(typed))
		case float64:
			query.Set(input.ToQuery, strconv.FormatFloat(typed, 'f', -1, 64))
		default:
			return "", fmt.Errorf("%s is not a string, number or boolean, so it cannot go in a query string", describeInput(input))
		}
	}
	target.RawQuery = query.Encode()
	return target.String(), nil
}

// validateReferences checks, before any step runs, that every input points to an
// output declared by an earlier step, and that names and step names are unique.
func validateReferences(steps []dinovav1alpha1.WorkflowStep) error {
	declared := map[string]map[string]bool{} // step -> output -> declared by the step itself
	index := map[string]int{}
	for i, step := range steps {
		if _, exists := index[step.Name]; exists {
			return fmt.Errorf("step name %q is used more than once", step.Name)
		}
		index[step.Name] = i
		own := map[string]bool{}
		for _, output := range stepOutputs(step) {
			if _, exists := own[output.Name]; exists {
				return fmt.Errorf("step %q declares output %q more than once", step.Name, output.Name)
			}
			own[output.Name] = true
		}
		declared[step.Name] = own
	}
	check := func(current int, inputs []dinovav1alpha1.WorkflowInput, allowSameStep bool, available map[string]bool) error {
		for _, input := range inputs {
			if input.Step == "" {
				continue // a parameter or a ConfigMap key, see validateParameters
			}
			position, known := index[input.Step]
			switch {
			case !known:
				return fmt.Errorf("step %q uses output %q of unknown step %q", steps[current].Name, input.Output, input.Step)
			case position > current || position == current && !allowSameStep:
				return fmt.Errorf("step %q can only use outputs of earlier steps, not of %q", steps[current].Name, input.Step)
			case position == current && !available[input.Output]:
				return fmt.Errorf("step %q poll can only use outputs declared by the step's initial request, not %q", steps[current].Name, input.Output)
			case position < current && !declared[input.Step][input.Output]:
				return fmt.Errorf("step %q uses output %q that step %q does not declare", steps[current].Name, input.Output, input.Step)
			}
		}
		return nil
	}
	for i, step := range steps {
		switch {
		case step.API != nil:
			if err := check(i, step.API.Inputs, false, nil); err != nil {
				return err
			}
			if err := check(i, selectReferences(step.API.Outputs), false, nil); err != nil {
				return err
			}
			if err := validateLocalTemplates(step.Name, step.API.Outputs); err != nil {
				return err
			}
			if step.API.Poll != nil {
				if err := check(i, selectReferences(step.API.Poll.Outputs), false, nil); err != nil {
					return err
				}
				initial := map[string]bool{}
				for _, output := range step.API.Outputs {
					initial[output.Name] = true
				}
				if err := check(i, step.API.Poll.Inputs, true, initial); err != nil {
					return err
				}
			}
		case step.Terraform != nil:
			for _, input := range step.Terraform.Inputs {
				if input.ToQuery != "" || input.ToPath != "" {
					return fmt.Errorf("step %q is a Terraform step: input %q must use to, not toQuery or toPath", steps[i].Name, input.Output)
				}
			}
			if err := check(i, step.Terraform.Inputs, false, nil); err != nil {
				return err
			}
			if err := check(i, stateKeyReferences(step.Terraform.State.Key), false, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// stepOutputs returns every output a step publishes: its own and its poll's.
func stepOutputs(step dinovav1alpha1.WorkflowStep) []dinovav1alpha1.WorkflowOutput {
	if step.Terraform != nil {
		return slices.Clone(step.Terraform.Outputs)
	}
	if step.API == nil {
		return nil
	}
	outputs := slices.Clone(step.API.Outputs)
	if step.API.Poll != nil {
		outputs = append(outputs, step.API.Poll.Outputs...)
	}
	return outputs
}

func (r *Handler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func outputsSecretName(run *templateRun) string {
	return run.childName("outputs", "outputs")
}

func outputSecretKey(step, output string) string { return step + "." + output }

// captureOutputs extracts the declared outputs from response and records them:
// non-sensitive values in the step status, sensitive ones in the workflow's Secret.
// Outputs named in forceSensitive (for Terraform, those the module marked sensitive) are
// kept in the Secret even when the step does not say so. It is idempotent, so a retry
// after a partial failure rewrites the same values.
func (r *Handler) captureOutputs(ctx context.Context, run *templateRun, state *dinovav1alpha1.WorkflowStepStatus, outputs []dinovav1alpha1.WorkflowOutput, response []byte, forceSensitive ...string) error {
	plain, templated := splitTemplated(outputs)
	values, err := extractOutputsWith(response, plain, r.conditionResolver(ctx, run))
	if err != nil {
		return err
	}
	if len(templated) > 0 {
		rendered, err := r.renderTemplatedOutputs(ctx, run, templated)
		if err != nil {
			return err
		}
		if values == nil {
			values = map[string][]byte{}
		}
		maps.Copy(values, rendered)
	}
	if len(values) == 0 {
		return nil
	}
	if len(state.Outputs)+len(state.SensitiveOutputs)+len(values) > maxOutputsPerStep {
		return &outputFailure{"OutputTooLarge", fmt.Sprintf("Step publishes more than %d outputs", maxOutputsPerStep)}
	}
	secretData := map[string][]byte{}
	for _, output := range outputs {
		if output.Sensitive || slices.Contains(forceSensitive, output.Name) {
			secretData[outputSecretKey(state.Name, output.Name)] = values[output.Name]
			if !slices.Contains(state.SensitiveOutputs, output.Name) {
				state.SensitiveOutputs = append(state.SensitiveOutputs, output.Name)
			}
			continue
		}
		if state.Outputs == nil {
			state.Outputs = map[string]string{}
		}
		state.Outputs[output.Name] = string(values[output.Name])
	}
	if len(secretData) == 0 {
		return nil
	}
	return r.writeOutputsSecret(ctx, run, secretData)
}

func (r *Handler) writeOutputsSecret(ctx context.Context, run *templateRun, data map[string][]byte) error {
	key := types.NamespacedName{Namespace: run.owner.Namespace, Name: outputsSecretName(run)}
	secret := &corev1.Secret{}
	err := r.reader().Get(ctx, key, secret)
	switch {
	case apierrors.IsNotFound(err):
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Labels: map[string]string{"dinova.one/workflow": run.owner.Name}},
			Type:       corev1.SecretTypeOpaque, Data: data,
		}
		if err := controllerutil.SetControllerReference(run.owner, secret, r.Scheme); err != nil {
			return err
		}
		// AlreadyExists is returned too: the retry then merges into the existing Secret.
		if err := r.Create(ctx, secret); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		maps.Copy(secret.Data, data)
		if err := r.Update(ctx, secret); err != nil {
			return err
		}
	}
	run.status.OutputsSecretName = key.Name
	return nil
}

// outputResolver returns a lookup for published output values. The Secret is read at
// most once per resolver.
func (r *Handler) outputResolver(ctx context.Context, run *templateRun) func(step, output string) ([]byte, error) {
	var secret *corev1.Secret
	return func(step, output string) ([]byte, error) {
		state := run.findStep(step)
		if state == nil {
			return nil, fmt.Errorf("%w: step %q has no recorded outputs", errOutputNotPublished, step)
		}
		if value, ok := state.Outputs[output]; ok {
			return []byte(value), nil
		}
		if !slices.Contains(state.SensitiveOutputs, output) {
			return nil, fmt.Errorf("%w: output %q of step %q", errOutputNotPublished, output, step)
		}
		if secret == nil {
			secret = &corev1.Secret{}
			if err := r.reader().Get(ctx, types.NamespacedName{Namespace: run.owner.Namespace, Name: outputsSecretName(run)}, secret); err != nil {
				if apierrors.IsNotFound(err) {
					return nil, fmt.Errorf("%w: output %q of step %q, the outputs Secret does not exist", errOutputNotPublished, output, step)
				}
				return nil, fmt.Errorf("read outputs Secret: %w", err)
			}
		}
		value, ok := secret.Data[outputSecretKey(step, output)]
		if !ok {
			return nil, fmt.Errorf("sensitive output %q of step %q is missing from the outputs Secret", output, step)
		}
		return value, nil
	}
}

// configMapLookup reads the ConfigMap values a select compares against. Each ConfigMap is
// read at most once per lookup. A missing ConfigMap or key fails the workflow, because
// retrying cannot make it appear within this reconcile.
func (r *Handler) configMapLookup(ctx context.Context, namespace string) configMapLookup {
	configMaps := map[string]*corev1.ConfigMap{}
	return func(ref dinovav1alpha1.WorkflowConfigMapKeyReference) (string, error) {
		configMap, cached := configMaps[ref.Name]
		if !cached {
			configMap = &corev1.ConfigMap{}
			err := r.reader().Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, configMap)
			if apierrors.IsNotFound(err) {
				return "", &outputFailure{"ConfigMapUnavailable", fmt.Sprintf("ConfigMap %q used by an output select was not found", ref.Name)}
			}
			if err != nil {
				return "", err
			}
			configMaps[ref.Name] = configMap
		}
		value, ok := configMap.Data[ref.Key]
		if !ok {
			return "", &outputFailure{"ConfigMapKeyMissing", fmt.Sprintf("ConfigMap %q has no key %q", ref.Name, ref.Key)}
		}
		return value, nil
	}
}

func describeInput(input dinovav1alpha1.WorkflowInput) string {
	switch {
	case input.Parameter != "":
		return "parameter " + input.Parameter
	case input.ConfigMapKeyRef != nil:
		return fmt.Sprintf("ConfigMap value %s/%s", input.ConfigMapKeyRef.Name, input.ConfigMapKeyRef.Key)
	}
	return fmt.Sprintf("output %s.%s", input.Step, input.Output)
}

// validateParameters checks, before any step runs, that every input that reads a
// parameter names one the template declares.
func validateParameters(spec dinovav1alpha1.WorkflowTemplateSpec) error {
	declared := make(map[string]bool, len(spec.Parameters))
	for _, parameter := range spec.Parameters {
		declared[parameter.Name] = true
	}
	for _, step := range append(slices.Clone(spec.Steps), spec.DeleteSteps...) {
		var inputs []dinovav1alpha1.WorkflowInput
		switch {
		case step.API != nil:
			inputs = append(inputs, step.API.Inputs...)
			if step.API.Poll != nil {
				inputs = append(inputs, step.API.Poll.Inputs...)
			}
		case step.Terraform != nil:
			inputs = append(inputs, step.Terraform.Inputs...)
		}
		for _, input := range inputs {
			if input.Parameter != "" && !declared[input.Parameter] {
				return fmt.Errorf("step %q reads parameter %q, which the template does not declare", step.Name, input.Parameter)
			}
		}
		for _, name := range selectParameters(step) {
			if !declared[name] {
				return fmt.Errorf("step %q uses parameter %q in an output, and the template does not declare it", step.Name, name)
			}
		}
	}
	return nil
}

// inputResolver resolves inputs from step outputs, Workflow parameters and ConfigMap keys.
func (r *Handler) inputResolver(ctx context.Context, run *templateRun) inputResolver {
	outputs := r.outputResolver(ctx, run)
	lookup := r.configMapLookup(ctx, run.owner.Namespace)
	return func(input dinovav1alpha1.WorkflowInput) ([]byte, error) {
		switch {
		case input.Parameter != "":
			for _, parameter := range run.parameters {
				if parameter.Name == input.Parameter {
					return json.Marshal(parameter.Value)
				}
			}
			return nil, &outputFailure{"ParameterMissing", fmt.Sprintf("Parameter %q is not declared in spec.parameters", input.Parameter)}
		case input.ConfigMapKeyRef != nil:
			value, err := lookup(*input.ConfigMapKeyRef)
			if err != nil {
				return nil, err
			}
			return json.Marshal(value)
		}
		return outputs(input.Step, input.Output)
	}
}

// selectReferences returns the outputs of other steps that the select conditions and the
// templates of the outputs use, as inputs, so that they are checked like any other reference.
func selectReferences(outputs []dinovav1alpha1.WorkflowOutput) []dinovav1alpha1.WorkflowInput {
	var references []dinovav1alpha1.WorkflowInput
	for _, output := range outputs {
		if output.Template != "" {
			references = append(references, parseTemplate(output.Template).steps...)
		}
		if output.Select == nil {
			continue
		}
		for _, condition := range output.Select.Where {
			if condition.EqualsOutput != nil {
				references = append(references, dinovav1alpha1.WorkflowInput{Step: condition.EqualsOutput.Step, Output: condition.EqualsOutput.Output})
			}
		}
	}
	return references
}

// conditionResolver resolves select conditions that compare with a ConfigMap key or with
// the output of an earlier step.
func (r *Handler) conditionResolver(ctx context.Context, run *templateRun) conditionResolver {
	lookup := r.configMapLookup(ctx, run.owner.Namespace)
	outputs := r.outputResolver(ctx, run)
	return func(condition dinovav1alpha1.WorkflowCondition) (any, error) {
		if condition.EqualsParameter != "" {
			for _, parameter := range run.parameters {
				if parameter.Name == condition.EqualsParameter {
					return parameter.Value, nil
				}
			}
			return nil, &outputFailure{"ParameterMissing", fmt.Sprintf("Parameter %q is not declared in spec.parameters", condition.EqualsParameter)}
		}
		if condition.EqualsFrom != nil {
			value, err := lookup(*condition.EqualsFrom)
			if err != nil {
				return nil, err
			}
			return value, nil
		}
		raw, err := outputs(condition.EqualsOutput.Step, condition.EqualsOutput.Output)
		if err != nil {
			return nil, &outputFailure{"OutputUnavailable", fmt.Sprintf("Select compares with output %s.%s, which is not available", condition.EqualsOutput.Step, condition.EqualsOutput.Output)}
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, &outputFailure{"OutputExtractionFailed", fmt.Sprintf("Output %s.%s is not valid JSON", condition.EqualsOutput.Step, condition.EqualsOutput.Output)}
		}
		return value, nil
	}
}

// applyPathInputs sets each path input (ToPath) into its {placeholder} in the path of the
// endpoint. The host and the query string are never touched, so the origin checked against
// the allowlist stays the same.
func applyPathInputs(endpoint string, inputs []dinovav1alpha1.WorkflowInput, resolve inputResolver) (string, error) {
	var pathInputs []dinovav1alpha1.WorkflowInput
	for _, input := range inputs {
		if input.ToPath != "" {
			pathInputs = append(pathInputs, input)
		}
	}
	if len(pathInputs) == 0 {
		return endpoint, nil
	}
	scheme, rest, found := strings.Cut(endpoint, "://")
	if !found {
		return "", errors.New("endpoint is not a valid URL")
	}
	hostAndPath, query, hasQuery := strings.Cut(rest, "?")
	host, path, hasPath := strings.Cut(hostAndPath, "/")
	if !hasPath {
		return "", errors.New("endpoint has no path to put a value in")
	}
	seen := make(map[string]struct{}, len(pathInputs))
	for _, input := range pathInputs {
		if _, duplicate := seen[input.ToPath]; duplicate {
			return "", fmt.Errorf("duplicate path placeholder %q", input.ToPath)
		}
		seen[input.ToPath] = struct{}{}
		placeholder := "{" + input.ToPath + "}"
		if !strings.Contains(path, placeholder) {
			return "", fmt.Errorf("endpoint path has no %s placeholder", placeholder)
		}
		raw, err := resolve(input)
		if err != nil {
			return "", err
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", fmt.Errorf("%s is not valid JSON", describeInput(input))
		}
		var text string
		switch typed := value.(type) {
		case string:
			text = typed
		case bool:
			text = strconv.FormatBool(typed)
		case float64:
			text = strconv.FormatFloat(typed, 'f', -1, 64)
		default:
			return "", fmt.Errorf("%s is not a string, number or boolean, so it cannot go in a path", describeInput(input))
		}
		if text == "" {
			return "", fmt.Errorf("%s is empty, so it cannot go in a path", describeInput(input))
		}
		path = strings.ReplaceAll(path, placeholder, url.PathEscape(text))
	}
	result := scheme + "://" + host + "/" + path
	if hasQuery {
		result += "?" + query
	}
	return result, nil
}

// selectParameters returns the parameters that the select conditions of a step's outputs
// compare with.
func selectParameters(step dinovav1alpha1.WorkflowStep) []string {
	if step.API == nil {
		return nil
	}
	outputs := slices.Clone(step.API.Outputs)
	if step.API.Poll != nil {
		outputs = append(outputs, step.API.Poll.Outputs...)
	}
	var names []string
	for _, output := range outputs {
		if output.Template != "" {
			names = append(names, parseTemplate(output.Template).parameters...)
		}
		if output.Select == nil {
			continue
		}
		for _, condition := range output.Select.Where {
			if condition.EqualsParameter != "" {
				names = append(names, condition.EqualsParameter)
			}
		}
	}
	return names
}

// validateLocalTemplates checks that {{outputs.<name>}} names a template output declared
// above the one that uses it, in the same step.
func validateLocalTemplates(step string, outputs []dinovav1alpha1.WorkflowOutput) error {
	declared := map[string]bool{}
	for _, output := range outputs {
		if output.Template != "" {
			for _, name := range parseTemplate(output.Template).local {
				if !declared[name] {
					return fmt.Errorf("step %q: output %q uses {{outputs.%s}}, which is not a template output declared above it", step, output.Name, name)
				}
			}
		}
		if output.Template != "" {
			declared[output.Name] = true
		}
	}
	return nil
}

// asText returns the text of a string, a number or a boolean, for an input that must set one
// into the body as a string. Nothing else has a text that means the same.
func asText(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case bool:
		return strconv.FormatBool(typed), nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	}
	return "", errors.New("asString needs a string, a number or a boolean")
}

// asNumber returns the number a value holds: a number as it is, a string that spells one. It is
// for an input that must set a number into the body, when the value comes from a parameter, which
// is always a string.
func asNumber(value any) (float64, error) {
	switch typed := value.(type) {
	case float64:
		return typed, nil
	case string:
		number, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return 0, fmt.Errorf("asNumber needs a number, and %q is not one", typed)
		}
		return number, nil
	}
	return 0, errors.New("asNumber needs a number or a string that holds one")
}
