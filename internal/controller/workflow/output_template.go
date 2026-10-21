package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// templatePlaceholder matches {{parameters.<name>}}, {{steps.<step>.outputs.<name>}} and
// {{outputs.<name>}} in the template of an output.
var templatePlaceholder = regexp.MustCompile(
	`\{\{\s*(?:parameters\.([a-zA-Z][a-zA-Z0-9_-]*)` +
		`|steps\.([a-z0-9](?:[-a-z0-9]*[a-z0-9])?)\.outputs\.([a-zA-Z][a-zA-Z0-9_-]*)` +
		`|outputs\.([a-zA-Z][a-zA-Z0-9_-]*))\s*\}\}`)

// templateRefs are the values a template uses.
type templateRefs struct {
	parameters []string
	steps      []dinovav1alpha1.WorkflowInput // step outputs
	local      []string                       // outputs of the same step
}

func parseTemplate(template string) templateRefs {
	var refs templateRefs
	for _, match := range templatePlaceholder.FindAllStringSubmatch(template, -1) {
		switch {
		case match[1] != "":
			refs.parameters = append(refs.parameters, match[1])
		case match[2] != "":
			refs.steps = append(refs.steps, dinovav1alpha1.WorkflowInput{Step: match[2], Output: match[3]})
		default:
			refs.local = append(refs.local, match[4])
		}
	}
	return refs
}

// templateSource returns the JSON-encoded value a placeholder names.
type templateSource struct {
	parameter func(name string) ([]byte, error)
	step      func(step, output string) ([]byte, error)
	local     func(name string) ([]byte, error)
}

// renderTemplate replaces every placeholder with its value, as text. Anything left that
// looks like a placeholder is an error: a typo must not end up in a published value.
func renderTemplate(template string, source templateSource) (string, error) {
	var failure error
	rendered := templatePlaceholder.ReplaceAllStringFunc(template, func(placeholder string) string {
		if failure != nil {
			return ""
		}
		match := templatePlaceholder.FindStringSubmatch(placeholder)
		var (
			raw  []byte
			err  error
			name string
		)
		switch {
		case match[1] != "":
			name = "parameter " + match[1]
			raw, err = source.parameter(match[1])
		case match[2] != "":
			name = fmt.Sprintf("output %s.%s", match[2], match[3])
			raw, err = source.step(match[2], match[3])
		default:
			name = "output " + match[4]
			raw, err = source.local(match[4])
		}
		if err != nil {
			failure = err
			return ""
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			failure = fmt.Errorf("%s is not valid JSON", name)
			return ""
		}
		switch typed := value.(type) {
		case string:
			return typed
		case float64, bool:
			return strings.Trim(string(raw), `"`)
		}
		failure = fmt.Errorf("%s must be a string, number or boolean to be part of a template", name)
		return ""
	})
	if failure != nil {
		return "", failure
	}
	if strings.Contains(rendered, "{{") || strings.Contains(rendered, "}}") {
		return "", fmt.Errorf("template %q has an unsupported placeholder; use {{parameters.<name>}}, {{steps.<step>.outputs.<name>}} or {{outputs.<name>}}", template)
	}
	return rendered, nil
}

// renderTemplatedOutputs computes the template outputs of a step, in order, so that one can
// use the ones declared above it. It returns the JSON-encoded values by output name.
func (r *Handler) renderTemplatedOutputs(ctx context.Context, run *templateRun, outputs []dinovav1alpha1.WorkflowOutput) (map[string][]byte, error) {
	values := map[string][]byte{}
	steps := r.outputResolver(ctx, run)
	source := templateSource{
		parameter: func(name string) ([]byte, error) {
			for _, parameter := range run.parameters {
				if parameter.Name == name {
					return json.Marshal(parameter.Value)
				}
			}
			return nil, &outputFailure{"ParameterMissing", fmt.Sprintf("Parameter %q is not declared in spec.parameters", name)}
		},
		step: func(step, output string) ([]byte, error) {
			raw, err := steps(step, output)
			if err != nil {
				return nil, &outputFailure{"OutputUnavailable", fmt.Sprintf("Template uses output %s.%s, which is not available", step, output)}
			}
			return raw, nil
		},
		local: func(name string) ([]byte, error) {
			if raw, ok := values[name]; ok {
				return raw, nil
			}
			return nil, &outputFailure{"OutputUnavailable", fmt.Sprintf("Template uses output %q, which is not declared above it in the step", name)}
		},
	}
	for _, output := range outputs {
		text, err := renderTemplate(output.Template, source)
		if err != nil {
			var failure *outputFailure
			if errors.As(err, &failure) {
				return nil, err
			}
			return nil, &outputFailure{"OutputExtractionFailed", fmt.Sprintf("Output %q: %s", output.Name, err)}
		}
		encoded, err := json.Marshal(text)
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

// splitTemplated separates the outputs built from a template from the ones read from a response.
func splitTemplated(outputs []dinovav1alpha1.WorkflowOutput) (plain, templated []dinovav1alpha1.WorkflowOutput) {
	for _, output := range outputs {
		if output.Template != "" {
			templated = append(templated, output)
		} else {
			plain = append(plain, output)
		}
	}
	return plain, templated
}
