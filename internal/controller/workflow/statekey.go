package workflow

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// stateKeyPlaceholder matches {{steps.<step>.outputs.<name>}} in a Terraform state key.
var stateKeyPlaceholder = regexp.MustCompile(`\{\{\s*steps\.([a-z0-9]([-a-z0-9]*[a-z0-9])?)\.outputs\.([a-zA-Z][a-zA-Z0-9_-]*)\s*\}\}`)

// stateKeyText is what a substituted value may be made of: the same characters a key
// segment may use, so a value can never add or remove path segments.
var stateKeyText = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// stateKeyReferences lists the step outputs a state key uses.
func stateKeyReferences(key string) []dinovav1alpha1.WorkflowInput {
	matches := stateKeyPlaceholder.FindAllStringSubmatch(key, -1)
	references := make([]dinovav1alpha1.WorkflowInput, 0, len(matches))
	for _, match := range matches {
		references = append(references, dinovav1alpha1.WorkflowInput{Step: match[1], Output: match[3]})
	}
	return references
}

// renderStateKey replaces every placeholder of key with the published output it names.
// Values must be a string, number or boolean made of key characters, so the result is
// always a plain, safe key. Anything left that looks like a placeholder is an error:
// a typo must not end up as part of a state path.
func renderStateKey(key string, resolve func(step, output string) ([]byte, error)) (string, error) {
	var failure error
	rendered := stateKeyPlaceholder.ReplaceAllStringFunc(key, func(placeholder string) string {
		if failure != nil {
			return ""
		}
		match := stateKeyPlaceholder.FindStringSubmatch(placeholder)
		raw, err := resolve(match[1], match[3])
		if err != nil {
			failure = err
			return ""
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			failure = fmt.Errorf("output %s.%s is not valid JSON", match[1], match[3])
			return ""
		}
		var text string
		switch typed := value.(type) {
		case string:
			text = typed
		case float64, bool:
			text = strings.Trim(string(raw), `"`)
		default:
			failure = fmt.Errorf("output %s.%s must be a string, number or boolean to be part of a state key", match[1], match[3])
			return ""
		}
		if !stateKeyText.MatchString(text) || text == "." || text == ".." {
			failure = fmt.Errorf("output %s.%s has a value that is not allowed in a state key (use letters, digits, '.', '_' and '-')", match[1], match[3])
			return ""
		}
		return text
	})
	if failure != nil {
		return "", failure
	}
	if strings.ContainsAny(rendered, "{} ") {
		return "", fmt.Errorf("state key %q has an unsupported placeholder; use {{steps.<step>.outputs.<name>}}", key)
	}
	return rendered, nil
}
