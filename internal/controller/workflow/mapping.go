package workflow

import (
	"errors"
	"fmt"
	"slices"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/jsonptr"
)

func getPointer(document any, pointer string) (any, error) { return jsonptr.Get(document, pointer) }

func setObjectPointer(document map[string]any, pointer string, value any) error {
	segments, err := pointerSegments(pointer)
	if err != nil || len(segments) == 0 {
		return errors.New("target must be a non-root JSON pointer into an object")
	}
	current := document
	for _, segment := range segments[:len(segments)-1] {
		next, ok := current[segment]
		if !ok {
			child := make(map[string]any)
			current[segment] = child
			current = child
			continue
		}
		child, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("target parent %q is %T, not a JSON object", segment, next)
		}
		current = child
	}
	current[segments[len(segments)-1]] = value
	return nil
}

func pointerSegments(pointer string) ([]string, error) { return jsonptr.Segments(pointer) }

// responsePointers returns, sorted and without repeats, the JSON Pointers a step reads from the
// response of its own request (pollRequest false) or of a poll request of another template
// (true), so that the ApiCall keeps only those values. Empty when it reads nothing.
func responsePointers(apiStep *dinovav1alpha1.WorkflowAPIStep, pollRequest bool) []string {
	var pointers []string
	addOutputs := func(outputs []dinovav1alpha1.WorkflowOutput) {
		for _, output := range outputs {
			switch {
			case output.Template != "":
			case output.Select != nil:
				pointers = append(pointers, output.Select.Array) // from is relative to an element
			default:
				pointers = append(pointers, output.From)
			}
		}
	}
	poll := apiStep.Poll
	pollReads := poll != nil && (pollRequest || !separatePoll(apiStep))
	if !pollRequest {
		for _, expectation := range apiStep.Expect {
			pointers = append(pointers, expectation.Pointer)
		}
		addOutputs(finalOutputs(apiStep))
	} else {
		addOutputs(poll.Outputs)
	}
	if pollReads {
		pointers = append(pointers, poll.ConditionJSONPointer)
		if recover := poll.Recover; recover != nil {
			if recover.ProgressPointer != "" {
				pointers = append(pointers, recover.ProgressPointer)
			}
			for _, condition := range recover.Unless {
				pointers = append(pointers, condition.Pointer)
			}
			for _, input := range recover.Inputs {
				if input.FromResponse != "" {
					pointers = append(pointers, input.FromResponse)
				}
			}
		}
	}
	slices.Sort(pointers)
	return slices.Compact(pointers)
}
