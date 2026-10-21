package workflow

import (
	"errors"
	"fmt"

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
