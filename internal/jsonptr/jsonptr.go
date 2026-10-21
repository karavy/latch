// Package jsonptr reads values of decoded JSON documents by JSON Pointer (RFC 6901).
package jsonptr

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Get returns the value the pointer names in a document decoded into any. An empty pointer
// names the whole document.
func Get(document any, pointer string) (any, error) {
	if pointer == "" {
		return document, nil
	}
	segments, err := Segments(pointer)
	if err != nil {
		return nil, err
	}
	value := document
	for _, segment := range segments {
		switch current := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = current[segment]
			if !ok {
				return nil, fmt.Errorf("key %q does not exist", segment)
			}
		case []any:
			index, parseErr := strconv.Atoi(segment)
			if parseErr != nil || index < 0 || index >= len(current) {
				return nil, fmt.Errorf("array index %q does not exist", segment)
			}
			value = current[index]
		default:
			return nil, fmt.Errorf("cannot traverse through %T", value)
		}
	}
	return value, nil
}

// Segments splits a pointer into its unescaped reference tokens.
func Segments(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("JSON pointer must be empty or start with '/'")
	}
	raw := strings.Split(pointer[1:], "/")
	segments := make([]string, len(raw))
	for i, part := range raw {
		var b strings.Builder
		for j := 0; j < len(part); j++ {
			if part[j] != '~' {
				b.WriteByte(part[j])
				continue
			}
			if j+1 >= len(part) {
				return nil, errors.New("invalid '~' escape in JSON pointer")
			}
			j++
			switch part[j] {
			case '0':
				b.WriteByte('~')
			case '1':
				b.WriteByte('/')
			default:
				return nil, errors.New("invalid '~' escape in JSON pointer")
			}
		}
		segments[i] = b.String()
	}
	return segments, nil
}
