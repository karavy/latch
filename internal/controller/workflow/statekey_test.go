package workflow

import (
	"fmt"
	"testing"
)

func resolve(values map[string]string) func(step, output string) ([]byte, error) {
	return func(step, output string) ([]byte, error) {
		if value, ok := values[step+"."+output]; ok {
			return []byte(value), nil
		}
		return nil, fmt.Errorf("no output %s.%s", step, output)
	}
}

func TestRenderStateKey(t *testing.T) {
	values := map[string]string{
		"create-vdc.name": `"acme"`, "create-vdc.id": `42`, "create-vdc.live": `true`, "tf.net-id": `"net.1_a-b"`,
	}
	for key, want := range map[string]string{
		"vdc/network":                           "vdc/network",
		"vdc/{{steps.create-vdc.outputs.name}}": "vdc/acme",
		"vdc/{{ steps.create-vdc.outputs.name }}/{{steps.create-vdc.outputs.id}}": "vdc/acme/42",
		"{{steps.create-vdc.outputs.live}}/x":                                     "true/x",
		"a-{{steps.tf.outputs.net-id}}-b":                                         "a-net.1_a-b-b",
	} {
		got, err := renderStateKey(key, resolve(values))
		if err != nil || got != want {
			t.Errorf("renderStateKey(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
}

func TestRenderStateKeyRejectsValuesThatCouldChangeThePath(t *testing.T) {
	values := map[string]string{
		"s.slash": `"a/b"`, "s.dots": `".."`, "s.dot": `"."`, "s.space": `"a b"`, "s.empty": `""`,
		"s.object": `{"a":1}`, "s.list": `["a"]`, "s.null": `null`, "s.broken": `{`, "s.glob": `"a*"`,
	}
	for _, name := range []string{"slash", "dots", "dot", "space", "empty", "object", "list", "null", "broken", "glob"} {
		key := "vdc/{{steps.s.outputs." + name + "}}"
		if got, err := renderStateKey(key, resolve(map[string]string{"s." + name: values["s."+name]})); err == nil {
			t.Errorf("value %q was accepted and rendered %q", values["s."+name], got)
		}
	}
	if _, err := renderStateKey("vdc/{{steps.s.outputs.missing}}", resolve(nil)); err == nil {
		t.Error("an unknown output must be an error")
	}
}

func TestRenderStateKeyRejectsUnsupportedPlaceholders(t *testing.T) {
	for _, key := range []string{
		"vdc/{{steps.s.outputs}}", "vdc/{{ .Values.x }}", "vdc/{{steps.S.outputs.x}}",
		"vdc/{steps.s.outputs.x}", "vdc/{{steps.s.outputs.x", "vdc/ x", "vdc/}}",
	} {
		if got, err := renderStateKey(key, resolve(map[string]string{"s.x": `"v"`})); err == nil {
			t.Errorf("%q rendered %q instead of failing", key, got)
		}
	}
}

func TestStateKeyReferences(t *testing.T) {
	refs := stateKeyReferences("vdc/{{steps.a.outputs.x}}/{{ steps.b-c.outputs.y_1 }}")
	if len(refs) != 2 || refs[0].Step != "a" || refs[0].Output != "x" || refs[1].Step != "b-c" || refs[1].Output != "y_1" {
		t.Fatalf("references = %+v", refs)
	}
	if refs := stateKeyReferences("plain/key"); len(refs) != 0 {
		t.Fatalf("references = %+v", refs)
	}
}
