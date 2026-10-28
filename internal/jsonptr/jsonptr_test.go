package jsonptr

import (
	"encoding/json"
	"testing"
)

func TestKeep(t *testing.T) {
	const document = `{"id":7,"status":"failed","log_output":"long","steps":[{"n":1,"ok":true},{"n":2,"ok":false}],"items":[1,2],"a/b":{"c":1,"d":2}}`
	for name, tc := range map[string]struct {
		pointers []string
		want     string
	}{
		"top-level fields":           {[]string{"/status", "/id"}, `{"id":7,"status":"failed"}`},
		"array element keeps index":  {[]string{"/steps/1/ok"}, `{"steps":[null,{"ok":false}]}`},
		"whole array":                {[]string{"/items"}, `{"items":[1,2]}`},
		"field inside a kept value":  {[]string{"/steps", "/steps/0/n"}, `{"steps":[{"n":1,"ok":true},{"n":2,"ok":false}]}`},
		"kept value after its field": {[]string{"/steps/0/n", "/steps"}, `{"steps":[{"n":1,"ok":true},{"n":2,"ok":false}]}`},
		"escaped key":                {[]string{"/a~1b/d"}, `{"a/b":{"d":2}}`},
		"missing pointer ignored":    {[]string{"/missing", "/steps/5", "/status/x", "/status"}, `{"status":"failed"}`},
		"nothing found":              {[]string{"/missing"}, `null`},
		"empty pointer keeps all":    {[]string{"/status", ""}, document},
	} {
		var parsed any
		if err := json.Unmarshal([]byte(document), &parsed); err != nil {
			t.Fatal(err)
		}
		kept, err := Keep(parsed, tc.pointers)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, _ := json.Marshal(kept)
		var want any
		_ = json.Unmarshal([]byte(tc.want), &want)
		wantJSON, _ := json.Marshal(want)
		if string(got) != string(wantJSON) {
			t.Errorf("%s: got %s, want %s", name, got, wantJSON)
		}
	}
	if _, err := Keep(map[string]any{}, []string{"no-slash"}); err == nil {
		t.Error("an invalid pointer was accepted")
	}
}
