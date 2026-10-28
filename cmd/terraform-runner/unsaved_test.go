package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testdata/apply.jsonl is "apply -json" output as OpenTofu writes it, cut to the fields the
// runner reads; testdata/state.json has the instance whose create failed after it got an ID,
// which OpenTofu saved tainted.
func testdata(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestApplyWatcherFindsUnsavedCreates(t *testing.T) {
	applyOutput, state := string(testdata(t, "apply.jsonl")), testdata(t, "state.json")
	var out bytes.Buffer
	watcher := newApplyWatcher(&out)
	// The output arrives in pieces that do not follow lines.
	for i := 0; i < len(applyOutput); i += 97 {
		if _, err := watcher.Write([]byte(applyOutput[i:min(i+97, len(applyOutput))])); err != nil {
			t.Fatal(err)
		}
	}
	inState, err := stateAddresses(state)
	if err != nil {
		t.Fatal(err)
	}
	unsaved := watcher.unsaved(inState)
	addresses := make([]string, 0, len(unsaved))
	for _, resource := range unsaved {
		addresses = append(addresses, resource.Address)
	}
	// The router (accepted, its wait failed) and the port (interrupted, no error) may exist; the
	// instance created or saved tainted is in the state; the floating IP was refused (409).
	want := "openstack_networking_port_v2.p openstack_networking_router_v2.r"
	if got := strings.Join(addresses, " "); got != want {
		t.Errorf("unsaved = %q, want %q", got, want)
	}
	if unsaved[1].Error == "" || unsaved[0].Error != "" {
		t.Errorf("errors = %+v", unsaved)
	}
	printed := out.String()
	for _, want := range []string{
		"openstack_networking_router_v2.r: Creating...", "Error: Error waiting for", "retries exhausted",
	} {
		if !strings.Contains(printed, want) {
			t.Errorf("the log lacks %q:\n%s", want, printed)
		}
	}
	if strings.Contains(printed, `"@level"`) {
		t.Errorf("the log shows raw JSON:\n%s", printed)
	}
}

func TestStateAddresses(t *testing.T) {
	inState, err := stateAddresses(testdata(t, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`openstack_compute_instance_v2.vm["a"]`, "openstack_compute_instance_v2.tainted",
		"openstack_networking_floatingip_v2.other[1]", "module.net.openstack_networking_network_v2.n"} {
		if !inState[want] {
			t.Errorf("%s missing from %v", want, inState)
		}
	}
	if inState["data.openstack_networking_network_v2.public"] || inState["openstack_networking_network_v2.public"] {
		t.Error("data sources are not resources that can be left unsaved")
	}
	if empty, err := stateAddresses(nil); err != nil || len(empty) != 0 {
		t.Errorf("no state: %v %v", empty, err)
	}
}
