package main

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

const computeBase = "/compute/v2.1/" + osProjectID

func TestKeystoneToken(t *testing.T) {
	m := newMockServer(t)
	body := `{"auth":{"identity":{"methods":["password"],"password":{"user":{"name":"admin","domain":{"name":"Default"},"password":"%s"}}}}}`
	status, out, header := m.do(t, http.MethodPost, "/identity/v3/auth/tokens", fmt.Sprintf(body, "Password01!"))
	if status != http.StatusCreated || header.Get("X-Subject-Token") == "" {
		t.Fatalf("token: %d %v", status, out)
	}
	catalog := out["token"].(map[string]any)["catalog"].([]any)
	if len(catalog) != 4 {
		t.Errorf("catalog = %v", catalog)
	}
	if status, _, _ := m.do(t, http.MethodPost, "/identity/v3/auth/tokens", fmt.Sprintf(body, "wrong")); status != http.StatusUnauthorized {
		t.Errorf("wrong password: %d", status)
	}
}

func TestRouterUsesThePoolAndOneSubnetHasOneRouter(t *testing.T) {
	m := newMockServer(t)
	m.do(t, http.MethodPut, "/__mock/config?service=openstack", `{"floatingIPPool":1}`)
	status, out, _ := m.do(t, http.MethodPost, "/network/v2.0/routers", `{"router":{"name":"router-test"}}`)
	if status != http.StatusCreated {
		t.Fatalf("router: %d %v", status, out)
	}
	first := out["router"].(map[string]any)["id"].(string)
	if status, out, _ := m.do(t, http.MethodPost, "/network/v2.0/routers", `{"router":{"name":"router-test"}}`); status != http.StatusConflict ||
		out["NeutronError"].(map[string]any)["type"] != "IpAddressGenerationFailure" {
		t.Errorf("second router with a full pool: %d %v", status, out)
	}
	m.do(t, http.MethodPut, "/__mock/config?service=openstack", `{"floatingIPPool":6}`)
	_, out, _ = m.do(t, http.MethodPost, "/network/v2.0/routers", `{"router":{"name":"router-test"}}`)
	second := out["router"].(map[string]any)["id"].(string)
	iface := `{"subnet_id":"` + osPrivateSubnetID + `"}`
	if status, _, _ := m.do(t, http.MethodPut, "/network/v2.0/routers/"+first+"/add_router_interface", iface); status != http.StatusOK {
		t.Fatalf("interface: %d", status)
	}
	if status, _, _ := m.do(t, http.MethodPut, "/network/v2.0/routers/"+second+"/add_router_interface", iface); status != http.StatusConflict {
		t.Errorf("the fixed subnet on a second router (R1): %d, want 409", status)
	}
	_, dup, _ := m.do(t, http.MethodGet, "/__mock/duplicates", "")
	if dup["openstack"] == nil {
		t.Errorf("two routers with one name are duplicates: %v", dup)
	}
	if status, _, _ := m.do(t, http.MethodDelete, "/network/v2.0/routers/"+first, ""); status != http.StatusConflict {
		t.Errorf("router with an interface deleted: %d", status)
	}
}

func TestServerBuildsDeletesAndFails(t *testing.T) {
	m := newMockServer(t)
	create := `{"server":{"name":"vm-1","flavorRef":"2","networks":[{"uuid":"` + osPrivateNetID + `"}],"security_groups":[{"name":"default"}]}}`
	status, out, _ := m.do(t, http.MethodPost, computeBase+"/servers", create)
	if status != http.StatusAccepted {
		t.Fatalf("create: %d %v", status, out)
	}
	id := out["server"].(map[string]any)["id"].(string)
	server := func() (int, string) {
		status, out, _ := m.do(t, http.MethodGet, computeBase+"/servers/"+id, "")
		if status != http.StatusOK {
			return status, ""
		}
		return status, out["server"].(map[string]any)["status"].(string)
	}
	if _, got := server(); got != "BUILD" {
		t.Errorf("right after create: %s", got)
	}
	m.now = m.now.Add(3 * time.Second)
	if _, got := server(); got != "ACTIVE" {
		t.Errorf("after buildSeconds: %s", got)
	}
	m.do(t, http.MethodDelete, computeBase+"/servers/"+id, "")
	if status, _ := server(); status != http.StatusOK {
		t.Errorf("deletion is not instant: %d", status)
	}
	m.now = m.now.Add(2 * time.Second)
	if status, _ := server(); status != http.StatusNotFound {
		t.Errorf("after deleteSeconds: %d", status)
	}

	m.do(t, http.MethodPost, "/__mock/next/servers/error", "")
	_, out, _ = m.do(t, http.MethodPost, computeBase+"/servers", create)
	id = out["server"].(map[string]any)["id"].(string)
	m.now = m.now.Add(3 * time.Second)
	if _, got := server(); got != "ERROR" {
		t.Errorf("next server error: %s", got)
	}
	if status, _, _ := m.do(t, http.MethodPost, computeBase+"/servers", `{"server":{"name":"x","flavorRef":"99","networks":[{"uuid":"`+osPrivateNetID+`"}]}}`); status != http.StatusBadRequest {
		t.Errorf("unknown flavor: %d", status)
	}
}

func TestFloatingIPAssociationThroughNova(t *testing.T) {
	m := newMockServer(t)
	_, out, _ := m.do(t, http.MethodPost, computeBase+"/servers", `{"server":{"name":"vm-1","flavorRef":"2","networks":[{"uuid":"`+osPrivateNetID+`"}]}}`)
	id := out["server"].(map[string]any)["id"].(string)
	_, out, _ = m.do(t, http.MethodPost, "/network/v2.0/floatingips", `{"floatingip":{"floating_network_id":"`+osPublicNetID+`"}}`)
	fip := out["floatingip"].(map[string]any)
	if fip["status"] != "DOWN" {
		t.Errorf("new floating IP: %v", fip)
	}
	if status, _, _ := m.do(t, http.MethodPost, computeBase+"/servers/"+id+"/action", `{"addFloatingIp":{"address":"`+fip["floating_ip_address"].(string)+`"}}`); status != http.StatusAccepted {
		t.Fatalf("addFloatingIp: %d", status)
	}
	_, out, _ = m.do(t, http.MethodGet, "/network/v2.0/floatingips/"+fip["id"].(string), "")
	if got := out["floatingip"].(map[string]any); got["status"] != "ACTIVE" || got["fixed_ip_address"] == nil {
		t.Errorf("associated floating IP: %v", got)
	}
}

func TestShrunkPoolCountsTheAddressesInUse(t *testing.T) {
	m := newMockServer(t)
	for range 3 {
		if status, _, _ := m.do(t, http.MethodPost, "/network/v2.0/floatingips", `{"floatingip":{"floating_network_id":"`+osPublicNetID+`"}}`); status != http.StatusCreated {
			t.Fatalf("floating IP: %d", status)
		}
	}
	m.do(t, http.MethodPut, "/__mock/config?service=openstack", `{"floatingIPPool":2}`)
	if status, _, _ := m.do(t, http.MethodPost, "/network/v2.0/floatingips", `{"floatingip":{"floating_network_id":"`+osPublicNetID+`"}}`); status != http.StatusConflict {
		t.Errorf("a pool shrunk below what is in use still allocates: %d", status)
	}
}
