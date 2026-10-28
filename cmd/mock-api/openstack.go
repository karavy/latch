package main

// A simulated OpenStack (Keystone, Neutron, Nova, Glance) with the calls terraform-provider-openstack
// makes for the Virtuozzo steps of latch. Resources live in memory; slow operations (a server in
// BUILD, a deletion) advance with the clock when they are read.

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	osProjectID       = "0dda9763b34145e9a0eddd8c1a0686e0"
	osUserID          = "5b1d3c2a9f6e4d7c8b0a1e2f3d4c5b6a"
	osPublicNetID     = "bddac06b-160e-4b6e-ba74-a089fbfcb07e"
	osPublicSubnetID  = "ff86a324-b8d3-4e10-bd0f-0396d5592b36"
	osPrivateNetID    = "72bf3501-768b-44da-ac91-9021def8afdf"
	osPrivateSubnetID = "a6c3e3aa-f3b3-44da-98ab-1a20411c7234"
	osImageID         = "854add2b-e306-4ad8-b633-eb9a7e1ffbbd"

	osBuild           = "BUILD"
	osActive          = "ACTIVE"
	osError           = "ERROR"
	osRouterInterface = "network:router_interface"
	osNetworks        = "networks"
	osSubnets         = "subnets"
)

type openstackConfig struct {
	Username       string  `json:"username"`
	Password       string  `json:"password"`
	BuildSeconds   float64 `json:"buildSeconds"`
	DeleteSeconds  float64 `json:"deleteSeconds"`
	RouterSeconds  float64 `json:"routerSeconds"`
	FloatingIPPool int     `json:"floatingIPPool"`
}

type osNetwork struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	External bool     `json:"router:external"`
	Subnets  []string `json:"subnets"`
}

type osSubnet struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	NetworkID string `json:"network_id"`
	CIDR      string `json:"cidr"`
	Gateway   string `json:"gateway_ip"`
	prefix    string // the first three octets
	poolFrom  int
	poolTo    int
	used      map[int]string // last octet -> owner
}

type osRouter struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	GatewayIP string    `json:"gateway_ip,omitempty"`
	Status    string    `json:"status"`
	Created   time.Time `json:"created_at"`
	activeAt  time.Time
}

type osPort struct {
	ID          string `json:"id"`
	NetworkID   string `json:"network_id"`
	SubnetID    string `json:"subnet_id"`
	IP          string `json:"ip_address"`
	DeviceID    string `json:"device_id"`
	DeviceOwner string `json:"device_owner"`
	MAC         string `json:"mac_address"`
}

type osFloatingIP struct {
	ID      string  `json:"id"`
	Address string  `json:"floating_ip_address"`
	PortID  *string `json:"port_id"`
}

type osServer struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Status         string            `json:"status"`
	PowerState     int               `json:"power_state"`
	FlavorID       string            `json:"flavor_id"`
	KeyName        string            `json:"key_name"`
	SecurityGroups []string          `json:"security_groups"`
	Metadata       map[string]string `json:"metadata"`
	Tags           []string          `json:"tags"`
	PortID         string            `json:"port_id"`
	VolumeID       string            `json:"volume_id"`
	Created        time.Time         `json:"created"`
	activeAt       time.Time
	deleteAt       *time.Time
	stuck          bool
	failBuild      bool
}

type osFlavor struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	VCPUs int    `json:"vcpus"`
	RAM   int    `json:"ram"`
	Disk  int    `json:"disk"`
}

type openstackService struct {
	mu        sync.Mutex
	engine    *engine
	cfg       openstackConfig
	networks  map[string]*osNetwork
	subnets   map[string]*osSubnet
	routers   map[string]*osRouter
	ports     map[string]*osPort
	fips      map[string]*osFloatingIP
	servers   map[string]*osServer
	flavors   []osFlavor
	nextError bool
	macs      int
}

func newOpenStackService() *openstackService {
	s := &openstackService{cfg: openstackConfig{
		Username: "admin", Password: "Password01!", BuildSeconds: 3, DeleteSeconds: 2, FloatingIPPool: 6,
	}}
	s.reset()
	return s
}

func (s *openstackService) name() string { return "openstack" }

func (s *openstackService) now() time.Time {
	if s.engine != nil {
		return s.engine.clock()
	}
	return time.Now()
}

func (s *openstackService) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.networks = map[string]*osNetwork{
		osPublicNetID:  {ID: osPublicNetID, Name: "public", External: true, Subnets: []string{osPublicSubnetID}},
		osPrivateNetID: {ID: osPrivateNetID, Name: "private", Subnets: []string{osPrivateSubnetID}},
	}
	pool := max(s.cfg.FloatingIPPool, 1)
	s.subnets = map[string]*osSubnet{
		osPublicSubnetID: {ID: osPublicSubnetID, Name: "public-subnet", NetworkID: osPublicNetID, CIDR: "192.168.20.0/24",
			Gateway: "192.168.20.1", prefix: "192.168.20.", poolFrom: 221, poolTo: 220 + pool, used: map[int]string{}},
		osPrivateSubnetID: {ID: osPrivateSubnetID, Name: "private-subnet", NetworkID: osPrivateNetID, CIDR: "192.168.128.0/24",
			Gateway: "192.168.128.1", prefix: "192.168.128.", poolFrom: 10, poolTo: 250, used: map[int]string{}},
	}
	s.routers, s.ports, s.fips, s.servers = map[string]*osRouter{}, map[string]*osPort{}, map[string]*osFloatingIP{}, map[string]*osServer{}
	s.flavors = []osFlavor{
		{ID: "1", Name: "small", VCPUs: 1, RAM: 2048, Disk: 0},
		{ID: "2", Name: "medium", VCPUs: 2, RAM: 4096, Disk: 0},
		{ID: "3", Name: "large", VCPUs: 4, RAM: 8192, Disk: 0},
	}
	s.nextError = false
}

func (s *openstackService) config() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

func (s *openstackService) setConfig(raw []byte) error {
	s.mu.Lock()
	cfg := s.cfg
	if err := json.Unmarshal(raw, &cfg); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("openstack config: %w", err)
	}
	poolChanged := cfg.FloatingIPPool != s.cfg.FloatingIPPool
	s.cfg = cfg
	if poolChanged {
		subnet := s.subnets[osPublicSubnetID]
		subnet.poolTo = subnet.poolFrom + max(cfg.FloatingIPPool, 1) - 1
	}
	s.mu.Unlock()
	return nil
}

// ---- helpers ------------------------------------------------------------------------------

func (s *openstackService) allocate(subnet *osSubnet, owner string) (string, bool) {
	// A pool shrunk with the config may still have addresses in use beyond its new end: they count.
	if len(subnet.used) >= subnet.poolTo-subnet.poolFrom+1 {
		return "", false
	}
	for octet := subnet.poolFrom; octet <= subnet.poolTo; octet++ {
		if _, taken := subnet.used[octet]; !taken {
			subnet.used[octet] = owner
			return fmt.Sprintf("%s%d", subnet.prefix, octet), true
		}
	}
	return "", false
}

func (s *openstackService) release(subnetID, ip string) {
	subnet := s.subnets[subnetID]
	if subnet == nil || !strings.HasPrefix(ip, subnet.prefix) {
		return
	}
	var octet int
	_, _ = fmt.Sscanf(strings.TrimPrefix(ip, subnet.prefix), "%d", &octet)
	delete(subnet.used, octet)
}

func (s *openstackService) newMAC() string {
	s.macs++
	return fmt.Sprintf("fa:16:3e:%02x:%02x:%02x", s.macs>>16&0xff, s.macs>>8&0xff, s.macs&0xff)
}

func neutronError(w http.ResponseWriter, status int, kind, message string) {
	writeJSON(w, status, map[string]any{"NeutronError": map[string]string{"type": kind, "message": message, "detail": ""}})
}

func novaError(w http.ResponseWriter, status int, message string) {
	key := map[int]string{400: "badRequest", 404: "itemNotFound", 409: "conflictingRequest"}[status]
	writeJSON(w, status, map[string]any{cmpOr(key, "computeFault"): map[string]any{"code": status, "message": message}})
}

// segments splits a path after its prefix.
func segments(path, prefix string) []string {
	rest := strings.Trim(strings.TrimPrefix(path, prefix), "/")
	if rest == "" {
		return nil
	}
	return strings.Split(rest, "/")
}

// ---- Keystone -----------------------------------------------------------------------------

func (s *openstackService) identity(w http.ResponseWriter, r *http.Request) {
	parts := segments(r.URL.Path, "/identity")
	switch {
	case len(parts) <= 1 && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"version": map[string]any{
			"id": "v3.14", "status": "stable", "links": []map[string]string{{"rel": "self", "href": baseURL(r) + "/identity/v3/"}},
			"media-types": []map[string]string{{"base": "application/json", "type": "application/vnd.openstack.identity-v3+json"}},
		}})
	case len(parts) == 3 && parts[1] == "auth" && parts[2] == "tokens" && r.Method == http.MethodPost:
		var body struct {
			Auth struct {
				Identity struct {
					Password struct {
						User struct {
							Name     string `json:"name"`
							Password string `json:"password"`
						} `json:"user"`
					} `json:"password"`
				} `json:"identity"`
			} `json:"auth"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		cfg := s.cfg
		s.mu.Unlock()
		user := body.Auth.Identity.Password.User
		if user.Name != cfg.Username || user.Password != cfg.Password {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{"code": 401, "title": "Unauthorized", "message": "The request you have made requires authentication."}})
			return
		}
		w.Header().Set("X-Subject-Token", "mock-token-"+newUUID())
		writeJSON(w, http.StatusCreated, s.token(r))
	case len(parts) == 3 && parts[1] == "auth" && parts[2] == "tokens" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		writeJSON(w, http.StatusOK, s.token(r))
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

func (s *openstackService) token(r *http.Request) map[string]any {
	base := baseURL(r)
	endpoint := func(kind, name, endpointURL string) map[string]any {
		return map[string]any{"type": kind, "name": name, "id": kind + "-service", "endpoints": []map[string]string{
			{"id": kind + "-public", "interface": "public", "region": "RegionOne", "region_id": "RegionOne", "url": endpointURL},
			{"id": kind + "-internal", "interface": "internal", "region": "RegionOne", "region_id": "RegionOne", "url": endpointURL},
		}}
	}
	now := s.now().UTC()
	domain := map[string]string{"id": "default", "name": "Default"}
	return map[string]any{"token": map[string]any{
		"methods": []string{"password"}, "issued_at": now.Format(time.RFC3339), "expires_at": now.Add(12 * time.Hour).Format(time.RFC3339),
		"user":    map[string]any{"id": osUserID, "name": "admin", "domain": domain},
		"project": map[string]any{"id": osProjectID, "name": "admin", "domain": domain},
		"roles":   []map[string]string{{"id": "admin-role", "name": "admin"}},
		"catalog": []map[string]any{
			endpoint("identity", "keystone", base+"/identity/v3"),
			endpoint("compute", "nova", base+"/compute/v2.1/"+osProjectID),
			endpoint("network", "neutron", base+"/network"),
			endpoint("image", "glance", base+"/image"),
		},
	}}
}

func baseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

// ---- Glance -------------------------------------------------------------------------------

func (s *openstackService) image(w http.ResponseWriter, r *http.Request) {
	parts := segments(r.URL.Path, "/image")
	img := map[string]any{
		"id": osImageID, "name": "ubuntu-2604-kube-v1.36.1", "status": "active", "visibility": "public",
		"created_at": "2026-09-01T10:00:00Z", "updated_at": "2026-09-01T10:00:00Z", "size": 3221225472,
		"disk_format": "qcow2", "container_format": "bare", "min_disk": 0, "min_ram": 0, "protected": false,
		"checksum": "0b1f1d9e1f2a3b4c5d6e7f8091a2b3c4", "tags": []string{}, "file": "/v2/images/" + osImageID + "/file",
		"self": "/v2/images/" + osImageID, "schema": "/v2/schemas/image", "owner": osProjectID,
	}
	switch {
	case len(parts) == 0 && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"versions": []map[string]any{{"id": "v2.16", "status": "CURRENT",
			"links": []map[string]string{{"rel": "self", "href": baseURL(r) + "/image/v2/"}}}}})
	case len(parts) == 2 && parts[0] == "v2" && parts[1] == "images" && r.Method == http.MethodGet:
		images := []map[string]any{}
		if name := r.URL.Query().Get("name"); name == "" || name == img["name"] {
			images = append(images, img)
		}
		writeJSON(w, http.StatusOK, map[string]any{"images": images, "schema": "/v2/schemas/images", "first": "/v2/images"})
	case len(parts) == 3 && parts[0] == "v2" && parts[1] == "images" && r.Method == http.MethodGet:
		if parts[2] != osImageID {
			writeJSON(w, http.StatusNotFound, map[string]string{"message": "No image found with ID " + parts[2]})
			return
		}
		writeJSON(w, http.StatusOK, img)
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

// ---- Neutron ------------------------------------------------------------------------------

func (s *openstackService) network(w http.ResponseWriter, r *http.Request) {
	parts := segments(r.URL.Path, "/network")
	if len(parts) == 0 && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{"versions": []map[string]any{{"id": "v2.0", "status": "CURRENT",
			"links": []map[string]string{{"rel": "self", "href": baseURL(r) + "/network/v2.0/"}}}}})
		return
	}
	if len(parts) < 2 || parts[0] != "v2.0" {
		s.engine.unhandledCall("openstack", w, r)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	kind, rest := parts[1], parts[2:]
	switch kind {
	case osNetworks, osSubnets, "security-groups":
		s.networkLookup(w, r, kind, rest)
	case "routers":
		s.routerCall(w, r, rest)
	case "ports":
		s.portCall(w, r, rest)
	case "floatingips":
		s.floatingIPCall(w, r, rest)
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

// networkLookup serves the read-only Neutron resources: networks, subnets, security groups.
func (s *openstackService) networkLookup(w http.ResponseWriter, r *http.Request, kind string, rest []string) {
	q := r.URL.Query()
	if r.Method != http.MethodGet {
		s.engine.unhandledCall("openstack", w, r)
		return
	}
	switch {
	case kind == osNetworks && len(rest) == 0:
		out := []map[string]any{}
		for _, network := range s.sortedNetworks() {
			if match(q, "name", network.Name) && match(q, "id", network.ID) && match(q, "router:external", fmt.Sprint(network.External)) {
				out = append(out, s.networkJSON(network))
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{osNetworks: out})
	case kind == osNetworks && len(rest) == 1:
		network := s.networks[rest[0]]
		if network == nil {
			neutronError(w, http.StatusNotFound, "NetworkNotFound", "Network "+rest[0]+" could not be found.")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"network": s.networkJSON(network)})
	case kind == osSubnets && len(rest) == 0:
		out := []map[string]any{}
		for _, subnet := range s.subnets {
			if match(q, "name", subnet.Name) && match(q, "id", subnet.ID) && match(q, "network_id", subnet.NetworkID) {
				out = append(out, subnetJSON(subnet))
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"subnets": out})
	case kind == osSubnets && len(rest) == 1:
		subnet := s.subnets[rest[0]]
		if subnet == nil {
			neutronError(w, http.StatusNotFound, "SubnetNotFound", "Subnet "+rest[0]+" could not be found.")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"subnet": subnetJSON(subnet)})
	case kind == "security-groups" && len(rest) == 0:
		out := []map[string]any{}
		if match(q, "name", "default") {
			out = append(out, map[string]any{"id": "default-secgroup", "name": "default", "description": "Default security group",
				"tenant_id": osProjectID, "project_id": osProjectID, "security_group_rules": []any{}})
		}
		writeJSON(w, http.StatusOK, map[string]any{"security_groups": out})
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

func match(q url.Values, key, value string) bool {
	wanted, ok := q[key]
	return !ok || slices.Contains(wanted, value)
}

func (s *openstackService) sortedNetworks() []*osNetwork {
	out := make([]*osNetwork, 0, len(s.networks))
	for _, network := range s.networks {
		out = append(out, network)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *openstackService) networkJSON(n *osNetwork) map[string]any {
	return map[string]any{"id": n.ID, "name": n.Name, "status": osActive, "admin_state_up": true, "router:external": n.External,
		"subnets": n.Subnets, "shared": false, "mtu": 1500, "tenant_id": osProjectID, "project_id": osProjectID,
		"availability_zones": []string{"nova"}, "tags": []string{}, "description": "", "port_security_enabled": true}
}

func subnetJSON(sn *osSubnet) map[string]any {
	return map[string]any{"id": sn.ID, "name": sn.Name, "network_id": sn.NetworkID, "cidr": sn.CIDR, "gateway_ip": sn.Gateway,
		"ip_version": 4, "enable_dhcp": true, "tenant_id": osProjectID, "project_id": osProjectID,
		"allocation_pools": []map[string]string{{"start": fmt.Sprintf("%s%d", sn.prefix, sn.poolFrom), "end": fmt.Sprintf("%s%d", sn.prefix, sn.poolTo)}},
		"dns_nameservers":  []string{}, "host_routes": []any{}, "tags": []string{}, "description": ""}
}

func (s *openstackService) routerJSON(router *osRouter) map[string]any {
	if router.Status == osBuild && !s.now().Before(router.activeAt) {
		router.Status = osActive
	}
	gateway := map[string]any{"network_id": osPublicNetID, "enable_snat": true,
		"external_fixed_ips": []map[string]string{{"subnet_id": osPublicSubnetID, "ip_address": router.GatewayIP}}}
	return map[string]any{"id": router.ID, "name": router.Name, "status": router.Status, "admin_state_up": true,
		"external_gateway_info": gateway, "tenant_id": osProjectID, "project_id": osProjectID, "distributed": false, "ha": false,
		"routes": []any{}, "tags": []string{}, "description": "", "availability_zone_hints": []string{},
		"created_at": router.Created.UTC().Format(time.RFC3339), "updated_at": router.Created.UTC().Format(time.RFC3339)}
}

func (s *openstackService) routerCall(w http.ResponseWriter, r *http.Request, rest []string) {
	switch {
	case len(rest) == 0 && r.Method == http.MethodGet:
		out := []map[string]any{}
		for _, router := range s.routers {
			if match(r.URL.Query(), "name", router.Name) && match(r.URL.Query(), "id", router.ID) {
				out = append(out, s.routerJSON(router))
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"routers": out})
	case len(rest) == 0 && r.Method == http.MethodPost:
		var body struct {
			Router struct {
				Name string `json:"name"`
			} `json:"router"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		id := newUUID()
		ip, ok := s.allocate(s.subnets[osPublicSubnetID], "router:"+id)
		if !ok {
			neutronError(w, http.StatusConflict, "IpAddressGenerationFailure", "No more IP addresses available on network "+osPublicNetID+".")
			return
		}
		now := s.now()
		router := &osRouter{ID: id, Name: body.Router.Name, GatewayIP: ip, Status: osActive, Created: now}
		if s.cfg.RouterSeconds > 0 {
			router.Status, router.activeAt = osBuild, now.Add(seconds(s.cfg.RouterSeconds))
		}
		s.routers[id] = router
		writeJSON(w, http.StatusCreated, map[string]any{"router": s.routerJSON(router)})
	case len(rest) == 1:
		router := s.routers[rest[0]]
		if router == nil {
			neutronError(w, http.StatusNotFound, "RouterNotFound", "Router "+rest[0]+" could not be found")
			return
		}
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]any{"router": s.routerJSON(router)})
		case http.MethodPut:
			var body struct {
				Router struct {
					Name *string `json:"name"`
				} `json:"router"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Router.Name != nil {
				router.Name = *body.Router.Name
			}
			writeJSON(w, http.StatusOK, map[string]any{"router": s.routerJSON(router)})
		case http.MethodDelete:
			for _, port := range s.ports {
				if port.DeviceID == router.ID && port.DeviceOwner == osRouterInterface {
					neutronError(w, http.StatusConflict, "RouterInUse", "Router "+router.ID+" still has ports")
					return
				}
			}
			s.release(osPublicSubnetID, router.GatewayIP)
			delete(s.routers, router.ID)
			w.WriteHeader(http.StatusNoContent)
		default:
			s.engine.unhandledCall("openstack", w, r)
		}
	case len(rest) == 2 && r.Method == http.MethodPut && (rest[1] == "add_router_interface" || rest[1] == "remove_router_interface"):
		router := s.routers[rest[0]]
		if router == nil {
			neutronError(w, http.StatusNotFound, "RouterNotFound", "Router "+rest[0]+" could not be found")
			return
		}
		var body struct {
			SubnetID string `json:"subnet_id"`
			PortID   string `json:"port_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if rest[1] == "add_router_interface" {
			s.addInterface(w, router, body.SubnetID)
		} else {
			s.removeInterface(w, router, body.SubnetID, body.PortID)
		}
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

func interfaceJSON(router *osRouter, port *osPort) map[string]any {
	return map[string]any{"id": router.ID, "subnet_id": port.SubnetID, "subnet_ids": []string{port.SubnetID}, "port_id": port.ID,
		"network_id": port.NetworkID, "tenant_id": osProjectID, "project_id": osProjectID, "tags": []string{}}
}

func (s *openstackService) addInterface(w http.ResponseWriter, router *osRouter, subnetID string) {
	subnet := s.subnets[subnetID]
	if subnet == nil {
		neutronError(w, http.StatusNotFound, "SubnetNotFound", "Subnet "+subnetID+" could not be found.")
		return
	}
	for _, port := range s.ports {
		if port.SubnetID == subnetID && port.IP == subnet.Gateway {
			neutronError(w, http.StatusConflict, "IpAddressAlreadyAllocated",
				fmt.Sprintf("IP address %s already allocated in subnet %s", subnet.Gateway, subnetID))
			return
		}
	}
	port := &osPort{ID: newUUID(), NetworkID: subnet.NetworkID, SubnetID: subnetID, IP: subnet.Gateway, DeviceID: router.ID,
		DeviceOwner: osRouterInterface, MAC: s.newMAC()}
	s.ports[port.ID] = port
	writeJSON(w, http.StatusOK, interfaceJSON(router, port))
}

func (s *openstackService) removeInterface(w http.ResponseWriter, router *osRouter, subnetID, portID string) {
	var port *osPort
	for _, candidate := range s.ports {
		if candidate.DeviceID == router.ID && candidate.DeviceOwner == osRouterInterface &&
			(candidate.ID == portID || portID == "" && candidate.SubnetID == subnetID) {
			port = candidate
		}
	}
	if port == nil {
		neutronError(w, http.StatusNotFound, "RouterInterfaceNotFoundForSubnet", "Router "+router.ID+" has no interface on subnet "+subnetID)
		return
	}
	for _, fip := range s.fips {
		if fip.PortID == nil {
			continue
		}
		if target := s.ports[*fip.PortID]; target != nil && target.SubnetID == port.SubnetID {
			neutronError(w, http.StatusConflict, "RouterInterfaceInUseByFloatingIP",
				fmt.Sprintf("Router interface for subnet %s on router %s cannot be deleted, as it is required by one or more floating IPs.", port.SubnetID, router.ID))
			return
		}
	}
	delete(s.ports, port.ID)
	writeJSON(w, http.StatusOK, interfaceJSON(router, port))
}

func (s *openstackService) portJSON(port *osPort) map[string]any {
	return map[string]any{"id": port.ID, "name": "", "network_id": port.NetworkID, "device_id": port.DeviceID, "device_owner": port.DeviceOwner,
		"status": osActive, "admin_state_up": true, "mac_address": port.MAC, "tenant_id": osProjectID, "project_id": osProjectID,
		"fixed_ips": []map[string]string{{"subnet_id": port.SubnetID, "ip_address": port.IP}}, "security_groups": []string{},
		"allowed_address_pairs": []any{}, "tags": []string{}, "description": "", "binding:vnic_type": "normal"}
}

func (s *openstackService) portCall(w http.ResponseWriter, r *http.Request, rest []string) {
	q := r.URL.Query()
	switch {
	case len(rest) == 0 && r.Method == http.MethodGet:
		out := []map[string]any{}
		for _, port := range s.ports {
			fixed := q.Get("fixed_ips")
			if match(q, "device_id", port.DeviceID) && match(q, "network_id", port.NetworkID) && match(q, "device_owner", port.DeviceOwner) &&
				match(q, "id", port.ID) && (fixed == "" || fixed == "subnet_id="+port.SubnetID || fixed == "ip_address="+port.IP) {
				out = append(out, s.portJSON(port))
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ports": out})
	case len(rest) == 1 && r.Method == http.MethodGet:
		port := s.ports[rest[0]]
		if port == nil {
			neutronError(w, http.StatusNotFound, "PortNotFound", "Port "+rest[0]+" could not be found.")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"port": s.portJSON(port)})
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

func (s *openstackService) floatingIPJSON(fip *osFloatingIP) map[string]any {
	out := map[string]any{"id": fip.ID, "floating_ip_address": fip.Address, "floating_network_id": osPublicNetID, "port_id": nil,
		"fixed_ip_address": nil, "router_id": nil, "status": "DOWN", "tenant_id": osProjectID, "project_id": osProjectID,
		"description": "", "tags": []string{}, "dns_name": "", "dns_domain": ""}
	if fip.PortID != nil {
		if port := s.ports[*fip.PortID]; port != nil {
			out["port_id"], out["fixed_ip_address"], out["status"] = port.ID, port.IP, osActive
			for _, iface := range s.ports {
				if iface.DeviceOwner == osRouterInterface && iface.SubnetID == port.SubnetID {
					out["router_id"] = iface.DeviceID
				}
			}
		}
	}
	return out
}

func (s *openstackService) floatingIPCall(w http.ResponseWriter, r *http.Request, rest []string) {
	q := r.URL.Query()
	switch {
	case len(rest) == 0 && r.Method == http.MethodGet:
		out := []map[string]any{}
		for _, fip := range s.fips {
			port := ""
			if fip.PortID != nil {
				port = *fip.PortID
			}
			if match(q, "floating_ip_address", fip.Address) && match(q, "id", fip.ID) && match(q, "port_id", port) {
				out = append(out, s.floatingIPJSON(fip))
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"floatingips": out})
	case len(rest) == 0 && r.Method == http.MethodPost:
		var body struct {
			FloatingIP struct {
				NetworkID string  `json:"floating_network_id"`
				PortID    *string `json:"port_id"`
			} `json:"floatingip"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.FloatingIP.NetworkID != osPublicNetID {
			neutronError(w, http.StatusBadRequest, "BadRequest", "floating_network_id must be the external network")
			return
		}
		id := newUUID()
		ip, ok := s.allocate(s.subnets[osPublicSubnetID], "floatingip:"+id)
		if !ok {
			neutronError(w, http.StatusConflict, "IpAddressGenerationFailure", "No more IP addresses available on network "+osPublicNetID+".")
			return
		}
		fip := &osFloatingIP{ID: id, Address: ip, PortID: body.FloatingIP.PortID}
		s.fips[id] = fip
		writeJSON(w, http.StatusCreated, map[string]any{"floatingip": s.floatingIPJSON(fip)})
	case len(rest) == 1:
		fip := s.fips[rest[0]]
		if fip == nil {
			neutronError(w, http.StatusNotFound, "FloatingIPNotFound", "Floating IP "+rest[0]+" could not be found")
			return
		}
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]any{"floatingip": s.floatingIPJSON(fip)})
		case http.MethodPut:
			var body struct {
				FloatingIP map[string]*string `json:"floatingip"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if portID, ok := body.FloatingIP["port_id"]; ok {
				if portID != nil && s.ports[*portID] == nil {
					neutronError(w, http.StatusNotFound, "PortNotFound", "Port "+*portID+" could not be found.")
					return
				}
				fip.PortID = portID
			}
			writeJSON(w, http.StatusOK, map[string]any{"floatingip": s.floatingIPJSON(fip)})
		case http.MethodDelete:
			s.release(osPublicSubnetID, fip.Address)
			delete(s.fips, fip.ID)
			w.WriteHeader(http.StatusNoContent)
		default:
			s.engine.unhandledCall("openstack", w, r)
		}
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

// ---- Nova ---------------------------------------------------------------------------------

func seconds(v float64) time.Duration { return time.Duration(v * float64(time.Second)) }

// advance moves a server along BUILD and deletion with the clock. It returns false once the
// server is gone.
func (s *openstackService) advance(server *osServer) bool {
	now := s.now()
	if server.deleteAt != nil && !server.stuck && !now.Before(*server.deleteAt) {
		s.removeServer(server)
		return false
	}
	if server.Status == osBuild && !server.stuck && !now.Before(server.activeAt) {
		if server.failBuild {
			server.Status, server.PowerState = osError, 0
		} else {
			server.Status, server.PowerState = osActive, 1
		}
	}
	return true
}

func (s *openstackService) removeServer(server *osServer) {
	if port := s.ports[server.PortID]; port != nil {
		for _, fip := range s.fips {
			if fip.PortID != nil && *fip.PortID == port.ID {
				fip.PortID = nil
			}
		}
		s.release(port.SubnetID, port.IP)
		delete(s.ports, port.ID)
	}
	delete(s.servers, server.ID)
}

func (s *openstackService) serverJSON(r *http.Request, server *osServer) map[string]any {
	addresses := []map[string]any{}
	accessIP := ""
	if port := s.ports[server.PortID]; port != nil {
		accessIP = port.IP
		addresses = append(addresses, map[string]any{"addr": port.IP, "version": 4, "OS-EXT-IPS:type": "fixed", "OS-EXT-IPS-MAC:mac_addr": port.MAC})
		for _, fip := range s.fips {
			if fip.PortID != nil && *fip.PortID == port.ID {
				addresses = append(addresses, map[string]any{"addr": fip.Address, "version": 4, "OS-EXT-IPS:type": "floating", "OS-EXT-IPS-MAC:mac_addr": port.MAC})
			}
		}
	}
	vmState := map[string]string{osBuild: "building", osActive: "active", "SHUTOFF": "stopped", osError: "error"}[server.Status]
	groups := make([]map[string]string, 0, len(server.SecurityGroups))
	for _, name := range server.SecurityGroups {
		groups = append(groups, map[string]string{"name": name})
	}
	var taskState any
	if server.deleteAt != nil {
		taskState = "deleting"
	} else if server.Status == osBuild {
		taskState = "spawning"
	}
	created := server.Created.UTC().Format(time.RFC3339)
	out := map[string]any{
		"id": server.ID, "name": server.Name, "status": server.Status, "tenant_id": osProjectID, "user_id": osUserID,
		"metadata": server.Metadata, "hostId": "mockhost", "image": "", "key_name": server.KeyName,
		"flavor":  map[string]any{"id": server.FlavorID, "links": []map[string]string{{"rel": "bookmark", "href": baseURL(r) + "/compute/flavors/" + server.FlavorID}}},
		"created": created, "updated": created, "addresses": map[string]any{"private": addresses},
		"accessIPv4": accessIP, "accessIPv6": "", "security_groups": groups, "progress": 0, "config_drive": "",
		"OS-EXT-STS:vm_state": vmState, "OS-EXT-STS:power_state": server.PowerState, "OS-EXT-STS:task_state": taskState,
		"OS-EXT-AZ:availability_zone": "nova", "OS-DCF:diskConfig": "MANUAL", "locked": false, "tags": append([]string{}, server.Tags...),
		"os-extended-volumes:volumes_attached": []map[string]any{{"id": server.VolumeID, "delete_on_termination": true}},
		"links":                                []map[string]string{{"rel": "self", "href": baseURL(r) + "/compute/v2.1/" + osProjectID + "/servers/" + server.ID}},
	}
	if server.Status == osError {
		out["fault"] = map[string]any{"code": 500, "message": "Mock: the server went into ERROR", "created": created}
	}
	return out
}

func (s *openstackService) findServer(idOrName string) *osServer {
	if server := s.servers[idOrName]; server != nil {
		return server
	}
	for _, server := range s.servers {
		if server.Name == idOrName {
			return server
		}
	}
	return nil
}

func (s *openstackService) compute(w http.ResponseWriter, r *http.Request) {
	parts := segments(r.URL.Path, "/compute")
	if len(parts) <= 1 && r.Method == http.MethodGet {
		version := map[string]any{"id": "v2.1", "status": "CURRENT", "version": "2.79", "min_version": "2.1",
			"links": []map[string]string{{"rel": "self", "href": baseURL(r) + "/compute/v2.1/"}}}
		if len(parts) == 1 {
			writeJSON(w, http.StatusOK, map[string]any{"version": version})
		} else {
			writeJSON(w, http.StatusOK, map[string]any{"versions": []any{version}})
		}
		return
	}
	if len(parts) < 3 || parts[0] != "v2.1" || parts[1] != osProjectID {
		s.engine.unhandledCall("openstack", w, r)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	kind, rest := parts[2], parts[3:]
	switch kind {
	case "flavors":
		s.flavorCall(w, r, rest)
	case "servers":
		s.serverCall(w, r, rest)
	case "os-keypairs":
		if len(rest) == 1 && r.Method == http.MethodGet && rest[0] == "capi-key" {
			writeJSON(w, http.StatusOK, map[string]any{"keypair": map[string]any{"name": "capi-key", "type": "ssh",
				"public_key": "ssh-ed25519 AAAAmock capi-key", "fingerprint": "mock:fingerprint", "user_id": osUserID}})
			return
		}
		novaError(w, http.StatusNotFound, "Keypair not found")
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

func (s *openstackService) flavorCall(w http.ResponseWriter, r *http.Request, rest []string) {
	flavorJSON := func(f osFlavor) map[string]any {
		return map[string]any{"id": f.ID, "name": f.Name, "vcpus": f.VCPUs, "ram": f.RAM, "disk": f.Disk, "OS-FLV-EXT-DATA:ephemeral": 0,
			"swap": "", "rxtx_factor": 1.0, "os-flavor-access:is_public": true, "links": []any{}}
	}
	switch {
	case r.Method == http.MethodGet && (len(rest) == 0 || len(rest) == 1 && rest[0] == "detail"):
		out := make([]map[string]any, 0, len(s.flavors))
		for _, f := range s.flavors {
			out = append(out, flavorJSON(f))
		}
		writeJSON(w, http.StatusOK, map[string]any{"flavors": out})
	case r.Method == http.MethodGet && len(rest) == 1:
		for _, f := range s.flavors {
			if f.ID == rest[0] {
				writeJSON(w, http.StatusOK, map[string]any{"flavor": flavorJSON(f)})
				return
			}
		}
		novaError(w, http.StatusNotFound, "Flavor "+rest[0]+" could not be found.")
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

func (s *openstackService) serverCall(w http.ResponseWriter, r *http.Request, rest []string) {
	switch {
	case len(rest) == 0 && r.Method == http.MethodPost:
		s.createServer(w, r)
	case r.Method == http.MethodGet && (len(rest) == 0 || len(rest) == 1 && rest[0] == "detail"):
		out := []map[string]any{}
		for _, server := range s.servers {
			if s.advance(server) && match(r.URL.Query(), "name", server.Name) {
				out = append(out, s.serverJSON(r, server))
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"servers": out})
	case len(rest) >= 1:
		server := s.servers[rest[0]]
		if server == nil || !s.advance(server) {
			novaError(w, http.StatusNotFound, "Instance "+rest[0]+" could not be found.")
			return
		}
		s.serverSubCall(w, r, server, rest[1:])
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

func (s *openstackService) createServer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Server struct {
			Name           string            `json:"name"`
			FlavorRef      string            `json:"flavorRef"`
			KeyName        string            `json:"key_name"`
			Metadata       map[string]string `json:"metadata"`
			SecurityGroups []struct {
				Name string `json:"name"`
			} `json:"security_groups"`
			Networks []struct {
				UUID string `json:"uuid"`
				Port string `json:"port"`
			} `json:"networks"`
		} `json:"server"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		novaError(w, http.StatusBadRequest, "Malformed request body")
		return
	}
	req := body.Server
	flavorOK := false
	for _, f := range s.flavors {
		flavorOK = flavorOK || f.ID == req.FlavorRef
	}
	if !flavorOK {
		novaError(w, http.StatusBadRequest, "Flavor "+req.FlavorRef+" could not be found.")
		return
	}
	if len(req.Networks) != 1 || s.networks[req.Networks[0].UUID] == nil {
		novaError(w, http.StatusBadRequest, "The mock needs exactly one existing network in networks")
		return
	}
	groups := []string{}
	for _, group := range req.SecurityGroups {
		if group.Name != "default" {
			novaError(w, http.StatusBadRequest, "Security group "+group.Name+" not found for project "+osProjectID+".")
			return
		}
		groups = append(groups, group.Name)
	}
	if len(groups) == 0 {
		groups = []string{"default"}
	}
	id := newUUID()
	network := s.networks[req.Networks[0].UUID]
	subnet := s.subnets[network.Subnets[0]]
	ip, ok := s.allocate(subnet, "server:"+id)
	if !ok {
		novaError(w, http.StatusConflict, "No more IP addresses available on network "+network.ID)
		return
	}
	port := &osPort{ID: newUUID(), NetworkID: network.ID, SubnetID: subnet.ID, IP: ip, DeviceID: id, DeviceOwner: "compute:nova", MAC: s.newMAC()}
	s.ports[port.ID] = port
	now := s.now()
	metadata := req.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	server := &osServer{ID: id, Name: req.Name, Status: osBuild, FlavorID: req.FlavorRef, KeyName: req.KeyName, SecurityGroups: groups,
		Metadata: metadata, PortID: port.ID, VolumeID: newUUID(), Created: now, activeAt: now.Add(seconds(s.cfg.BuildSeconds)), failBuild: s.nextError}
	s.nextError = false
	s.servers[id] = server
	writeJSON(w, http.StatusAccepted, map[string]any{"server": map[string]any{
		"id": id, "adminPass": "mock-admin-pass", "OS-DCF:diskConfig": "MANUAL", "security_groups": []map[string]string{{"name": groups[0]}},
		"links": []map[string]string{{"rel": "self", "href": baseURL(r) + "/compute/v2.1/" + osProjectID + "/servers/" + id}},
	}})
}

func (s *openstackService) serverSubCall(w http.ResponseWriter, r *http.Request, server *osServer, rest []string) {
	switch {
	case len(rest) == 0 && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"server": s.serverJSON(r, server)})
	case len(rest) == 0 && r.Method == http.MethodPut:
		var body struct {
			Server struct {
				Name *string `json:"name"`
			} `json:"server"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Server.Name != nil {
			server.Name = *body.Server.Name
		}
		writeJSON(w, http.StatusOK, map[string]any{"server": s.serverJSON(r, server)})
	case len(rest) == 0 && r.Method == http.MethodDelete:
		if server.deleteAt == nil {
			at := s.now().Add(seconds(s.cfg.DeleteSeconds))
			server.deleteAt = &at
		}
		w.WriteHeader(http.StatusNoContent)
	case len(rest) >= 1 && rest[0] != "action":
		s.serverDetailCall(w, r, server, rest)
	case len(rest) == 1 && rest[0] == "action" && r.Method == http.MethodPost:
		s.serverAction(w, r, server)
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

// serverDetailCall serves the parts of a server: metadata, tags, security groups, interfaces,
// volume attachments.
func (s *openstackService) serverDetailCall(w http.ResponseWriter, r *http.Request, server *osServer, rest []string) {
	switch {
	case len(rest) == 1 && rest[0] == "metadata":
		switch r.Method {
		case http.MethodGet:
		case http.MethodPost, http.MethodPut:
			var body struct {
				Metadata map[string]string `json:"metadata"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if r.Method == http.MethodPut {
				server.Metadata = map[string]string{}
			}
			maps.Copy(server.Metadata, body.Metadata)
		default:
			s.engine.unhandledCall("openstack", w, r)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"metadata": server.Metadata})
	case len(rest) == 2 && rest[0] == "metadata" && r.Method == http.MethodDelete:
		delete(server.Metadata, rest[1])
		w.WriteHeader(http.StatusNoContent)
	case len(rest) == 1 && rest[0] == "tags" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"tags": append([]string{}, server.Tags...)})
	case len(rest) == 1 && rest[0] == "tags" && r.Method == http.MethodPut:
		var body struct {
			Tags []string `json:"tags"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		server.Tags = body.Tags
		writeJSON(w, http.StatusOK, map[string]any{"tags": server.Tags})
	case len(rest) == 1 && rest[0] == "os-security-groups" && r.Method == http.MethodGet:
		out := []map[string]any{}
		for _, name := range server.SecurityGroups {
			out = append(out, map[string]any{"id": name + "-secgroup", "name": name, "description": "", "tenant_id": osProjectID, "rules": []any{}})
		}
		writeJSON(w, http.StatusOK, map[string]any{"security_groups": out})
	case len(rest) == 1 && rest[0] == "os-interface" && r.Method == http.MethodGet:
		out := []map[string]any{}
		if port := s.ports[server.PortID]; port != nil {
			out = append(out, map[string]any{"port_id": port.ID, "net_id": port.NetworkID, "mac_addr": port.MAC, "port_state": osActive,
				"fixed_ips": []map[string]string{{"subnet_id": port.SubnetID, "ip_address": port.IP}}})
		}
		writeJSON(w, http.StatusOK, map[string]any{"interfaceAttachments": out})
	case len(rest) == 1 && rest[0] == "os-volume_attachments" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"volumeAttachments": []map[string]any{
			{"id": server.VolumeID, "volumeId": server.VolumeID, "serverId": server.ID, "device": "/dev/vda"}}})
	default:
		s.engine.unhandledCall("openstack", w, r)
	}
}

func (s *openstackService) serverAction(w http.ResponseWriter, r *http.Request, server *osServer) {
	var body map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body) != 1 {
		novaError(w, http.StatusBadRequest, "An action needs exactly one key")
		return
	}
	var args struct {
		Name    string `json:"name"`
		Address string `json:"address"`
	}
	for action, raw := range body {
		_ = json.Unmarshal(raw, &args)
		switch action {
		case "os-stop":
			server.Status, server.PowerState = "SHUTOFF", 4
		case "os-start":
			server.Status, server.PowerState = osActive, 1
		case "addSecurityGroup":
			server.SecurityGroups = appendUnique(server.SecurityGroups, args.Name)
		case "removeSecurityGroup":
			kept := server.SecurityGroups[:0]
			for _, name := range server.SecurityGroups {
				if name != args.Name {
					kept = append(kept, name)
				}
			}
			server.SecurityGroups = kept
		case "addFloatingIp", "removeFloatingIp":
			var fip *osFloatingIP
			for _, candidate := range s.fips {
				if candidate.Address == args.Address {
					fip = candidate
				}
			}
			if fip == nil {
				novaError(w, http.StatusNotFound, "floating IP not found")
				return
			}
			if action == "addFloatingIp" {
				port := server.PortID
				fip.PortID = &port
			} else {
				fip.PortID = nil
			}
		default:
			s.engine.unhandledCall("openstack", w, r)
			return
		}
	}
	w.WriteHeader(http.StatusAccepted)
}

func appendUnique(list []string, value string) []string {
	if slices.Contains(list, value) {
		return list
	}
	return append(list, value)
}

// ---- state and controls -------------------------------------------------------------------

func (s *openstackService) state() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	servers := make([]map[string]any, 0, len(s.servers))
	for _, server := range s.servers {
		if !s.advance(server) {
			continue
		}
		ip := ""
		if port := s.ports[server.PortID]; port != nil {
			ip = port.IP
		}
		servers = append(servers, map[string]any{"id": server.ID, "name": server.Name, "status": server.Status, "power_state": server.PowerState,
			"ip": ip, "security_groups": server.SecurityGroups, "deleting": server.deleteAt != nil, "stuck": server.stuck})
	}
	routers := make([]map[string]any, 0, len(s.routers))
	for _, router := range s.routers {
		routers = append(routers, map[string]any{"id": router.ID, "name": router.Name, "status": router.Status, "gateway_ip": router.GatewayIP})
	}
	ports := make([]*osPort, 0, len(s.ports))
	for _, port := range s.ports {
		ports = append(ports, port)
	}
	fips := make([]map[string]any, 0, len(s.fips))
	for _, fip := range s.fips {
		fips = append(fips, s.floatingIPJSON(fip))
	}
	pool := s.subnets[osPublicSubnetID]
	return map[string]any{"servers": servers, "routers": routers, "ports": ports, "floatingips": fips,
		"publicPool": map[string]int{"size": pool.poolTo - pool.poolFrom + 1, "used": len(pool.used)}, "nextServerError": s.nextError}
}

func (s *openstackService) duplicates() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]string{}
	servers, routers := make([]string, 0, len(s.servers)), make([]string, 0, len(s.routers))
	for _, server := range s.servers {
		servers = append(servers, server.Name)
	}
	for _, router := range s.routers {
		routers = append(routers, router.Name)
	}
	if dup := duplicateNames(servers); len(dup) > 0 {
		out["servers"] = dup
	}
	if dup := duplicateNames(routers); len(dup) > 0 {
		out["routers"] = dup
	}
	return out
}

// control serves the OpenStack controls under /__mock/: changes made "by hand" or by Virtuozzo.
func (s *openstackService) control(w http.ResponseWriter, r *http.Request, path string) bool {
	parts := strings.Split(path, "/")
	if r.Method != http.MethodPost {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case path == "next/servers/error":
		s.nextError = true
	case len(parts) == 3 && parts[0] == "servers":
		server := s.findServer(parts[1])
		if server == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no server " + parts[1]})
			return true
		}
		switch parts[2] {
		case "error":
			server.Status, server.PowerState, server.failBuild = osError, 0, true
		case "stuck":
			server.stuck = true
		case "unstuck":
			server.stuck = false
		case "delete":
			s.removeServer(server)
		case "stop":
			server.Status, server.PowerState = "SHUTOFF", 4
		case "remove-security-group":
			server.SecurityGroups = []string{}
		default:
			return false
		}
	case len(parts) == 3 && parts[0] == "routers" && parts[2] == "delete":
		var router *osRouter
		for _, candidate := range s.routers {
			if candidate.ID == parts[1] || candidate.Name == parts[1] {
				router = candidate
			}
		}
		if router == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no router " + parts[1]})
			return true
		}
		for id, port := range s.ports {
			if port.DeviceID == router.ID {
				delete(s.ports, id)
			}
		}
		s.release(osPublicSubnetID, router.GatewayIP)
		delete(s.routers, router.ID)
	case len(parts) == 3 && parts[0] == "floatingips" && parts[2] == "release":
		for id, fip := range s.fips {
			if fip.Address == parts[1] || fip.ID == parts[1] {
				s.release(osPublicSubnetID, fip.Address)
				delete(s.fips, id)
			}
		}
	default:
		return false
	}
	writeJSON(w, http.StatusOK, map[string]string{"done": path})
	return true
}
