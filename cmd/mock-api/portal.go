package main

// A simulated admin portal with the calls of the FortiGate (p2p) workflow of latch: iTop blocks
// and subnets, P2P segments, portal lookups, the delivery and its job of 14 phases. Resources live
// in memory; a job advances with the clock when it is read.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	portalTotalSteps = 14
	portalFortiGate  = "fortigate-vm"

	jobQueued  = "queued"
	jobRunning = "running"
	jobSuccess = "success"
	jobFailed  = "failed"
)

type portalConfig struct {
	APIKey       string  `json:"apiKey"`
	PhaseSeconds float64 `json:"phaseSeconds"`
	LockSeconds  float64 `json:"lockSeconds"`
	LogKiB       int     `json:"logKiB"`
	FreeSubnets  int     `json:"freeSubnets"`
}

type portalSubnet struct {
	ID        int    `json:"id"`
	IP        string `json:"ip"`
	Mask      int    `json:"mask"`
	Available bool   `json:"available"`
	BlockID   int    `json:"block_id"`
	NSXIP     string `json:"nsx_ip"`
	TenantIP  string `json:"tenant_ip"`
}

type portalSegment struct {
	Name     string `json:"name"`
	EnvName  string `json:"env_name"`
	Client   string `json:"client_name"`
	SubnetID int    `json:"itop_subnet_id"`
	Network  string `json:"network"`
	InUse    bool   `json:"in_use"`
}

type portalEnv struct {
	ID          int     `json:"id"`
	Type        string  `json:"env_type"`
	VCloudName  *string `json:"vcloud_env_name"`
	VCenterName string  `json:"vcenter_env_name"`
	Label       string  `json:"label"`
}

type portalTemplate struct {
	ID      int    `json:"id"`
	Label   string `json:"label"`
	VCenter string `json:"vcenter_env_name"`
	// lockedBy is the job that holds the template; -1 for a lock made with the mock control.
	lockedBy int
}

// jobScript says how the next job breaks: phases where it fails (one per attempt), phases where
// it waits for a person, the phase where the rollback fails, or that it never ends.
type jobScript struct {
	FailAt         []int          `json:"failAt,omitempty"`
	WaitAt         map[int]string `json:"waitAt,omitempty"`
	ManualReviewAt int            `json:"manualReviewAt,omitempty"`
	NeverEnds      bool           `json:"neverEnds,omitempty"`
}

type portalJob struct {
	ID            int        `json:"id"`
	Status        string     `json:"status"`
	VMName        string     `json:"vm_name"`
	TemplateID    int        `json:"template_id"`
	LastCompleted int        `json:"last_completed_step"`
	Total         int        `json:"total_steps"`
	LicensingMode string     `json:"licensing_mode"`
	LicenseSerial *string    `json:"license_serial"`
	ManualReview  bool       `json:"manual_review_required"`
	Rollback      bool       `json:"rollback_attempted"`
	AssignedIP    string     `json:"assigned_ip"`
	Retries       int        `json:"retries"`
	Created       time.Time  `json:"created_at"`
	FailedAt      *time.Time `json:"failed_at,omitempty"`
	Stuck         bool       `json:"stuck,omitempty"`
	Script        jobScript  `json:"script"`
	log           []string
	nextAt        time.Time
	resolved      map[int]bool
}

type portalService struct {
	mu        sync.Mutex
	engine    *engine
	cfg       portalConfig
	subnets   []*portalSubnet
	segments  map[string]*portalSegment
	envs      []portalEnv
	templates []*portalTemplate
	jobs      map[int]*portalJob
	nextJob   int
	script    jobScript
}

func newPortalService() *portalService {
	s := &portalService{cfg: portalConfig{APIKey: "mock-portal-key", PhaseSeconds: 2, LockSeconds: 600, FreeSubnets: 2}}
	s.reset()
	return s
}

func (s *portalService) name() string { return "portal" }

func (s *portalService) now() time.Time {
	if s.engine != nil {
		return s.engine.clock()
	}
	return time.Now()
}

func (s *portalService) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subnets = nil
	for i := range 4 {
		ip := fmt.Sprintf("178.239.21.%d", 30+2*i)
		s.subnets = append(s.subnets, &portalSubnet{ID: 501 + i, IP: ip, Mask: 31, Available: i < s.cfg.FreeSubnets, BlockID: 7,
			NSXIP: ip, TenantIP: fmt.Sprintf("178.239.21.%d", 31+2*i)})
	}
	vcloud := "vcloud-r1.dinova.cloud"
	s.envs = []portalEnv{
		{ID: 3, Type: "vcloud", VCloudName: &vcloud, VCenterName: "vcenter-r1.dinova.cloud", Label: "vCloud R1"},
		{ID: 1, Type: "vcenter", VCenterName: "vcenter-r1.dinova.cloud", Label: "vCenter R1"},
	}
	s.templates = []*portalTemplate{
		{ID: 4, Label: "FortiOS 7.4.8 SMALL", VCenter: "vcenter-r1.dinova.cloud"},
		{ID: 5, Label: "FortiOS 7.4.8 MEDIUM", VCenter: "vcenter-r1.dinova.cloud"},
		{ID: 6, Label: "FortiOS 7.2.11", VCenter: "vcenter-r1.dinova.cloud"},
	}
	s.segments, s.jobs, s.nextJob, s.script = map[string]*portalSegment{}, map[int]*portalJob{}, 100, jobScript{}
}

func (s *portalService) config() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

func (s *portalService) setConfig(raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := s.cfg
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("portal config: %w", err)
	}
	if cfg.FreeSubnets != s.cfg.FreeSubnets {
		for i, subnet := range s.subnets {
			subnet.Available = i < cfg.FreeSubnets && !s.subnetUsed(subnet.ID)
		}
	}
	s.cfg = cfg
	return nil
}

func (s *portalService) subnetUsed(id int) bool {
	for _, segment := range s.segments {
		if segment.SubnetID == id {
			return true
		}
	}
	return false
}

// portalError answers in the form of the portal's api_error().
func portalError(w http.ResponseWriter, status int, code, message string, detail any) {
	writeJSON(w, status, map[string]any{"detail": map[string]any{"code": code, "message": message, "detail": detail}})
}

// validationError answers in the form of a Pydantic validation error, naming the field.
func validationError(w http.ResponseWriter, field, message, kind string) {
	writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"detail": []map[string]any{
		{"loc": []string{"body", field}, "msg": message, "type": kind},
	}})
}

func (s *portalService) serve(w http.ResponseWriter, r *http.Request) {
	parts := segments(r.URL.Path, "/portal/api/v1")
	s.mu.Lock()
	key := s.cfg.APIKey
	s.mu.Unlock()
	if r.Header.Get("X-API-Key") != key {
		portalError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing or invalid API key", nil)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q := r.URL.Query()
	path := strings.Join(parts, "/")
	if lookup, ok := s.lookups()[path]; ok && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, lookup(q))
		return
	}
	switch {
	case r.Method == http.MethodPost && path == "p2p/segments/create":
		s.createSegment(w, r)
	case len(parts) == 4 && parts[0] == portalFortiGate && parts[1] == "vcloud" && r.Method == http.MethodGet &&
		(parts[3] == "orgs" || parts[3] == "provider-storage-profiles"):
		s.vcloudLookup(w, parts[2], parts[3])
	case len(parts) == 4 && parts[0] == portalFortiGate && parts[1] == "vcloud" && parts[3] == "deliver" && r.Method == http.MethodPost:
		s.deliver(w, r, parts[2])
	case len(parts) == 4 && parts[0] == portalFortiGate && parts[1] == "envs" && parts[3] == "nsx-segment" && r.Method == http.MethodDelete:
		s.deleteSegment(w, parts[2], q.Get("name"))
	case len(parts) >= 2 && parts[0] == portalFortiGate && parts[1] == "jobs":
		s.jobCall(w, r, parts[2:])
	default:
		s.engine.unhandledCall("portal", w, r)
	}
}

// lookups are the read-only calls whose answer depends only on the query, by path.
func (s *portalService) lookups() map[string]func(q url.Values) any {
	return map[string]func(q url.Values) any{
		"p2p/itop/blocks": func(url.Values) any {
			return map[string]any{"blocks": []map[string]any{{"id": 7, "name": "BLK-AUTOMAZIONE-P2P"}, {"id": 8, "name": "BLK-CLIENTI-LEGACY"}}}
		},
		"p2p/itop/block-subnets": func(q url.Values) any {
			out := []*portalSubnet{}
			for _, subnet := range s.subnets {
				if q.Get("block_id") == strconv.Itoa(subnet.BlockID) && (q.Get("mask") == "" || q.Get("mask") == strconv.Itoa(subnet.Mask)) {
					out = append(out, subnet)
				}
			}
			return map[string]any{"subnets": out}
		},
		"p2p/segments/name-check": func(q url.Values) any {
			ok := true
			for _, segment := range s.segments {
				ok = ok && (segment.Client != q.Get("client_name") || segment.EnvName != q.Get("env_name"))
			}
			return map[string]any{"ok": ok}
		},
		portalFortiGate + "/envs": func(url.Values) any { return s.envs },
		portalFortiGate + "/templates": func(q url.Values) any {
			out := []map[string]any{}
			for _, template := range s.templates {
				if q.Get("vcenter_env_name") == "" || q.Get("vcenter_env_name") == template.VCenter {
					out = append(out, s.templateJSON(template))
				}
			}
			return out
		},
		portalFortiGate + "/flexvm/programs": func(url.Values) any {
			return []map[string]any{{"id": 11, "serial": "ELAVMR0000000657", "name": "Dinova FlexVM"}}
		},
		portalFortiGate + "/flexvm/configs": func(q url.Values) any {
			if q.Get("program_id") != "11" {
				return []map[string]any{}
			}
			return []map[string]any{
				{"id": 3619, "name": "Istanza FortiGate SMALL - Funzionalità UTP Bundle", "program_id": 11},
				{"id": 3620, "name": "Istanza FortiGate MEDIUM - Funzionalità UTP Bundle", "program_id": 11},
			}
		},
	}
}

func (s *portalService) vcloudLookup(w http.ResponseWriter, envID, what string) {
	if envID != "3" {
		portalError(w, http.StatusNotFound, "ENV_NOT_FOUND", "vCloud environment "+envID+" not found", nil)
		return
	}
	if what == "orgs" {
		writeJSON(w, http.StatusOK, []map[string]any{
			{"id": "urn:vcloud:org:1f2e3d4c-0000-4000-8000-000000000017", "name": "testav17"},
			{"id": "urn:vcloud:org:1f2e3d4c-0000-4000-8000-000000000018", "name": "testav18"},
		})
		return
	}
	writeJSON(w, http.StatusOK, []map[string]any{{"name": "Cloud_Gold"}, {"name": "DATA4_ENT"}})
}

func (s *portalService) templateJSON(template *portalTemplate) map[string]any {
	out := map[string]any{"id": template.ID, "label": template.Label, "vcenter_env_name": template.VCenter,
		"locked_by_job_id": nil, "locked_job_vm_name": nil, "locked_job_status": nil}
	if job := s.lockHolder(template); job != nil {
		out["locked_by_job_id"], out["locked_job_vm_name"], out["locked_job_status"] = job.ID, job.VMName, job.Status
	} else if template.lockedBy == -1 {
		out["locked_by_job_id"], out["locked_job_vm_name"], out["locked_job_status"] = 1, "fw-altro-cliente", jobRunning
	}
	return out
}

// lockHolder returns the job that holds the template: until it succeeds, or until lockSeconds
// after it failed without being resumed.
func (s *portalService) lockHolder(template *portalTemplate) *portalJob {
	if template.lockedBy <= 0 {
		return nil
	}
	job := s.jobs[template.lockedBy]
	if job == nil {
		template.lockedBy = 0
		return nil
	}
	s.advance(job)
	if job.Status == jobSuccess || job.Status == jobFailed && job.FailedAt != nil && s.now().Sub(*job.FailedAt) > seconds(s.cfg.LockSeconds) {
		template.lockedBy = 0
		return nil
	}
	return job
}

func (s *portalService) createSegment(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if !readJSON(w, r, &body, func(w http.ResponseWriter, message string) {
		portalError(w, http.StatusBadRequest, "BAD_REQUEST", message, nil)
	}) {
		return
	}
	str := func(key string) string { return fmt.Sprint(body[key]) }
	for _, field := range []string{"env_name", "client_name", "subnet_ip"} {
		if v, ok := body[field].(string); !ok || v == "" {
			validationError(w, field, "Field required", "missing")
			return
		}
	}
	name := "P2P-" + str("client_name") + "-NET"
	if s.segments[name] != nil {
		portalError(w, http.StatusConflict, "SEGMENT_EXISTS", "Segment "+name+" already exists", map[string]string{"name": name})
		return
	}
	var subnet *portalSubnet
	for _, candidate := range s.subnets {
		if candidate.IP == str("subnet_ip") {
			subnet = candidate
		}
	}
	if subnet == nil || !subnet.Available {
		portalError(w, http.StatusUnprocessableEntity, "SUBNET_NOT_AVAILABLE", "Subnet "+str("subnet_ip")+" is not free in iTop", nil)
		return
	}
	subnet.Available = false
	segment := &portalSegment{Name: name, EnvName: str("env_name"), Client: str("client_name"), SubnetID: subnet.ID,
		Network: fmt.Sprintf("%s/%d", subnet.IP, subnet.Mask)}
	s.segments[name] = segment
	writeJSON(w, http.StatusCreated, map[string]any{"segment": segment, "t1_name": "T1-" + name, "itop_subnet_id": subnet.ID})
}

func (s *portalService) deleteSegment(w http.ResponseWriter, envID, name string) {
	envName := ""
	for _, env := range s.envs {
		if strconv.Itoa(env.ID) == envID && env.VCloudName != nil {
			envName = *env.VCloudName
		}
	}
	if envName == "" {
		portalError(w, http.StatusNotFound, "ENV_NOT_FOUND", "Environment "+envID+" not found", nil)
		return
	}
	if !regexp.MustCompile(`^P2P-[A-Za-z0-9._-]+$`).MatchString(name) {
		portalError(w, http.StatusUnprocessableEntity, "SEGMENT_NAME_NOT_AUTOMATION", "Segment "+name+" is not named by the automation", nil)
		return
	}
	segment := s.segments[name]
	if segment == nil || segment.EnvName != envName {
		writeJSON(w, http.StatusOK, map[string]string{"result": "not_found", "name": name})
		return
	}
	if segment.InUse {
		portalError(w, http.StatusConflict, "SEGMENT_IN_USE", "Segment "+name+" has ports attached", nil)
		return
	}
	for _, subnet := range s.subnets {
		if subnet.ID == segment.SubnetID {
			subnet.Available = true
		}
	}
	delete(s.segments, name)
	writeJSON(w, http.StatusOK, map[string]string{"result": "deleted", "name": name})
}

func (s *portalService) deliver(w http.ResponseWriter, r *http.Request, envID string) {
	var body map[string]any
	if !readJSON(w, r, &body, func(w http.ResponseWriter, message string) {
		portalError(w, http.StatusBadRequest, "BAD_REQUEST", message, nil)
	}) {
		return
	}
	if envID != "3" {
		portalError(w, http.StatusBadRequest, "ENV_TYPE_MISMATCH", "Environment "+envID+" is not a vCloud environment", nil)
		return
	}
	for _, field := range []string{"vm_name", "org_id", "vdc_name", "assigned_ip", "flexvm_config_id", "nsx_segment_name"} {
		if v, ok := body[field].(string); !ok || v == "" {
			validationError(w, field, "Input should be a valid string", "string_type")
			return
		}
	}
	for _, field := range []string{"template_id", "cpu_count", "ram_gb"} {
		if _, ok := body[field].(float64); !ok {
			if text, isText := body[field].(string); !isText || !regexp.MustCompile(`^[0-9]+$`).MatchString(text) {
				validationError(w, field, "Input should be a valid integer", "int_type")
				return
			}
		}
	}
	templateID, _ := strconv.Atoi(fmt.Sprint(body["template_id"]))
	var template *portalTemplate
	for _, candidate := range s.templates {
		if candidate.ID == templateID {
			template = candidate
		}
	}
	if template == nil {
		portalError(w, http.StatusNotFound, "TEMPLATE_NOT_FOUND", fmt.Sprintf("Template %d not found", templateID), nil)
		return
	}
	job := s.newJob(fmt.Sprint(body["vm_name"]), templateID, fmt.Sprint(body["assigned_ip"]))
	if holder := s.lockHolder(template); holder != nil || template.lockedBy == -1 {
		// Like the portal: the job row exists before the lock is taken, and stays, failed.
		now := s.now()
		job.Status, job.FailedAt, job.Script = jobFailed, &now, jobScript{}
		job.log = append(job.log, "TEMPLATE_LOCKED: the template is in use by another delivery")
		lockedBy := any(nil)
		if holder != nil {
			lockedBy = holder.ID
		}
		portalError(w, http.StatusConflict, "TEMPLATE_LOCKED", fmt.Sprintf("Template %d is in use by another delivery", templateID),
			map[string]any{"locked_by_job_id": lockedBy, "job_id": job.ID})
		return
	}
	template.lockedBy = job.ID
	if segment := s.segments[fmt.Sprint(body["nsx_segment_name"])]; segment != nil {
		segment.InUse = true
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"job_id": job.ID, "status": job.Status})
}

func (s *portalService) newJob(vmName string, templateID int, ip string) *portalJob {
	s.nextJob++
	now := s.now()
	job := &portalJob{ID: s.nextJob, Status: jobQueued, VMName: vmName, TemplateID: templateID, Total: portalTotalSteps,
		LicensingMode: "flexvm", AssignedIP: ip, Created: now, Script: s.script, nextAt: now.Add(seconds(s.cfg.PhaseSeconds)),
		resolved: map[int]bool{}, log: []string{fmt.Sprintf("[%s] delivery queued", now.UTC().Format(time.RFC3339))}}
	s.script = jobScript{}
	s.jobs[job.ID] = job
	return job
}

// advance moves a job along its phases with the clock, following its script.
func (s *portalService) advance(job *portalJob) {
	phase := seconds(s.cfg.PhaseSeconds)
	now := s.now()
	for !job.Stuck && (job.Status == jobQueued || job.Status == jobRunning) && !now.Before(job.nextAt) {
		at := job.nextAt
		job.nextAt = at.Add(phase)
		if job.Status == jobQueued {
			job.Status = jobRunning
			continue
		}
		step := job.LastCompleted + 1
		if wait, ok := job.Script.WaitAt[step]; ok && !job.resolved[step] {
			job.Status = wait
			job.log = append(job.log, fmt.Sprintf("phase %d: %s", step, wait))
			return
		}
		if len(job.Script.FailAt) > 0 && job.Script.FailAt[0] == step {
			job.Script.FailAt = job.Script.FailAt[1:]
			failedAt := at
			job.Status, job.FailedAt, job.Rollback = jobFailed, &failedAt, true
			job.ManualReview = job.Script.ManualReviewAt == step
			job.log = append(job.log, fmt.Sprintf("phase %d: failed, rollback to snapshot attempted", step))
			return
		}
		if job.Script.NeverEnds && step == 7 {
			job.Stuck = true
			job.log = append(job.log, "phase 7: waiting for the VM to come back (never)")
			return
		}
		job.LastCompleted = step
		job.log = append(job.log, fmt.Sprintf("phase %d/%d completed", step, portalTotalSteps))
		if step == portalTotalSteps {
			job.Status = jobSuccess
			serial := fmt.Sprintf("FGVMELTM%08d", job.ID)
			job.LicenseSerial = &serial
			job.log = append(job.log, "delivery completed")
		}
	}
}

func (s *portalService) jobJSON(job *portalJob, withLog bool) map[string]any {
	s.advance(job)
	raw, _ := json.Marshal(job)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	delete(out, "script")
	delete(out, "stuck")
	if withLog {
		logText := strings.Join(job.log, "\n") + "\n"
		if pad := s.cfg.LogKiB*1024 - len(logText); pad > 0 {
			logText += strings.Repeat("debug: verbose provisioning output\n", pad/35+1)
		}
		out["log_output"] = logText
	}
	return out
}

func (s *portalService) jobCall(w http.ResponseWriter, r *http.Request, rest []string) {
	if len(rest) == 0 && r.Method == http.MethodGet {
		ids := make([]int, 0, len(s.jobs))
		for id := range s.jobs {
			ids = append(ids, id)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(ids)))
		out := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			out = append(out, s.jobJSON(s.jobs[id], false))
		}
		writeJSON(w, http.StatusOK, out)
		return
	}
	if len(rest) == 0 {
		s.engine.unhandledCall("portal", w, r)
		return
	}
	id, _ := strconv.Atoi(rest[0])
	job := s.jobs[id]
	if job == nil {
		portalError(w, http.StatusNotFound, "JOB_NOT_FOUND", "Job "+rest[0]+" not found", nil)
		return
	}
	s.advance(job)
	action := strings.Join(rest[1:], "/")
	switch {
	case action == "" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, s.jobJSON(job, true))
	case action == "retry" && r.Method == http.MethodPost:
		s.retry(w, r, job)
	case action == "license-decision" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"job_id": job.ID, "entitlement": map[string]any{"serial": "FGVMELTM00000001", "status": "ACTIVE"}})
	case action == "license-decision" && r.Method == http.MethodPost:
		s.resume(w, job, "waiting_license_decision")
	case action == "src-ips" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"available": []map[string]string{{"ip": "10.255.0.10", "status": "free"}}})
	case action == "src-ip" && r.Method == http.MethodPost:
		s.resume(w, job, "waiting_src_ip")
	default:
		s.engine.unhandledCall("portal", w, r)
	}
}

func (s *portalService) resume(w http.ResponseWriter, job *portalJob, waiting string) {
	if job.Status != waiting {
		portalError(w, http.StatusConflict, "JOB_NOT_WAITING", fmt.Sprintf("Job %d is %s, not %s", job.ID, job.Status, waiting), nil)
		return
	}
	job.resolved[job.LastCompleted+1] = true
	job.Status, job.nextAt = jobRunning, s.now().Add(seconds(s.cfg.PhaseSeconds))
	writeJSON(w, http.StatusOK, map[string]any{"job_id": job.ID, "status": job.Status})
}

func (s *portalService) retry(w http.ResponseWriter, r *http.Request, job *portalJob) {
	var body map[string]any
	if !readJSON(w, r, &body, func(w http.ResponseWriter, message string) {
		portalError(w, http.StatusBadRequest, "BAD_REQUEST", message, nil)
	}) {
		return
	}
	if job.Status != jobFailed {
		portalError(w, http.StatusConflict, "JOB_NOT_FAILED", fmt.Sprintf("Job %d is %s: only a failed job can be resumed", job.ID, job.Status), nil)
		return
	}
	from, ok := body["resume_from_step"].(float64)
	if !ok || from != float64(int(from)) {
		validationError(w, "resume_from_step", "Input should be a valid integer", "int_type")
		return
	}
	step := int(from)
	if step < 1 || step > job.LastCompleted+1 {
		portalError(w, http.StatusUnprocessableEntity, "INVALID_RESUME_STEP",
			fmt.Sprintf("resume_from_step must be between 1 and %d", job.LastCompleted+1), map[string]int{"last_completed_step": job.LastCompleted})
		return
	}
	if step == 1 {
		job.log = append(job.log, "resume from phase 1: the VM of the previous attempt is deleted")
	}
	now := s.now()
	job.LastCompleted, job.Retries = step-1, job.Retries+1
	job.Status, job.FailedAt, job.ManualReview, job.nextAt = jobQueued, nil, false, now.Add(seconds(s.cfg.PhaseSeconds))
	job.log = append(job.log, fmt.Sprintf("[%s] resumed from phase %d", now.UTC().Format(time.RFC3339), step))
	if template := s.template(job.TemplateID); template != nil {
		template.lockedBy = job.ID
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_id": job.ID, "status": job.Status, "resume_from_step": step})
}

func (s *portalService) template(id int) *portalTemplate {
	for _, template := range s.templates {
		if template.ID == id {
			return template
		}
	}
	return nil
}

// ---- state and controls -------------------------------------------------------------------

func (s *portalService) state() any {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs := make([]map[string]any, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, s.jobJSON(job, false))
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i]["id"].(float64) < jobs[j]["id"].(float64) })
	templates := make([]map[string]any, 0, len(s.templates))
	for _, template := range s.templates {
		templates = append(templates, s.templateJSON(template))
	}
	segments := make([]*portalSegment, 0, len(s.segments))
	for _, segment := range s.segments {
		segments = append(segments, segment)
	}
	return map[string]any{"subnets": s.subnets, "segments": segments, "templates": templates, "jobs": jobs, "nextJobScript": s.script}
}

func (s *portalService) duplicates() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var active []string
	for _, job := range s.jobs {
		s.advance(job)
		if job.Status != jobSuccess && job.Status != jobFailed {
			active = append(active, job.VMName)
		}
	}
	if dup := duplicateNames(active); len(dup) > 0 {
		return map[string][]string{"activeJobsForVM": dup}
	}
	return nil
}

// control serves the portal controls, /__mock/portal/...
func (s *portalService) control(w http.ResponseWriter, r *http.Request, path string) bool {
	if !strings.HasPrefix(path, "portal/") || r.Method != http.MethodPost {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(path, "portal/"), "/")
	s.mu.Lock()
	defer s.mu.Unlock()
	bad := func(w http.ResponseWriter, message string) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": message})
	}
	switch {
	case parts[0] == "next-job" && len(parts) == 1:
		script := jobScript{}
		if !readJSON(w, r, &script, bad) {
			return true
		}
		s.script = script
		writeJSON(w, http.StatusOK, map[string]any{"nextJobScript": script})
		return true
	case parts[0] == "jobs" && len(parts) == 3:
		id, _ := strconv.Atoi(parts[1])
		job := s.jobs[id]
		if job == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no job " + parts[1]})
			return true
		}
		s.advance(job)
		var body struct {
			Status       string `json:"status"`
			ManualReview bool   `json:"manualReview"`
		}
		if r.ContentLength > 0 && !readJSON(w, r, &body, bad) {
			return true
		}
		now := s.now()
		switch parts[2] {
		case "fail":
			job.Status, job.FailedAt, job.Rollback, job.ManualReview = jobFailed, &now, true, body.ManualReview
		case "wait":
			job.Status = cmpOr(body.Status, "waiting_license_decision")
		case "stuck":
			job.Stuck = true
		case "unstuck":
			job.Stuck, job.nextAt = false, now
		default:
			return false
		}
		writeJSON(w, http.StatusOK, s.jobJSON(job, false))
		return true
	case parts[0] == "templates" && len(parts) == 3 && (parts[2] == "lock" || parts[2] == "unlock"):
		id, _ := strconv.Atoi(parts[1])
		template := s.template(id)
		if template == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no template " + parts[1]})
			return true
		}
		template.lockedBy = map[string]int{"lock": -1, "unlock": 0}[parts[2]]
		writeJSON(w, http.StatusOK, s.templateJSON(template))
		return true
	case path == "portal/subnets/exhaust":
		for _, subnet := range s.subnets {
			subnet.Available = false
		}
		writeJSON(w, http.StatusOK, map[string]any{"subnets": s.subnets})
		return true
	case path == "portal/segments":
		var body struct {
			Client  string `json:"client_name"`
			EnvName string `json:"env_name"`
		}
		if !readJSON(w, r, &body, bad) {
			return true
		}
		name := "P2P-" + body.Client + "-NET"
		s.segments[name] = &portalSegment{Name: name, EnvName: cmpOr(body.EnvName, "vcloud-r1.dinova.cloud"), Client: body.Client, SubnetID: -1}
		writeJSON(w, http.StatusOK, s.segments[name])
		return true
	}
	return false
}
