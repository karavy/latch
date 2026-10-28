package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

const portalBase = "/portal/api/v1"

func (m *mockServer) portal(t *testing.T, method, path, body string) (int, any) {
	t.Helper()
	req, _ := http.NewRequest(method, m.URL+portalBase+path, strings.NewReader(body))
	req.Header.Set("X-API-Key", "mock-portal-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	var out any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func (m *mockServer) job(t *testing.T, id string) map[string]any {
	t.Helper()
	_, out := m.portal(t, http.MethodGet, "/fortigate-vm/jobs/"+id, "")
	return out.(map[string]any)
}

// waitJob moves the clock one phase at a time until the job is no longer queued or running.
func (m *mockServer) waitJob(t *testing.T, id string) map[string]any {
	t.Helper()
	for range 40 {
		job := m.job(t, id)
		if status := job["status"]; status != jobQueued && status != jobRunning {
			return job
		}
		m.now = m.now.Add(2 * time.Second)
	}
	t.Fatalf("job %s never ended", id)
	return nil
}

const deliverBody = `{"vm_name":"fw-exmachina-01","template_id":4,"org_id":"urn:vcloud:org:x","org_name":"testav17","vdc_name":"VDC",
"assigned_ip":"178.239.21.31","gateway":"178.239.21.30","cpu_count":1,"ram_gb":2,"flexvm_config_id":"3619","nsx_segment_name":"P2P-EXMACHINA-NET"}`

func TestPortalLookupsAndSegment(t *testing.T) {
	m := newMockServer(t)
	if status, _, _ := m.do(t, http.MethodGet, portalBase+"/fortigate-vm/envs", ""); status != http.StatusUnauthorized {
		t.Errorf("without API key: %d", status)
	}
	_, blocks := m.portal(t, http.MethodGet, "/p2p/itop/blocks", "")
	if !strings.Contains(mustJSON(blocks), `"BLK-AUTOMAZIONE-P2P"`) {
		t.Errorf("blocks = %v", blocks)
	}
	_, subnets := m.portal(t, http.MethodGet, "/p2p/itop/block-subnets?mask=31&block_id=7", "")
	list := subnets.(map[string]any)["subnets"].([]any)
	if len(list) != 4 || list[0].(map[string]any)["available"] != true || list[2].(map[string]any)["available"] != false {
		t.Errorf("subnets = %v", list)
	}
	_, check := m.portal(t, http.MethodGet, "/p2p/segments/name-check?client_name=EXMACHINA&env_name=vcloud-r1.dinova.cloud", "")
	if check.(map[string]any)["ok"] != true {
		t.Errorf("name-check before the segment = %v", check)
	}
	create := `{"env_name":"vcloud-r1.dinova.cloud","client_name":"EXMACHINA","subnet_ip":"178.239.21.30","subnet_mask":31,"itop_block_id":7}`
	if status, out := m.portal(t, http.MethodPost, "/p2p/segments/create", create); status != http.StatusCreated {
		t.Fatalf("create segment: %d %v", status, out)
	}
	_, check = m.portal(t, http.MethodGet, "/p2p/segments/name-check?client_name=EXMACHINA&env_name=vcloud-r1.dinova.cloud", "")
	if check.(map[string]any)["ok"] != false {
		t.Errorf("name-check after the segment = %v", check)
	}
	if status, _ := m.portal(t, http.MethodPost, "/p2p/segments/create", create); status != http.StatusConflict {
		t.Errorf("same segment again: %d", status)
	}
	_, subnets = m.portal(t, http.MethodGet, "/p2p/itop/block-subnets?mask=31&block_id=7", "")
	if subnets.(map[string]any)["subnets"].([]any)[0].(map[string]any)["available"] != false {
		t.Errorf("the used subnet is still free")
	}
	if status, out := m.portal(t, http.MethodDelete, "/fortigate-vm/envs/3/nsx-segment?name=P2P-EXMACHINA-NET", ""); status != http.StatusOK ||
		out.(map[string]any)["result"] != "deleted" {
		t.Errorf("delete segment: %d %v", status, out)
	}
	if _, out := m.portal(t, http.MethodDelete, "/fortigate-vm/envs/3/nsx-segment?name=P2P-EXMACHINA-NET", ""); out.(map[string]any)["result"] != "not_found" {
		t.Errorf("delete again: %v", out)
	}
}

func TestPortalJobSucceedsAndLocksTheTemplate(t *testing.T) {
	m := newMockServer(t)
	status, out := m.portal(t, http.MethodPost, "/fortigate-vm/vcloud/3/deliver", deliverBody)
	if status != http.StatusAccepted {
		t.Fatalf("deliver: %d %v", status, out)
	}
	id := mustJSON(out.(map[string]any)["job_id"])
	// The template is held: a second delivery is refused, and leaves a failed job behind.
	status, out = m.portal(t, http.MethodPost, "/fortigate-vm/vcloud/3/deliver", deliverBody)
	if status != http.StatusConflict || !strings.Contains(mustJSON(out), "TEMPLATE_LOCKED") {
		t.Errorf("second delivery: %d %v", status, out)
	}
	job := m.waitJob(t, id)
	if job["status"] != jobSuccess || job["last_completed_step"] != float64(14) || job["license_serial"] == nil {
		t.Errorf("job = %v", job)
	}
	_, jobs := m.portal(t, http.MethodGet, "/fortigate-vm/jobs", "")
	if list := jobs.([]any); len(list) != 2 || list[0].(map[string]any)["status"] != jobFailed || list[0].(map[string]any)["log_output"] != nil { // newest first
		t.Errorf("jobs = %v", jobs)
	}
	if status, _ := m.portal(t, http.MethodPost, "/fortigate-vm/vcloud/3/deliver", deliverBody); status != http.StatusAccepted {
		t.Errorf("delivery after the job ended: %d", status)
	}
	if status, out := m.portal(t, http.MethodPost, "/fortigate-vm/vcloud/3/deliver", `{"vm_name":"x","template_id":"four"}`); status != http.StatusUnprocessableEntity ||
		!strings.Contains(mustJSON(out), `"loc"`) {
		t.Errorf("invalid delivery: %d %v", status, out)
	}
}

func TestPortalJobFailsAndResumes(t *testing.T) {
	m := newMockServer(t)
	m.do(t, http.MethodPost, "/__mock/portal/next-job", `{"failAt":[3,6,6],"manualReviewAt":0}`)
	_, out := m.portal(t, http.MethodPost, "/fortigate-vm/vcloud/3/deliver", deliverBody)
	id := mustJSON(out.(map[string]any)["job_id"])
	var resumes []float64
	for range 4 {
		job := m.waitJob(t, id)
		if job["status"] == jobSuccess {
			break
		}
		if job["status"] != jobFailed || job["rollback_attempted"] != true {
			t.Fatalf("job = %v", job)
		}
		last := job["last_completed_step"].(float64)
		if status, _ := m.portal(t, http.MethodPost, "/fortigate-vm/jobs/"+id+"/retry", `{"resume_from_step":`+mustJSON(last+2)+`}`); status != http.StatusUnprocessableEntity {
			t.Errorf("resume beyond the last completed phase + 1: %d", status)
		}
		if status, out := m.portal(t, http.MethodPost, "/fortigate-vm/jobs/"+id+"/retry", `{"resume_from_step":`+mustJSON(last+1)+`}`); status != http.StatusOK {
			t.Fatalf("retry: %d %v", status, out)
		}
		resumes = append(resumes, last+1)
		if status, _ := m.portal(t, http.MethodPost, "/fortigate-vm/jobs/"+id+"/retry", `{"resume_from_step":1}`); status != http.StatusConflict {
			t.Errorf("retry of a job that is not failed: %d", status)
		}
	}
	if job := m.job(t, id); job["status"] != jobSuccess || mustJSON(resumes) != "[3,6,6]" {
		t.Errorf("status %v after resuming from %v", job["status"], resumes)
	}
}

func TestPortalJobWaitsForAPersonAndManualReview(t *testing.T) {
	m := newMockServer(t)
	m.do(t, http.MethodPost, "/__mock/portal/next-job", `{"waitAt":{"5":"waiting_license_decision"},"failAt":[9],"manualReviewAt":9}`)
	m.do(t, http.MethodPut, "/__mock/config?service=portal", `{"logKiB":100}`)
	_, out := m.portal(t, http.MethodPost, "/fortigate-vm/vcloud/3/deliver", deliverBody)
	id := mustJSON(out.(map[string]any)["job_id"])
	if job := m.waitJob(t, id); job["status"] != "waiting_license_decision" || job["last_completed_step"] != float64(4) {
		t.Fatalf("job = %v", job["status"])
	}
	if status, _ := m.portal(t, http.MethodPost, "/fortigate-vm/jobs/"+id+"/license-decision", `{"terminate_existing":true}`); status != http.StatusOK {
		t.Fatalf("license decision: %d", status)
	}
	job := m.waitJob(t, id)
	if job["status"] != jobFailed || job["manual_review_required"] != true || job["last_completed_step"] != float64(8) {
		t.Errorf("job = %v", job["status"])
	}
	if size := len(job["log_output"].(string)); size < 100*1024 {
		t.Errorf("log_output is %d bytes, want at least 100 KiB", size)
	}
}

func mustJSON(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}
