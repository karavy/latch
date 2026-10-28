package main

// The mock engine shared by the simulated services (OpenStack, admin portal): a request log, the
// calls nobody implemented, fault rules that break chosen requests, and the /__mock/ control API.
// Each service keeps its own resources and registers itself with the engine.

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	mathrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	actionReset = "reset"
	actionHang  = "hang"

	maxMockBody       = 1 << 20
	maxMockRequests   = 500
	maxUnhandledCalls = 200
)

// mockService is a simulated API: its resources, its configuration and its own controls.
type mockService interface {
	name() string
	reset()
	state() any
	// duplicates returns, by resource kind, the names that more than one resource uses.
	duplicates() map[string][]string
	config() any
	setConfig(raw []byte) error
	// control handles /__mock/<path> requests the engine does not know; false if none matches.
	control(w http.ResponseWriter, r *http.Request, path string) bool
}

// mockRequest is one request to a simulated service, as the log keeps it.
type mockRequest struct {
	ID       uint64    `json:"id"`
	Time     time.Time `json:"time"`
	Service  string    `json:"service"`
	Method   string    `json:"method"`
	Path     string    `json:"path"`
	Query    string    `json:"query,omitempty"`
	Status   int       `json:"status"`
	Fault    string    `json:"fault,omitempty"`
	Duration string    `json:"duration"`
}

type faultMatch struct {
	Service string `json:"service,omitempty"`
	Method  string `json:"method,omitempty"`
	Path    string `json:"path,omitempty"`
	Body    string `json:"body,omitempty"`
	service *regexp.Regexp
	method  *regexp.Regexp
	path    *regexp.Regexp
	body    *regexp.Regexp
}

type faultAction struct {
	// Type is status, delay, reset, hang or body.
	Type   string  `json:"type"`
	Status int     `json:"status,omitempty"`
	Body   string  `json:"body,omitempty"`
	Second float64 `json:"seconds,omitempty"`
	// AfterHandler runs the request first, then breaks the connection (reset, hang): the work is
	// done, the answer is lost.
	AfterHandler bool `json:"afterHandler,omitempty"`
}

// faultRule breaks the requests it matches, in the order the rules were added.
type faultRule struct {
	Name             string      `json:"name"`
	Match            faultMatch  `json:"match"`
	Action           faultAction `json:"action"`
	After            int         `json:"after,omitempty"`
	Times            int         `json:"times,omitempty"`
	Probability      float64     `json:"probability,omitempty"`
	ExpiresInSeconds float64     `json:"expiresInSeconds,omitempty"`
	Seen             int         `json:"seen"`
	Applied          int         `json:"applied"`
	Expires          *time.Time  `json:"expires,omitempty"`
}

type engine struct {
	mu        sync.Mutex
	services  map[string]mockService
	rules     []*faultRule
	requests  []mockRequest
	unhandled []mockRequest
	nextID    uint64
	now       func() time.Time
}

func newEngine(services ...mockService) *engine {
	e := &engine{services: map[string]mockService{}, now: time.Now}
	for _, service := range services {
		e.services[service.name()] = service
	}
	return e
}

func (e *engine) clock() time.Time { return e.now() }

// serve wraps the handler of a simulated service: it applies the fault rules and logs the request.
func (e *engine) serve(service string, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		body, err := io.ReadAll(io.LimitReader(r.Body, maxMockBody+1))
		if err != nil || len(body) > maxMockBody {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		rule := e.matchRule(service, r, body)
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		faultName := ""
		if rule != nil {
			faultName = rule.Name
			e.applyFault(rule, recorder, r, handler)
		} else {
			handler.ServeHTTP(recorder, r)
		}
		e.record(mockRequest{
			Time: start.UTC(), Service: service, Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery,
			Status: recorder.status, Fault: faultName, Duration: time.Since(start).Round(time.Millisecond).String(),
		}, false)
	})
}

// unhandledCall answers a call of a simulated service that the mock does not implement, and
// keeps it: the mock never invents an answer.
func (e *engine) unhandledCall(service string, w http.ResponseWriter, r *http.Request) {
	e.record(mockRequest{Time: time.Now().UTC(), Service: service, Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Status: http.StatusNotImplemented}, true)
	log.Printf("mock %s: call not implemented: %s %s?%s", service, r.Method, r.URL.Path, r.URL.RawQuery)
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"error": fmt.Sprintf("the %s mock does not implement %s %s", service, r.Method, r.URL.Path),
	})
}

func (e *engine) record(entry mockRequest, unhandled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nextID++
	entry.ID = e.nextID
	if unhandled {
		e.unhandled = appendBounded(e.unhandled, entry, maxUnhandledCalls)
		return
	}
	e.requests = appendBounded(e.requests, entry, maxMockRequests)
}

func appendBounded(entries []mockRequest, entry mockRequest, limit int) []mockRequest {
	if len(entries) == limit {
		entries = append(entries[:0], entries[1:]...)
	}
	return append(entries, entry)
}

// matchRule returns the first rule that applies to the request, and counts it.
func (e *engine) matchRule(service string, r *http.Request, body []byte) *faultRule {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	kept := e.rules[:0]
	for _, rule := range e.rules {
		if rule.Expires == nil || now.Before(*rule.Expires) {
			kept = append(kept, rule)
		}
	}
	e.rules = kept
	for _, rule := range e.rules {
		m := rule.Match
		if m.service != nil && !m.service.MatchString(service) || m.method != nil && !m.method.MatchString(r.Method) ||
			m.path != nil && !m.path.MatchString(r.URL.Path) || m.body != nil && !m.body.Match(body) {
			continue
		}
		rule.Seen++
		if rule.Seen <= rule.After || rule.Times > 0 && rule.Applied >= rule.Times {
			continue
		}
		if rule.Probability > 0 && rule.Probability < 1 && mathrand.Float64() >= rule.Probability {
			continue
		}
		rule.Applied++
		return rule
	}
	return nil
}

func (e *engine) applyFault(rule *faultRule, w *statusRecorder, r *http.Request, handler http.Handler) {
	action := rule.Action
	wait := time.Duration(action.Second * float64(time.Second))
	switch action.Type {
	case "status":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(action.Status)
		if action.Body != "" {
			_, _ = w.Write([]byte(action.Body))
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "injected by mock fault " + rule.Name, "status": action.Status})
		}
	case "delay":
		time.Sleep(wait)
		handler.ServeHTTP(w, r)
	case actionReset, actionHang:
		if action.AfterHandler {
			handler.ServeHTTP(httptest.NewRecorder(), r)
		}
		w.status = 0
		breakConnection(w.ResponseWriter, action.Type == actionHang, wait)
	case "body":
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, r)
		cut := recorder.Body.Bytes()
		if action.Body != "" {
			cut = []byte(action.Body)
		} else if len(cut) > 1 {
			cut = cut[:len(cut)/2]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(cut)
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "unknown fault action " + action.Type})
	}
}

// breakConnection closes the client's connection without an HTTP answer: at once with a reset,
// or after holding it open (hang) for the given time, or until the client gives up.
func breakConnection(w http.ResponseWriter, hang bool, wait time.Duration) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		panic(http.ErrAbortHandler)
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	defer func() { _ = conn.Close() }()
	if hang {
		if wait <= 0 {
			wait = 10 * time.Minute
		}
		_ = conn.SetDeadline(time.Now().Add(wait))
		_, _ = io.Copy(io.Discard, conn) // returns when the client closes, or at the deadline
		return
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0) // RST instead of FIN, like a refused connection
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// control serves /__mock/.
func (e *engine) control(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/__mock"), "/")
	switch {
	case path == "state" && r.Method == http.MethodGet:
		states := map[string]any{}
		for name, service := range e.services {
			states[name] = service.state()
		}
		writeJSON(w, http.StatusOK, states)
	case path == "duplicates" && r.Method == http.MethodGet:
		all := map[string]map[string][]string{}
		for name, service := range e.services {
			if found := service.duplicates(); len(found) > 0 {
				all[name] = found
			}
		}
		writeJSON(w, http.StatusOK, all)
	case path == "requests" || path == "unhandled":
		e.logControl(w, r, path == "unhandled")
	case path == "reset" && r.Method == http.MethodPost:
		only := r.URL.Query().Get("service")
		for name, service := range e.services {
			if only == "" || only == name {
				service.reset()
			}
		}
		if only == "" {
			e.mu.Lock()
			e.rules, e.requests, e.unhandled = nil, nil, nil
			e.mu.Unlock()
		}
		writeJSON(w, http.StatusOK, map[string]string{"reset": cmpOr(only, "all")})
	case path == "config":
		e.configControl(w, r)
	case path == "faults" || strings.HasPrefix(path, "faults/"):
		e.faultControl(w, r, strings.TrimPrefix(strings.TrimPrefix(path, "faults"), "/"))
	default:
		for _, service := range e.services {
			if service.control(w, r, path) {
				return
			}
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown mock control " + r.Method + " /__mock/" + path})
	}
}

func (e *engine) logControl(w http.ResponseWriter, r *http.Request, unhandled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		entries := e.requests
		if unhandled {
			entries = e.unhandled
		}
		since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
		service := r.URL.Query().Get("service")
		out := []mockRequest{}
		for _, entry := range entries {
			if entry.ID > since && (service == "" || entry.Service == service) {
				out = append(out, entry)
			}
		}
		writeJSON(w, http.StatusOK, out)
	case http.MethodDelete:
		if unhandled {
			e.unhandled = nil
		} else {
			e.requests = nil
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *engine) configControl(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		all := map[string]any{}
		for name, service := range e.services {
			all[name] = service.config()
		}
		writeJSON(w, http.StatusOK, all)
	case http.MethodPut:
		service := e.services[r.URL.Query().Get("service")]
		if service == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "config needs ?service= one of the simulated services"})
			return
		}
		raw, _ := io.ReadAll(io.LimitReader(r.Body, maxMockBody))
		if err := service.setConfig(raw); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, service.config())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (e *engine) faultControl(w http.ResponseWriter, r *http.Request, name string) {
	switch {
	case r.Method == http.MethodGet && name == "":
		e.mu.Lock()
		rules := append([]*faultRule{}, e.rules...)
		e.mu.Unlock()
		writeJSON(w, http.StatusOK, rules)
	case r.Method == http.MethodPost && name == "":
		rule := &faultRule{}
		if err := json.NewDecoder(io.LimitReader(r.Body, maxMockBody)).Decode(rule); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "fault rule is not valid JSON: " + err.Error()})
			return
		}
		if err := rule.compile(e.now()); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		e.mu.Lock()
		kept := e.rules[:0]
		for _, existing := range e.rules {
			if existing.Name != rule.Name {
				kept = append(kept, existing)
			}
		}
		e.rules = append(kept, rule)
		e.mu.Unlock()
		writeJSON(w, http.StatusCreated, rule)
	case r.Method == http.MethodDelete:
		e.mu.Lock()
		kept := e.rules[:0]
		for _, existing := range e.rules {
			if name != "" && existing.Name != name {
				kept = append(kept, existing)
			}
		}
		e.rules = kept
		e.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (rule *faultRule) compile(now time.Time) error {
	if rule.Name == "" {
		return errors.New("a fault rule needs a name")
	}
	switch rule.Action.Type {
	case "status":
		if rule.Action.Status < 100 || rule.Action.Status > 599 {
			return errors.New("a status fault needs action.status between 100 and 599")
		}
	case "delay", actionReset, actionHang, "body":
	default:
		return fmt.Errorf("action.type %q is not one of status, delay, reset, hang, body", rule.Action.Type)
	}
	var err error
	compile := func(expr string) *regexp.Regexp {
		if expr == "" || err != nil {
			return nil
		}
		var re *regexp.Regexp
		re, err = regexp.Compile(expr)
		return re
	}
	rule.Match.service = compile("^(" + rule.Match.Service + ")$")
	if rule.Match.Service == "" {
		rule.Match.service = nil
	}
	rule.Match.method = compile(rule.Match.Method)
	rule.Match.path = compile(rule.Match.Path)
	rule.Match.body = compile(rule.Match.Body)
	if err != nil {
		return fmt.Errorf("fault rule %s has an invalid regexp: %w", rule.Name, err)
	}
	if rule.ExpiresInSeconds > 0 {
		expires := now.Add(time.Duration(rule.ExpiresInSeconds * float64(time.Second)))
		rule.Expires = &expires
	}
	rule.Seen, rule.Applied = 0, 0
	return nil
}

// newUUID returns a random UUID, as OpenStack and the portal use for ids.
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// duplicateNames returns the names used more than once.
func duplicateNames(names []string) []string {
	count := map[string]int{}
	for _, name := range names {
		count[name]++
	}
	var dup []string
	for name, n := range count {
		if n > 1 {
			dup = append(dup, name)
		}
	}
	sort.Strings(dup)
	return dup
}

func cmpOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// readJSON decodes a JSON request body into v; it answers 400 itself and returns false on error.
func readJSON(w http.ResponseWriter, r *http.Request, v any, badRequest func(w http.ResponseWriter, message string)) bool {
	if err := json.NewDecoder(io.LimitReader(r.Body, maxMockBody)).Decode(v); err != nil {
		badRequest(w, "request body is not valid JSON: "+err.Error())
		return false
	}
	return true
}
