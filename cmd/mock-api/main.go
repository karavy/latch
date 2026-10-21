package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultPort       = "8080"
	maxRequestBody    = 64 << 10
	maxCapturedEvents = 100
	redactedValue     = "[REDACTED]"
)

type capturedRequest struct {
	ID            uint64              `json:"id"`
	ReceivedAt    time.Time           `json:"receivedAt"`
	Method        string              `json:"method"`
	Path          string              `json:"path"`
	Query         map[string][]string `json:"query,omitempty"`
	Headers       http.Header         `json:"headers,omitempty"`
	Body          string              `json:"body,omitempty"`
	BodyTruncated bool                `json:"bodyTruncated,omitempty"`
	RemoteAddr    string              `json:"remoteAddr,omitempty"`
}

type requestStore struct {
	mu       sync.RWMutex
	nextID   uint64
	requests []capturedRequest

	// resourcesMu guards the in-memory resources created through /vdcs and /networks.
	resourcesMu sync.Mutex
	vdcs        map[string]string
	networks    int
}

func main() {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = defaultPort
	}
	store := &requestStore{}
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           newHandler(store),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("mock API listening on %s", server.Addr)
	certFile, keyFile := os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE")
	var err error
	if certFile != "" && keyFile != "" {
		_, certErr := os.Stat(certFile)
		_, keyErr := os.Stat(keyFile)
		if errors.Is(certErr, os.ErrNotExist) && errors.Is(keyErr, os.ErrNotExist) {
			log.Print("TLS certificate not mounted; serving HTTP for unauthenticated debug requests")
			err = server.ListenAndServe()
		} else if certErr != nil || keyErr != nil {
			log.Fatalf("TLS certificate or key is unavailable: certificate=%v key=%v", certErr, keyErr)
		} else {
			err = server.ListenAndServeTLS(certFile, keyFile)
		}
	} else {
		err = server.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newHandler(store *requestStore) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		store.token(w, r)
	})
	mux.HandleFunc("/__debug/requests", store.history)
	mux.HandleFunc("/status/", store.status)
	mux.HandleFunc("/vdcs", store.createVDC)
	mux.HandleFunc("/networks", store.createNetwork)
	mux.HandleFunc("/", store.echo)
	return mux
}

func (s *requestStore) echo(w http.ResponseWriter, r *http.Request) {
	var entry capturedRequest
	if err := s.capture(&entry, r); err != nil {
		http.Error(w, "could not read request body", http.StatusBadRequest)
		return
	}
	s.append(&entry)
	writeJSON(w, http.StatusOK, entry)
}

// createVDC is the first call of a sequence: it creates a VDC and returns its id.
func (s *requestStore) createVDC(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readResource(w, r, "name")
	if !ok {
		return
	}
	s.resourcesMu.Lock()
	if s.vdcs == nil {
		s.vdcs = map[string]string{}
	}
	id := fmt.Sprintf("vdc-%d", len(s.vdcs)+1)
	s.vdcs[id] = body["name"]
	s.resourcesMu.Unlock()
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "name": body["name"], "status": "created"})
}

// createNetwork is the second call of a sequence: it needs the id returned by
// createVDC, so it fails with 404 when it runs first or without the mapped id.
func (s *requestStore) createNetwork(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readResource(w, r, "vdcId", "name")
	if !ok {
		return
	}
	s.resourcesMu.Lock()
	_, found := s.vdcs[body["vdcId"]]
	if found {
		s.networks++
	}
	id := fmt.Sprintf("net-%d", s.networks)
	s.resourcesMu.Unlock()
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "vdc not found", "vdcId": body["vdcId"]})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "vdcId": body["vdcId"], "name": body["name"], "status": "created"})
}

// readResource accepts a POST with a flat JSON object whose listed string fields
// are all required. It records the (redacted) request and answers errors itself.
func (s *requestStore) readResource(w http.ResponseWriter, r *http.Request, required ...string) (map[string]string, bool) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil, false
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil || len(raw) > maxRequestBody {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return nil, false
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	var entry capturedRequest
	if err := s.capture(&entry, r); err != nil {
		http.Error(w, "could not read request body", http.StatusBadRequest)
		return nil, false
	}
	s.append(&entry)
	var body map[string]string
	if err := json.Unmarshal(raw, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be a JSON object of strings"})
		return nil, false
	}
	for _, field := range required {
		if body[field] == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": field + " is required"})
			return nil, false
		}
	}
	return body, true
}

func (s *requestStore) token(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
		http.Error(w, "expected application/x-www-form-urlencoded", http.StatusUnsupportedMediaType)
		return
	}
	rawBody, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil || len(rawBody) > maxRequestBody {
		http.Error(w, "invalid token request body", http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(rawBody))
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(rawBody))
	if r.Form.Get("grant_type") != "client_credentials" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_grant_type"})
		return
	}
	var entry capturedRequest
	if err := s.capture(&entry, r); err != nil {
		http.Error(w, "could not read token request", http.StatusBadRequest)
		return
	}
	s.append(&entry)
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token": "mock-debug-access-token",
		"token_type":   "Bearer",
		"expires_in":   300,
	})
}

func (s *requestStore) status(w http.ResponseWriter, r *http.Request) {
	code, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/status/"))
	if err != nil || code < 200 || code > 599 {
		http.Error(w, "status must be between 200 and 599", http.StatusBadRequest)
		return
	}
	var entry capturedRequest
	if err := s.capture(&entry, r); err != nil {
		http.Error(w, "could not read request body", http.StatusBadRequest)
		return
	}
	s.append(&entry)
	writeJSON(w, code, map[string]any{"mock": true, "status": code, "request": entry})
}

func (s *requestStore) capture(entry *capturedRequest, r *http.Request) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		return err
	}
	truncated := len(body) > maxRequestBody
	if truncated {
		body = body[:maxRequestBody]
	}
	*entry = capturedRequest{
		ReceivedAt: time.Now().UTC(), Method: r.Method, Path: r.URL.Path,
		Query: sanitizeValues(r.URL.Query()), Headers: sanitizeHeaders(r.Header),
		Body:          sanitizeBody(r.Header.Get("Content-Type"), body),
		BodyTruncated: truncated, RemoteAddr: r.RemoteAddr,
	}
	return nil
}

func (s *requestStore) append(entry *capturedRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	entry.ID = s.nextID
	if len(s.requests) == maxCapturedEvents {
		copy(s.requests, s.requests[1:])
		s.requests = s.requests[:len(s.requests)-1]
	}
	s.requests = append(s.requests, *entry)
}

func (s *requestStore) list(w http.ResponseWriter, r *http.Request) {
	limit := maxCapturedEvents
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
		if parsed < limit {
			limit = parsed
		}
	}
	s.mu.RLock()
	start := len(s.requests) - limit
	start = max(0, start)
	entries := append(make([]capturedRequest, 0, len(s.requests)-start), s.requests[start:]...)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, entries)
}

func (s *requestStore) history(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.list(w, r)
	case http.MethodDelete:
		s.clear(w, r)
	default:
		w.Header().Set("Allow", "GET, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *requestStore) clear(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.requests = nil
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func sanitizeHeaders(headers http.Header) http.Header {
	clean := make(http.Header)
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		if i == 64 {
			clean.Set("X-Debug-Headers-Truncated", "true")
			break
		}
		values := headers.Values(key)
		if sensitiveName(key) {
			clean[key] = []string{redactAuthorization(values)}
			continue
		}
		if len(values) > 8 {
			values = values[:8]
		}
		for j, value := range values {
			if len(value) > 1024 {
				values[j] = value[:1024] + "…"
			}
		}
		clean[key] = append([]string(nil), values...)
	}
	return clean
}

func sensitiveName(name string) bool {
	key := strings.ToLower(strings.ReplaceAll(name, "-", ""))
	for _, marker := range []string{"authorization", "cookie", "token", "secret", "password", "credential", "apikey", "clientsecret"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

func redactAuthorization(values []string) string {
	if len(values) == 0 {
		return redactedValue
	}
	parts := strings.Fields(values[0])
	if len(parts) > 0 && strings.EqualFold(parts[0], "basic") {
		return "Basic " + redactedValue
	}
	if len(parts) > 0 && strings.EqualFold(parts[0], "bearer") {
		return "Bearer " + redactedValue
	}
	return redactedValue
}

func sanitizeValues(values url.Values) map[string][]string {
	clean := make(map[string][]string, len(values))
	for key, value := range values {
		if sensitiveName(key) {
			clean[key] = []string{redactedValue}
			continue
		}
		if len(value) > 8 {
			value = value[:8]
		}
		clean[key] = append([]string(nil), value...)
	}
	return clean
}

func sanitizeBody(contentType string, body []byte) string {
	mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0]))
	switch mediaType {
	case "application/x-www-form-urlencoded":
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return string(body)
		}
		clean := url.Values{}
		for key, entries := range values {
			if sensitiveName(key) {
				clean[key] = []string{redactedValue}
			} else {
				clean[key] = entries
			}
		}
		return clean.Encode()
	case "application/json", "application/problem+json":
		var value any
		if err := json.Unmarshal(body, &value); err != nil {
			return string(body)
		}
		redactJSON(value)
		clean, err := json.Marshal(value)
		if err == nil {
			return string(clean)
		}
	}
	return string(body)
}

func redactJSON(value any) {
	switch object := value.(type) {
	case map[string]any:
		for key, child := range object {
			if sensitiveName(key) {
				object[key] = redactedValue
			} else {
				redactJSON(child)
			}
		}
	case []any:
		for _, child := range object {
			redactJSON(child)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("could not encode mock API response: %v", err)
	}
}
