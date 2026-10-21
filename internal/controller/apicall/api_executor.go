package apicall

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

const apiResponseLimit = 64 << 10

// reasonRequestFailed is the reason of an attempt that failed at the network level.
const reasonRequestFailed = "RequestFailed"

type operationKey struct {
	Name       types.NamespacedName
	UID        types.UID
	Generation int64
	// Attempt numbers the requests of an ApiCall that sends the same one again (1 for the first).
	Attempt int32
}

type apiResult struct {
	Response    *dinovav1alpha1.APIResponse
	Reason      string
	Message     string
	Success     bool
	CompletedAt metav1.Time
}

type apiOperation struct {
	key     operationKey
	request apiengine.Request
	oauth   *oauth2Credentials
	timeout time.Duration
	store   bool
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	done    bool
	result  apiResult
}

// APIExecutor is a leader-only manager Runnable with a bounded worker pool.
// Only the reconciler writes Kubernetes status. Completed results stay in memory
// until their status update succeeds; channel events are an optimization over polling.
type APIExecutor struct {
	client  *apiengine.Client
	workers int
	limit   int
	queue   chan *apiOperation
	events  chan event.GenericEvent
	mu      sync.Mutex
	ctx     context.Context
	jobs    map[operationKey]*apiOperation
}

// NewAPIExecutor creates workers without starting goroutines outside manager lifecycle.
func NewAPIExecutor(apiClient *apiengine.Client, workers, queueSize int) (*APIExecutor, error) {
	if apiClient == nil || workers < 1 || workers > 64 || queueSize < 1 || queueSize > 4096 {
		return nil, errors.New("API executor requires a client, 1-64 workers and a queue size of 1-4096")
	}
	return &APIExecutor{
		client: apiClient, workers: workers, limit: workers + queueSize,
		queue:  make(chan *apiOperation, workers+queueSize),
		events: make(chan event.GenericEvent, workers+queueSize),
		jobs:   make(map[operationKey]*apiOperation),
	}, nil
}

// NeedLeaderElection ensures only the elected manager starts HTTP workers.
func (e *APIExecutor) NeedLeaderElection() bool { return true }

// Events exposes completion notifications without exposing executor internals.
func (e *APIExecutor) Events() <-chan event.GenericEvent { return e.events }

// Ready reports whether the manager has started the executor and is still active.
func (e *APIExecutor) Ready() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ctx != nil && e.ctx.Err() == nil
}

// Start owns and joins all worker goroutines on shutdown or leadership loss.
func (e *APIExecutor) Start(ctx context.Context) error {
	e.mu.Lock()
	e.ctx = ctx
	e.mu.Unlock()
	var workers sync.WaitGroup
	for range e.workers {
		workers.Go(func() { e.work(ctx) })
	}
	<-ctx.Done()
	workers.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, job := range e.jobs {
		job.cancel()
		job.request = apiengine.Request{}
		job.oauth = nil
		delete(e.jobs, key)
	}
	return nil
}

func (e *APIExecutor) work(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-e.queue:
			e.mu.Lock()
			valid := e.jobs[job.key] == job && job.ctx.Err() == nil
			request := job.request
			oauth := job.oauth
			// Transfer request ownership to the worker. Cancellation can now clear
			// queued credentials without racing with request construction.
			job.request = apiengine.Request{}
			job.oauth = nil
			e.mu.Unlock()
			if !valid {
				continue
			}
			log := logf.FromContext(ctx).WithValues("namespace", job.key.Name.Namespace,
				"name", job.key.Name.Name, "generation", job.key.Generation)
			log.V(1).Info("Started ApiCall request")
			callCtx, cancelCall := context.WithTimeout(job.ctx, job.timeout)
			var response *apiengine.Response
			var err error
			if oauth != nil {
				var token string
				token, err = requestOAuth2Token(callCtx, e.client, oauth)
				if err == nil {
					request.Headers.Set("Authorization", "Bearer "+token)
					response, err = e.client.Do(callCtx, request)
				}
				token = ""
				request.Headers.Del("Authorization")
				request.Body = nil
				request.Headers = nil
				oauth.ClientID, oauth.ClientSecret = "", ""
				oauth.TokenURL, oauth.Audience, oauth.Method = "", "", ""
				oauth.Scopes = nil
			} else {
				response, err = e.client.Do(callCtx, request)
			}
			result := apiCallResult(response, err, job.store)
			log.V(1).Info("Completed ApiCall request", "reason", result.Reason)
			cancelCall()
			e.mu.Lock()
			current := e.jobs[job.key]
			if current == job {
				job.result, job.done = result, true
			}
			e.mu.Unlock()
			if current == job {
				select {
				case e.events <- event.GenericEvent{Object: &dinovav1alpha1.ApiCall{ObjectMeta: metav1.ObjectMeta{
					Name: job.key.Name.Name, Namespace: job.key.Name.Namespace,
				}}}:
				default: // RequeueAfter remains a reliable fallback.
				}
			}
		}
	}
}

func apiCallResult(response *apiengine.Response, err error, store bool) apiResult {
	result := apiResult{CompletedAt: metav1.Now()}
	if response != nil {
		result.Response = &dinovav1alpha1.APIResponse{
			StatusCode:  response.StatusCode,
			ContentType: response.Headers.Get("Content-Type"),
			Truncated:   errors.Is(err, apiengine.ErrResponseTooLarge),
		}
		if len(result.Response.ContentType) > 256 {
			result.Response.ContentType = result.Response.ContentType[:256]
		}
		if store {
			result.Response.Body = response.Body
		}
	}
	switch {
	case errors.Is(err, apiengine.ErrResponseTooLarge):
		result.Reason, result.Message = "ResponseTooLarge", "Response exceeded the 64 KiB limit; the body is incomplete"
	case errors.Is(err, context.DeadlineExceeded):
		result.Reason, result.Message = "Timeout", "Request timed out; the remote operation may have taken effect"
	case errors.Is(err, context.Canceled):
		result.Reason, result.Message = "Canceled", "Request was canceled; the remote operation may have taken effect"
	case err != nil:
		result.Reason, result.Message = reasonRequestFailed, "Request failed or response was incomplete; the remote operation may have taken effect"
	case response == nil:
		result.Reason, result.Message = reasonRequestFailed, "API engine returned no response"
	case !response.IsSuccess():
		result.Reason, result.Message = "HTTPError", fmt.Sprintf("Remote API returned HTTP %d", response.StatusCode)
	default:
		result.Success = true
		result.Reason, result.Message = "Completed", fmt.Sprintf("Remote API returned HTTP %d", response.StatusCode)
	}
	return result
}

// reserve must precede the durable Running checkpoint, activate must follow it.
func (e *APIExecutor) reserve(key operationKey, request apiengine.Request, timeout time.Duration, store bool) bool {
	return e.reserveWithOAuth(key, request, nil, timeout, store)
}

func (e *APIExecutor) reserveWithOAuth(key operationKey, request apiengine.Request, oauth *oauth2Credentials, timeout time.Duration, store bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ctx == nil || e.ctx.Err() != nil || len(e.jobs) >= e.limit || e.jobs[key] != nil {
		return false
	}
	jobCtx, cancelJob := context.WithCancel(e.ctx)
	e.jobs[key] = &apiOperation{key: key, request: request, oauth: oauth, timeout: timeout, store: store, ctx: jobCtx, cancel: cancelJob}
	return true
}

func (e *APIExecutor) activate(key operationKey) {
	e.mu.Lock()
	defer e.mu.Unlock()
	job := e.jobs[key]
	if job == nil || job.started {
		return
	}
	select {
	case e.queue <- job:
		job.started = true
	default:
		// Canceled queue entries can briefly occupy slots; reconciliation retries activation.
	}
}

func (e *APIExecutor) lookup(key operationKey) (apiResult, bool, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	job := e.jobs[key]
	if job == nil {
		return apiResult{}, false, false
	}
	return job.result, job.done, true
}

func (e *APIExecutor) forget(key operationKey) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if job := e.jobs[key]; job != nil {
		job.cancel()
		job.request = apiengine.Request{}
		job.oauth = nil
		delete(e.jobs, key)
	}
}

// cancelObsolete drops deleted/replaced resources and superseded generations.
func (e *APIExecutor) cancelObsolete(name types.NamespacedName, keep *operationKey) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, job := range e.jobs {
		if key.Name == name && (keep == nil || key != *keep) {
			job.cancel()
			job.request = apiengine.Request{}
			job.oauth = nil
			delete(e.jobs, key)
		}
	}
}
