package apicall

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/jsonptr"
)

const (
	defaultRetryAttempts = 3
	defaultRetryInterval = 5 * time.Second
)

func retryMaxAttempts(retry *dinovav1alpha1.ApiCallRetry) int32 {
	if retry.MaxAttempts == 0 {
		return defaultRetryAttempts
	}
	return retry.MaxAttempts
}

func retryInterval(retry *dinovav1alpha1.ApiCallRetry) time.Duration {
	if retry.IntervalSeconds == 0 {
		return defaultRetryInterval
	}
	return time.Duration(retry.IntervalSeconds) * time.Second
}

// retryTransportErrors is true unless the ApiCall says otherwise.
func retryTransportErrors(retry *dinovav1alpha1.ApiCallRetry) bool {
	return retry.OnTransportErrors == nil || *retry.OnTransportErrors
}

// retryable reports whether a failed attempt is one the ApiCall was told to try again: an HTTP
// status in onStatusCodes, or a timeout or network failure when those are accepted. An invalid
// request, an oversized response and a lost execution are never retried.
func retryable(retry *dinovav1alpha1.ApiCallRetry, result apiResult) bool {
	switch result.Reason {
	case "HTTPError":
		return result.Response != nil && slices.Contains(retry.OnStatusCodes, int32(result.Response.StatusCode))
	case "Timeout", reasonRequestFailed:
		return retryTransportErrors(retry)
	}
	return false
}

// conditionMet reports whether a JSON response satisfies the condition. A response that is not
// JSON, or lacks the field, is an error rather than "not yet": waiting would not change it.
func conditionMet(condition *dinovav1alpha1.ApiCallCondition, body []byte) (bool, error) {
	var document any
	if err := json.Unmarshal(body, &document); err != nil {
		return false, fmt.Errorf("the response is not valid JSON: %w", err)
	}
	actual, err := jsonptr.Get(document, condition.JSONPointer)
	if err != nil {
		return false, fmt.Errorf("the response has nothing at %s: %w", condition.JSONPointer, err)
	}
	if condition.EqualsJSON == "" {
		return true, nil
	}
	var expected any
	if err := json.Unmarshal([]byte(condition.EqualsJSON), &expected); err != nil {
		return false, fmt.Errorf("equalsJSON is not valid JSON: %w", err)
	}
	return reflect.DeepEqual(actual, expected), nil
}

// conditionFailed reports whether a JSON response has one of the values that end the wait as a
// failure, and which one. A response that cannot be read is not a failure here: conditionMet
// already reports it.
func conditionFailed(condition *dinovav1alpha1.ApiCallCondition, body []byte) (string, bool) {
	if len(condition.FailWhenJSON) == 0 {
		return "", false
	}
	var document any
	if err := json.Unmarshal(body, &document); err != nil {
		return "", false
	}
	actual, err := jsonptr.Get(document, condition.JSONPointer)
	if err != nil {
		return "", false
	}
	for _, candidate := range condition.FailWhenJSON {
		var failed any
		if json.Unmarshal([]byte(candidate), &failed) == nil && reflect.DeepEqual(actual, failed) {
			return candidate, true
		}
	}
	return "", false
}

// verdict is what the ApiCall does after an attempt.
type verdict struct {
	again   bool   // send the request again, after the interval
	success bool   // otherwise: the ApiCall is done, and it succeeded or failed
	reason  string // of the final state, or of the wait
	message string
}

// judge decides, for an ApiCall that has a retry policy, what an attempt that just finished
// means: done, try again, or failed. attempts counts the requests sent, this one included.
func judge(retry *dinovav1alpha1.ApiCallRetry, attempts int32, result apiResult) verdict {
	exhausted := attempts >= retryMaxAttempts(retry)
	if result.Success {
		if retry.Until == nil {
			return verdict{success: true, reason: result.Reason, message: result.Message}
		}
		met, err := conditionMet(retry.Until, bodyOf(result))
		switch {
		case err != nil:
			return verdict{reason: "ConditionInvalid", message: "Cannot check retry.until: " + err.Error()}
		case met:
			return verdict{success: true, reason: result.Reason, message: result.Message}
		}
		if value, failed := conditionFailed(retry.Until, bodyOf(result)); failed {
			return verdict{reason: "PollFailed", message: fmt.Sprintf("The response has %s at %s, which ends the wait", value, retry.Until.JSONPointer)}
		}
		if exhausted {
			return verdict{reason: "PollTimeout", message: fmt.Sprintf("The response did not satisfy retry.until in %d attempts", attempts)}
		}
		return verdict{again: true, reason: "WaitingForCondition", message: fmt.Sprintf("Attempt %d of %d: the response does not satisfy retry.until yet", attempts, retryMaxAttempts(retry))}
	}
	if !retryable(retry, result) {
		return verdict{reason: result.Reason, message: result.Message}
	}
	if exhausted {
		reason := "RetriesExhausted"
		if retry.Until != nil {
			reason = "PollTimeout"
		}
		return verdict{reason: reason, message: fmt.Sprintf("The request failed in all %d attempts; the last one: %s", attempts, result.Message)}
	}
	return verdict{again: true, reason: "RetryScheduled", message: fmt.Sprintf("Attempt %d of %d failed (%s): %s", attempts, retryMaxAttempts(retry), result.Reason, result.Message)}
}

func bodyOf(result apiResult) []byte {
	if result.Response == nil {
		return nil
	}
	return result.Response.Body
}

// keptPointers returns the JSON Pointers whose values the status keeps from a response: the
// spec's, and the one retry.until reads. Nil keeps the whole body.
func keptPointers(spec *dinovav1alpha1.ApiCallSpec) []string {
	if len(spec.KeepJSONPointers) == 0 {
		return nil
	}
	keep := spec.KeepJSONPointers
	if spec.Retry != nil && spec.Retry.Until != nil {
		keep = append(slices.Clone(keep), spec.Retry.Until.JSONPointer)
	}
	return keep
}
