package apicall

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// OriginsConfigMapKey is the ConfigMap key that holds the allowed origins.
const OriginsConfigMapKey = "origins"

// ParseOrigins splits origins separated by commas or newlines. Blank entries and
// lines that start with # are ignored, so a ConfigMap can document each origin.
func ParseOrigins(text string) []string {
	var origins []string
	for line := range strings.SplitSeq(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		for value := range strings.SplitSeq(line, ",") {
			if value = strings.TrimSpace(value); value != "" {
				origins = append(origins, value)
			}
		}
	}
	return origins
}

// allowedOrigins returns the origins from the manager flag plus the ones in the
// allowlist ConfigMap, if one is configured. The ConfigMap is read on every call, so an
// edit applies to the next request without restarting the operator. A ConfigMap that
// does not exist adds nothing (the allowlist then denies everything it does not list);
// any other read error is returned, so the caller retries instead of failing the call.
// An entry that is not an exact origin is ignored and reported in the log.
func (r *Handler) allowedOrigins(ctx context.Context) ([]string, error) {
	if r.OriginsConfigMap.Name == "" {
		return r.AllowedOrigins, nil
	}
	configMap := &corev1.ConfigMap{}
	if err := r.reader().Get(ctx, r.OriginsConfigMap, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return r.AllowedOrigins, nil
		}
		return nil, errOriginsUnavailable
	}
	origins := append([]string(nil), r.AllowedOrigins...)
	for _, value := range ParseOrigins(configMap.Data[OriginsConfigMapKey]) {
		if ValidateAPIOrigins([]string{value}) != nil {
			log.FromContext(ctx).Info("Ignored invalid origin in ConfigMap", "configMap", r.OriginsConfigMap.String(), "origin", value)
			continue
		}
		origins = append(origins, value)
	}
	return origins, nil
}
