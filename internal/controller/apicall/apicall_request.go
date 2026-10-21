/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apicall

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

var errInvalidRequest = errors.New("invalid API request configuration")
var errCredentialsUnavailable = errors.New("authentication Secret or key is unavailable")
var errOriginsUnavailable = errors.New("origin allowlist ConfigMap could not be read")

const (
	apiAuthBearer          = "Bearer"
	apiAuthBasic           = "Basic"
	apiAuthKey             = "APIKey"
	apiAuthOAuth2          = "OAuth2ClientCredentials"
	oauthClientSecretBasic = "ClientSecretBasic"
	oauthClientSecretPost  = "ClientSecretPost"
	apiSecretReadTimeout   = 5 * time.Second
)

// oauth2Credentials are held only in the bounded in-memory execution queue.
type oauth2Credentials struct {
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scopes       []string
	Audience     string
	Method       string
}

// origin normalizes default ports, without accepting credentials, fragments or opaque URLs.
func origin(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return "", errInvalidRequest
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	port := u.Port()
	if port != "" && (u.Scheme != "http" || port != "80") && (u.Scheme != "https" || port != "443") {
		host += ":" + port
	}
	return u.Scheme + "://" + host, nil
}

// ValidateAPIOrigins rejects configuration that is not an exact HTTP(S) origin.
func ValidateAPIOrigins(origins []string) error {
	for _, value := range origins {
		u, err := url.Parse(value)
		if err != nil || u == nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery {
			return errInvalidRequest
		}
		if _, err := origin(value); err != nil {
			return err
		}
	}
	return nil
}

func (r *Handler) buildRequest(ctx context.Context, call *dinovav1alpha1.ApiCall) (apiengine.Request, *oauth2Credentials, error) {
	request := apiengine.Request{Method: call.Spec.Method, Endpoint: call.Spec.Endpoint, Body: []byte(call.Spec.Body), Headers: make(http.Header)}
	switch request.Method {
	case http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete:
	default:
		return request, nil, errInvalidRequest
	}
	endpointOrigin, err := origin(request.Endpoint)
	if err != nil || len(call.Spec.Body) > 65536 || len(call.Spec.Headers) > 32 {
		return request, nil, errInvalidRequest
	}
	if allowed, err := r.originAllowed(ctx, endpointOrigin); err != nil || !allowed {
		if err == nil {
			err = errInvalidRequest
		}
		return request, nil, err
	}
	for key, value := range call.Spec.Headers {
		if !validHeaderName(key) || len(key) > 128 || len(value) > 4096 || strings.ContainsAny(value, "\r\n") || sensitiveHeader(key) {
			return request, nil, errInvalidRequest
		}
		request.Headers.Set(key, value)
	}
	var oauth *oauth2Credentials
	if call.Spec.Authentication != nil {
		switch call.Spec.Authentication.Type {
		case apiAuthOAuth2:
			if !strings.HasPrefix(endpointOrigin, "https://") {
				return request, nil, errInvalidRequest
			}
			oauth, err = r.oauth2Credentials(ctx, call)
			if err != nil {
				return request, nil, err
			}
		default:
			// Never send Secret credentials over plaintext HTTP.
			if !strings.HasPrefix(endpointOrigin, "https://") {
				return request, nil, errInvalidRequest
			}
			if err := r.applyAuthentication(ctx, call, &request); err != nil {
				return request, nil, err
			}
		}
	}
	return request, oauth, nil
}

func (r *Handler) oauth2Credentials(ctx context.Context, call *dinovav1alpha1.ApiCall) (*oauth2Credentials, error) {
	auth := call.Spec.Authentication
	tokenOrigin, err := origin(auth.TokenURL)
	if err != nil || !strings.HasPrefix(tokenOrigin, "https://") {
		return nil, errInvalidRequest
	}
	if allowed, err := r.originAllowed(ctx, tokenOrigin); err != nil || !allowed {
		if err == nil {
			err = errInvalidRequest
		}
		return nil, err
	}
	method := auth.ClientAuthenticationMethod
	if method == "" {
		method = oauthClientSecretBasic
	}
	if method != oauthClientSecretBasic && method != oauthClientSecretPost {
		return nil, errInvalidRequest
	}
	clientID, err := r.secretValue(ctx, call.Namespace, auth.ClientIDSecretRef)
	if err != nil {
		return nil, err
	}
	clientSecret, err := r.secretValue(ctx, call.Namespace, auth.ClientSecretSecretRef)
	if err != nil {
		return nil, err
	}
	for _, scope := range auth.Scopes {
		if len(scope) > 256 || strings.ContainsAny(scope, "\r\n") {
			return nil, errInvalidRequest
		}
	}
	if len(auth.Scopes) > 32 || len(auth.Audience) > 2048 || strings.ContainsAny(auth.Audience, "\r\n") {
		return nil, errInvalidRequest
	}
	return &oauth2Credentials{
		TokenURL: auth.TokenURL, ClientID: clientID, ClientSecret: clientSecret,
		Scopes: append([]string(nil), auth.Scopes...), Audience: auth.Audience, Method: method,
	}, nil
}

func requestOAuth2Token(ctx context.Context, client *apiengine.Client, credentials *oauth2Credentials) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	if len(credentials.Scopes) > 0 {
		form.Set("scope", strings.Join(credentials.Scopes, " "))
	}
	if credentials.Audience != "" {
		form.Set("audience", credentials.Audience)
	}
	request := apiengine.Request{
		Method: http.MethodPost, Endpoint: credentials.TokenURL,
		Headers: http.Header{"Content-Type": {"application/x-www-form-urlencoded"}, "Accept": {"application/json"}},
		Body:    []byte(form.Encode()),
	}
	if credentials.Method == oauthClientSecretBasic {
		// RFC 6749 §2.3.1 form-encodes client credentials before HTTP Basic auth.
		encodedID := url.QueryEscape(credentials.ClientID)
		encodedSecret := url.QueryEscape(credentials.ClientSecret)
		request.Headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(encodedID+":"+encodedSecret)))
	} else {
		form.Set("client_id", credentials.ClientID)
		form.Set("client_secret", credentials.ClientSecret)
		request.Body = []byte(form.Encode())
	}
	response, err := client.Do(ctx, request)
	if err != nil {
		return "", fmt.Errorf("OAuth token request failed: %w", err)
	}
	if response == nil || !response.IsSuccess() {
		if response == nil {
			return "", errors.New("OAuth token endpoint returned no response")
		}
		return "", fmt.Errorf("OAuth token endpoint returned HTTP %d", response.StatusCode)
	}
	var token struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := response.DecodeJSON(&token); err != nil {
		return "", errors.New("OAuth token endpoint returned an invalid JSON response")
	}
	if strings.TrimSpace(token.AccessToken) == "" || strings.ContainsAny(token.AccessToken, "\r\n") || !strings.EqualFold(token.TokenType, "Bearer") {
		return "", errors.New("OAuth token endpoint returned an invalid bearer token")
	}
	return token.AccessToken, nil
}

func (r *Handler) originAllowed(ctx context.Context, endpointOrigin string) (bool, error) {
	origins, err := r.allowedOrigins(ctx)
	if err != nil {
		return false, err
	}
	for _, allowed := range origins {
		allowedOrigin, err := origin(allowed)
		if err == nil && allowedOrigin == endpointOrigin {
			return true, nil
		}
	}
	return false, nil
}

func validHeaderName(key string) bool {
	if key == "" {
		return false
	}
	for _, char := range key {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && !strings.ContainsRune("!#$%&'*+-.^_`|~", char) {
			return false
		}
	}
	return true
}

func sensitiveHeader(key string) bool {
	switch strings.ToLower(key) {
	case "authorization", "proxy-authorization", "cookie", "host", "connection", "transfer-encoding", "content-length":
		return true
	default:
		return false
	}
}

func (r *Handler) secretValue(ctx context.Context, namespace string, ref *dinovav1alpha1.APISecretKeyReference) (string, error) {
	if ref == nil || ref.Name == "" || ref.Key == "" {
		return "", errInvalidRequest
	}
	secret := &corev1.Secret{}
	secretCtx, cancelRead := context.WithTimeout(ctx, apiSecretReadTimeout)
	defer cancelRead()
	if err := r.reader().Get(secretCtx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, secret); err != nil {
		// Return a sanitized error: neither Secret data nor endpoint is exposed.
		return "", errCredentialsUnavailable
	}
	value, ok := secret.Data[ref.Key]
	if !ok || len(value) == 0 {
		return "", errCredentialsUnavailable
	}
	if len(value) > 8192 {
		return "", errInvalidRequest
	}
	return string(value), nil
}

func (r *Handler) applyAuthentication(ctx context.Context, call *dinovav1alpha1.ApiCall, request *apiengine.Request) error {
	auth := call.Spec.Authentication
	switch auth.Type {
	case apiAuthBearer, apiAuthKey:
		token, err := r.secretValue(ctx, call.Namespace, auth.TokenSecretRef)
		if err != nil {
			return err
		}
		if strings.ContainsAny(token, "\r\n") {
			return errInvalidRequest
		}
		if auth.Type == apiAuthBearer {
			request.Headers.Set("Authorization", "Bearer "+token)
		} else {
			if !validHeaderName(auth.HeaderName) || sensitiveHeader(auth.HeaderName) {
				return errInvalidRequest
			}
			request.Headers.Set(auth.HeaderName, token)
		}
	case apiAuthBasic:
		username, err := r.secretValue(ctx, call.Namespace, auth.UsernameSecretRef)
		if err != nil {
			return err
		}
		password, err := r.secretValue(ctx, call.Namespace, auth.PasswordSecretRef)
		if err != nil {
			return err
		}
		if strings.Contains(username, ":") {
			return errInvalidRequest
		}
		request.Headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(username+":"+password)))
	default:
		return errInvalidRequest
	}
	return nil
}
