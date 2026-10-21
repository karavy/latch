package terraform

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestObjectKeySeparatesNamespaces(t *testing.T) {
	if got := ObjectKey("team-a", "vdc/acme/network"); got != "team-a/vdc/acme/network.tfstate" {
		t.Fatalf("ObjectKey = %q", got)
	}
	if ObjectKey("team-a", "vdc") == ObjectKey("team-b", "vdc") {
		t.Fatal("the same key in two namespaces must not share a state")
	}
}

func TestNewS3StateConfig(t *testing.T) {
	none, err := NewS3StateConfig("", "", "us-east-1", "", "")
	if err != nil || none.Configured() {
		t.Fatalf("an empty configuration must mean no storage: %+v %v", none, err)
	}
	config, err := NewS3StateConfig("http://minio:9000", "tfstate", "us-east-1", "ak", "sk")
	if err != nil || !config.Configured() {
		t.Fatalf("complete configuration rejected: %+v %v", config, err)
	}
	if backend := config.Backend("ns/key.tfstate"); backend.Bucket != "tfstate" || backend.Key != "ns/key.tfstate" {
		t.Fatalf("backend = %+v", backend)
	}
	for name, args := range map[string][5]string{
		"endpoint without bucket": {"http://minio:9000", "", "us-east-1", "ak", "sk"},
		"bucket without endpoint": {"", "tfstate", "us-east-1", "ak", "sk"},
		"no region":               {"http://minio:9000", "tfstate", "", "ak", "sk"},
		"no credentials":          {"http://minio:9000", "tfstate", "us-east-1", "", ""},
		"half credentials":        {"http://minio:9000", "tfstate", "us-east-1", "ak", ""},
		"invalid endpoint":        {"minio:9000", "tfstate", "us-east-1", "ak", "sk"},
		"invalid bucket":          {"http://minio:9000", "Bad_Bucket", "us-east-1", "ak", "sk"},
	} {
		if _, err := NewS3StateConfig(args[0], args[1], args[2], args[3], args[4]); err == nil {
			t.Errorf("%s: a partial or invalid configuration must be an error", name)
		}
	}
}

func TestStateOutputsReturnsADocumentAndTheSensitiveNames(t *testing.T) {
	document, sensitive, err := StateOutputs([]byte(`{"version":4,"outputs":{
		"vpc_id":{"value":"vpc-1","type":"string"},
		"admin_password":{"value":"s3cr3t","type":"string","sensitive":true}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(document) != `{"admin_password":"s3cr3t","vpc_id":"vpc-1"}` {
		t.Errorf("document = %s", document)
	}
	if !reflect.DeepEqual(sensitive, []string{"admin_password"}) {
		t.Errorf("sensitive = %v", sensitive)
	}
	if _, _, err := StateOutputs([]byte(`not json`)); err == nil {
		t.Error("an invalid state must be an error")
	}
}

func TestS3StateReaderReadsObjectsAndReportsMissingState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tfstate/team-a/vdc.tfstate":
			_, _ = w.Write([]byte(`{"version":4}`))
		default:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`))
		}
	}))
	defer server.Close()
	reader := S3StateReader{Endpoint: server.URL, Region: "us-east-1", Bucket: "tfstate", AccessKeyID: "ak", SecretAccessKey: "sk"}

	data, err := reader.Read(context.Background(), "team-a/vdc.tfstate")
	if err != nil || string(data) != `{"version":4}` {
		t.Fatalf("Read = %q, %v", data, err)
	}
	_, err = reader.Read(context.Background(), "team-a/other.tfstate")
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("a missing state must be ErrStateNotFound so the workflow can tell it from an access error, got %v", err)
	}
	if err != nil && !strings.Contains(err.Error(), "team-a/other.tfstate") {
		t.Errorf("the error should name the key: %v", err)
	}
}
