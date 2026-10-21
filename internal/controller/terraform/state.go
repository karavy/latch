package terraform

import (
	"context"
	"errors"
	"fmt"

	tfengine "github.com/karavy/terraform-engine"
	"github.com/karavy/terraform-engine/s3utils"
)

// ErrStateNotFound is returned by a StateReader when no state exists for a key.
var ErrStateNotFound = errors.New("terraform state not found")

// StateReader reads Terraform state files from the state storage. The operator uses it
// to publish the outputs of a finished step; writing the state is done by the Job.
type StateReader interface {
	Read(ctx context.Context, key string) ([]byte, error)
}

// StateConfig describes the S3-compatible storage that holds every Terraform state.
type StateConfig struct {
	Endpoint string
	Bucket   string
	Region   string
	// Reader reads state objects; it is nil when the storage is not configured.
	Reader StateReader
}

// Configured reports whether Terraform steps can run: without a state storage a Job would
// fall back to a local state that is lost with the workspace, so they must not start.
func (c StateConfig) Configured() bool {
	return c.Endpoint != "" && c.Bucket != "" && c.Region != "" && c.Reader != nil
}

// Backend returns the engine's backend description for an object key.
func (c StateConfig) Backend(key string) tfengine.S3Backend {
	return tfengine.S3Backend{Endpoint: c.Endpoint, Bucket: c.Bucket, Region: c.Region, Key: key}
}

// ObjectKey returns the storage key of the state for a step key: states of different
// namespaces never collide, whatever key the workflow author chose.
func ObjectKey(namespace, stateKey string) string {
	return namespace + "/" + stateKey + ".tfstate"
}

// S3StateReader reads state objects from an S3-compatible bucket.
type S3StateReader struct {
	Endpoint, Region, Bucket, AccessKeyID, SecretAccessKey string
}

// Read returns the content of the state object, or ErrStateNotFound.
func (r S3StateReader) Read(ctx context.Context, key string) ([]byte, error) {
	client := s3utils.NewClient(r.Endpoint, r.Region, r.AccessKeyID, r.SecretAccessKey)
	data, err := s3utils.ReadObject(ctx, client, r.Bucket, key)
	if errors.Is(err, s3utils.ErrObjectNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrStateNotFound, key)
	}
	return data, err
}

// StateOutputs reads the outputs of a state. It returns them as one JSON object
// {"name": value}, which the workflow reads with JSON Pointers, and the names Terraform
// marked sensitive.
func StateOutputs(state []byte) (document []byte, sensitive []string, err error) {
	outputs, err := tfengine.OutputsFromState(state)
	if err != nil {
		return nil, nil, err
	}
	document, err = tfengine.OutputsJSON(outputs)
	if err != nil {
		return nil, nil, err
	}
	return document, tfengine.SensitiveOutputNames(outputs), nil
}

// NewS3StateConfig builds the state storage configuration from the operator's flags and
// credentials. Leaving everything empty is valid and means "no storage": Terraform steps
// then fail with a clear reason. A partial configuration is a mistake and is an error.
func NewS3StateConfig(endpoint, bucket, region, accessKeyID, secretAccessKey string) (StateConfig, error) {
	if endpoint == "" && bucket == "" && accessKeyID == "" && secretAccessKey == "" {
		return StateConfig{}, nil
	}
	if endpoint == "" || bucket == "" || region == "" {
		return StateConfig{}, errors.New("the Terraform state storage needs --tfstate-s3-endpoint, --tfstate-s3-bucket and --tfstate-s3-region")
	}
	if accessKeyID == "" || secretAccessKey == "" {
		return StateConfig{}, errors.New("the Terraform state storage needs TFSTATE_S3_ACCESS_KEY_ID and TFSTATE_S3_SECRET_ACCESS_KEY to read states")
	}
	config := StateConfig{
		Endpoint: endpoint, Bucket: bucket, Region: region,
		Reader: S3StateReader{Endpoint: endpoint, Region: region, Bucket: bucket, AccessKeyID: accessKeyID, SecretAccessKey: secretAccessKey},
	}
	// A throwaway key checks endpoint, bucket and region like the Job will.
	if err := config.Backend("validate").Validate(); err != nil {
		return StateConfig{}, fmt.Errorf("invalid Terraform state storage: %w", err)
	}
	return config, nil
}
