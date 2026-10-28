package terraform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	tfengine "github.com/karavy/terraform-engine"
	"github.com/karavy/terraform-engine/s3utils"
)

// ErrStateNotFound is returned by a StateReader when no state exists for a key.
var ErrStateNotFound = errors.New("terraform state not found")

const (
	// releasedLockSuffix names, after the state key, the record of a lock the operator removed.
	releasedLockSuffix = ".released-lock"
	// unsavedSuffix names, after the state key, the record the runner writes of the resources
	// an apply began to create and that are not in the state (cmd/terraform-runner).
	unsavedSuffix = ".unsaved-resources"
)

// StateReader reads Terraform state files from the state storage. The operator uses it
// to publish the outputs of a finished step; writing the state is done by the Job.
type StateReader interface {
	Read(ctx context.Context, key string) ([]byte, error)
}

// PlanCleaner deletes the saved plans of a Plan workflow from the state storage.
type PlanCleaner interface {
	DeletePrefix(ctx context.Context, prefix string) error
}

// StateConfig describes the S3-compatible storage that holds every Terraform state.
type StateConfig struct {
	Endpoint string
	Bucket   string
	Region   string
	// Reader reads state objects; it is nil when the storage is not configured.
	Reader StateReader
	// Plans deletes saved plans that are no longer needed; nil when the storage is not
	// configured, and then there is nothing to delete.
	Plans PlanCleaner
	// Locks reads and removes the lock files OpenTofu keeps next to the states; nil when the
	// storage is not configured.
	Locks LockStore
}

// StateLock is the lock file OpenTofu writes next to a state while it works on it
// (use_lockfile): "<state key>.tflock".
type StateLock struct {
	ID        string    `json:"ID"`
	Operation string    `json:"Operation"`
	Who       string    `json:"Who"`
	Version   string    `json:"Version"`
	Created   time.Time `json:"Created"`
	Path      string    `json:"Path"`
}

// ReleasedLock records, next to a state, a lock that the operator removed after its runner died:
// "<state key>.released-lock". Resources the runner created after its last save of the state
// are not in the state, so applying again may create them twice; the record stays until someone
// accepts it (see the Workflow's acceptReleasedStateLocks).
type ReleasedLock struct {
	StateLock
	ReleasedAt time.Time `json:"ReleasedAt"`
	// Workflow is "<namespace>/<name>" of the Workflow that removed the lock, Step its step.
	Workflow string `json:"Workflow"`
	Step     string `json:"Step"`
}

// UnsavedResources is the record the runner writes next to a state when an apply that failed
// or was interrupted began to create resources that are not in the state: they may exist in the
// cloud, and applying again may create them twice. The format is that of cmd/terraform-runner.
type UnsavedResources struct {
	ID        string            `json:"ID"`
	Resources []UnsavedResource `json:"Resources"`
	// Who is "runner@<pod>", Workflow "<namespace>/<name>" and Step the step that wrote it.
	Who      string    `json:"Who"`
	Workflow string    `json:"Workflow"`
	Step     string    `json:"Step"`
	Created  time.Time `json:"Created"`
}

// UnsavedResource is a resource whose create began and is not in the state, with the error
// OpenTofu reported for it, if any.
type UnsavedResource struct {
	Address string `json:"Address"`
	Error   string `json:"Error,omitempty"`
}

// LockStore reads the lock of a state, and removes it; it keeps the record of the locks it
// removed, and reads the record of the resources an apply did not save.
type LockStore interface {
	// ReadLock returns the lock of the state, or nil when the state is not locked.
	ReadLock(ctx context.Context, stateKey string) (*StateLock, error)
	// RemoveLock removes the lock of the state if it is still the one with this ID.
	RemoveLock(ctx context.Context, stateKey, id string) error
	// MarkReleased records a lock removed from the state.
	MarkReleased(ctx context.Context, stateKey string, released ReleasedLock) error
	// ReadReleased returns the record of the lock removed from the state, or nil.
	ReadReleased(ctx context.Context, stateKey string) (*ReleasedLock, error)
	// ClearReleased deletes the record if it is still the one of the lock with this ID.
	ClearReleased(ctx context.Context, stateKey, id string) error
	// ReadUnsaved returns the record of the resources not saved in the state, or nil.
	ReadUnsaved(ctx context.Context, stateKey string) (*UnsavedResources, error)
	// ClearUnsaved deletes that record if it still has this ID.
	ClearUnsaved(ctx context.Context, stateKey, id string) error
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

// DeletePrefix deletes every object whose key starts with prefix.
func (r S3StateReader) DeletePrefix(ctx context.Context, prefix string) error {
	client := s3utils.NewClient(r.Endpoint, r.Region, r.AccessKeyID, r.SecretAccessKey)
	_, err := s3utils.DeletePrefix(ctx, client, r.Bucket, prefix)
	return err
}

// ReadLock returns the lock of a state, or nil when there is none.
func (r S3StateReader) ReadLock(ctx context.Context, stateKey string) (*StateLock, error) {
	raw, err := r.Read(ctx, stateKey+".tflock")
	switch {
	case errors.Is(err, ErrStateNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	}
	lock := &StateLock{}
	if err := json.Unmarshal(raw, lock); err != nil {
		return nil, fmt.Errorf("the lock of %s is not valid JSON: %w", stateKey, err)
	}
	return lock, nil
}

// RemoveLock removes the lock of a state if it still has the given ID, so that a lock taken in
// the meantime by another run is never removed.
func (r S3StateReader) RemoveLock(ctx context.Context, stateKey, id string) error {
	lock, err := r.ReadLock(ctx, stateKey)
	if err != nil || lock == nil {
		return err
	}
	if lock.ID != id {
		return fmt.Errorf("the lock of %s changed (now %s): left in place", stateKey, lock.ID)
	}
	client := s3utils.NewClient(r.Endpoint, r.Region, r.AccessKeyID, r.SecretAccessKey)
	_, err = s3utils.DeletePrefix(ctx, client, r.Bucket, stateKey+".tflock")
	return err
}

// MarkReleased writes the record of a removed lock next to the state.
func (r S3StateReader) MarkReleased(ctx context.Context, stateKey string, released ReleasedLock) error {
	raw, err := json.Marshal(released)
	if err != nil {
		return err
	}
	client := s3utils.NewClient(r.Endpoint, r.Region, r.AccessKeyID, r.SecretAccessKey)
	return s3utils.WriteObject(ctx, client, r.Bucket, stateKey+releasedLockSuffix, raw)
}

// ReadReleased returns the record of the lock removed from a state, or nil when there is none.
func (r S3StateReader) ReadReleased(ctx context.Context, stateKey string) (*ReleasedLock, error) {
	raw, err := r.Read(ctx, stateKey+releasedLockSuffix)
	switch {
	case errors.Is(err, ErrStateNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	}
	released := &ReleasedLock{}
	if err := json.Unmarshal(raw, released); err != nil {
		return nil, fmt.Errorf("the released lock record of %s is not valid JSON: %w", stateKey, err)
	}
	return released, nil
}

// ClearReleased deletes the record of a removed lock if it is still the one of the lock with
// this ID, so that the record of a later release is never lost.
func (r S3StateReader) ClearReleased(ctx context.Context, stateKey, id string) error {
	released, err := r.ReadReleased(ctx, stateKey)
	if err != nil || released == nil {
		return err
	}
	if released.ID != id {
		return fmt.Errorf("the released lock record of %s changed (now %s): left in place", stateKey, released.ID)
	}
	client := s3utils.NewClient(r.Endpoint, r.Region, r.AccessKeyID, r.SecretAccessKey)
	_, err = s3utils.DeletePrefix(ctx, client, r.Bucket, stateKey+releasedLockSuffix)
	return err
}

// ReadUnsaved returns the record of the resources an apply did not save in a state, or nil.
func (r S3StateReader) ReadUnsaved(ctx context.Context, stateKey string) (*UnsavedResources, error) {
	raw, err := r.Read(ctx, stateKey+unsavedSuffix)
	switch {
	case errors.Is(err, ErrStateNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	}
	unsaved := &UnsavedResources{}
	if err := json.Unmarshal(raw, unsaved); err != nil {
		return nil, fmt.Errorf("the unsaved resources record of %s is not valid JSON: %w", stateKey, err)
	}
	return unsaved, nil
}

// ClearUnsaved deletes the record of the unsaved resources of a state if it still has this ID.
func (r S3StateReader) ClearUnsaved(ctx context.Context, stateKey, id string) error {
	unsaved, err := r.ReadUnsaved(ctx, stateKey)
	if err != nil || unsaved == nil {
		return err
	}
	if unsaved.ID != id {
		return fmt.Errorf("the unsaved resources record of %s changed (now %s): left in place", stateKey, unsaved.ID)
	}
	client := s3utils.NewClient(r.Endpoint, r.Region, r.AccessKeyID, r.SecretAccessKey)
	_, err = s3utils.DeletePrefix(ctx, client, r.Bucket, stateKey+unsavedSuffix)
	return err
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
	storage := S3StateReader{Endpoint: endpoint, Region: region, Bucket: bucket, AccessKeyID: accessKeyID, SecretAccessKey: secretAccessKey}
	config := StateConfig{Endpoint: endpoint, Bucket: bucket, Region: region, Reader: storage, Plans: storage, Locks: storage}
	// A throwaway key checks endpoint, bucket and region like the Job will.
	if err := config.Backend("validate").Validate(); err != nil {
		return StateConfig{}, fmt.Errorf("invalid Terraform state storage: %w", err)
	}
	return config, nil
}

// StateHasResources reports whether a Terraform state manages any resource: a state left by a
// destroy, or by a configuration that was never applied, has none.
func StateHasResources(state []byte) (bool, error) {
	var document struct {
		Resources []json.RawMessage `json:"resources"`
	}
	if err := json.Unmarshal(state, &document); err != nil {
		return false, err
	}
	return len(document.Resources) > 0, nil
}
