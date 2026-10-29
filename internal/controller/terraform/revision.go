package terraform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

const (
	// TemplateLabel names, on a ControllerRevision, the TerraformTemplate it is a revision of.
	TemplateLabel = "dinova.one/terraform-template"
	// DigestAnnotation is the SHA-256 of the files a ControllerRevision, or a copy of them,
	// holds.
	DigestAnnotation = "dinova.one/digest"
	// RevisionAnnotation is the revision number of the files a copy holds.
	RevisionAnnotation = "dinova.one/terraform-revision"
	// ConfigMapIndex indexes TerraformTemplates by the ConfigMaps their files are read from.
	ConfigMapIndex = "spec.files.configMapKeyRef.name"

	conditionReady = "Ready"
)

// ErrRevisionPending means that the operator has not made the revision of the current spec
// yet: a Workflow waits for it instead of running an older one.
var ErrRevisionPending = errors.New("the TerraformTemplate has no revision of its current spec yet")

// SourceError is a problem with the files of a TerraformTemplate that only a change of the
// template or of its ConfigMaps can fix. Reason is the reason of the Ready condition.
type SourceError struct {
	Reason, Message string
}

func (e *SourceError) Error() string { return e.Message }

// Digest is the SHA-256 of a set of files, by name and content, as "sha256:<hex>".
func Digest(files map[string]string) string {
	names := slices.Sorted(func(yield func(string) bool) {
		for name := range files {
			if !yield(name) {
				return
			}
		}
	})
	hash := sha256.New()
	for _, name := range names {
		hash.Write([]byte(name))
		hash.Write([]byte{0})
		hash.Write([]byte(files[name]))
		hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

// ConfigMapNames returns the ConfigMaps a TerraformTemplate reads, for ConfigMapIndex.
func ConfigMapNames(template *dinovav1alpha1.TerraformTemplate) []string {
	var names []string
	for _, file := range template.Spec.Files {
		if !slices.Contains(names, file.ConfigMapKeyRef.Name) {
			names = append(names, file.ConfigMapKeyRef.Name)
		}
	}
	return names
}

// CurrentRevision returns the name of the ControllerRevision a Workflow must run now:
// ErrRevisionPending while the operator has not caught up with the spec, a *SourceError when
// the files cannot be read.
func CurrentRevision(template *dinovav1alpha1.TerraformTemplate) (string, error) {
	ready := meta.FindStatusCondition(template.Status.Conditions, conditionReady)
	if ready == nil || template.Status.ObservedGeneration != template.Generation {
		return "", ErrRevisionPending
	}
	if ready.Status != metav1.ConditionTrue {
		return "", &SourceError{Reason: ready.Reason, Message: fmt.Sprintf("TerraformTemplate %q is not ready: %s", template.Name, ready.Message)}
	}
	return template.Status.RevisionName, nil
}

// RevisionFiles returns the files a ControllerRevision holds, after checking that they still
// match its digest.
func RevisionFiles(revision *appsv1.ControllerRevision) (map[string]string, error) {
	files := map[string]string{}
	if err := json.Unmarshal(revision.Data.Raw, &files); err != nil {
		return nil, fmt.Errorf("revision %q does not hold Terraform files: %w", revision.Name, err)
	}
	if digest := Digest(files); digest != revision.Annotations[DigestAnnotation] {
		return nil, fmt.Errorf("revision %q holds files with digest %s, not %s", revision.Name, digest, revision.Annotations[DigestAnnotation])
	}
	return files, nil
}

// Revisions keeps the revisions of TerraformTemplates: every time the content of the files of
// a template changes, it snapshots them in a new ControllerRevision with the next number, and
// it reports the current one in the template status.
type Revisions struct {
	Client client.Client
	Scheme *runtime.Scheme
	// Reader reads the ConfigMaps. It should not be a cache: right after a ConfigMap is
	// deleted and created again, a cache may still hold the old one.
	Reader client.Reader
	// AllowMutableSources lets a template with spec.allowMutableSources read ConfigMaps that
	// are not immutable (the --allow-mutable-terraform-sources flag).
	AllowMutableSources bool
}

// Reconcile makes the revision of the current files of a TerraformTemplate.
func (r *Revisions) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	template := &dinovav1alpha1.TerraformTemplate{}
	if err := r.Client.Get(ctx, req.NamespacedName, template); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	files, sources, err := r.resolve(ctx, template)
	var sourceError *SourceError
	if errors.As(err, &sourceError) {
		return ctrl.Result{}, r.setReady(ctx, template, metav1.ConditionFalse, sourceError.Reason, sourceError.Message)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	digest := Digest(files)
	revisions, err := r.revisions(ctx, template)
	if err != nil {
		return ctrl.Result{}, err
	}
	var revision *appsv1.ControllerRevision
	if index := slices.IndexFunc(revisions, func(revision appsv1.ControllerRevision) bool {
		return revision.Annotations[DigestAnnotation] == digest
	}); index >= 0 {
		revision = &revisions[index]
	} else {
		if revision, err = r.create(ctx, template, files, digest, revisions); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Created TerraformTemplate revision", "template", template.Name, "revision", revision.Revision, "digest", digest)
	}
	if err := r.prune(ctx, template, revisions, revision.Name); err != nil {
		return ctrl.Result{}, err
	}
	template.Status.Revision = revision.Revision
	template.Status.RevisionName = revision.Name
	template.Status.Digest = digest
	template.Status.Sources = sources
	message := fmt.Sprintf("Revision %d (%s)", revision.Revision, digest)
	if mutable := mutableSources(sources); len(mutable) > 0 {
		message += fmt.Sprintf("; ConfigMaps %s are mutable: an edit changes the code of the next runs", strings.Join(mutable, ", "))
	}
	return ctrl.Result{}, r.setReady(ctx, template, metav1.ConditionTrue, "RevisionReady", message)
}

// mutableSources returns the names of the sources that are not immutable.
func mutableSources(sources []dinovav1alpha1.TerraformTemplateSource) []string {
	var names []string
	for _, source := range sources {
		if source.Mutable {
			names = append(names, strconv.Quote(source.Name))
		}
	}
	return names
}

// resolve reads the files of a template from its ConfigMaps, which must be immutable unless
// both the template and the operator allow mutable sources.
func (r *Revisions) resolve(ctx context.Context, template *dinovav1alpha1.TerraformTemplate) (map[string]string, []dinovav1alpha1.TerraformTemplateSource, error) {
	reader := r.Reader
	if reader == nil {
		reader = r.Client
	}
	configMaps := map[string]*corev1.ConfigMap{}
	var readError error // a failed read, which a retry may fix
	lookup := func(ref dinovav1alpha1.WorkflowConfigMapKeyReference) (string, error) {
		configMap, cached := configMaps[ref.Name]
		if !cached {
			configMap = &corev1.ConfigMap{}
			err := reader.Get(ctx, types.NamespacedName{Namespace: template.Namespace, Name: ref.Name}, configMap)
			if apierrors.IsNotFound(err) {
				return "", &SourceError{"ConfigMapUnavailable", fmt.Sprintf("ConfigMap %q was not found", ref.Name)}
			}
			if err != nil {
				readError = err
				return "", err
			}
			if err := r.checkMutable(template, configMap); err != nil {
				return "", err
			}
			configMaps[ref.Name] = configMap
		}
		value, ok := configMap.Data[ref.Key]
		if !ok {
			return "", &SourceError{"ConfigMapKeyMissing", fmt.Sprintf("ConfigMap %q has no key %q", ref.Name, ref.Key)}
		}
		return value, nil
	}
	files, err := ConfigMapData(template, lookup)
	var sourceError *SourceError
	switch {
	case readError != nil:
		return nil, nil, readError
	case errors.As(err, &sourceError):
		return nil, nil, err
	case err != nil: // the spec itself is wrong, for example two files with one name
		return nil, nil, &SourceError{"InvalidFiles", err.Error()}
	}
	sources := make([]dinovav1alpha1.TerraformTemplateSource, 0, len(configMaps))
	for name, configMap := range configMaps {
		sources = append(sources, dinovav1alpha1.TerraformTemplateSource{Name: name, UID: string(configMap.UID), Mutable: !immutable(configMap)})
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Name < sources[j].Name })
	return files, sources, nil
}

// checkMutable refuses a ConfigMap that is not immutable, unless both the template and the
// operator allow mutable sources.
func (r *Revisions) checkMutable(template *dinovav1alpha1.TerraformTemplate, configMap *corev1.ConfigMap) error {
	switch {
	case immutable(configMap):
		return nil
	case !template.Spec.AllowMutableSources:
		return &SourceError{"ConfigMapNotImmutable", fmt.Sprintf("ConfigMap %q holds Terraform files and must be immutable (immutable: true)", configMap.Name)}
	case !r.AllowMutableSources:
		return &SourceError{"MutableSourcesDisabled", fmt.Sprintf("ConfigMap %q is not immutable: the template allows mutable sources, but the operator was not started with --allow-mutable-terraform-sources", configMap.Name)}
	}
	return nil
}

func immutable(configMap *corev1.ConfigMap) bool {
	return configMap.Immutable != nil && *configMap.Immutable
}

// revisions lists the revisions of a template, newest first.
func (r *Revisions) revisions(ctx context.Context, template *dinovav1alpha1.TerraformTemplate) ([]appsv1.ControllerRevision, error) {
	list := &appsv1.ControllerRevisionList{}
	if err := r.Client.List(ctx, list, client.InNamespace(template.Namespace), client.MatchingLabels{TemplateLabel: template.Name}); err != nil {
		return nil, err
	}
	revisions := slices.DeleteFunc(list.Items, func(revision appsv1.ControllerRevision) bool {
		return !metav1.IsControlledBy(&revision, template)
	})
	sort.Slice(revisions, func(i, j int) bool { return revisions[i].Revision > revisions[j].Revision })
	return revisions, nil
}

// create snapshots files in a new ControllerRevision, numbered after every revision the
// template had, so that numbers never go back even when old revisions were deleted.
func (r *Revisions) create(ctx context.Context, template *dinovav1alpha1.TerraformTemplate, files map[string]string, digest string, revisions []appsv1.ControllerRevision) (*appsv1.ControllerRevision, error) {
	number := template.Status.Revision
	if len(revisions) > 0 {
		number = max(number, revisions[0].Revision)
	}
	raw, err := json.Marshal(files)
	if err != nil {
		return nil, err
	}
	revision := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: template.Namespace, Name: revisionName(template.Name, digest),
			Labels:      map[string]string{TemplateLabel: template.Name},
			Annotations: map[string]string{DigestAnnotation: digest},
		},
		Data:     runtime.RawExtension{Raw: raw},
		Revision: number + 1,
	}
	if err := controllerutil.SetControllerReference(template, revision, r.Scheme); err != nil {
		return nil, err
	}
	err = r.Client.Create(ctx, revision)
	if !apierrors.IsAlreadyExists(err) {
		return revision, err
	}
	// The name comes from the digest. An existing revision of this template (one the cache did
	// not list yet) is this one; one of a deleted template with the same name and the same
	// files is removed by the garbage collector, and the next attempt creates it again.
	existing := &appsv1.ControllerRevision{}
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(revision), existing); err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(existing, template) {
		return nil, fmt.Errorf("revision %q belongs to a deleted TerraformTemplate %q and is not removed yet", existing.Name, template.Name)
	}
	return existing, nil
}

// prune deletes the oldest revisions beyond the history limit, never the current one.
func (r *Revisions) prune(ctx context.Context, template *dinovav1alpha1.TerraformTemplate, revisions []appsv1.ControllerRevision, current string) error {
	limit := int(template.Spec.RevisionHistoryLimit)
	if limit < 1 {
		limit = 10
	}
	kept := 1 // the current revision
	for i := range revisions {
		revision := &revisions[i]
		if revision.Name == current {
			continue
		}
		if kept < limit {
			kept++
			continue
		}
		if err := r.Client.Delete(ctx, revision); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func (r *Revisions) setReady(ctx context.Context, template *dinovav1alpha1.TerraformTemplate, status metav1.ConditionStatus, reason, message string) error {
	template.Status.ObservedGeneration = template.Generation
	meta.SetStatusCondition(&template.Status.Conditions, metav1.Condition{
		Type: conditionReady, Status: status, Reason: reason, Message: message, ObservedGeneration: template.Generation,
	})
	return r.Client.Status().Update(ctx, template)
}

// revisionName is "<template>-<first 10 hex digits of the digest>", short enough for any
// template name.
func revisionName(template, digest string) string {
	suffix := digest[len("sha256:"):][:10]
	if len(template) > 242 {
		template = template[:242]
	}
	return template + "-" + suffix
}
