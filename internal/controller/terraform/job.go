package terraform

import (
	"cmp"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	tfengine "github.com/karavy/terraform-engine"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// stepName mirrors the CRD pattern of WorkflowStep.Name.
var stepName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const (
	// VariablesKey is the key, in the input Secret, of the generated variables file.
	VariablesKey = "terraform.auto.tfvars.json"

	workspaceDir = "/workspace"
	templateDir  = "/templates"
	inputDir     = "/input"
	runnerUser   = int64(65532)
	jobTTL       = int32(86400)
	// stopGraceSeconds is how long a runner pod has after SIGTERM: OpenTofu, interrupted,
	// finishes the requests in flight for up to 60 seconds, then the runner records what it may
	// have created without saving it.
	stopGraceSeconds = 120
	// stopMargin is how long before the Job's deadline the runner stops OpenTofu by itself.
	stopMargin = 120

	// DefaultTimeoutSeconds bounds the Job when a step does not set its own timeout.
	DefaultTimeoutSeconds = int32(1800)
)

// JobConfig describes the Job that runs terraform-engine for one step.
type JobConfig struct {
	Namespace, Name string
	// StepName names the step's own folder in the workspace. Step names are DNS labels,
	// so they are always safe path components.
	StepName string
	// RunnerImage is the image of cmd/terraform-runner.
	RunnerImage string
	// WorkspacePVC holds the Terraform working files and state, kept across runs.
	WorkspacePVC string
	// TemplateConfigMap holds the files of the step's TerraformTemplate.
	TemplateConfigMap string
	// InputSecret holds the generated terraform.auto.tfvars.json.
	InputSecret string
	// TimeoutSeconds is the Job's active deadline; zero means DefaultTimeoutSeconds.
	TimeoutSeconds int32
	// State is where the runner keeps the Terraform state, as StateBackend describes it.
	State tfengine.S3Backend
	// EnvFromSecrets are exposed as environment variables, typically provider and storage credentials.
	EnvFromSecrets []string
	// Labels are set on the Job.
	Labels map[string]string
	// Mode is what the runner does: ModeApply (the default), ModePlan, ModeApplyPlan or
	// ModeDestroy.
	Mode string
	// PlanKey is the object key of the saved plan, for ModePlan and ModeApplyPlan.
	PlanKey string
	// RenderedPrefix is where the runner stores the files it runs, see RenderedPrefix. Empty
	// stores nothing.
	RenderedPrefix string
	// RenderedMetadata is a JSON object that the runner copies into the manifest.json it
	// stores with the files, to say which run they belong to.
	RenderedMetadata string
}

// The modes of the runner (TF_MODE), see JobConfig.Mode.
const (
	ModeApply     = "apply"
	ModePlan      = "plan"
	ModeApplyPlan = "apply-plan"
	ModeDestroy   = "destroy"
)

// PlanKey is the object key of the saved plan of a step of a Plan workflow, next to the
// states: "<namespace>/plans/<workflow>-<uid>/<template>/<step>.tfplan". The readable plan
// is at the same key plus ".txt", its summary plus ".json".
func PlanKey(namespace, workflow, uid, template, step string) string {
	return fmt.Sprintf("%s%s/%s.tfplan", PlanPrefix(namespace, workflow, uid), template, step)
}

// PlanPrefix is the prefix of the keys of every plan a Plan workflow saved.
func PlanPrefix(namespace, workflow, uid string) string {
	return fmt.Sprintf("%s/plans/%s-%s/", namespace, workflow, uid)
}

// RenderedPrefix is the prefix of the object keys under which the Job of a step stores the
// files it ran, as rendered from the TerraformTemplate, with a manifest.json: next to the
// states and the plans, "<namespace>/rendered/<workflow>-<uid>/<template>/<step>/".
func RenderedPrefix(namespace, workflow, uid, template, step string) string {
	return fmt.Sprintf("%s/rendered/%s-%s/%s/%s/", namespace, workflow, uid, template, step)
}

// stopAfter is when the runner stops OpenTofu by itself (TF_STOP_AFTER_SECONDS): before the
// Job's deadline, by stopMargin or a quarter of the timeout, so that OpenTofu has the time to
// finish, save the state and release the lock instead of being killed.
func stopAfter(timeout int32) int32 {
	return timeout - min(stopMargin, timeout/4)
}

// NewJob builds a Job without retries, with a read-only root filesystem and no
// service account token: the runner only needs the mounted files and the network.
func NewJob(cfg JobConfig) (*batchv1.Job, error) {
	if cfg.RunnerImage == "" {
		return nil, errors.New("runner image for Terraform is not configured")
	}
	if !stepName.MatchString(cfg.StepName) {
		return nil, errors.New("step name for Terraform must be a DNS label")
	}
	if err := cfg.State.Validate(); err != nil {
		return nil, fmt.Errorf("terraform state backend: %w", err)
	}
	if (cfg.Mode == ModePlan || cfg.Mode == ModeApplyPlan) && cfg.PlanKey == "" {
		return nil, errors.New("a Terraform plan needs a key to be saved at")
	}
	timeout := cfg.TimeoutSeconds
	if timeout == 0 {
		timeout = DefaultTimeoutSeconds
	}
	deadline, backoff, fsGroup := int64(timeout), int32(0), runnerUser
	container := corev1.Container{
		Name: "terraform", Image: cfg.RunnerImage, ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			{Name: "WORKSPACE_DIR", Value: workspaceDir + "/" + cfg.StepName},
			{Name: "TEMPLATE_DIR", Value: templateDir},
			{Name: "VARIABLES_FILE", Value: inputDir + "/" + VariablesKey},
			{Name: "TF_STATE_ENDPOINT", Value: cfg.State.Endpoint},
			{Name: "TF_STATE_BUCKET", Value: cfg.State.Bucket},
			{Name: "TF_STATE_REGION", Value: cfg.State.Region},
			{Name: "TF_STATE_KEY", Value: cfg.State.Key},
			{Name: "TF_IN_AUTOMATION", Value: "true"},
			{Name: "TF_MODE", Value: cmp.Or(cfg.Mode, ModeApply)},
			{Name: "TF_PLAN_KEY", Value: cfg.PlanKey},
			{Name: "TF_RENDERED_PREFIX", Value: cfg.RenderedPrefix},
			{Name: "TF_RENDERED_METADATA", Value: cfg.RenderedMetadata},
			{Name: "TF_STOP_AFTER_SECONDS", Value: strconv.Itoa(int(stopAfter(timeout)))},
		},
		EnvFrom: []corev1.EnvFromSource{},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "workspace", MountPath: workspaceDir},
			{Name: "templates", MountPath: templateDir, ReadOnly: true},
			{Name: "input", MountPath: inputDir, ReadOnly: true},
			{Name: "tmp", MountPath: "/tmp"},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false), ReadOnlyRootFilesystem: new(true),
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	for _, secretName := range cfg.EnvFromSecrets {
		container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: secretName}}})
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: cfg.Namespace, Name: cfg.Name, Labels: cfg.Labels},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, ActiveDeadlineSeconds: &deadline, TTLSecondsAfterFinished: new(jobTTL),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: new(false),
					TerminationGracePeriodSeconds: new(int64(stopGraceSeconds)),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: new(true), RunAsUser: new(runnerUser), RunAsGroup: new(runnerUser), FSGroup: &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{container},
					Volumes: []corev1.Volume{
						{Name: "workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: cfg.WorkspacePVC}}},
						{Name: "templates", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cfg.TemplateConfigMap}}}},
						{Name: "input", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cfg.InputSecret}}},
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}, nil
}
