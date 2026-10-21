package terraform

import (
	"errors"
	"fmt"
	"regexp"

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
