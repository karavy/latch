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

package controller

import (
	"context"
	"os"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
	"github.com/karavy/latch/internal/controller/workflow"
)

// WorkflowReconciler reconciles a Workflow object
type WorkflowReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	TerraformRunnerImage string
	APIReader            client.Reader
	TerraformState       terraform.StateConfig
	// Recorder records Events about the Workflows; set by SetupWithManager.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=dinova.one,resources=workflows,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dinova.one,resources=workflows/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dinova.one,resources=workflows/finalizers,verbs=update
// +kubebuilder:rbac:groups=dinova.one,resources=apicalls,verbs=get;create
// +kubebuilder:rbac:groups=dinova.one,resources=apicalltemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=dinova.one,resources=workflowtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;create
// +kubebuilder:rbac:groups=dinova.one,resources=terraformtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;create;update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Workflow object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *WorkflowReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	helper := &workflow.Handler{
		Client: r.Client, Scheme: r.Scheme, TerraformRunnerImage: r.TerraformRunnerImage, APIReader: r.APIReader, TerraformState: r.TerraformState,
		Recorder: r.Recorder,
	}
	return helper.Reconcile(ctx, req)
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkflowReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	r.Recorder = mgr.GetEventRecorder("workflow-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&dinovav1alpha1.Workflow{}).
		Owns(&dinovav1alpha1.ApiCall{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Named("workflow").
		Complete(r)
}

// TerraformRunnerImage returns the configured Terraform Job image or its local-development default.
func TerraformRunnerImage() string {
	if image := os.Getenv("TERRAFORM_RUNNER_IMAGE"); image != "" {
		return image
	}
	return "latch-terraform-runner:dev"
}
