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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	tfengine "github.com/karavy/terraform-engine"
)

// BaseClusterReconciler reconciles a BaseCluster object
type BaseClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIClient is initialized once at startup and reused across reconciliations.
	APIClient *apiengine.Client
}

// +kubebuilder:rbac:groups=dinova.one,resources=baseclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dinova.one,resources=baseclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dinova.one,resources=baseclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the BaseCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *BaseClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	// TODO(user): your logic here

	return ctrl.Result{}, nil
}

// ApplyTerraformRule delegates template application to the shared Terraform engine.
func ApplyTerraformRule(ruleName, resourceName string, spec any, baseFile string) error {
	return tfengine.ApplyOperatorRule(ruleName, resourceName, spec, baseFile)
}

// ApplyAPIRequest delegates GET, PUT, POST and DELETE to the shared API engine.
// Callers must authorize endpoints and supply credentials securely before calling.
// HTTP error statuses are preserved; callers decide how to handle them, and must
// not treat a partial response returned alongside an error as a complete result.
func (r *BaseClusterReconciler) ApplyAPIRequest(ctx context.Context, request apiengine.Request) (*apiengine.Response, error) {
	return r.APIClient.Do(ctx, request)
}

// SetupWithManager sets up the controller with the Manager.
func (r *BaseClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dinovav1alpha1.BaseCluster{}).
		Named("basecluster").
		Complete(r)
}
