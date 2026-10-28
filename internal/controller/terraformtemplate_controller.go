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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

// TerraformTemplateReconciler reconciles a TerraformTemplate object: it snapshots its files
// in a new revision every time their content changes.
type TerraformTemplateReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader reads the ConfigMaps without the cache (see terraform.Revisions.Reader).
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=dinova.one,resources=terraformtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=dinova.one,resources=terraformtemplates/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;create;delete

// Reconcile makes the revision of the current files of a TerraformTemplate.
func (r *TerraformTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	revisions := &terraform.Revisions{Client: r.Client, Scheme: r.Scheme, Reader: r.APIReader}
	return revisions.Reconcile(ctx, req)
}

// SetupWithManager sets up the controller with the Manager. ConfigMaps are watched by their
// metadata only, so that the cache does not hold every ConfigMap of the cluster: a change of
// one reconciles the templates that read it.
func (r *TerraformTemplateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.APIReader = mgr.GetAPIReader()
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &dinovav1alpha1.TerraformTemplate{}, terraform.ConfigMapIndex,
		func(object client.Object) []string {
			return terraform.ConfigMapNames(object.(*dinovav1alpha1.TerraformTemplate))
		}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&dinovav1alpha1.TerraformTemplate{}).
		Owns(&appsv1.ControllerRevision{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.templatesReading), builder.OnlyMetadata).
		Named("terraformtemplate").
		Complete(r)
}

// templatesReading returns the TerraformTemplates that read a ConfigMap.
func (r *TerraformTemplateReconciler) templatesReading(ctx context.Context, configMap client.Object) []reconcile.Request {
	templates := &dinovav1alpha1.TerraformTemplateList{}
	if err := r.List(ctx, templates, client.InNamespace(configMap.GetNamespace()), client.MatchingFields{terraform.ConfigMapIndex: configMap.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(templates.Items))
	for _, template := range templates.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: template.Namespace, Name: template.Name}})
	}
	return requests
}
