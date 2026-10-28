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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/pipeline"
)

// WorkflowPipelineReconciler reconciles a WorkflowPipeline object: it creates the
// WorkflowInstances and Workflows of its stages in the order of their dependencies.
type WorkflowPipelineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=dinova.one,resources=workflowpipelines,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=dinova.one,resources=workflowpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dinova.one,resources=workflowpipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups=dinova.one,resources=workflowinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dinova.one,resources=workflows,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=dinova.one,resources=workflowtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=dinova.one,resources=apicalltemplates,verbs=get;list;watch

// Reconcile starts the stages of a WorkflowPipeline and keeps its status.
func (r *WorkflowPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	helper := &pipeline.Handler{Client: r.Client, Scheme: r.Scheme}
	return helper.Reconcile(ctx, req)
}

// SetupWithManager sets up the controller with the Manager. A pipeline watches the objects of
// its stages, whose status says when the next stages can start, and the WorkflowTemplates they
// run, which decide the parameters a stage takes.
func (r *WorkflowPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &dinovav1alpha1.WorkflowPipeline{}, pipeline.TemplateIndex,
		func(object client.Object) []string {
			return pipeline.TemplateNames(object.(*dinovav1alpha1.WorkflowPipeline))
		}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&dinovav1alpha1.WorkflowPipeline{}).
		Owns(&dinovav1alpha1.WorkflowInstance{}).
		Owns(&dinovav1alpha1.Workflow{}).
		Watches(&dinovav1alpha1.WorkflowTemplate{}, handler.EnqueueRequestsFromMapFunc(r.pipelinesRunning)).
		Named("workflowpipeline").
		Complete(r)
}

// pipelinesRunning returns the pipelines with a stage that runs a WorkflowTemplate.
func (r *WorkflowPipelineReconciler) pipelinesRunning(ctx context.Context, template client.Object) []reconcile.Request {
	pipelines := &dinovav1alpha1.WorkflowPipelineList{}
	if err := r.List(ctx, pipelines, client.InNamespace(template.GetNamespace()), client.MatchingFields{pipeline.TemplateIndex: template.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(pipelines.Items))
	for _, item := range pipelines.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: item.Namespace, Name: item.Name}})
	}
	return requests
}
