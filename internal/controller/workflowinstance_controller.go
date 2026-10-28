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
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/instance"
)

// WorkflowInstanceReconciler reconciles a WorkflowInstance object: it runs its WorkflowTemplate
// as a new Workflow when what it runs changes, and every interval.
type WorkflowInstanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Now is the clock; time.Now when nil.
	Now func() time.Time
	// Recorder records Events about the instances; set by SetupWithManager.
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=dinova.one,resources=workflowinstances,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=dinova.one,resources=workflowinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups=dinova.one,resources=workflowinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dinova.one,resources=workflows,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=dinova.one,resources=workflowtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=dinova.one,resources=terraformtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=dinova.one,resources=apicalltemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile starts the runs of a WorkflowInstance and keeps its status.
func (r *WorkflowInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	helper := &instance.Handler{Client: r.Client, Scheme: r.Scheme, Now: r.Now, Recorder: r.Recorder}
	return helper.Reconcile(ctx, req)
}

// SetupWithManager sets up the controller with the Manager. Besides its runs, an instance
// watches what it runs: its WorkflowTemplate and, through it, the TerraformTemplates, whose
// status changes when the operator makes a new revision, and the ApiCallTemplates of its API
// steps. A change of the code therefore starts a run at once, without waiting for the interval.
func (r *WorkflowInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Recorder = mgr.GetEventRecorder("workflowinstance-controller")
	indexer := mgr.GetFieldIndexer()
	if err := indexer.IndexField(context.Background(), &dinovav1alpha1.WorkflowInstance{}, instance.TemplateIndex,
		func(object client.Object) []string {
			return []string{object.(*dinovav1alpha1.WorkflowInstance).Spec.TemplateRef.Name}
		}); err != nil {
		return err
	}
	if err := indexer.IndexField(context.Background(), &dinovav1alpha1.WorkflowTemplate{}, instance.TerraformTemplateIndex,
		func(object client.Object) []string {
			return instance.TerraformTemplateNames(object.(*dinovav1alpha1.WorkflowTemplate))
		}); err != nil {
		return err
	}
	if err := indexer.IndexField(context.Background(), &dinovav1alpha1.WorkflowTemplate{}, instance.APICallTemplateIndex,
		func(object client.Object) []string {
			return instance.APICallTemplateNames(object.(*dinovav1alpha1.WorkflowTemplate))
		}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&dinovav1alpha1.WorkflowInstance{}).
		Owns(&dinovav1alpha1.Workflow{}).
		Watches(&dinovav1alpha1.Workflow{}, handler.EnqueueRequestsFromMapFunc(destroyedInstance)).
		Watches(&dinovav1alpha1.WorkflowTemplate{}, handler.EnqueueRequestsFromMapFunc(r.instancesRunning)).
		Watches(&dinovav1alpha1.TerraformTemplate{}, handler.EnqueueRequestsFromMapFunc(r.instancesRunningTerraform)).
		Watches(&dinovav1alpha1.ApiCallTemplate{}, handler.EnqueueRequestsFromMapFunc(r.instancesSending)).
		Named("workflowinstance").
		Complete(r)
}

// instancesRunning returns the instances that run a WorkflowTemplate.
func (r *WorkflowInstanceReconciler) instancesRunning(ctx context.Context, template client.Object) []reconcile.Request {
	instances := &dinovav1alpha1.WorkflowInstanceList{}
	if err := r.List(ctx, instances, client.InNamespace(template.GetNamespace()), client.MatchingFields{instance.TemplateIndex: template.GetName()}); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(instances.Items))
	for _, item := range instances.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: item.Namespace, Name: item.Name}})
	}
	return requests
}

// instancesRunningTerraform returns the instances whose WorkflowTemplate runs a TerraformTemplate.
func (r *WorkflowInstanceReconciler) instancesRunningTerraform(ctx context.Context, terraformTemplate client.Object) []reconcile.Request {
	templates := &dinovav1alpha1.WorkflowTemplateList{}
	if err := r.List(ctx, templates, client.InNamespace(terraformTemplate.GetNamespace()), client.MatchingFields{instance.TerraformTemplateIndex: terraformTemplate.GetName()}); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range templates.Items {
		requests = append(requests, r.instancesRunning(ctx, &templates.Items[i])...)
	}
	return requests
}

// instancesSending returns the instances whose WorkflowTemplate sends an ApiCallTemplate.
func (r *WorkflowInstanceReconciler) instancesSending(ctx context.Context, request client.Object) []reconcile.Request {
	templates := &dinovav1alpha1.WorkflowTemplateList{}
	if err := r.List(ctx, templates, client.InNamespace(request.GetNamespace()), client.MatchingFields{instance.APICallTemplateIndex: request.GetName()}); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range templates.Items {
		requests = append(requests, r.instancesRunning(ctx, &templates.Items[i])...)
	}
	return requests
}

// destroyedInstance maps a Destroy Workflow to the deleted instance whose resources it destroys:
// it is not owned by the instance, so Owns does not see it.
func destroyedInstance(_ context.Context, object client.Object) []reconcile.Request {
	run, ok := object.(*dinovav1alpha1.Workflow)
	if !ok {
		return nil
	}
	name := instance.DestroyRunInstance(run)
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: run.Namespace, Name: name}}}
}
