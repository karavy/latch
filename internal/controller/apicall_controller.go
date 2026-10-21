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
	"errors"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/apicall"
)

// ApiCallReconciler reconciles a ApiCall object
type ApiCallReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	APIReader      client.Reader
	Executor       *apicall.APIExecutor
	AllowedOrigins []string
	// AllowedOriginsConfigMap names a ConfigMap whose "origins" key adds to AllowedOrigins.
	AllowedOriginsConfigMap types.NamespacedName
}

// +kubebuilder:rbac:groups=dinova.one,resources=apicalls,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dinova.one,resources=apicalls/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dinova.one,resources=apicalls/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get

// Reconcile delegates the state machine to the ApiCall helpers. HTTP waits live
// in the manager-owned executor, never in a reconciliation worker.
func (r *ApiCallReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	helper := &apicall.Handler{
		Client: r.Client, APIReader: r.APIReader,
		Executor: r.Executor, AllowedOrigins: r.AllowedOrigins, OriginsConfigMap: r.AllowedOriginsConfigMap,
	}
	return helper.Reconcile(ctx, req)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ApiCallReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Executor == nil {
		return errors.New("configure the API executor before setting up the controller")
	}
	if err := apicall.ValidateAPIOrigins(r.AllowedOrigins); err != nil {
		return err
	}
	r.APIReader = mgr.GetAPIReader()
	if err := mgr.Add(r.Executor); err != nil {
		return err
	}
	// Ignore our own status updates to avoid feedback loops, but reconcile deletion
	// timestamps even when another controller holds a finalizer. Worker events are
	// an independent source and are not filtered by this predicate.
	changes := predicate.Or(predicate.GenerationChangedPredicate{}, predicate.Funcs{
		UpdateFunc: func(update event.UpdateEvent) bool {
			return update.ObjectOld.GetDeletionTimestamp().IsZero() && !update.ObjectNew.GetDeletionTimestamp().IsZero()
		},
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&dinovav1alpha1.ApiCall{}, builder.WithPredicates(changes)).
		WatchesRawSource(source.Channel(r.Executor.Events(), &handler.EnqueueRequestForObject{})).
		Named("apicall").
		Complete(r)
}
