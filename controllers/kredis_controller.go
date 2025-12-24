/*
Copyright 2025.

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

package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/controllers/cluster"
	"github.com/hkpark130/kredis-operator/controllers/resource"
)

// KredisReconciler reconciles a Kredis object
type KredisReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	ClusterManager *cluster.ClusterManager
}

//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop
func (r *KredisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Kredis instance
	kredis := &cachev1alpha1.Kredis{}
	err := r.Get(ctx, req.NamespacedName, kredis)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Kredis resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Kredis")
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling Kredis",
		"name", kredis.Name,
		"masters", kredis.Spec.Masters,
		"replicas", kredis.Spec.Replicas,
		"totalNodes", kredis.Spec.Masters+(kredis.Spec.Masters*kredis.Spec.Replicas))

	// 1. Create headless service
	if err := r.reconcileService(ctx, kredis); err != nil {
		logger.Error(err, "Failed to reconcile service")
		return ctrl.Result{}, err
	}

	// 2. Create unified StatefulSet
	if err := r.reconcileStatefulSet(ctx, kredis); err != nil {
		logger.Error(err, "Failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	// 3. Manage Redis cluster state
	clusterReady, delta, err := r.ClusterManager.ReconcileCluster(ctx, kredis)
	if err != nil {
		// 오류가 발생해도 delta에 담긴 진행중/실패 상태를 우선 반영하여 중복 작업을 방지
		if delta != nil {
			if uerr := r.updateStatus(ctx, kredis, delta); uerr != nil {
				logger.Error(uerr, "Failed to update status after reconcile error")
			}
		}
		logger.Error(err, "Failed to reconcile Redis cluster")
		return ctrl.Result{RequeueAfter: time.Minute}, err // 클러스터 조정 실패 시 1분 후 재시도
	}

	// 4. Update status
	if err := r.updateStatus(ctx, kredis, delta); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{RequeueAfter: time.Second * 10}, err
	}

	// Adjust reconcile interval based on cluster state and in-progress
	inProgress := false
	if delta != nil && strings.Contains(delta.LastClusterOperation, "-in-progress") {
		inProgress = true
	} else if strings.Contains(kredis.Status.LastClusterOperation, "-in-progress") {
		inProgress = true
	}

	if clusterReady {
		// Cluster is healthy - check less frequently
		logger.V(1).Info("Cluster is healthy - scheduling next check in 10 minutes")
		return ctrl.Result{RequeueAfter: time.Minute * 10}, nil
	} else if inProgress {
		// While operations are in progress, poll more frequently
		logger.V(1).Info("Operation in progress - scheduling next check in 10 seconds")
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	} else {
		// Cluster needs attention - check more frequently
		logger.V(1).Info("Cluster needs attention - scheduling next check in 2 minutes")
		return ctrl.Result{RequeueAfter: time.Minute * 2}, nil
	}
}

func (r *KredisReconciler) reconcileService(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	foundService := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: kredis.Name, Namespace: kredis.Namespace}, foundService)

	if err != nil && errors.IsNotFound(err) {
		svc := resource.CreateRedisService(kredis, r.Scheme)
		logger.Info("Creating Redis headless service", "name", svc.Name)
		return r.Create(ctx, svc)
	}

	return err
}

func (r *KredisReconciler) reconcileStatefulSet(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	foundStatefulSet := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: kredis.Name, Namespace: kredis.Namespace}, foundStatefulSet)

	expectedReplicas := kredis.Spec.Masters + (kredis.Spec.Masters * kredis.Spec.Replicas)

	if err != nil && errors.IsNotFound(err) {
		// Create new StatefulSet
		sts := resource.CreateRedisStatefulSet(kredis, r.Scheme)
		logger.Info("Creating unified Redis StatefulSet",
			"name", sts.Name,
			"totalReplicas", expectedReplicas,
			"masters", kredis.Spec.Masters,
			"replicasPerMaster", kredis.Spec.Replicas)
		return r.Create(ctx, sts)
	} else if err != nil {
		return err
	}

	// Check if update needed
	if *foundStatefulSet.Spec.Replicas != expectedReplicas {
		logger.Info("Updating StatefulSet replicas",
			"current", *foundStatefulSet.Spec.Replicas,
			"expected", expectedReplicas)

		foundStatefulSet.Spec.Replicas = &expectedReplicas

		return r.Update(ctx, foundStatefulSet)
	}

	return nil
}

func (r *KredisReconciler) updateStatus(ctx context.Context, kredis *cachev1alpha1.Kredis, delta *cluster.ClusterStatusDelta) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get the latest version of Kredis
		currentKredis := &cachev1alpha1.Kredis{}
		err := r.Get(ctx, types.NamespacedName{Name: kredis.Name, Namespace: kredis.Namespace}, currentKredis)
		if err != nil {
			return err
		}

		// Calculate the new status
		foundStatefulSet := &appsv1.StatefulSet{}
		err = r.Get(ctx, types.NamespacedName{Name: kredis.Name, Namespace: kredis.Namespace}, foundStatefulSet)
		if err != nil {
			return err
		}

		// Merge delta from ClusterManager first
		if delta != nil {
			if delta.LastClusterOperation != "" {
				currentKredis.Status.LastClusterOperation = delta.LastClusterOperation
			}
			if delta.KnownClusterNodes != nil {
				currentKredis.Status.KnownClusterNodes = *delta.KnownClusterNodes
			}
			if len(delta.JoinedPods) > 0 {
				currentKredis.Status.JoinedPods = mergeUnique(currentKredis.Status.JoinedPods, delta.JoinedPods)
			}
			if len(delta.ClusterNodes) > 0 {
				currentKredis.Status.ClusterNodes = delta.ClusterNodes
			}
			if delta.ClusterState != "" {
				currentKredis.Status.ClusterState = delta.ClusterState
			}
		}

		newStatus := r.calculateStatus(currentKredis, foundStatefulSet)

		// If the status has not changed, do nothing
		if reflect.DeepEqual(currentKredis.Status, newStatus) {
			return nil
		}

		currentKredis.Status = newStatus
		return r.Status().Update(ctx, currentKredis)
	})
}

func (r *KredisReconciler) calculateStatus(kredis *cachev1alpha1.Kredis, sts *appsv1.StatefulSet) cachev1alpha1.KredisStatus {
	newStatus := kredis.Status.DeepCopy()

	newStatus.Replicas = sts.Status.Replicas
	newStatus.ReadyReplicas = sts.Status.ReadyReplicas

	expectedReplicas := kredis.Spec.Masters + (kredis.Spec.Masters * kredis.Spec.Replicas)

	if sts.Status.ReadyReplicas == expectedReplicas {
		newStatus.Phase = "Ready"
	} else if sts.Status.Replicas > 0 {
		newStatus.Phase = "Pending"
	} else {
		newStatus.Phase = "Creating"
	}

	// Update condition
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "NotReady",
		Message:            fmt.Sprintf("Ready replicas: %d/%d", newStatus.ReadyReplicas, expectedReplicas),
		LastTransitionTime: metav1.Now(),
	}

	if newStatus.ReadyReplicas == expectedReplicas && expectedReplicas > 0 {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "AllReady"
		condition.Message = "All Redis nodes are ready"
	}

	newStatus.Conditions = []metav1.Condition{condition}

	return *newStatus
}

// mergeUnique returns a union of two string slices, preserving existing order where possible.
func mergeUnique(base []string, add []string) []string {
	set := map[string]struct{}{}
	out := make([]string, 0, len(base)+len(add))
	for _, v := range base {
		if _, ok := set[v]; !ok {
			set[v] = struct{}{}
			out = append(out, v)
		}
	}
	for _, v := range add {
		if _, ok := set[v]; !ok {
			set[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

// SetupWithManager sets up the controller with the Manager.
func (r *KredisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.Kredis{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}
