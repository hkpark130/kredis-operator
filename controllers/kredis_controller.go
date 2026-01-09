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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/controllers/cluster"
	"github.com/hkpark130/kredis-operator/controllers/resource"
)

const (
	// kredisFinalizer is the finalizer name for Kredis resources
	kredisFinalizer = "cache.docker.direa.synology.me/finalizer"
)

// KredisReconciler reconciles a Kredis object
type KredisReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	ClusterManager *cluster.ClusterManager
	Autoscaler     *cluster.Autoscaler
}

//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods/exec,verbs=create
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list;watch

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

	// Handle finalizer for cleanup on deletion
	if kredis.ObjectMeta.DeletionTimestamp.IsZero() {
		// Resource is not being deleted - ensure finalizer is present
		if !controllerutil.ContainsFinalizer(kredis, kredisFinalizer) {
			controllerutil.AddFinalizer(kredis, kredisFinalizer)
			if err := r.Update(ctx, kredis); err != nil {
				logger.Error(err, "Failed to add finalizer")
				return ctrl.Result{}, err
			}
			logger.Info("Added finalizer to Kredis resource")
		}
	} else {
		// Resource is being deleted - run cleanup
		if controllerutil.ContainsFinalizer(kredis, kredisFinalizer) {
			logger.Info("Kredis resource is being deleted, running cleanup")

			// Cleanup: Delete all Jobs associated with this Kredis instance
			if err := r.cleanupKredisResources(ctx, kredis); err != nil {
				logger.Error(err, "Failed to cleanup Kredis resources")
				// Don't block deletion, just log the error
			}

			// Remove finalizer to allow deletion
			controllerutil.RemoveFinalizer(kredis, kredisFinalizer)
			if err := r.Update(ctx, kredis); err != nil {
				logger.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
			logger.Info("Removed finalizer, Kredis resource will be deleted")
		}
		return ctrl.Result{}, nil
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

	// 4. Evaluate autoscaling if cluster is stable
	if clusterReady && r.Autoscaler != nil && kredis.Spec.Autoscaling.Enabled {
		pods, err := r.getKredisPods(ctx, kredis)
		if err == nil {
			result, err := r.Autoscaler.EvaluateAutoscaling(ctx, kredis, pods)
			if err != nil {
				logger.Error(err, "Failed to evaluate autoscaling")
			} else {
				// Update metrics in status
				if delta == nil {
					delta = &cluster.ClusterStatusDelta{}
				}
				// Store current metrics in delta for status update
				kredis.Status.CurrentMemoryUsagePercent = result.MemoryUsagePercent
				kredis.Status.CurrentCPUUsagePercent = result.CPUUsagePercent

				if result.Decision.ShouldScale {
					logger.Info("Autoscaling triggered",
						"scaleType", result.Decision.ScaleType,
						"reason", result.Decision.Reason)
					if err := r.Autoscaler.ApplyAutoscaling(ctx, kredis, result.Decision); err != nil {
						logger.Error(err, "Failed to apply autoscaling")
					} else {
						// Re-fetch kredis after spec change to avoid conflicts
						return ctrl.Result{RequeueAfter: time.Second * 5}, nil
					}
				}
			}
		}
	}

	// 5. Update status
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
		logger.V(1).Info("Operation in progress - scheduling next check in 5 seconds")
		return ctrl.Result{RequeueAfter: time.Second * 5}, nil
	} else {
		// Cluster needs attention - check more frequently
		logger.V(1).Info("Cluster needs attention - scheduling next check in 15 seconds")
		return ctrl.Result{RequeueAfter: time.Second * 15}, nil
	}
}

func (r *KredisReconciler) reconcileService(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	// Master 서비스 생성 (쓰기용)
	masterSvcName := kredis.Name + "-master"
	foundMasterSvc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: masterSvcName, Namespace: kredis.Namespace}, foundMasterSvc)
	if err != nil && errors.IsNotFound(err) {
		svc := resource.CreateRedisMasterService(kredis, r.Scheme)
		logger.Info("Creating Redis master service", "name", svc.Name)
		if err := r.Create(ctx, svc); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// Slave 서비스 생성 (읽기용)
	slaveSvcName := kredis.Name + "-slave"
	foundSlaveSvc := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: slaveSvcName, Namespace: kredis.Namespace}, foundSlaveSvc)
	if err != nil && errors.IsNotFound(err) {
		svc := resource.CreateRedisSlaveService(kredis, r.Scheme)
		logger.Info("Creating Redis slave service", "name", svc.Name)
		if err := r.Create(ctx, svc); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return nil
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

	currentReplicas := *foundStatefulSet.Spec.Replicas

	// Check if update needed
	if currentReplicas != expectedReplicas {
		// IMPORTANT: Scale-down safety check
		// Do NOT shrink StatefulSet immediately during scale-down!
		// Scale-down requires:
		// 1. Migrate slots from masters being removed (reshard)
		// 2. Execute CLUSTER FORGET on all remaining nodes
		// 3. Only then shrink StatefulSet
		if currentReplicas > expectedReplicas {
			// This is a scale-down operation
			logger.Info("Scale-down detected - StatefulSet shrink is BLOCKED until cluster operations complete",
				"currentReplicas", currentReplicas,
				"desiredReplicas", expectedReplicas,
				"scaleDownReady", kredis.Status.ScaleDownReady,
				"pendingScaleDown", len(kredis.Status.PendingScaleDown))

			// Only allow shrinking if ScaleDownReady is true
			// This flag is set by scaleDownCluster after all nodes are safely removed
			if !kredis.Status.ScaleDownReady {
				logger.Info("Blocking StatefulSet shrink - scale-down operations not complete")
				// Store the desired replicas in status for the controller to use later
				if kredis.Status.DesiredStatefulSetReplicas == nil || *kredis.Status.DesiredStatefulSetReplicas != expectedReplicas {
					// Update status to record desired replicas
					// This will be done via updateStatus later in the reconcile loop
					logger.V(1).Info("Recording desired StatefulSet replicas for after scale-down",
						"desiredReplicas", expectedReplicas)
				}
				return nil // Block the update
			}

			// ScaleDownReady is true - safe to shrink
			logger.Info("Scale-down complete - now shrinking StatefulSet",
				"from", currentReplicas,
				"to", expectedReplicas)

			foundStatefulSet.Spec.Replicas = &expectedReplicas
			if err := r.Update(ctx, foundStatefulSet); err != nil {
				return err
			}

			// Reset ScaleDownReady flag after successful shrink
			// This is done via a status update
			return r.resetScaleDownStatus(ctx, kredis)
		}

		// Scale-up: Safe to update immediately
		logger.Info("Updating StatefulSet replicas (scale-up)",
			"current", currentReplicas,
			"expected", expectedReplicas)

		foundStatefulSet.Spec.Replicas = &expectedReplicas
		return r.Update(ctx, foundStatefulSet)
	}

	return nil
}

// resetScaleDownStatus clears the scale-down status flags after StatefulSet shrink
func (r *KredisReconciler) resetScaleDownStatus(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get latest version
		currentKredis := &cachev1alpha1.Kredis{}
		if err := r.Get(ctx, types.NamespacedName{Name: kredis.Name, Namespace: kredis.Namespace}, currentKredis); err != nil {
			return err
		}

		// Reset scale-down flags
		currentKredis.Status.ScaleDownReady = false
		currentKredis.Status.PendingScaleDown = nil
		currentKredis.Status.DesiredStatefulSetReplicas = nil

		logger.Info("Resetting scale-down status flags after successful StatefulSet shrink")
		return r.Status().Update(ctx, currentKredis)
	})
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
			if len(delta.ClusterNodes) > 0 {
				currentKredis.Status.ClusterNodes = delta.ClusterNodes
			}
			if delta.ClusterState != "" {
				currentKredis.Status.ClusterState = delta.ClusterState
			}
			// Scale-down related fields
			if delta.PendingScaleDown != nil {
				currentKredis.Status.PendingScaleDown = delta.PendingScaleDown
			}
			if delta.ScaleDownReady != nil {
				currentKredis.Status.ScaleDownReady = *delta.ScaleDownReady
			}
			if delta.DesiredStatefulSetReplicas != nil {
				currentKredis.Status.DesiredStatefulSetReplicas = delta.DesiredStatefulSetReplicas
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

	// Determine ClusterState based on current situation
	// Priority: Scale-down > Scaling detection > delta state > success state

	// 1. Check if scale-down is in progress
	isScaleDownInProgress := strings.Contains(kredis.Status.LastClusterOperation, "scaledown-") ||
		len(kredis.Status.PendingScaleDown) > 0

	// 2. Check if scaling is in progress (StatefulSet replicas != expected)
	// This catches the case where Spec changed but pods aren't ready yet
	// But exclude scale-down case (where we intentionally block StatefulSet shrink)
	isScaling := (sts.Status.Replicas != expectedReplicas || sts.Status.ReadyReplicas != expectedReplicas) && !isScaleDownInProgress

	// 3. Check if cluster operations are in progress
	isOperationInProgress := strings.Contains(kredis.Status.LastClusterOperation, "-in-progress") ||
		strings.Contains(kredis.Status.LastClusterOperation, "-pending") ||
		strings.Contains(kredis.Status.LastClusterOperation, "-needed")

	// 4. Determine ClusterState
	if newStatus.ClusterState == "" {
		// Initial state - no cluster operations have run yet
		newStatus.ClusterState = string(cachev1alpha1.ClusterStateCreating)
	} else if isScaleDownInProgress {
		// Scale-down takes precedence
		newStatus.ClusterState = string(cachev1alpha1.ClusterStateScalingDown)
	} else if isScaling && newStatus.ClusterState == string(cachev1alpha1.ClusterStateRunning) {
		// Was Running but now scaling (spec changed, waiting for pods)
		newStatus.ClusterState = string(cachev1alpha1.ClusterStateScaling)
	} else if isOperationInProgress {
		// Keep the current state if an operation is in progress
		// (delta should have set this, don't override)
	} else if strings.Contains(kredis.Status.LastClusterOperation, "-success") {
		// If last operation was successful and all pods are ready, show Running
		if sts.Status.ReadyReplicas == expectedReplicas && !isScaling {
			newStatus.ClusterState = string(cachev1alpha1.ClusterStateRunning)
		}
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

// getKredisPods returns all pods belonging to this Kredis instance
func (r *KredisReconciler) getKredisPods(ctx context.Context, kredis *cachev1alpha1.Kredis) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(kredis.Namespace),
		client.MatchingLabels{
			"app":                        "kredis",
			"app.kubernetes.io/instance": kredis.Name,
		},
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// cleanupKredisResources cleans up all resources associated with a Kredis instance during deletion.
// This includes Jobs, and any other resources that might be orphaned.
func (r *KredisReconciler) cleanupKredisResources(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	// Cleanup Jobs
	if err := r.ClusterManager.JobManager.CleanupCompletedJobs(ctx, kredis); err != nil {
		logger.Error(err, "Failed to cleanup Jobs during deletion")
		// Continue with other cleanup
	}

	// Delete all Jobs (including running ones) for this Kredis instance
	jobList := &batchv1.JobList{}
	listOpts := []client.ListOption{
		client.InNamespace(kredis.Namespace),
		client.MatchingLabels{
			resource.LabelKredisName: kredis.Name,
		},
	}

	if err := r.List(ctx, jobList, listOpts...); err != nil {
		logger.Error(err, "Failed to list Jobs for cleanup")
	} else {
		for _, job := range jobList.Items {
			deletePolicy := metav1.DeletePropagationBackground
			if err := r.Delete(ctx, &job, &client.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete Job", "job", job.Name)
			} else {
				logger.Info("Deleted Job during cleanup", "job", job.Name)
			}
		}
	}

	logger.Info("Kredis resource cleanup completed")
	return nil
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
