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

			// Cleanup: Delete all resources associated with this Kredis instance
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

	totalNodes := kredis.Spec.Masters + (kredis.Spec.Masters * kredis.Spec.Replicas)
	logger.Info("Reconciling Kredis",
		"name", kredis.Name,
		"masters", kredis.Spec.Masters,
		"replicas", kredis.Spec.Replicas,
		"totalNodes", totalNodes)

	// 1. Create services (headless for DNS, master/slave for routing)
	if err := r.reconcileServices(ctx, kredis); err != nil {
		logger.Error(err, "Failed to reconcile services")
		return ctrl.Result{}, err
	}

	// 2. Reconcile Pods and PVCs (direct pod management instead of StatefulSet)
	if err := r.reconcilePods(ctx, kredis); err != nil {
		logger.Error(err, "Failed to reconcile pods")
		return ctrl.Result{RequeueAfter: time.Second * 10}, err
	}

	// 3. Delete pods that are ready for deletion (after scale-down)
	if err := r.deleteMarkedPods(ctx, kredis); err != nil {
		logger.Error(err, "Failed to delete marked pods")
		// Continue - don't block reconciliation
	}

	// 4. Manage Redis cluster state
	clusterReady, delta, err := r.ClusterManager.ReconcileCluster(ctx, kredis)
	if err != nil {
		// 오류가 발생해도 delta에 담긴 진행중/실패 상태를 우선 반영
		if delta != nil {
			if uerr := r.updateStatus(ctx, kredis, delta); uerr != nil {
				logger.Error(uerr, "Failed to update status after reconcile error")
			}
		}
		logger.Error(err, "Failed to reconcile Redis cluster")
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	// #region agent log
	{
		// Autoscaling gate visibility: helps debug why autoscaling doesn't trigger under load.
		// We log only when autoscaling is enabled to keep volume reasonable.
		if kredis.Spec.Autoscaling.Enabled {
			logger.Info("agent_debug",
				"sessionId", "debug-session",
				"runId", "run1",
				"hypothesisId", "A1",
				"location", "controllers/kredis_controller.go:Reconcile",
				"message", "autoscaling gate check",
				"data", map[string]interface{}{
					"clusterReady":       clusterReady,
					"autoscalerNil":      r.Autoscaler == nil,
					"statusClusterState": kredis.Status.ClusterState,
					"statusLastOp":       kredis.Status.LastClusterOperation,
					"lastScaleType":      kredis.Status.LastScaleType,
					"readyReplicas":      kredis.Status.ReadyReplicas,
					"replicas":           kredis.Status.Replicas,
					"specMasters":        kredis.Spec.Masters,
					"specReplicas":       kredis.Spec.Replicas,
				},
				"timestamp", time.Now().UnixMilli(),
			)
		}
	}
	// #endregion

	// 4. Evaluate autoscaling if cluster is stable
	if clusterReady && r.Autoscaler != nil && kredis.Spec.Autoscaling.Enabled {
		pods, err := r.getKredisPods(ctx, kredis)
		if err == nil {
			result, err := r.Autoscaler.EvaluateAutoscaling(ctx, kredis, pods)
			if err != nil {
				logger.Error(err, "Failed to evaluate autoscaling")
			} else {
				if delta == nil {
					delta = &cluster.ClusterStatusDelta{}
				}
				kredis.Status.CurrentMemoryUsagePercent = result.MemoryUsagePercent
				kredis.Status.CurrentCPUUsagePercent = result.CPUUsagePercent

				if result.Decision.ShouldScale {
					logger.Info("Autoscaling triggered",
						"scaleType", result.Decision.ScaleType,
						"reason", result.Decision.Reason)
					if err := r.Autoscaler.ApplyAutoscaling(ctx, kredis, result.Decision); err != nil {
						logger.Error(err, "Failed to apply autoscaling")
					} else {
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

	// Adjust reconcile interval based on cluster state
	inProgress := false
	if delta != nil && strings.Contains(delta.LastClusterOperation, "-in-progress") {
		inProgress = true
	} else if strings.Contains(kredis.Status.LastClusterOperation, "-in-progress") {
		inProgress = true
	}

	if clusterReady {
		if kredis.Spec.Autoscaling.Enabled {
			interval := time.Second * 30
			if kredis.Spec.Autoscaling.ScaleUpStabilizationWindowSeconds > 0 &&
				kredis.Spec.Autoscaling.ScaleUpStabilizationWindowSeconds < 30 {
				interval = time.Duration(kredis.Spec.Autoscaling.ScaleUpStabilizationWindowSeconds) * time.Second
			}
			logger.V(1).Info("Cluster is healthy with autoscaling - scheduling next check", "interval", interval)
			return ctrl.Result{RequeueAfter: interval}, nil
		}
		logger.V(1).Info("Cluster is healthy - scheduling next check in 10 minutes")
		return ctrl.Result{RequeueAfter: time.Minute * 10}, nil
	} else if inProgress {
		logger.V(1).Info("Operation in progress - scheduling next check in 5 seconds")
		return ctrl.Result{RequeueAfter: time.Second * 5}, nil
	} else {
		logger.V(1).Info("Cluster needs attention - scheduling next check in 15 seconds")
		return ctrl.Result{RequeueAfter: time.Second * 15}, nil
	}
}

// reconcileServices creates/updates all required services
func (r *KredisReconciler) reconcileServices(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	// 1. Headless service for pod DNS resolution
	headlessSvcName := kredis.Name
	foundHeadlessSvc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: headlessSvcName, Namespace: kredis.Namespace}, foundHeadlessSvc)
	if err != nil && errors.IsNotFound(err) {
		svc := resource.CreateRedisHeadlessService(kredis, r.Scheme)
		logger.Info("Creating Redis headless service", "name", svc.Name)
		if err := r.Create(ctx, svc); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// 2. Master service (for writes)
	masterSvcName := kredis.Name + "-master"
	foundMasterSvc := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: masterSvcName, Namespace: kredis.Namespace}, foundMasterSvc)
	if err != nil && errors.IsNotFound(err) {
		svc := resource.CreateRedisMasterService(kredis, r.Scheme)
		logger.Info("Creating Redis master service", "name", svc.Name)
		if err := r.Create(ctx, svc); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// 3. Slave service (for reads)
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

	// 4. Metrics service (for Prometheus scraping) - only if exporter is enabled
	if kredis.Spec.Exporter.Enabled {
		metricsSvcName := kredis.Name + "-metrics"
		foundMetricsSvc := &corev1.Service{}
		err = r.Get(ctx, types.NamespacedName{Name: metricsSvcName, Namespace: kredis.Namespace}, foundMetricsSvc)
		if err != nil && errors.IsNotFound(err) {
			svc := resource.CreateRedisMetricsService(kredis, r.Scheme)
			if svc != nil {
				logger.Info("Creating Redis metrics service for Prometheus", "name", svc.Name)
				if err := r.Create(ctx, svc); err != nil {
					return err
				}
			}
		} else if err != nil {
			return err
		}
	}

	return nil
}

// reconcilePods manages Redis pods directly (instead of StatefulSet)
// This allows selective pod deletion for proper Redis Cluster scale-down
func (r *KredisReconciler) reconcilePods(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	// Get expected pod names based on current spec
	expectedPodNames := resource.GetExpectedPodNames(kredis.Name, kredis.Spec.Masters, kredis.Spec.Replicas)

	// Get current pods
	currentPods, err := r.getKredisPods(ctx, kredis)
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	currentPodMap := make(map[string]*corev1.Pod)
	for i := range currentPods {
		currentPodMap[currentPods[i].Name] = &currentPods[i]
	}

	// Create missing pods (scale-up)
	for _, expectedName := range expectedPodNames {
		if _, exists := currentPodMap[expectedName]; !exists {
			// Pod doesn't exist - create it
			if err := r.createPodWithPVCs(ctx, kredis, expectedName); err != nil {
				logger.Error(err, "Failed to create pod", "pod", expectedName)
				return err
			}
			logger.Info("Created pod", "pod", expectedName)
		}
	}

	// Note: Pod deletion (scale-down) is handled by ClusterManager
	// after Redis Cluster operations (slot migration, CLUSTER FORGET) are complete
	// This ensures data safety during scale-down

	return nil
}

// createPodWithPVCs creates a pod along with its required PVCs
func (r *KredisReconciler) createPodWithPVCs(ctx context.Context, kredis *cachev1alpha1.Kredis, podName string) error {
	logger := log.FromContext(ctx)

	// Parse pod name to get shard and instance indices
	shardIdx, instanceIdx, err := resource.ParsePodName(kredis.Name, podName)
	if err != nil {
		return fmt.Errorf("failed to parse pod name %s: %w", podName, err)
	}

	// 1. Create Data PVC if not exists
	dataPVCName := resource.DataPVCName(podName)
	dataPVC := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, types.NamespacedName{Name: dataPVCName, Namespace: kredis.Namespace}, dataPVC)
	if err != nil && errors.IsNotFound(err) {
		pvc := resource.CreateDataPVC(kredis, podName, r.Scheme)
		logger.Info("Creating data PVC", "pvc", dataPVCName)
		if err := r.Create(ctx, pvc); err != nil {
			return fmt.Errorf("failed to create data PVC: %w", err)
		}
	} else if err != nil {
		return err
	}

	// 2. Create Logs PVC if not exists
	logsPVCName := resource.LogsPVCName(podName)
	logsPVC := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, types.NamespacedName{Name: logsPVCName, Namespace: kredis.Namespace}, logsPVC)
	if err != nil && errors.IsNotFound(err) {
		pvc := resource.CreateLogsPVC(kredis, podName, r.Scheme)
		logger.Info("Creating logs PVC", "pvc", logsPVCName)
		if err := r.Create(ctx, pvc); err != nil {
			return fmt.Errorf("failed to create logs PVC: %w", err)
		}
	} else if err != nil {
		return err
	}

	// 3. Create Pod
	// Note: Initial role is determined by instance index (0 = master, 1+ = slave)
	// Actual role may change after failover - Redis Cluster manages this
	pod := resource.CreateShardPod(kredis, shardIdx, instanceIdx, r.Scheme)

	if err := r.Create(ctx, pod); err != nil {
		return fmt.Errorf("failed to create pod: %w", err)
	}

	return nil
}

// deleteMarkedPods deletes pods that have been marked for deletion after scale-down
func (r *KredisReconciler) deleteMarkedPods(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	// Check if there are pods to delete
	if !kredis.Status.ScaleDownReady || len(kredis.Status.PodsToDelete) == 0 {
		return nil
	}

	logger.Info("Deleting pods after scale-down", "pods", kredis.Status.PodsToDelete)

	for _, podName := range kredis.Status.PodsToDelete {
		if err := r.DeletePodWithPVCs(ctx, kredis, podName, true); err != nil {
			logger.Error(err, "Failed to delete pod", "pod", podName)
			// Continue with other pods
		}
	}

	// Clear the PodsToDelete and ScaleDownReady flags
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentKredis := &cachev1alpha1.Kredis{}
		if err := r.Get(ctx, types.NamespacedName{Name: kredis.Name, Namespace: kredis.Namespace}, currentKredis); err != nil {
			return err
		}

		currentKredis.Status.ScaleDownReady = false
		currentKredis.Status.PodsToDelete = nil
		currentKredis.Status.PendingScaleDown = nil

		logger.Info("Cleared scale-down status after pod deletion")
		return r.Status().Update(ctx, currentKredis)
	})
}

// DeletePodWithPVCs deletes a pod and optionally its PVCs
// This is called by ClusterManager after Redis Cluster operations are complete
func (r *KredisReconciler) DeletePodWithPVCs(ctx context.Context, kredis *cachev1alpha1.Kredis, podName string, deletePVCs bool) error {
	logger := log.FromContext(ctx)

	// Delete the pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: kredis.Namespace,
		},
	}
	if err := r.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete pod %s: %w", podName, err)
	}
	logger.Info("Deleted pod", "pod", podName)

	// Optionally delete PVCs
	if deletePVCs {
		// Delete data PVC
		dataPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resource.DataPVCName(podName),
				Namespace: kredis.Namespace,
			},
		}
		if err := r.Delete(ctx, dataPVC); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete data PVC", "pvc", dataPVC.Name)
		}

		// Delete logs PVC
		logsPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resource.LogsPVCName(podName),
				Namespace: kredis.Namespace,
			},
		}
		if err := r.Delete(ctx, logsPVC); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete logs PVC", "pvc", logsPVC.Name)
		}
	}

	return nil
}

func (r *KredisReconciler) updateStatus(ctx context.Context, kredis *cachev1alpha1.Kredis, delta *cluster.ClusterStatusDelta) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		currentKredis := &cachev1alpha1.Kredis{}
		err := r.Get(ctx, types.NamespacedName{Name: kredis.Name, Namespace: kredis.Namespace}, currentKredis)
		if err != nil {
			return err
		}

		// Merge delta from ClusterManager
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
			if delta.PendingScaleDown != nil {
				currentKredis.Status.PendingScaleDown = delta.PendingScaleDown
			}
			if delta.ScaleDownReady != nil {
				currentKredis.Status.ScaleDownReady = *delta.ScaleDownReady
			}
			if delta.PodsToDelete != nil {
				currentKredis.Status.PodsToDelete = delta.PodsToDelete
			}
			if delta.LastScaleTime != nil {
				currentKredis.Status.LastScaleTime = &metav1.Time{Time: *delta.LastScaleTime}
			}
			if delta.LastScaleType != "" {
				currentKredis.Status.LastScaleType = delta.LastScaleType
			}
		}

		newStatus := r.calculateStatus(ctx, currentKredis)

		if reflect.DeepEqual(currentKredis.Status, newStatus) {
			return nil
		}

		currentKredis.Status = newStatus
		return r.Status().Update(ctx, currentKredis)
	})
}

func (r *KredisReconciler) calculateStatus(ctx context.Context, kredis *cachev1alpha1.Kredis) cachev1alpha1.KredisStatus {
	newStatus := kredis.Status.DeepCopy()

	// Count pods
	pods, _ := r.getKredisPods(ctx, kredis)
	readyCount := int32(0)
	for _, pod := range pods {
		if isPodReady(&pod) {
			readyCount++
		}
	}

	newStatus.Replicas = int32(len(pods))
	newStatus.ReadyReplicas = readyCount

	expectedReplicas := kredis.Spec.Masters + (kredis.Spec.Masters * kredis.Spec.Replicas)

	if readyCount == expectedReplicas {
		newStatus.Phase = "Ready"
	} else if len(pods) > 0 {
		newStatus.Phase = "Pending"
	} else {
		newStatus.Phase = "Creating"
	}

	// Determine ClusterState
	// Check for specific in-progress states (more explicit than checking !success)
	lastOp := kredis.Status.LastClusterOperation
	isScaleDownInProgress := strings.Contains(lastOp, "scaledown-migrate-in-progress") ||
		strings.Contains(lastOp, "scaledown-forget-in-progress") ||
		strings.Contains(lastOp, "scaledown-in-progress") ||
		strings.Contains(lastOp, "scaledown-migrate-retry") ||
		len(kredis.Status.PendingScaleDown) > 0

	isScaling := (int32(len(pods)) != expectedReplicas || readyCount != expectedReplicas) && !isScaleDownInProgress

	isOperationInProgress := strings.Contains(kredis.Status.LastClusterOperation, "-in-progress") ||
		strings.Contains(kredis.Status.LastClusterOperation, "-pending") ||
		strings.Contains(kredis.Status.LastClusterOperation, "-needed")

	if newStatus.ClusterState == "" {
		newStatus.ClusterState = string(cachev1alpha1.ClusterStateCreating)
	} else if isScaleDownInProgress {
		newStatus.ClusterState = string(cachev1alpha1.ClusterStateScalingDown)
	} else if isScaling && newStatus.ClusterState == string(cachev1alpha1.ClusterStateRunning) {
		newStatus.ClusterState = string(cachev1alpha1.ClusterStateScaling)
	} else if isOperationInProgress {
		// Keep the current state
	} else if strings.Contains(kredis.Status.LastClusterOperation, "-success") {
		if readyCount == expectedReplicas && !isScaling {
			newStatus.ClusterState = string(cachev1alpha1.ClusterStateRunning)
		}
	}

	// Update condition
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "NotReady",
		Message:            fmt.Sprintf("Ready replicas: %d/%d", readyCount, expectedReplicas),
		LastTransitionTime: metav1.Now(),
	}

	if readyCount == expectedReplicas && expectedReplicas > 0 {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "AllReady"
		condition.Message = "All Redis nodes are ready"
	}

	newStatus.Conditions = []metav1.Condition{condition}

	return *newStatus
}

func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
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

	// Filter out terminating pods (DeletionTimestamp != nil)
	// This prevents race conditions during scale-down where deleted pods
	// are still visible but should not be considered for pod reconciliation
	var activePods []corev1.Pod
	for _, pod := range podList.Items {
		if pod.DeletionTimestamp == nil {
			activePods = append(activePods, pod)
		}
	}

	return activePods, nil
}

// cleanupKredisResources cleans up all resources associated with a Kredis instance
func (r *KredisReconciler) cleanupKredisResources(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	// Cleanup Jobs
	if r.ClusterManager != nil && r.ClusterManager.JobManager != nil {
		if err := r.ClusterManager.JobManager.CleanupCompletedJobs(ctx, kredis); err != nil {
			logger.Error(err, "Failed to cleanup Jobs during deletion")
		}
	}

	// Delete all Jobs
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
			}
		}
	}

	// Delete all Pods (they should be deleted by owner reference, but ensure cleanup)
	pods, err := r.getKredisPods(ctx, kredis)
	if err != nil {
		logger.Error(err, "Failed to list pods for cleanup")
	} else {
		for _, pod := range pods {
			if err := r.Delete(ctx, &pod); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete pod", "pod", pod.Name)
			}
		}
	}

	// Delete all PVCs
	pvcList := &corev1.PersistentVolumeClaimList{}
	pvcListOpts := []client.ListOption{
		client.InNamespace(kredis.Namespace),
		client.MatchingLabels{
			"app":                        "kredis",
			"app.kubernetes.io/instance": kredis.Name,
		},
	}
	if err := r.List(ctx, pvcList, pvcListOpts...); err != nil {
		logger.Error(err, "Failed to list PVCs for cleanup")
	} else {
		for _, pvc := range pvcList.Items {
			if err := r.Delete(ctx, &pvc); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete PVC", "pvc", pvc.Name)
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
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}
