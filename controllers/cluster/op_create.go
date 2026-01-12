package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// createCluster initializes a new Redis cluster using a Job.
// Non-blocking: Creates a Job and returns immediately, subsequent reconciles check Job status.
func (cm *ClusterManager) createCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	// If creation is already in progress, check Job status
	if strings.Contains(lastOp, "create-in-progress") {
		return cm.checkCreateJobAndFinalize(ctx, kredis, pods, delta)
	}

	// If reset is pending, check if all nodes have completed reset
	if strings.Contains(lastOp, "create-reset-pending") {
		if !cm.areAllNodesReset(ctx, pods, kredis.Spec.BasePort) {
			logger.Info("Nodes still resetting, waiting for next reconcile")
			return nil
		}
		logger.Info("All nodes reset complete, proceeding to cluster creation")
		// Fall through to create cluster Job
		return cm.createClusterJob(ctx, kredis, pods, delta)
	}

	// Check which pods actually need reset (optimization: skip already clean nodes)
	podsNeedingReset := cm.findPodsNeedingReset(ctx, pods, kredis.Spec.BasePort)
	if len(podsNeedingReset) == 0 {
		logger.Info("All nodes are already clean, skipping reset phase")
		// All nodes are clean, proceed directly to cluster creation
		return cm.createClusterJob(ctx, kredis, pods, delta)
	}

	logger.Info("Resetting nodes that need cleanup before cluster creation",
		"totalPods", len(pods),
		"podsNeedingReset", len(podsNeedingReset))

	for _, pod := range podsNeedingReset {
		if _, err := cm.resetNodeIfNeeded(ctx, pod, kredis.Spec.BasePort); err != nil {
			return fmt.Errorf("failed to reset node %s: %w", pod.Name, err)
		}
	}

	// Verify all nodes are reset (each node should only see itself in cluster nodes)
	// This replaces the blocking time.Sleep with proper state verification
	if !cm.areAllNodesReset(ctx, pods, kredis.Spec.BasePort) {
		logger.Info("Not all nodes have completed reset, waiting for next reconcile")
		delta.LastClusterOperation = fmt.Sprintf("create-reset-pending:%d", time.Now().Unix())
		return nil // Return without error - next reconcile will retry
	}

	// All nodes are reset, proceed to create cluster
	return cm.createClusterJob(ctx, kredis, pods, delta)
}

// createClusterJob creates the cluster creation Job after all nodes are reset.
func (cm *ClusterManager) createClusterJob(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Prepare node addresses
	var nodeAddrs []string
	for _, pod := range pods {
		if pod.Status.PodIP != "" {
			nodeAddrs = append(nodeAddrs, fmt.Sprintf("%s:%d", pod.Status.PodIP, kredis.Spec.BasePort))
		}
	}
	if len(nodeAddrs) != len(pods) {
		return fmt.Errorf("not all pods have an IP address yet")
	}
	logger.Info("Node list for cluster creation", "nodeAddrs", strings.Join(nodeAddrs, " "), "nodeCount", len(nodeAddrs))

	// Create the cluster creation Job
	if err := cm.JobManager.CreateClusterJob(ctx, kredis, nodeAddrs, int(kredis.Spec.Replicas)); err != nil {
		return fmt.Errorf("failed to create cluster job: %w", err)
	}

	delta.LastClusterOperation = fmt.Sprintf("create-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateCreating)

	logger.Info("Cluster creation Job created, waiting for completion")
	return nil
}

// checkCreateJobAndFinalize checks the cluster creation Job status and finalizes if complete.
func (cm *ClusterManager) checkCreateJobAndFinalize(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Check Job status
	jobResult, err := cm.JobManager.GetJobStatus(ctx, kredis, JobTypeCreate)
	if err != nil {
		logger.Error(err, "Failed to get create Job status")
		return nil // Will retry in next reconcile
	}

	switch jobResult.Status {
	case JobStatusNotFound:
		// Job not found - might have been cleaned up, check cluster health directly
		logger.Info("Create Job not found, checking cluster health directly")
		return cm.verifyClusterCreation(ctx, kredis, pods, delta)

	case JobStatusPending, JobStatusRunning:
		// Job is still in progress - just return and wait
		logger.Info("Create cluster Job still running", "job", jobResult.JobName)
		return nil

	case JobStatusSucceeded:
		logger.Info("Create cluster Job succeeded", "job", jobResult.JobName)
		return cm.verifyClusterCreation(ctx, kredis, pods, delta)

	case JobStatusFailed:
		logger.Error(nil, "Create cluster Job failed", "job", jobResult.JobName, "message", jobResult.Message)
		delta.LastClusterOperation = fmt.Sprintf("create-failed:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
		// Cleanup the failed job so we can retry
		_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)
		return fmt.Errorf("create cluster Job failed: %s", jobResult.Message)
	}

	return nil
}

// verifyClusterCreation checks if the cluster was created successfully and marks it as complete.
func (cm *ClusterManager) verifyClusterCreation(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Find a pod to check cluster health
	var commandPod *corev1.Pod
	for i := range pods {
		if cm.isPodReady(pods[i]) {
			commandPod = &pods[i]
			break
		}
	}
	if commandPod == nil {
		logger.Info("No ready pod found to verify cluster creation")
		return nil
	}

	// Check if cluster is healthy
	isHealthy, err := cm.PodExecutor.IsClusterHealthy(ctx, *commandPod, kredis.Spec.BasePort)
	if err != nil {
		logger.Error(err, "Failed to check cluster health")
		return nil
	}

	if !isHealthy {
		logger.Info("Cluster not yet healthy after creation, waiting...")
		return nil
	}

	// Cluster is healthy - mark as success
	logger.Info("Cluster created and healthy!")
	delta.LastClusterOperation = fmt.Sprintf("create-success:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateInitialized)

	// Set known cluster nodes count
	known := len(pods)
	delta.KnownClusterNodes = &known

	// Update LastScaleTime to enable stabilization window for autoscaling
	// This prevents autoscaling from triggering immediately after cluster creation
	now := time.Now()
	delta.LastScaleTime = &now
	delta.LastScaleType = "create"

	// Cleanup completed jobs
	_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)

	return nil
}

// areAllNodesReset is now in utils.go as a common function
