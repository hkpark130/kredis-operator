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

// rebalanceCluster implements a 2-phase rebalancing strategy using Kubernetes Jobs:
// Phase 1 (reshard): Create Jobs to move slots to new empty masters (handled by op_reshard.go)
// Phase 2 (rebalance): Create a Job to evenly distribute all slots
// Non-blocking: Creates Jobs and returns immediately, subsequent reconciles check Job status.
func (cm *ClusterManager) rebalanceCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	masterPod := cm.findMasterPod(pods, kredis, clusterState)
	if masterPod == nil {
		return fmt.Errorf("no master pod found for rebalancing")
	}

	// Determine current phase based on lastOp
	switch {
	case strings.Contains(lastOp, "rebalance-needed"), strings.Contains(lastOp, "reshard-in-progress"):
		// Phase 1: Reshard to new empty masters (delegated to op_reshard.go)
		return cm.reshardCluster(ctx, kredis, masterPod, clusterState, delta)

	case strings.Contains(lastOp, "rebalance-in-progress"):
		// Phase 2: Rebalance for even distribution via Job
		return cm.executeRebalancePhase(ctx, kredis, masterPod, delta)

	default:
		// Fallback: start from reshard phase
		logger.Info("Unknown rebalance state, starting from reshard phase", "lastOp", lastOp)
		return cm.reshardCluster(ctx, kredis, masterPod, clusterState, delta)
	}
}

// executeRebalancePhase performs Phase 2: rebalance via Job for even slot distribution.
// At this point, all masters should have at least some slots (from reshard phase).
func (cm *ClusterManager) executeRebalancePhase(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Check existing rebalance Job status
	jobResult, err := cm.JobManager.GetJobStatus(ctx, kredis, JobTypeRebalance)
	if err != nil {
		logger.Error(err, "Failed to get rebalance Job status")
		return nil // Will retry in next reconcile
	}

	switch jobResult.Status {
	case JobStatusNotFound:
		// No rebalance Job - create one
		logger.Info("Creating rebalance Job for even slot distribution")
		clusterAddr := fmt.Sprintf("%s:%d", masterPod.Status.PodIP, kredis.Spec.BasePort)

		if err := cm.JobManager.CreateRebalanceJob(ctx, kredis, clusterAddr); err != nil {
			logger.Error(err, "Failed to create rebalance Job")
			return err
		}

		delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
		return nil

	case JobStatusPending, JobStatusRunning:
		// Job is still in progress - just return and wait
		logger.Info("Rebalance Job still running", "job", jobResult.JobName)
		return nil

	case JobStatusSucceeded:
		logger.Info("Rebalance Job succeeded - verifying cluster state", "job", jobResult.JobName)

		// Non-blocking: check stability, next reconcile will verify if not stable
		isHealthy, _ := cm.PodExecutor.IsClusterHealthy(ctx, *masterPod, kredis.Spec.BasePort)
		if !isHealthy {
			logger.Info("Cluster still stabilizing after rebalance, will check in next reconcile")
			return nil
		}

		// Verify all masters have slots
		ok, err := cm.checkAllMastersHaveSlots(ctx, *masterPod, kredis.Spec.BasePort)
		if err != nil {
			return fmt.Errorf("failed to verify slots distribution: %w", err)
		}
		if !ok {
			logger.Info("Some masters still have no slots after rebalance")
			// Might need another rebalance round
			delta.LastClusterOperation = fmt.Sprintf("rebalance-needed:%d", time.Now().Unix())
			return nil
		}

		// Success!
		logger.Info("Rebalance completed successfully - all masters have slots")
		delta.LastClusterOperation = fmt.Sprintf("rebalance-success:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)

		// Cleanup completed Jobs
		_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)
		return nil

	case JobStatusFailed:
		logger.Error(nil, "Rebalance Job failed", "job", jobResult.JobName, "message", jobResult.Message)
		delta.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
		return fmt.Errorf("rebalance Job failed: %s", jobResult.Message)
	}

	return nil
}
