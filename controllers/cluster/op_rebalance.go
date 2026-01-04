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

// rebalanceCluster implements a 2-phase rebalancing strategy:
// Phase 1 (reshard): Use reshard to move slots to new empty masters (handled by op_reshard.go)
// Phase 2 (rebalance): Use rebalance to evenly distribute all slots
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
		// Phase 2: Rebalance for even distribution
		return cm.executeRebalancePhase(ctx, kredis, masterPod, delta)

	default:
		// Fallback: start from reshard phase
		logger.Info("Unknown rebalance state, starting from reshard phase", "lastOp", lastOp)
		return cm.reshardCluster(ctx, kredis, masterPod, clusterState, delta)
	}
}

// executeRebalancePhase performs Phase 2: rebalance for even slot distribution.
// At this point, all masters should have at least some slots (from reshard phase).
func (cm *ClusterManager) executeRebalancePhase(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)

	// Check if rebalance is still in progress
	if !cm.checkIfRebalanceInProgress(ctx, *masterPod, kredis.Spec.BasePort) {
		// Not in progress - trigger rebalance
		logger.Info("Triggering rebalance for even slot distribution")
		if _, err := cm.PodExecutor.RebalanceCluster(ctx, *masterPod, kredis.Spec.BasePort); err != nil {
			logger.Error(err, "Rebalance command failed")
			// Continue to check stabilization anyway
		}
	}

	logger.Info("Waiting for cluster to stabilize after rebalance...")
	if err := cm.waitForClusterStabilization(ctx, kredis, masterPod); err != nil {
		logger.Info("Cluster still stabilizing, will retry", "error", err)
		return nil // Will retry in next reconcile
	}

	// Verify all masters have slots
	ok, err := cm.checkAllMastersHaveSlots(ctx, *masterPod, kredis.Spec.BasePort)
	if err != nil {
		return fmt.Errorf("failed to verify slots distribution: %w", err)
	}
	if !ok {
		logger.Info("Some masters still have no slots, rebalance might be ongoing")
		return nil // Will retry in next reconcile
	}

	// Success!
	logger.Info("Rebalance completed successfully - all masters have slots")
	delta.LastClusterOperation = fmt.Sprintf("rebalance-success:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)
	return nil
}
