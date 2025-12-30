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

const (
	// InitialReshardSlots is the number of slots to move via reshard before rebalance
	// This helps new empty masters quickly escape the 0-slot state
	InitialReshardSlots = 100
)

// rebalanceCluster implements a 2-phase rebalancing strategy:
// Phase 1 (reshard-pending): Use reshard to move a small number of slots to new empty masters
// Phase 2 (rebalance-pending): Use rebalance to evenly distribute all slots
func (cm *ClusterManager) rebalanceCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	masterPod := cm.findMasterPod(pods, kredis, clusterState)
	if masterPod == nil {
		return fmt.Errorf("no master pod found for rebalancing")
	}

	// Determine current phase based on lastOp
	switch {
	case strings.Contains(lastOp, "rebalance-needed"):
		// Start Phase 1: Reshard to new empty masters
		return cm.executeReshardPhase(ctx, kredis, masterPod, clusterState, delta)

	case strings.Contains(lastOp, "reshard-in-progress"):
		// Verify reshard completion, then move to Phase 2
		return cm.verifyReshardAndStartRebalance(ctx, kredis, masterPod, delta)

	case strings.Contains(lastOp, "rebalance-in-progress"):
		// Phase 2: Rebalance verification
		return cm.executeRebalancePhase(ctx, kredis, masterPod, delta)

	default:
		// Fallback: start from reshard phase
		logger.Info("Unknown rebalance state, starting from reshard phase", "lastOp", lastOp)
		return cm.executeReshardPhase(ctx, kredis, masterPod, clusterState, delta)
	}
}

// executeReshardPhase performs Phase 1: reshard slots to empty masters
func (cm *ClusterManager) executeReshardPhase(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Find empty masters (masters with 0 slots)
	emptyMasters := cm.findEmptyMasters(ctx, *masterPod, kredis.Spec.BasePort, clusterState)
	if len(emptyMasters) == 0 {
		// No empty masters, skip reshard and go directly to rebalance
		logger.Info("No empty masters found, proceeding directly to rebalance")
		delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)

		// Trigger rebalance immediately
		if _, err := cm.PodExecutor.RebalanceCluster(ctx, *masterPod, kredis.Spec.BasePort); err != nil {
			if !strings.Contains(err.Error(), "ERR Please use SETSLOT only with masters") {
				delta.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
				delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
				return fmt.Errorf("rebalance failed to start: %w", err)
			}
		}
		return nil
	}

	logger.Info("Starting reshard phase for empty masters", "emptyMasterCount", len(emptyMasters))
	delta.LastClusterOperation = fmt.Sprintf("reshard-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)

	// Reshard slots to each empty master
	for _, emptyMaster := range emptyMasters {
		logger.Info("Resharding slots to empty master", "nodeID", emptyMaster.NodeID, "podName", emptyMaster.PodName, "slots", InitialReshardSlots)

		result, err := cm.PodExecutor.ReshardCluster(ctx, *masterPod, kredis.Spec.BasePort, emptyMaster.NodeID, InitialReshardSlots, nil)
		if err != nil {
			// Log but continue - reshard might partially succeed
			logger.Error(err, "Reshard failed for empty master", "nodeID", emptyMaster.NodeID)
			// Don't fail completely, try next empty master
			continue
		}
		logger.Info("Reshard command completed", "nodeID", emptyMaster.NodeID, "stdout", result.Stdout)
	}

	// Next reconcile will verify and proceed to rebalance
	return nil
}

// verifyReshardAndStartRebalance verifies reshard completion and starts rebalance
func (cm *ClusterManager) verifyReshardAndStartRebalance(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Wait for cluster state to become ok
	healthy, err := cm.PodExecutor.IsClusterHealthy(ctx, *masterPod, kredis.Spec.BasePort)
	if err != nil {
		logger.Error(err, "Failed to check cluster health after reshard")
		return nil // Will retry in next reconcile
	}

	if !healthy {
		logger.Info("Cluster not yet healthy after reshard, waiting...")
		return nil // Will retry in next reconcile
	}

	logger.Info("Cluster is healthy after reshard, starting rebalance phase")

	// Move to Phase 2: Rebalance
	delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)

	// Trigger rebalance
	if _, err := cm.PodExecutor.RebalanceCluster(ctx, *masterPod, kredis.Spec.BasePort); err != nil {
		if strings.Contains(err.Error(), "ERR Please use SETSLOT only with masters") {
			logger.Info("Ignoring 'SETSLOT' error during rebalance trigger")
		} else {
			logger.Error(err, "Rebalance failed to start")
			// Don't fail - will retry in next reconcile
		}
	}

	return nil
}

// executeRebalancePhase performs Phase 2: rebalance verification and completion
func (cm *ClusterManager) executeRebalancePhase(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)

	// Check if rebalance is still in progress
	if !cm.checkIfRebalanceInProgress(ctx, *masterPod, kredis.Spec.BasePort) {
		// Not in progress - trigger again in case it didn't start properly
		if _, err := cm.PodExecutor.RebalanceCluster(ctx, *masterPod, kredis.Spec.BasePort); err != nil {
			if !strings.Contains(err.Error(), "ERR Please use SETSLOT only with masters") {
				logger.Error(err, "Rebalance trigger failed")
			}
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

// findEmptyMasters finds masters that have 0 slots assigned
func (cm *ClusterManager) findEmptyMasters(ctx context.Context, masterPod corev1.Pod, basePort int32, clusterState []cachev1alpha1.ClusterNode) []cachev1alpha1.ClusterNode {
	logger := log.FromContext(ctx)
	var emptyMasters []cachev1alpha1.ClusterNode

	// Get slot distribution from cluster nodes output
	nodes, err := cm.PodExecutor.GetRedisClusterNodes(ctx, masterPod, basePort)
	if err != nil {
		logger.Error(err, "Failed to get cluster nodes for empty master detection")
		return emptyMasters
	}

	// Create a map of node IDs that have slots (from cluster slots command)
	mastersWithSlots := make(map[string]bool)
	slotsResult, err := cm.PodExecutor.ExecuteRedisCommand(ctx, masterPod, basePort, "CLUSTER", "SLOTS")
	if err != nil {
		logger.Error(err, "Failed to get cluster slots")
		// Fall back to checking from cluster nodes output
		return cm.findEmptyMastersFromNodes(nodes, clusterState)
	}

	// Parse CLUSTER SLOTS output to identify masters with slots
	// Format: start_slot end_slot master_ip master_port master_id ...
	lines := strings.Split(slotsResult.Stdout, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Look for node ID patterns (40 character hex strings)
		if len(line) == 40 && isHexString(line) {
			mastersWithSlots[line] = true
		}
	}

	// Find masters without slots
	for _, node := range clusterState {
		if node.Role == "master" && node.NodeID != "" {
			if !mastersWithSlots[node.NodeID] {
				emptyMasters = append(emptyMasters, node)
				logger.Info("Found empty master", "nodeID", node.NodeID, "podName", node.PodName)
			}
		}
	}

	return emptyMasters
}

// findEmptyMastersFromNodes is a fallback method using cluster nodes output
func (cm *ClusterManager) findEmptyMastersFromNodes(nodes []RedisNodeInfo, clusterState []cachev1alpha1.ClusterNode) []cachev1alpha1.ClusterNode {
	var emptyMasters []cachev1alpha1.ClusterNode

	// Create nodeID to ClusterNode map
	nodeMap := make(map[string]cachev1alpha1.ClusterNode)
	for _, node := range clusterState {
		if node.Role == "master" {
			nodeMap[node.NodeID] = node
		}
	}

	// Nodes from GetRedisClusterNodes don't include slot info directly
	// So we rely on clusterState which should be accurate
	for _, node := range clusterState {
		if node.Role == "master" {
			// Check if this master has slots by looking at cluster nodes output
			// For now, add all new masters (those recently added)
			emptyMasters = append(emptyMasters, node)
		}
	}

	return emptyMasters
}

// isHexString checks if a string is a valid hex string
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
