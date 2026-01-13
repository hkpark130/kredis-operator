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

// Scale-down phases for tracking progress
const (
	ScaleDownPhasePending    = "pending"
	ScaleDownPhaseResharding = "resharding"
	ScaleDownPhaseForgetting = "forgetting"
	ScaleDownPhaseCompleted  = "completed"
)

// scaleDownCluster handles removing nodes from the cluster safely.
// Scale-down order:
//  1. Identify nodes to remove (based on pod naming: highest master index for master scale-down,
//     highest replica index for replica scale-down)
//  2. For masters: Migrate all slots to remaining masters using rebalance with weight=0
//  3. For all nodes: Execute CLUSTER FORGET on all remaining nodes
//  4. After all nodes are removed from cluster, set ScaleDownReady=true and PodsToDelete
//  5. Controller then deletes specific pods directly
func (cm *ClusterManager) scaleDownCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	// If scale-down is already in progress, continue from where we left off
	if strings.Contains(lastOp, "scaledown-migrate-in-progress") {
		return cm.checkScaleDownMigrateAndProceed(ctx, kredis, pods, clusterState, delta)
	}
	if strings.Contains(lastOp, "scaledown-migrate-retry") {
		// Retry migration after failure - restart the migration process
		return cm.retryScaleDownMigrate(ctx, kredis, pods, clusterState, delta, lastOp)
	}
	if strings.Contains(lastOp, "scaledown-forget-in-progress") {
		return cm.checkScaleDownForgetAndProceed(ctx, kredis, pods, clusterState, delta)
	}

	logger.Info("Starting scale-down operation")

	// Calculate which nodes need to be removed
	nodesToRemove := cm.identifyNodesToRemove(ctx, kredis, pods, clusterState)
	if len(nodesToRemove) == 0 {
		logger.Info("No nodes to remove, scale-down not needed")
		delta.LastClusterOperation = fmt.Sprintf("scaledown-success:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)
		return nil
	}

	logger.Info("Identified nodes to remove", "count", len(nodesToRemove))
	for _, node := range nodesToRemove {
		logger.Info("Node to remove", "pod", node.PodName, "nodeID", node.NodeID, "role", node.Role, "slots", node.SlotCount)
	}

	// Initialize PendingScaleDown if not set
	pendingNodes := kredis.Status.PendingScaleDown
	if len(pendingNodes) == 0 {
		pendingNodes = make([]cachev1alpha1.ScaleDownNode, 0, len(nodesToRemove))
		for _, node := range nodesToRemove {
			pendingNodes = append(pendingNodes, cachev1alpha1.ScaleDownNode{
				NodeID:  node.NodeID,
				PodName: node.PodName,
				Role:    node.Role,
				Phase:   ScaleDownPhasePending,
			})
		}
		// Update status with pending nodes via delta
		delta.PendingScaleDown = pendingNodes
	}

	// Find first node that needs processing
	for i, node := range pendingNodes {
		if node.Phase == ScaleDownPhaseCompleted {
			continue
		}

		// Process this node
		if node.Phase == ScaleDownPhasePending {
			// Find the cluster node info
			var clusterNode *cachev1alpha1.ClusterNode
			for _, cn := range clusterState {
				if cn.NodeID == node.NodeID {
					clusterNode = &cn
					break
				}
			}

			if clusterNode == nil {
				// Node not found in cluster - maybe already removed
				logger.Info("Node not found in cluster, marking as completed", "nodeID", node.NodeID)
				pendingNodes[i].Phase = ScaleDownPhaseCompleted
				delta.PendingScaleDown = pendingNodes
				continue
			}

			// If master with slots, need to migrate first
			if clusterNode.Role == "master" && clusterNode.SlotCount > 0 {
				logger.Info("Starting slot migration for master node", "pod", node.PodName, "slots", clusterNode.SlotCount)
				pendingNodes[i].Phase = ScaleDownPhaseResharding
				delta.PendingScaleDown = pendingNodes

				// Find a command pod from remaining nodes
				commandPod := cm.findCommandPodForScaleDown(pods, nodesToRemove, clusterState)
				if commandPod == nil {
					return fmt.Errorf("no command pod available for scale-down")
				}

				clusterAddr := clusterAddr(*commandPod, kredis.Spec.BasePort)

				// Create migrate job
				if err := cm.JobManager.CreateScaleDownMigrateJob(ctx, kredis, node.NodeID, "", clusterAddr, clusterNode.SlotCount); err != nil {
					return fmt.Errorf("failed to create scale-down migrate job: %w", err)
				}

				delta.LastClusterOperation = fmt.Sprintf("scaledown-migrate-in-progress:%s:%d", node.NodeID, time.Now().Unix())
				delta.ClusterState = string(cachev1alpha1.ClusterStateScalingDown)
				return nil
			}

			// No slots to migrate (replica or empty master), go directly to forget
			logger.Info("Node has no slots, proceeding to forget phase", "pod", node.PodName, "role", clusterNode.Role)
			pendingNodes[i].Phase = ScaleDownPhaseForgetting
			delta.PendingScaleDown = pendingNodes

			commandPod := cm.findCommandPodForScaleDown(pods, nodesToRemove, clusterState)
			if commandPod == nil {
				return fmt.Errorf("no command pod available for scale-down")
			}

			clusterAddr := clusterAddr(*commandPod, kredis.Spec.BasePort)

			if err := cm.JobManager.CreateScaleDownForgetJob(ctx, kredis, node.NodeID, clusterAddr); err != nil {
				return fmt.Errorf("failed to create scale-down forget job: %w", err)
			}

			delta.LastClusterOperation = fmt.Sprintf("scaledown-forget-in-progress:%s:%d", node.NodeID, time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateScalingDown)
			return nil
		}
	}

	// All nodes completed
	return cm.finalizeScaleDown(ctx, kredis, pods, clusterState, delta)
}

// identifyNodesToRemove determines which nodes need to be removed based on spec vs actual state.
// This function works with both new Pod naming (kredis-master-0) and legacy StatefulSet naming (kredis-0).
// It uses clusterState to determine node roles, not Pod names.
// Returns nodes in order they should be removed (replicas first, then masters).
func (cm *ClusterManager) identifyNodesToRemove(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode) []cachev1alpha1.ClusterNode {
	logger := log.FromContext(ctx)

	expectedTotal := cm.getExpectedPodCount(kredis)
	currentTotal := int32(len(clusterState))

	if currentTotal <= expectedTotal {
		logger.Info("No nodes to remove", "current", currentTotal, "expected", expectedTotal)
		return nil
	}

	nodesToRemove := int(currentTotal - expectedTotal)
	logger.Info("Calculating nodes to remove", "current", currentTotal, "expected", expectedTotal, "toRemove", nodesToRemove)

	// Separate masters and slaves from clusterState
	var masters, slaves []cachev1alpha1.ClusterNode
	for _, node := range clusterState {
		if node.NodeID != "" && node.Role != "unknown" && node.Role != "" {
			if node.Role == "master" {
				masters = append(masters, node)
			} else if node.Role == "slave" {
				slaves = append(slaves, node)
			}
		}
	}

	currentMasterCount := int32(len(masters))
	currentSlaveCount := int32(len(slaves))
	expectedMasters := kredis.Spec.Masters
	expectedSlaves := kredis.Spec.Masters * kredis.Spec.Replicas

	logger.Info("Current cluster composition",
		"masters", currentMasterCount,
		"slaves", currentSlaveCount,
		"expectedMasters", expectedMasters,
		"expectedSlaves", expectedSlaves)

	var result []cachev1alpha1.ClusterNode

	// Case 1: Need to remove masters (memory scale-down)
	mastersToRemove := currentMasterCount - expectedMasters
	if mastersToRemove > 0 {
		logger.Info("Master scale-down detected", "toRemove", mastersToRemove)

		// Sort masters by slot count (remove ones with fewest slots first, or empty ones)
		// For simplicity, just take masters from the end (assuming highest index)
		// But since we can't rely on Pod naming, we select masters with slots=0 first
		var emptyMasters, slottedMasters []cachev1alpha1.ClusterNode
		for _, m := range masters {
			if m.SlotCount == 0 {
				emptyMasters = append(emptyMasters, m)
			} else {
				slottedMasters = append(slottedMasters, m)
			}
		}

		// Remove empty masters first, then slotted ones from the end
		mastersToRemoveList := append(emptyMasters, slottedMasters...)
		if len(mastersToRemoveList) > int(mastersToRemove) {
			mastersToRemoveList = mastersToRemoveList[len(mastersToRemoveList)-int(mastersToRemove):]
		}

		// Build set of master NodeIDs being removed
		removeMasterIDs := make(map[string]struct{})
		for _, m := range mastersToRemoveList {
			removeMasterIDs[m.NodeID] = struct{}{}
		}

		// First add slaves of masters being removed
		for _, slave := range slaves {
			if _, shouldRemove := removeMasterIDs[slave.MasterID]; shouldRemove {
				result = append(result, slave)
				logger.Info("Marking slave for removal (master being removed)", "pod", slave.PodName, "masterID", slave.MasterID)
			}
		}

		// Then add the masters
		for _, m := range mastersToRemoveList {
			result = append(result, m)
			logger.Info("Marking master for removal", "pod", m.PodName, "slots", m.SlotCount)
		}
	}

	// Case 2: Need to remove replicas (CPU scale-down) - masters are at expected count
	if mastersToRemove <= 0 && currentSlaveCount > expectedSlaves {
		slavesToRemove := currentSlaveCount - expectedSlaves
		logger.Info("Replica scale-down detected", "toRemove", slavesToRemove)

		// Remove slaves evenly from each master
		// Build a map of masterID -> slaves
		masterSlaveMap := make(map[string][]cachev1alpha1.ClusterNode)
		for _, slave := range slaves {
			masterSlaveMap[slave.MasterID] = append(masterSlaveMap[slave.MasterID], slave)
		}

		// Calculate how many to remove per master
		perMasterRemove := int(slavesToRemove) / len(masterSlaveMap)
		if perMasterRemove == 0 {
			perMasterRemove = 1
		}

		removed := 0
		for masterID, slaveList := range masterSlaveMap {
			for i := 0; i < perMasterRemove && removed < int(slavesToRemove); i++ {
				if i < len(slaveList) {
					result = append(result, slaveList[len(slaveList)-1-i])
					logger.Info("Marking slave for removal", "pod", slaveList[len(slaveList)-1-i].PodName, "masterID", masterID)
					removed++
				}
			}
		}
	}

	logger.Info("Identified nodes to remove", "count", len(result))
	return result
}

// findCommandPodForScaleDown finds a pod to execute commands from, excluding nodes being removed
func (cm *ClusterManager) findCommandPodForScaleDown(pods []corev1.Pod, nodesToRemove []cachev1alpha1.ClusterNode, clusterState []cachev1alpha1.ClusterNode) *corev1.Pod {
	// Build set of nodes to remove
	removeSet := make(map[string]struct{})
	for _, node := range nodesToRemove {
		removeSet[node.PodName] = struct{}{}
	}

	// Find a master pod that is NOT being removed
	for _, node := range clusterState {
		if _, shouldRemove := removeSet[node.PodName]; shouldRemove {
			continue
		}
		if node.Role == "master" && node.NodeID != "" && node.SlotCount > 0 {
			for i := range pods {
				if pods[i].Name == node.PodName && cm.isPodReady(pods[i]) {
					return &pods[i]
				}
			}
		}
	}

	// Fallback to any remaining node
	for _, node := range clusterState {
		if _, shouldRemove := removeSet[node.PodName]; shouldRemove {
			continue
		}
		if node.NodeID != "" {
			for i := range pods {
				if pods[i].Name == node.PodName && cm.isPodReady(pods[i]) {
					return &pods[i]
				}
			}
		}
	}

	return nil
}

// retryScaleDownMigrate handles retry of failed migration job
func (cm *ClusterManager) retryScaleDownMigrate(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta, lastOp string) error {
	logger := log.FromContext(ctx)

	// Parse node ID from lastOp: "scaledown-migrate-retry:<nodeID>:<timestamp>"
	parts := strings.Split(lastOp, ":")
	if len(parts) < 2 {
		logger.Error(nil, "Invalid scaledown-migrate-retry format", "lastOp", lastOp)
		delta.LastClusterOperation = fmt.Sprintf("scaledown-failed:%d", time.Now().Unix())
		return nil
	}
	nodeID := parts[1]

	// Check if node still has slots
	var nodeSlots int
	for _, node := range clusterState {
		if node.NodeID == nodeID {
			nodeSlots = node.SlotCount
			break
		}
	}

	if nodeSlots == 0 {
		// No slots left, proceed to forget phase
		logger.Info("Node has no slots after retry, proceeding to forget", "nodeID", nodeID)
		return cm.proceedToForget(ctx, kredis, nodeID, pods, clusterState, delta)
	}

	// Still has slots, recreate the migrate job
	logger.Info("Retrying slot migration", "nodeID", nodeID, "remainingSlots", nodeSlots)

	nodesToRemove := cm.identifyNodesToRemove(ctx, kredis, pods, clusterState)
	commandPod := cm.findCommandPodForScaleDown(pods, nodesToRemove, clusterState)
	if commandPod == nil {
		return fmt.Errorf("no command pod available for retry migration")
	}

	clusterAddr := clusterAddr(*commandPod, kredis.Spec.BasePort)

	if err := cm.JobManager.CreateScaleDownMigrateJob(ctx, kredis, nodeID, "", clusterAddr, nodeSlots); err != nil {
		return fmt.Errorf("failed to create retry migrate job: %w", err)
	}

	delta.LastClusterOperation = fmt.Sprintf("scaledown-migrate-in-progress:%s:%d", nodeID, time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateScalingDown)
	return nil
}

// checkScaleDownMigrateAndProceed checks migrate job status and proceeds to forget phase
func (cm *ClusterManager) checkScaleDownMigrateAndProceed(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	// Parse node ID from lastOp: "scaledown-migrate-in-progress:<nodeID>:<timestamp>"
	parts := strings.Split(lastOp, ":")
	if len(parts) < 2 {
		logger.Error(nil, "Invalid scaledown-migrate-in-progress format", "lastOp", lastOp)
		delta.LastClusterOperation = fmt.Sprintf("scaledown-failed:%d", time.Now().Unix())
		return nil
	}
	nodeID := parts[1]

	// Check job status
	jobResult, err := cm.JobManager.GetScaleDownJobStatus(ctx, kredis, JobTypeScaleDownMigrate, nodeID)
	if err != nil {
		logger.Error(err, "Failed to get migrate job status")
		return nil
	}

	switch jobResult.Status {
	case JobStatusNotFound:
		// Job was cleaned up but operation not complete - recreate
		logger.Info("Migrate job not found, checking if slots are migrated")

		// Check if node still has slots
		var nodeSlots int
		for _, node := range clusterState {
			if node.NodeID == nodeID {
				nodeSlots = node.SlotCount
				break
			}
		}

		if nodeSlots > 0 {
			logger.Info("Node still has slots, recreating migrate job", "slots", nodeSlots)
			commandPod := cm.findCommandPodForScaleDown(pods, cm.identifyNodesToRemove(ctx, kredis, pods, clusterState), clusterState)
			if commandPod != nil {
				clusterAddr := clusterAddr(*commandPod, kredis.Spec.BasePort)
				if err := cm.JobManager.CreateScaleDownMigrateJob(ctx, kredis, nodeID, "", clusterAddr, nodeSlots); err != nil {
					logger.Error(err, "Failed to recreate migrate job")
				}
			}
			return nil
		}

		// No slots - proceed to forget
		logger.Info("Slots migrated, proceeding to forget phase")
		return cm.proceedToForget(ctx, kredis, nodeID, pods, clusterState, delta)

	case JobStatusPending, JobStatusRunning:
		logger.Info("Migrate job still running", "job", jobResult.JobName)
		return nil

	case JobStatusSucceeded:
		logger.Info("Migrate job succeeded", "job", jobResult.JobName)
		_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)

		// Verify slots are actually migrated
		var nodeSlots int
		// Re-discover cluster state to get updated slot counts
		freshState, _ := cm.discoverClusterState(ctx, kredis, pods)
		for _, node := range freshState {
			if node.NodeID == nodeID {
				nodeSlots = node.SlotCount
				break
			}
		}

		if nodeSlots > 0 {
			logger.Info("Node still has slots after migrate job, retrying", "slots", nodeSlots)
			commandPod := cm.findCommandPodForScaleDown(pods, cm.identifyNodesToRemove(ctx, kredis, pods, clusterState), clusterState)
			if commandPod != nil {
				clusterAddr := clusterAddr(*commandPod, kredis.Spec.BasePort)
				if err := cm.JobManager.CreateScaleDownMigrateJob(ctx, kredis, nodeID, "", clusterAddr, nodeSlots); err != nil {
					logger.Error(err, "Failed to create retry migrate job")
				}
			}
			return nil
		}

		// Proceed to forget phase
		return cm.proceedToForget(ctx, kredis, nodeID, pods, clusterState, delta)

	case JobStatusFailed:
		logger.Error(nil, "Migrate job failed", "job", jobResult.JobName)
		_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)

		// Try to repair and retry
		commandPod := cm.findCommandPodForScaleDown(pods, cm.identifyNodesToRemove(ctx, kredis, pods, clusterState), clusterState)
		if commandPod != nil {
			_ = cm.PodExecutor.RepairCluster(ctx, *commandPod, kredis.Spec.BasePort)
		}

		delta.LastClusterOperation = fmt.Sprintf("scaledown-migrate-retry:%s:%d", nodeID, time.Now().Unix())
		return nil
	}

	return nil
}

// proceedToForget transitions from migrate to forget phase
func (cm *ClusterManager) proceedToForget(ctx context.Context, kredis *cachev1alpha1.Kredis, nodeID string, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Update phase in PendingScaleDown via delta
	pendingNodes := make([]cachev1alpha1.ScaleDownNode, len(kredis.Status.PendingScaleDown))
	copy(pendingNodes, kredis.Status.PendingScaleDown)
	for i, node := range pendingNodes {
		if node.NodeID == nodeID {
			pendingNodes[i].Phase = ScaleDownPhaseForgetting
			break
		}
	}
	delta.PendingScaleDown = pendingNodes

	nodesToRemove := cm.identifyNodesToRemove(ctx, kredis, pods, clusterState)
	commandPod := cm.findCommandPodForScaleDown(pods, nodesToRemove, clusterState)
	if commandPod == nil {
		return fmt.Errorf("no command pod available for forget")
	}

	clusterAddr := clusterAddr(*commandPod, kredis.Spec.BasePort)

	if err := cm.JobManager.CreateScaleDownForgetJob(ctx, kredis, nodeID, clusterAddr); err != nil {
		return fmt.Errorf("failed to create forget job: %w", err)
	}

	logger.Info("Created forget job, transitioning to forget phase", "nodeID", nodeID)
	delta.LastClusterOperation = fmt.Sprintf("scaledown-forget-in-progress:%s:%d", nodeID, time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateScalingDown)
	return nil
}

// checkScaleDownForgetAndProceed checks forget job status and proceeds to next node or completion
func (cm *ClusterManager) checkScaleDownForgetAndProceed(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	// Parse node ID from lastOp: "scaledown-forget-in-progress:<nodeID>:<timestamp>"
	parts := strings.Split(lastOp, ":")
	if len(parts) < 2 {
		logger.Error(nil, "Invalid scaledown-forget-in-progress format", "lastOp", lastOp)
		delta.LastClusterOperation = fmt.Sprintf("scaledown-failed:%d", time.Now().Unix())
		return nil
	}
	nodeID := parts[1]

	// Check job status
	jobResult, err := cm.JobManager.GetScaleDownJobStatus(ctx, kredis, JobTypeScaleDownForget, nodeID)
	if err != nil {
		logger.Error(err, "Failed to get forget job status")
		return nil
	}

	switch jobResult.Status {
	case JobStatusNotFound:
		// Check if node is already forgotten (not in cluster state)
		nodeExists := false
		for _, node := range clusterState {
			if node.NodeID == nodeID {
				nodeExists = true
				break
			}
		}

		if nodeExists {
			// Recreate job
			nodesToRemove := cm.identifyNodesToRemove(ctx, kredis, pods, clusterState)
			commandPod := cm.findCommandPodForScaleDown(pods, nodesToRemove, clusterState)
			if commandPod != nil {
				clusterAddr := clusterAddr(*commandPod, kredis.Spec.BasePort)
				if err := cm.JobManager.CreateScaleDownForgetJob(ctx, kredis, nodeID, clusterAddr); err != nil {
					logger.Error(err, "Failed to recreate forget job")
				}
			}
			return nil
		}

		// Node already forgotten, proceed to next
		logger.Info("Node already removed from cluster", "nodeID", nodeID)
		return cm.markNodeCompletedAndContinue(ctx, kredis, nodeID, pods, clusterState, delta)

	case JobStatusPending, JobStatusRunning:
		logger.Info("Forget job still running", "job", jobResult.JobName)
		return nil

	case JobStatusSucceeded:
		logger.Info("Forget job succeeded", "job", jobResult.JobName)
		_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)
		return cm.markNodeCompletedAndContinue(ctx, kredis, nodeID, pods, clusterState, delta)

	case JobStatusFailed:
		logger.Error(nil, "Forget job failed - node may still be in cluster", "job", jobResult.JobName)
		_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)

		// Check if node is still in cluster
		nodeExists := false
		for _, node := range clusterState {
			if node.NodeID == nodeID {
				nodeExists = true
				break
			}
		}

		if nodeExists {
			// Retry forget
			nodesToRemove := cm.identifyNodesToRemove(ctx, kredis, pods, clusterState)
			commandPod := cm.findCommandPodForScaleDown(pods, nodesToRemove, clusterState)
			if commandPod != nil {
				clusterAddr := clusterAddr(*commandPod, kredis.Spec.BasePort)
				if err := cm.JobManager.CreateScaleDownForgetJob(ctx, kredis, nodeID, clusterAddr); err != nil {
					logger.Error(err, "Failed to create retry forget job")
				}
			}
			return nil
		}

		// Node already gone, continue
		return cm.markNodeCompletedAndContinue(ctx, kredis, nodeID, pods, clusterState, delta)
	}

	return nil
}

// markNodeCompletedAndContinue marks a node as completed and sets up for next reconcile
func (cm *ClusterManager) markNodeCompletedAndContinue(ctx context.Context, kredis *cachev1alpha1.Kredis, completedNodeID string, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Mark node as completed via delta
	pendingNodes := make([]cachev1alpha1.ScaleDownNode, len(kredis.Status.PendingScaleDown))
	copy(pendingNodes, kredis.Status.PendingScaleDown)
	for i, node := range pendingNodes {
		if node.NodeID == completedNodeID {
			pendingNodes[i].Phase = ScaleDownPhaseCompleted
			logger.Info("Marked node as completed", "nodeID", completedNodeID, "pod", node.PodName)
			break
		}
	}
	delta.PendingScaleDown = pendingNodes

	// Check if there are more nodes to process
	hasMoreNodes := false
	for _, node := range pendingNodes {
		if node.Phase != ScaleDownPhaseCompleted {
			hasMoreNodes = true
			logger.Info("More nodes pending for scale-down", "nextNodeID", node.NodeID, "phase", node.Phase)
			break
		}
	}

	if hasMoreNodes {
		// Set status for next reconcile to continue processing
		// DO NOT use recursive call - it causes infinite loop because kredis.Status is not yet updated
		delta.LastClusterOperation = fmt.Sprintf("scaledown-in-progress:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateScalingDown)
		return nil // Next reconcile will pick up where we left off
	}

	// All nodes completed
	return cm.finalizeScaleDown(ctx, kredis, pods, clusterState, delta)
}

// finalizeScaleDown completes the scale-down operation and marks pods for deletion
func (cm *ClusterManager) finalizeScaleDown(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Verify all nodes are properly removed from cluster
	nodesToRemove := cm.identifyNodesToRemove(ctx, kredis, pods, clusterState)

	// Re-discover cluster state
	freshState, _ := cm.discoverClusterState(ctx, kredis, pods)

	// Check if any nodes to remove are still in the cluster
	removeSet := make(map[string]struct{})
	for _, node := range kredis.Status.PendingScaleDown {
		removeSet[node.NodeID] = struct{}{}
	}

	stillInCluster := 0
	for _, node := range freshState {
		if _, shouldBeRemoved := removeSet[node.NodeID]; shouldBeRemoved {
			if node.NodeID != "" && node.Role != "unknown" {
				stillInCluster++
				logger.Info("Node still in cluster after scale-down", "nodeID", node.NodeID, "pod", node.PodName)
			}
		}
	}

	if stillInCluster > 0 && len(nodesToRemove) > 0 {
		logger.Info("Some nodes still in cluster, continuing scale-down", "count", stillInCluster)
		delta.LastClusterOperation = fmt.Sprintf("scaledown-in-progress:%d", time.Now().Unix())
		return nil
	}

	// All nodes successfully removed from cluster
	logger.Info("Scale-down complete - all nodes removed from cluster, ready to delete pods")

	// Set ScaleDownReady flag to allow controller to delete pods
	scaleDownReady := true
	delta.ScaleDownReady = &scaleDownReady

	// Collect pod names to delete
	var podsToDelete []string
	for _, node := range kredis.Status.PendingScaleDown {
		if node.PodName != "" {
			podsToDelete = append(podsToDelete, node.PodName)
		}
	}
	delta.PodsToDelete = podsToDelete

	// Clear PendingScaleDown via delta
	delta.PendingScaleDown = []cachev1alpha1.ScaleDownNode{} // Empty slice to clear

	delta.LastClusterOperation = fmt.Sprintf("scaledown-success:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)

	// Update LastScaleTime to enable stabilization window for autoscaling
	now := time.Now()
	delta.LastScaleTime = &now
	delta.LastScaleType = "masters-down" // Scale-down operation

	return nil
}
