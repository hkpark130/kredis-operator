package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

const (
	// InitialReshardSlots is the number of slots to move via reshard before rebalance.
	// This helps new empty masters quickly escape the 0-slot state.
	InitialReshardSlots = 100
)

// reshardCluster performs Phase 1 of rebalancing: move slots to empty masters via reshard.
// This is called when lastOp is "rebalance-needed" or "reshard-in-progress".
func (cm *ClusterManager) reshardCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	// Determine current phase
	if strings.Contains(lastOp, "reshard-in-progress") {
		// Verify reshard completion, then move to rebalance phase
		return cm.verifyReshardAndProceed(ctx, kredis, masterPod, delta)
	}

	// Start reshard phase: find empty masters and move slots to them
	emptyMasters := cm.findEmptyMasters(ctx, *masterPod, kredis.Spec.BasePort)
	if len(emptyMasters) == 0 {
		// No empty masters found, skip reshard and go directly to rebalance
		logger.Info("No empty masters found, proceeding directly to rebalance")
		delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
		return nil
	}

	logger.Info("Starting reshard phase for empty masters", "emptyMasterCount", len(emptyMasters))
	delta.LastClusterOperation = fmt.Sprintf("reshard-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)

	// Reshard slots to each empty master using --cluster-from all
	for _, emptyMaster := range emptyMasters {
		logger.Info("Resharding slots to empty master",
			"nodeID", emptyMaster.NodeID,
			"podName", emptyMaster.PodName,
			"slots", InitialReshardSlots)

		// Use --cluster-from all to take slots evenly from all existing masters
		result, err := cm.PodExecutor.ReshardCluster(ctx, *masterPod, kredis.Spec.BasePort, emptyMaster.NodeID, InitialReshardSlots, nil)
		if err != nil {
			// Reshard failed - this is a critical error, stop and retry in next reconcile
			logger.Error(err, "Reshard failed for empty master",
				"nodeID", emptyMaster.NodeID,
				"stdout", result.Stdout)
			delta.LastClusterOperation = fmt.Sprintf("reshard-failed:%d", time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
			return fmt.Errorf("reshard failed for node %s: %w", emptyMaster.NodeID, err)
		}
		logger.Info("Reshard command completed successfully",
			"nodeID", emptyMaster.NodeID,
			"podName", emptyMaster.PodName)
	}

	// Next reconcile will verify and proceed to rebalance
	return nil
}

// verifyReshardAndProceed checks if reshard is complete and cluster is healthy,
// then transitions to rebalance phase.
func (cm *ClusterManager) verifyReshardAndProceed(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Check cluster health
	healthy, err := cm.PodExecutor.IsClusterHealthy(ctx, *masterPod, kredis.Spec.BasePort)
	if err != nil {
		logger.Error(err, "Failed to check cluster health after reshard")
		return nil // Will retry in next reconcile
	}

	if !healthy {
		logger.Info("Cluster not yet healthy after reshard, waiting...")
		return nil // Will retry in next reconcile
	}

	// Verify all masters now have slots
	emptyMasters := cm.findEmptyMasters(ctx, *masterPod, kredis.Spec.BasePort)
	if len(emptyMasters) > 0 {
		logger.Info("Some masters still have no slots after reshard, retrying reshard",
			"emptyMasterCount", len(emptyMasters))
		// Retry reshard for remaining empty masters
		delta.LastClusterOperation = fmt.Sprintf("rebalance-needed:%d", time.Now().Unix())
		return nil
	}

	logger.Info("Reshard complete - all masters have slots, proceeding to rebalance phase")

	// Move to Phase 2: Rebalance
	delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)

	return nil
}

// findEmptyMasters finds masters that have 0 slots assigned by parsing CLUSTER NODES output.
// Masters with slots have slot ranges after "connected" (e.g., "0-5460", "5461-10922").
// Empty masters have no slot info after "connected".
func (cm *ClusterManager) findEmptyMasters(ctx context.Context, masterPod corev1.Pod, basePort int32) []EmptyMasterInfo {
	logger := log.FromContext(ctx)
	var emptyMasters []EmptyMasterInfo

	// Execute CLUSTER NODES command
	result, err := cm.PodExecutor.ExecuteRedisCommand(ctx, masterPod, basePort, "CLUSTER", "NODES")
	if err != nil {
		logger.Error(err, "Failed to execute CLUSTER NODES")
		return emptyMasters
	}

	lines := strings.Split(result.Stdout, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse the line
		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}

		nodeID := parts[0]
		ipPortStr := parts[1] // e.g., "10.244.0.5:7000@17000"
		flags := parts[2]     // e.g., "myself,master" or "master" or "slave"
		linkState := parts[7] // e.g., "connected"

		// Check if this is a master node
		if !strings.Contains(flags, "master") {
			continue
		}

		// Skip failed nodes
		if strings.Contains(flags, "fail") {
			continue
		}

		// Check if node is connected
		if linkState != "connected" {
			continue
		}

		// Check if this master has slots
		// Slots appear after position 7 (after "connected")
		hasSlots := false
		if len(parts) > 8 {
			// Check if any of the remaining parts look like slot ranges
			for i := 8; i < len(parts); i++ {
				part := parts[i]
				// Slot ranges look like "0-5460" or "5461" (single slot)
				if isSlotRange(part) {
					hasSlots = true
					break
				}
			}
		}

		if !hasSlots {
			ip := extractIP(ipPortStr)
			logger.Info("Found empty master (no slots)",
				"nodeID", nodeID,
				"ip", ip,
				"flags", flags)

			emptyMasters = append(emptyMasters, EmptyMasterInfo{
				NodeID: nodeID,
				IP:     ip,
			})
		}
	}

	return emptyMasters
}

// EmptyMasterInfo holds information about a master node with no slots
type EmptyMasterInfo struct {
	NodeID  string
	IP      string
	PodName string
}

// isSlotRange checks if a string looks like a slot range (e.g., "0-5460", "5461", "[5461-<-importing]")
func isSlotRange(s string) bool {
	// Skip importing/migrating indicators
	if strings.HasPrefix(s, "[") {
		return false
	}

	// Check for range like "0-5460"
	if strings.Contains(s, "-") {
		parts := strings.Split(s, "-")
		if len(parts) == 2 {
			_, err1 := strconv.Atoi(parts[0])
			_, err2 := strconv.Atoi(parts[1])
			return err1 == nil && err2 == nil
		}
		return false
	}

	// Check for single slot like "5461"
	_, err := strconv.Atoi(s)
	return err == nil
}

// extractIP extracts IP from "10.244.0.5:7000@17000" format
func extractIP(ipPortStr string) string {
	// Remove @cport if present
	if atIdx := strings.Index(ipPortStr, "@"); atIdx != -1 {
		ipPortStr = ipPortStr[:atIdx]
	}
	// Remove :port
	if colonIdx := strings.LastIndex(ipPortStr, ":"); colonIdx != -1 {
		return ipPortStr[:colonIdx]
	}
	return ipPortStr
}
