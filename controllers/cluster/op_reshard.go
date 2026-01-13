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

// reshardCluster performs Phase 1 of rebalancing: move slots to empty masters via reshard Job.
// This is called when lastOp is "rebalance-needed" or "reshard-in-progress".
// Non-blocking: Creates ONE Job at a time to avoid conflicts, subsequent reconciles handle the rest.
func (cm *ClusterManager) reshardCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	// Check if reshard is already in progress
	if strings.Contains(lastOp, "reshard-in-progress") {
		return cm.checkReshardJobsAndProceed(ctx, kredis, masterPod, delta)
	}

	// Start reshard phase: find empty masters
	emptyMasters := cm.findEmptyMasters(ctx, *masterPod, kredis.Spec.BasePort)
	if len(emptyMasters) == 0 {
		// No empty masters found, skip reshard and go directly to rebalance
		logger.Info("No empty masters found, proceeding directly to rebalance")
		delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
		return nil
	}

	// Check if empty masters are expected based on desired master count
	// If actual master count equals desired, empty masters are a transient state (e.g., during scale-down)
	// and should not trigger rebalancing
	actualMasterCount, err := cm.getActualMasterCount(ctx, []corev1.Pod{*masterPod}, kredis.Spec.BasePort)
	if err == nil {
		desiredMasters := int(kredis.Spec.Masters)
		mastersWithSlots := actualMasterCount - len(emptyMasters)

		// If masters with slots already equals or exceeds desired count, ignore empty masters
		// This handles cases like:
		// - Scale-down scenario: 5 masters → 3 masters (2 masters become empty before removal)
		// - Already at desired state: 3 masters with slots, spec wants 3
		if mastersWithSlots >= desiredMasters {
			logger.Info("Ignoring empty masters - current slot-holding masters meets desired count",
				"mastersWithSlots", mastersWithSlots,
				"desiredMasters", desiredMasters,
				"emptyMasters", len(emptyMasters))
			delta.LastClusterOperation = fmt.Sprintf("rebalance-success:%d", time.Now().Unix())

			// Check if scaling is still needed (more nodes to add, e.g., slaves)
			if cm.isScalingStillNeeded(ctx, kredis, masterPod) {
				logger.Info("Reshard skipped but scaling still in progress (more nodes to add)")
				delta.ClusterState = string(cachev1alpha1.ClusterStateScaling)
			} else {
				delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)
			}
			return nil
		}
	}

	logger.Info("Starting reshard phase", "emptyMasterCount", len(emptyMasters))
	delta.LastClusterOperation = fmt.Sprintf("reshard-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)

	// Build cluster address for redis-cli
	clusterAddr := fmt.Sprintf("%s:%d", masterPod.Status.PodIP, kredis.Spec.BasePort)

	// IMPORTANT: Create only ONE reshard Job at a time to avoid conflicts
	// Multiple concurrent reshard operations can cause cluster state inconsistency
	firstEmptyMaster := emptyMasters[0]
	targetAddr := fmt.Sprintf("%s:%d", firstEmptyMaster.IP, kredis.Spec.BasePort)

	logger.Info("Creating reshard Job for first empty master (sequential processing)",
		"nodeID", firstEmptyMaster.NodeID,
		"ip", firstEmptyMaster.IP,
		"slots", InitialReshardSlots,
		"remainingEmptyMasters", len(emptyMasters)-1)

	if err := cm.JobManager.CreateReshardJob(ctx, kredis, firstEmptyMaster.NodeID, targetAddr, clusterAddr, InitialReshardSlots); err != nil {
		logger.Error(err, "Failed to create reshard Job", "nodeID", firstEmptyMaster.NodeID)
		return err
	}

	// Return immediately - next reconcile will check Job status and process remaining empty masters
	return nil
}

// checkReshardJobsAndProceed monitors reshard Job status and proceeds to next empty master or rebalance.
// Processes ONE empty master at a time to avoid concurrent reshard conflicts.
func (cm *ClusterManager) checkReshardJobsAndProceed(ctx context.Context, kredis *cachev1alpha1.Kredis, masterPod *corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Check reshard Job status
	jobResult, err := cm.JobManager.GetJobStatus(ctx, kredis, JobTypeReshard)
	if err != nil {
		logger.Error(err, "Failed to get reshard Job status")
		return nil // Will retry in next reconcile
	}

	switch jobResult.Status {
	case JobStatusNotFound:
		// No reshard jobs - check if there are still empty masters
		emptyMasters := cm.findEmptyMasters(ctx, *masterPod, kredis.Spec.BasePort)
		if len(emptyMasters) > 0 {
			logger.Info("No reshard Job found but empty masters exist - creating Job for first one", "count", len(emptyMasters))
			// Create Job for first empty master only
			clusterAddr := fmt.Sprintf("%s:%d", masterPod.Status.PodIP, kredis.Spec.BasePort)
			firstEM := emptyMasters[0]
			targetAddr := fmt.Sprintf("%s:%d", firstEM.IP, kredis.Spec.BasePort)
			if err := cm.JobManager.CreateReshardJob(ctx, kredis, firstEM.NodeID, targetAddr, clusterAddr, InitialReshardSlots); err != nil {
				logger.Error(err, "Failed to create reshard Job")
			}
			return nil
		}
		// No empty masters, proceed to rebalance
		logger.Info("No empty masters remaining, proceeding to rebalance")
		delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
		return nil

	case JobStatusPending, JobStatusRunning:
		// Job is still in progress - just return and wait
		logger.Info("Reshard Job still running", "job", jobResult.JobName)
		return nil

	case JobStatusSucceeded:
		logger.Info("Reshard Job succeeded", "job", jobResult.JobName)

		// Wait for cluster to stabilize BEFORE cleaning up the Job
		// This ensures we can re-enter this case if stabilization fails
		healthy, err := cm.PodExecutor.IsClusterHealthy(ctx, *masterPod, kredis.Spec.BasePort)
		if err != nil || !healthy {
			logger.Info("Waiting for cluster to stabilize after reshard")
			return nil // Job still exists, will re-enter JobStatusSucceeded on next reconcile
		}

		// Cleanup the completed job only after cluster is stable
		_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)

		// Check if more empty masters exist
		emptyMasters := cm.findEmptyMasters(ctx, *masterPod, kredis.Spec.BasePort)
		if len(emptyMasters) > 0 {
			logger.Info("More empty masters found - creating Job for next one (sequential)", "count", len(emptyMasters))
			// Create Job for ONLY the first remaining empty master
			clusterAddr := fmt.Sprintf("%s:%d", masterPod.Status.PodIP, kredis.Spec.BasePort)
			firstEM := emptyMasters[0]
			targetAddr := fmt.Sprintf("%s:%d", firstEM.IP, kredis.Spec.BasePort)
			if err := cm.JobManager.CreateReshardJob(ctx, kredis, firstEM.NodeID, targetAddr, clusterAddr, InitialReshardSlots); err != nil {
				logger.Error(err, "Failed to create reshard Job")
			}
			return nil
		}

		// All empty masters handled, proceed to rebalance
		logger.Info("All reshard Jobs complete, proceeding to rebalance phase")
		delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
		return nil

	case JobStatusFailed:
		logger.Error(nil, "Reshard Job failed", "job", jobResult.JobName, "message", jobResult.Message)

		// Try to fix the cluster before retrying
		logger.Info("Attempting to fix cluster before retry")
		if err := cm.PodExecutor.RepairCluster(ctx, *masterPod, kredis.Spec.BasePort); err != nil {
			logger.Error(err, "Failed to repair cluster")
		}

		// Cleanup the failed job
		_ = cm.JobManager.CleanupCompletedJobs(ctx, kredis)

		// Reset to rebalance-needed to retry
		delta.LastClusterOperation = fmt.Sprintf("rebalance-needed:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
		return nil // Don't return error - let it retry
	}

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
