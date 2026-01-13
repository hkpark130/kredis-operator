package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// ========================================
// Common utility functions for cluster operations
// ========================================

// parsePodIndices extracts master index and replica index from pod name.
// Pod name format: <kredis-name>-<masterIdx>-<replicaIdx>
// Examples:
//   - "kredis-sample-0-0" -> (0, 0) - master 0
//   - "kredis-sample-0-1" -> (0, 1) - replica of master 0
//   - "kredis-sample-2-1" -> (2, 1) - replica of master 2
//
// Returns (-1, -1) if parsing fails.
func parsePodIndices(podName string) (masterIdx, replicaIdx int) {
	parts := strings.Split(podName, "-")
	if len(parts) < 2 {
		return -1, -1
	}

	// Last two parts are masterIdx and replicaIdx
	replicaStr := parts[len(parts)-1]
	masterStr := parts[len(parts)-2]

	masterIdx, err1 := strconv.Atoi(masterStr)
	replicaIdx, err2 := strconv.Atoi(replicaStr)

	if err1 != nil || err2 != nil {
		return -1, -1
	}

	return masterIdx, replicaIdx
}

// clusterAddr returns the cluster address in "ip:port" format for a given pod
func clusterAddr(pod corev1.Pod, port int32) string {
	return fmt.Sprintf("%s:%d", pod.Status.PodIP, port)
}

// clusterAddrFromNode returns the cluster address in "ip:port" format for a given ClusterNode
func clusterAddrFromNode(node cachev1alpha1.ClusterNode) string {
	return fmt.Sprintf("%s:%d", node.IP, node.Port)
}

// isNodeResetNeeded checks if a node needs to be reset before cluster operations.
// Returns true if the node needs reset (has data or knows other nodes).
// Returns false if the node is already in a clean state.
func (cm *ClusterManager) isNodeResetNeeded(ctx context.Context, pod corev1.Pod, port int32) bool {
	logger := log.FromContext(ctx)

	if !cm.isPodReady(pod) {
		// Pod not ready, can't check - assume reset is needed
		return true
	}

	// Check DBSIZE - if > 0, data exists and reset is needed
	dbsizeResult, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, port, "DBSIZE")
	if err != nil {
		logger.V(1).Info("Failed to get DBSIZE, assuming reset needed", "pod", pod.Name, "error", err)
		return true
	}
	// DBSIZE returns "(integer) N" format - check if it's not 0
	if !strings.Contains(dbsizeResult.Stdout, "0") {
		logger.V(1).Info("Node has data, reset needed", "pod", pod.Name, "dbsize", dbsizeResult.Stdout)
		return true
	}

	// Check CLUSTER INFO - if cluster_known_nodes > 1, node knows other nodes
	info, err := cm.PodExecutor.CheckRedisClusterInfo(ctx, pod, port)
	if err != nil {
		logger.V(1).Info("Failed to get cluster info, assuming reset needed", "pod", pod.Name, "error", err)
		return true
	}
	knownNodes := info["cluster_known_nodes"]
	if knownNodes != "1" && knownNodes != "" {
		logger.V(1).Info("Node knows other nodes, reset needed", "pod", pod.Name, "knownNodes", knownNodes)
		return true
	}

	// Node is already clean - no reset needed
	logger.V(1).Info("Node is already clean, no reset needed", "pod", pod.Name)
	return false
}

// isNodeReset checks if a single node has completed FLUSHALL + CLUSTER RESET.
// This is used to verify reset completion after initiating a reset.
// Checks both DBSIZE (must be 0) and cluster_known_nodes (must be 1)
func (cm *ClusterManager) isNodeReset(ctx context.Context, pod corev1.Pod, port int32) bool {
	logger := log.FromContext(ctx)

	if !cm.isPodReady(pod) {
		return false
	}

	// Check DBSIZE - must be 0 after FLUSHALL
	dbsizeResult, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, port, "DBSIZE")
	if err != nil {
		logger.V(1).Info("Failed to get DBSIZE", "pod", pod.Name, "error", err)
		return false
	}
	// DBSIZE returns "(integer) 0" or similar format
	trimmed := strings.TrimSpace(dbsizeResult.Stdout)
	numStr := strings.TrimPrefix(trimmed, "(integer) ") // "(integer) 0" → "0"
	num, err := strconv.Atoi(strings.TrimSpace(numStr))
	if err != nil || num != 0 {
		logger.V(1).Info("Node still has data (FLUSHALL not complete)", "pod", pod.Name, "dbsize", dbsizeResult.Stdout)
		return false
	}

	// Check CLUSTER INFO - cluster_known_nodes must be 1 after CLUSTER RESET
	info, err := cm.PodExecutor.CheckRedisClusterInfo(ctx, pod, port)
	if err != nil {
		logger.V(1).Info("Failed to get cluster info", "pod", pod.Name, "error", err)
		return false
	}
	knownNodes := info["cluster_known_nodes"]
	return knownNodes == "1"
}

// resetNode resets a Redis node by executing FLUSHALL and CLUSTER RESET commands.
// Should be called after checking isNodeResetNeeded() to avoid unnecessary resets.
func (cm *ClusterManager) resetNode(ctx context.Context, pod corev1.Pod, basePort int32) error {
	logger := log.FromContext(ctx)

	logger.V(1).Info("Executing FLUSHALL on pod", "pod", pod.Name)
	if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, basePort, "FLUSHALL"); err != nil {
		logger.Error(err, "Failed to flush node, but proceeding", "pod", pod.Name)
	}

	logger.V(1).Info("Executing CLUSTER RESET on pod", "pod", pod.Name)
	if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, basePort, "CLUSTER", "RESET"); err != nil {
		return fmt.Errorf("failed to reset node %s: %w", pod.Name, err)
	}

	return nil
}

// resetNodeIfNeeded checks if a node needs reset and performs reset if necessary.
// Returns (wasReset, error) - wasReset is true if reset was performed.
func (cm *ClusterManager) resetNodeIfNeeded(ctx context.Context, pod corev1.Pod, basePort int32) (bool, error) {
	logger := log.FromContext(ctx)

	if !cm.isNodeResetNeeded(ctx, pod, basePort) {
		logger.V(1).Info("Node already clean, skipping reset", "pod", pod.Name)
		return false, nil
	}

	if err := cm.resetNode(ctx, pod, basePort); err != nil {
		return false, err
	}

	return true, nil
}

// areAllNodesReset checks if all nodes have completed FLUSHALL + CLUSTER RESET.
// This is used during cluster creation to verify all nodes are clean before proceeding.
func (cm *ClusterManager) areAllNodesReset(ctx context.Context, pods []corev1.Pod, port int32) bool {
	logger := log.FromContext(ctx)

	for _, pod := range pods {
		if !cm.isNodeReset(ctx, pod, port) {
			logger.V(1).Info("Node not yet reset", "pod", pod.Name)
			return false
		}
	}

	logger.Info("All nodes have been reset (DBSIZE=0, cluster_known_nodes=1)")
	return true
}

// findPodsNeedingReset returns a list of pods that need to be reset before cluster operations.
func (cm *ClusterManager) findPodsNeedingReset(ctx context.Context, pods []corev1.Pod, port int32) []corev1.Pod {
	var needReset []corev1.Pod
	for _, pod := range pods {
		if cm.isNodeResetNeeded(ctx, pod, port) {
			needReset = append(needReset, pod)
		}
	}
	return needReset
}

// ========================================
// Node cluster membership functions
// ========================================

// isNodeInCluster checks if a pod is already part of the Redis cluster.
// Returns (isInCluster, nodeInfo) where nodeInfo contains the node's current state.
// A node is considered "in cluster" only if it has a valid NodeID and known role.
// Nodes with role="unknown" or empty NodeID are NOT considered in cluster.
func (cm *ClusterManager) isNodeInCluster(pod corev1.Pod, clusterState []cachev1alpha1.ClusterNode) (bool, *cachev1alpha1.ClusterNode) {
	for i, node := range clusterState {
		if node.PodName == pod.Name || node.IP == pod.Status.PodIP {
			// Only consider as "in cluster" if node has a valid NodeID and known role
			// Nodes with role="unknown" or no NodeID are not actually joined yet
			if node.NodeID != "" && node.Role != "unknown" && node.Role != "" {
				return true, &clusterState[i]
			}
			// Node found in state but not actually joined - return false
			return false, nil
		}
	}
	return false, nil
}
