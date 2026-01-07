package cluster

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// ========================================
// Common utility functions for cluster operations
// ========================================

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
	if !strings.Contains(dbsizeResult.Stdout, "0") {
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

// findMasterPod returns a ready master pod from JoinedPods if possible, otherwise any ready pod.
// Priority: JoinedPods masters > JoinedPods non-masters > any ready pod
// This ensures we use known cluster members for commands rather than new unjoined pods.
func (cm *ClusterManager) findMasterPodFromJoined(pods []corev1.Pod, kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode) *corev1.Pod {
	joinedPodsSet := buildJoinedPodsSet(kredis.Status.JoinedPods)
	podMap := make(map[string]corev1.Pod)
	for _, p := range pods {
		podMap[p.Name] = p
	}

	// 1st priority: JoinedPods masters
	for _, node := range clusterState {
		if _, isJoined := joinedPodsSet[node.PodName]; isJoined && node.Role == "master" {
			if pod, ok := podMap[node.PodName]; ok && cm.isPodReady(pod) {
				return &pod
			}
		}
	}

	return nil
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

// findNodeByPodName finds a cluster node by pod name
func findNodeByPodName(podName string, clusterState []cachev1alpha1.ClusterNode) *cachev1alpha1.ClusterNode {
	for i, node := range clusterState {
		if node.PodName == podName {
			return &clusterState[i]
		}
	}
	return nil
}

// findNodeByID finds a cluster node by node ID
func findNodeByID(nodeID string, clusterState []cachev1alpha1.ClusterNode) *cachev1alpha1.ClusterNode {
	for i, node := range clusterState {
		if node.NodeID == nodeID {
			return &clusterState[i]
		}
	}
	return nil
}

// ========================================
// JoinedPods cleanup functions
// ========================================

// CleanupStaleJoinedPods removes pod names from JoinedPods that:
// 1. No longer exist as actual pods
// 2. Are no longer part of the Redis cluster (based on clusterState)
// Returns the cleaned list of JoinedPods.
func (cm *ClusterManager) CleanupStaleJoinedPods(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode) []string {
	logger := log.FromContext(ctx)

	if len(kredis.Status.JoinedPods) == 0 {
		return nil
	}

	// Build sets for quick lookup
	existingPodNames := make(map[string]struct{})
	for _, pod := range pods {
		existingPodNames[pod.Name] = struct{}{}
	}

	// Build set of pods that are actually in the cluster (have a NodeID)
	clusterMemberPods := make(map[string]struct{})
	for _, node := range clusterState {
		if node.NodeID != "" && node.Status == "connected" {
			clusterMemberPods[node.PodName] = struct{}{}
		}
	}

	// Filter JoinedPods
	var cleanedJoinedPods []string
	var removedPods []string

	for _, podName := range kredis.Status.JoinedPods {
		// Check 1: Pod still exists?
		if _, exists := existingPodNames[podName]; !exists {
			removedPods = append(removedPods, podName+" (pod deleted)")
			continue
		}

		// Check 2: Pod is still in cluster? (has NodeID and is connected)
		// Note: If clusterState is empty/incomplete, we keep the pod to be safe
		if len(clusterState) > 0 {
			if _, inCluster := clusterMemberPods[podName]; !inCluster {
				// Double check - if the pod exists but has no NodeID, it might be a new pod
				// that's being added. Only remove if it was previously in cluster but now isn't.
				// For safety, only remove if pod exists but is not in cluster AND cluster has members
				if len(clusterMemberPods) > 0 {
					removedPods = append(removedPods, podName+" (not in cluster)")
					continue
				}
			}
		}

		// Pod is valid, keep it
		cleanedJoinedPods = append(cleanedJoinedPods, podName)
	}

	if len(removedPods) > 0 {
		logger.Info("Cleaned up stale JoinedPods",
			"removed", removedPods,
			"remaining", len(cleanedJoinedPods),
			"original", len(kredis.Status.JoinedPods))
	}

	return cleanedJoinedPods
}
