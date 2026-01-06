package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// scaleCluster handles adding or removing nodes from the cluster
// 노드를 하나씩 추가하고 scale-in-progress 상태로 반환, 다음 reconcile에서 나머지 처리
func (cm *ClusterManager) scaleCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	// If scale reset is pending, check if node has completed reset
	if strings.Contains(lastOp, "scale-reset-pending") {
		return cm.checkScaleResetAndProceed(ctx, kredis, pods, clusterState, delta)
	}

	logger.Info("Scaling up Redis cluster")

	delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateScaling)

	commandPod := cm.findMasterPod(pods, kredis, clusterState)
	if commandPod == nil {
		return fmt.Errorf("failed to find a pod for scaling command")
	}
	logger.Info("Using pod for scaling command", "pod", commandPod.Name)

	newPods := filterNewPods(pods, kredis.Status.JoinedPods)
	if len(newPods) == 0 {
		// 새 파드가 없으면 스케일 완료 확인
		logger.Info("No new pods to add; checking scale completion")
		return cm.finalizeScale(ctx, kredis, pods, clusterState, commandPod, delta)
	}

	// 1. 현재 JoinedPods에 포함된 마스터 노드 집계
	masterNodes := cm.getJoinedMasterNodes(kredis, clusterState)

	// 2. 새로 추가될 파드 이름 오름차순 정렬
	sort.Slice(newPods, func(i, j int) bool {
		return newPods[i].Name < newPods[j].Name
	})

	// 3. 마스터/슬레이브 분류
	mastersNeeded := int(kredis.Spec.Masters) - len(masterNodes)
	var targetPod corev1.Pod
	var isMaster bool

	if mastersNeeded > 0 {
		// 마스터가 부족하면 첫 번째 새 파드를 마스터로 추가
		targetPod = newPods[0]
		isMaster = true
	} else {
		// 마스터가 충분하면 슬레이브로 추가
		targetPod = newPods[0]
		isMaster = false
	}

	// 4. 노드 리셋
	if err := cm.resetNode(ctx, targetPod, kredis.Spec.BasePort); err != nil {
		return err
	}

	// 5. Verify node is reset before adding to cluster
	if !cm.isNodeReset(ctx, targetPod, kredis.Spec.BasePort) {
		logger.Info("Node not yet reset, waiting for next reconcile", "pod", targetPod.Name)
		// Store target pod info for next reconcile
		delta.LastClusterOperation = fmt.Sprintf("scale-reset-pending:%s:%v:%d", targetPod.Name, isMaster, time.Now().Unix())
		return nil
	}

	// 6. 노드 추가
	return cm.addNodeToCluster(ctx, kredis, targetPod, commandPod, isMaster, clusterState, delta)
}

// addNodeToCluster adds a single node to the cluster (master or slave)
func (cm *ClusterManager) addNodeToCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, targetPod corev1.Pod, commandPod *corev1.Pod, isMaster bool, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	masterNodes := cm.getJoinedMasterNodes(kredis, clusterState)

	if isMaster {
		logger.Info("Adding new master node to cluster", "pod", targetPod.Name, "ip", targetPod.Status.PodIP)
		if err := cm.PodExecutor.AddNodeToCluster(ctx, targetPod, *commandPod, kredis.Spec.BasePort, ""); err != nil {
			delta.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
			return fmt.Errorf("failed to add master node %s to cluster: %w", targetPod.Name, err)
		}
	} else {
		// 슬레이브로 추가할 마스터 선택
		targetMasterID, err := cm.selectMasterForReplica(kredis, clusterState, masterNodes)
		if err != nil {
			return err
		}
		logger.Info("Adding new slave node to cluster", "pod", targetPod.Name, "ip", targetPod.Status.PodIP, "masterID", targetMasterID)
		if err := cm.PodExecutor.AddNodeToCluster(ctx, targetPod, *commandPod, kredis.Spec.BasePort, targetMasterID); err != nil {
			delta.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
			return fmt.Errorf("failed to add slave node %s to cluster: %w", targetPod.Name, err)
		}
	}

	// JoinedPods에 추가
	if !containsString(delta.JoinedPods, targetPod.Name) {
		delta.JoinedPods = append(delta.JoinedPods, targetPod.Name)
	}

	// Update operation status
	delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateScaling)

	return nil
}

// checkScaleResetAndProceed checks if the pending reset is complete and proceeds with node addition
func (cm *ClusterManager) checkScaleResetAndProceed(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	// Parse target pod info from lastOp: "scale-reset-pending:podName:isMaster:timestamp"
	parts := strings.Split(lastOp, ":")
	if len(parts) < 3 {
		logger.Info("Invalid scale-reset-pending format, restarting scale", "lastOp", lastOp)
		delta.LastClusterOperation = ""
		return nil
	}

	targetPodName := parts[1]
	isMaster := parts[2] == "true"

	// Find target pod
	var targetPod *corev1.Pod
	for i := range pods {
		if pods[i].Name == targetPodName {
			targetPod = &pods[i]
			break
		}
	}
	if targetPod == nil {
		logger.Info("Target pod not found, restarting scale", "pod", targetPodName)
		delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
		return nil
	}

	// Check if node is reset
	if !cm.isNodeReset(ctx, *targetPod, kredis.Spec.BasePort) {
		logger.Info("Node still resetting, waiting for next reconcile", "pod", targetPodName)
		return nil
	}

	logger.Info("Node reset complete, proceeding to add to cluster", "pod", targetPodName, "isMaster", isMaster)

	// Find command pod
	commandPod := cm.findMasterPod(pods, kredis, clusterState)
	if commandPod == nil {
		return fmt.Errorf("failed to find a pod for scaling command")
	}

	return cm.addNodeToCluster(ctx, kredis, *targetPod, commandPod, isMaster, clusterState, delta)
}

// isNodeReset checks if a single node has completed FLUSHALL + CLUSTER RESET
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

// getJoinedMasterNodes returns master nodes that are in JoinedPods
func (cm *ClusterManager) getJoinedMasterNodes(kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode) []cachev1alpha1.ClusterNode {
	var masterNodes []cachev1alpha1.ClusterNode
	joinedPodsSet := make(map[string]struct{})
	for _, podName := range kredis.Status.JoinedPods {
		joinedPodsSet[podName] = struct{}{}
	}
	for _, node := range clusterState {
		if _, ok := joinedPodsSet[node.PodName]; ok && node.Role == "master" {
			masterNodes = append(masterNodes, node)
		}
	}
	return masterNodes
}

// resetNode resets a new node before adding to cluster
func (cm *ClusterManager) resetNode(ctx context.Context, pod corev1.Pod, basePort int32) error {
	logger := log.FromContext(ctx)

	logger.V(1).Info("Executing FLUSHALL on new pod", "pod", pod.Name)
	if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, basePort, "FLUSHALL"); err != nil {
		logger.Error(err, "Failed to flush new node, but proceeding", "pod", pod.Name)
	}

	logger.V(1).Info("Executing CLUSTER RESET on new pod", "pod", pod.Name)
	if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, basePort, "CLUSTER", "RESET"); err != nil {
		return fmt.Errorf("failed to reset new node %s: %w", pod.Name, err)
	}

	return nil
}

// selectMasterForReplica selects the best master for a new replica
func (cm *ClusterManager) selectMasterForReplica(kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode, masterNodes []cachev1alpha1.ClusterNode) (string, error) {
	joinedPodsSet := make(map[string]struct{})
	for _, podName := range kredis.Status.JoinedPods {
		joinedPodsSet[podName] = struct{}{}
	}

	// 각 마스터별 replica 수 집계
	replicaMap := make(map[string]int)
	for _, m := range masterNodes {
		replicaMap[m.NodeID] = 0
	}
	for _, node := range clusterState {
		if _, ok := joinedPodsSet[node.PodName]; ok && node.Role == "slave" && node.MasterID != "" {
			replicaMap[node.MasterID]++
		}
	}

	// replica가 spec보다 적은 마스터 중 가장 적은 마스터 선택
	var targetMasterID string
	minReplicas := int(kredis.Spec.Replicas) + 1
	for masterID, count := range replicaMap {
		if count < int(kredis.Spec.Replicas) && count < minReplicas {
			minReplicas = count
			targetMasterID = masterID
		}
	}

	if targetMasterID == "" {
		return "", fmt.Errorf("no master available for new replica")
	}

	return targetMasterID, nil
}

// finalizeScale checks if scale is complete and sets appropriate status
func (cm *ClusterManager) finalizeScale(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, commandPod *corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	expectedTotalPods := cm.getExpectedPodCount(kredis)

	// Non-blocking: check if all nodes are known
	if !cm.areAllNodesKnown(ctx, *commandPod, kredis.Spec.BasePort, int(expectedTotalPods)) {
		logger.Info("Not all nodes known yet, will check in next reconcile")
		return nil
	}

	// 모든 노드 인식됨
	known := int(expectedTotalPods)
	delta.KnownClusterNodes = &known

	// 현재 마스터 수 확인
	currentMasters := len(cm.getJoinedMasterNodes(kredis, clusterState))
	expectedMasters := int(kredis.Spec.Masters)

	// 마스터가 새로 추가되었는지 확인 (리밸런스 필요 여부)
	if currentMasters >= expectedMasters && currentMasters > 0 {
		// 마스터 수가 증가했다면 리밸런스 필요
		// (이전 상태에서 마스터가 추가된 경우)
		prevMasterCount := 0
		for _, node := range clusterState {
			if node.Role == "master" {
				prevMasterCount++
			}
		}

		if currentMasters > prevMasterCount || cm.needsRebalance(ctx, *commandPod, kredis.Spec.BasePort) {
			logger.Info("New masters added or rebalance needed", "currentMasters", currentMasters)
			delta.LastClusterOperation = fmt.Sprintf("rebalance-needed:%d", time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
			return nil
		}
	}

	logger.Info("Scale completed successfully")
	delta.LastClusterOperation = fmt.Sprintf("scale-success:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)
	return nil
}

// needsRebalance checks if any master has no slots assigned
func (cm *ClusterManager) needsRebalance(ctx context.Context, commandPod corev1.Pod, basePort int32) bool {
	ok, err := cm.checkAllMastersHaveSlots(ctx, commandPod, basePort)
	if err != nil {
		return true // 에러시 안전하게 리밸런스 진행
	}
	return !ok
}
