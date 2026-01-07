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
// 스케일업 순서: 1) 마스터 모두 추가 → 2) 리밸런싱 → 3) Slave 추가
// 이 순서를 지키지 않으면 빈 마스터에 slave를 붙이려다 실패하는 문제가 발생함
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

	// 리밸런싱 진행 중이면 스케일 작업 대기 (마스터 추가는 허용, 슬레이브만 대기)
	isRebalancing := strings.Contains(lastOp, "rebalance-in-progress") ||
		strings.Contains(lastOp, "reshard-in-progress")

	// clusterState 기반으로 새 파드 필터링 (실제 Redis 클러스터 상태 사용)
	// 이렇게 하면 여러 reconcile이 동시에 실행되어도 이미 클러스터에 조인된
	// 노드를 중복으로 추가하려는 시도를 방지할 수 있음
	newPods := filterNewPods(pods, clusterState)
	if len(newPods) == 0 {
		// 새 파드가 없으면 스케일 완료 확인
		logger.Info("No new pods to add; checking scale completion")
		return cm.finalizeScale(ctx, kredis, pods, clusterState, commandPod, delta)
	}

	// 1. 현재 JoinedPods에 포함된 마스터 노드 집계
	masterNodes := cm.getJoinedMasterNodes(kredis, clusterState)
	mastersNeeded := int(kredis.Spec.Masters) - len(masterNodes)

	// 2. 새로 추가될 파드 이름 오름차순 정렬
	sort.Slice(newPods, func(i, j int) bool {
		return newPods[i].Name < newPods[j].Name
	})

	// 3. 스케일업 전략 결정: 마스터 먼저 모두 추가 → 리밸런싱 → Slave 추가
	var targetPod corev1.Pod
	var isMaster bool

	if mastersNeeded > 0 {
		// Phase 1: 마스터가 부족하면 마스터부터 추가
		// 리밸런싱 중이어도 마스터 추가는 진행 (마스터가 모두 추가되어야 리밸런싱 가능)
		targetPod = newPods[0]
		isMaster = true
		logger.Info("Phase 1: Adding master node", "pod", targetPod.Name, "mastersNeeded", mastersNeeded)
	} else {
		// 마스터가 충분함 - 리밸런싱 필요 여부 확인
		// 모든 마스터가 슬롯을 가지고 있어야 slave를 안전하게 추가할 수 있음
		allMastersHaveSlots := cm.checkAllJoinedMastersHaveSlots(kredis, clusterState)
		if !allMastersHaveSlots {
			// Phase 2: 리밸런싱 필요 - scale에서 rebalance로 전환
			logger.Info("Phase 2: All masters added, but some have no slots. Triggering rebalance before adding slaves.")
			delta.LastClusterOperation = fmt.Sprintf("rebalance-needed:%d", time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
			return nil
		}

		// Phase 3: 슬레이브 추가 전 리밸런싱 완료 여부 확인
		// 리밸런싱이 진행 중이면 슬레이브 추가를 대기
		if isRebalancing {
			logger.Info("Rebalancing in progress, waiting before adding slaves",
				"lastOp", lastOp, "pendingSlaves", len(newPods))
			return nil
		}

		// Phase 3: 모든 마스터가 슬롯을 가지고 있고 리밸런싱 완료됨 -> Slave 추가
		targetPod = newPods[0]
		isMaster = false
		logger.Info("Phase 3: Adding slave node", "pod", targetPod.Name)
	}

	// 4. 노드가 이미 클러스터에 있는지 확인
	isInCluster, existingNode := cm.isNodeInCluster(targetPod, clusterState)
	if isInCluster {
		logger.Info("Node already in cluster, handling existing node",
			"pod", targetPod.Name,
			"role", existingNode.Role,
			"nodeID", existingNode.NodeID)
		return cm.handleExistingNode(ctx, kredis, targetPod, commandPod, existingNode, isMaster, clusterState, delta)
	}

	// 5. 노드 리셋 필요 여부 체크
	// 중요: 파드가 ready가 아니면 리셋을 시도하지 않고 다음 reconcile에서 다시 확인
	if !cm.isPodReady(targetPod) {
		logger.Info("Pod not ready yet, waiting before checking reset status", "pod", targetPod.Name)
		return nil
	}

	// 중요: 리셋 명령 실행 전에 상태를 먼저 저장하여 다음 reconcile에서 중복 리셋 방지
	needsReset := cm.isNodeResetNeeded(ctx, targetPod, kredis.Spec.BasePort)
	if needsReset {
		// 리셋 전에 pending 상태를 먼저 설정하여 다음 reconcile에서 중복 리셋 방지
		delta.LastClusterOperation = fmt.Sprintf("scale-reset-pending:%s:%v:%d", targetPod.Name, isMaster, time.Now().Unix())

		if err := cm.resetNode(ctx, targetPod, kredis.Spec.BasePort); err != nil {
			return err
		}

		// 리셋 명령 후 바로 완료 여부 확인
		if !cm.isNodeReset(ctx, targetPod, kredis.Spec.BasePort) {
			logger.Info("Node reset initiated, waiting for completion", "pod", targetPod.Name)
			return nil
		}
		// 리셋이 즉시 완료된 경우 계속 진행
		logger.Info("Node reset completed immediately", "pod", targetPod.Name)
	}

	// 6. 리셋이 필요 없었거나 즉시 완료된 경우, 노드 추가 진행

	// 7. 노드 추가
	return cm.addNodeToCluster(ctx, kredis, targetPod, commandPod, isMaster, clusterState, delta)
}

// checkAllJoinedMastersHaveSlots checks if all joined master nodes have at least one slot
func (cm *ClusterManager) checkAllJoinedMastersHaveSlots(kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode) bool {
	joinedPodsSet := make(map[string]struct{})
	for _, podName := range kredis.Status.JoinedPods {
		joinedPodsSet[podName] = struct{}{}
	}

	for _, node := range clusterState {
		if _, isJoined := joinedPodsSet[node.PodName]; isJoined && node.Role == "master" {
			if node.SlotCount == 0 {
				return false
			}
		}
	}
	return true
}

// handleExistingNode handles a node that is already in the cluster but not in JoinedPods
// This can happen when:
// 1. Previous add-node succeeded but JoinedPods wasn't updated
// 2. Node was added as master but should be slave (or vice versa)
func (cm *ClusterManager) handleExistingNode(ctx context.Context, kredis *cachev1alpha1.Kredis, targetPod corev1.Pod, commandPod *corev1.Pod, existingNode *cachev1alpha1.ClusterNode, shouldBeMaster bool, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	// Add to JoinedPods if not already there
	if !containsString(delta.JoinedPods, targetPod.Name) {
		delta.JoinedPods = append(delta.JoinedPods, targetPod.Name)
	}

	currentIsMaster := existingNode.Role == "master"

	// Case 1: Node is master, should be master -> OK
	if currentIsMaster && shouldBeMaster {
		logger.Info("Node already in cluster as master (correct role)", "pod", targetPod.Name)
		delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
		return nil
	}

	// Case 2: Node is slave, should be slave -> OK
	if !currentIsMaster && !shouldBeMaster {
		logger.Info("Node already in cluster as slave (correct role)", "pod", targetPod.Name)
		delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
		return nil
	}

	// Case 3: Node is master, should be slave -> Convert to slave
	if currentIsMaster && !shouldBeMaster {
		// Only convert if master has no slots (empty master)
		if existingNode.SlotCount == 0 {
			masterNodes := cm.getJoinedMasterNodes(kredis, clusterState)
			targetMasterID, err := cm.selectMasterForReplica(kredis, clusterState, masterNodes)
			if err != nil {
				logger.Info("No master available for replication, keeping as master for now", "pod", targetPod.Name)
				delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
				return nil
			}
			logger.Info("Converting empty master to slave", "pod", targetPod.Name, "targetMaster", targetMasterID)
			_, err = cm.PodExecutor.ReplicateNode(ctx, targetPod, kredis.Spec.BasePort, targetMasterID)
			if err != nil {
				return fmt.Errorf("failed to convert node %s to slave: %w", targetPod.Name, err)
			}
		} else {
			// Master has slots, can't safely convert - treat as valid master
			logger.Info("Node is master with slots, keeping as master", "pod", targetPod.Name, "slots", existingNode.SlotCount)
		}
		delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
		return nil
	}

	// Case 4: Node is slave, should be master -> Keep as is for now
	// This is a complex case - would need to failover or promote
	// For now, just acknowledge the node is in the cluster
	logger.Info("Node is slave but should be master, keeping as slave for now", "pod", targetPod.Name)
	delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
	return nil
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

// isNodeReset is now in utils.go as a common function

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

// resetNode is now in utils.go as a common function

// selectMasterForReplica selects the best master for a new replica.
// Selection priority:
// 1. Masters WITH slots that need more replicas (sorted by fewest replicas first)
// 2. Masters WITHOUT slots only if all masters with slots have enough replicas
//
// This prevents the redis-cli --cluster add-node bug where adding a replica to an
// empty master (no slots) can cause the node to be added as a master instead.
func (cm *ClusterManager) selectMasterForReplica(kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode, masterNodes []cachev1alpha1.ClusterNode) (string, error) {
	joinedPodsSet := make(map[string]struct{})
	for _, podName := range kredis.Status.JoinedPods {
		joinedPodsSet[podName] = struct{}{}
	}

	// 각 마스터별 replica 수 집계
	replicaMap := make(map[string]int)
	masterSlotMap := make(map[string]int) // nodeID -> slotCount
	for _, m := range masterNodes {
		replicaMap[m.NodeID] = 0
		masterSlotMap[m.NodeID] = m.SlotCount
	}
	for _, node := range clusterState {
		if _, ok := joinedPodsSet[node.PodName]; ok && node.Role == "slave" && node.MasterID != "" {
			replicaMap[node.MasterID]++
		}
	}

	// Phase 1: 슬롯이 있는 마스터 중 replica가 부족한 마스터 선택
	var targetMasterID string
	minReplicas := int(kredis.Spec.Replicas) + 1
	for masterID, count := range replicaMap {
		slotCount := masterSlotMap[masterID]
		// 슬롯이 있는 마스터만 우선 선택
		if slotCount > 0 && count < int(kredis.Spec.Replicas) && count < minReplicas {
			minReplicas = count
			targetMasterID = masterID
		}
	}

	// Phase 2: 슬롯 있는 마스터가 모두 충분한 replica를 가진 경우, 빈 마스터도 고려
	if targetMasterID == "" {
		minReplicas = int(kredis.Spec.Replicas) + 1
		for masterID, count := range replicaMap {
			if count < int(kredis.Spec.Replicas) && count < minReplicas {
				minReplicas = count
				targetMasterID = masterID
			}
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
