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
	"github.com/hkpark130/kredis-operator/controllers/resource"
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

	commandPod := cm.findMasterPod(pods, clusterState)
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

	// 2. 새로 추가될 파드 이름 오름차순 정렬
	sort.Slice(newPods, func(i, j int) bool {
		return newPods[i].Name < newPods[j].Name
	})

	// 3. 스케일업 전략 결정: 마스터 먼저 모두 추가 → 리밸런싱 → Slave 추가
	// Pod naming: kredis-{shard}-{instance} where instance 0 is initial master
	// Determine role based on current cluster state, not pod name
	// This handles failover scenarios where roles may have changed

	// Count current masters in cluster
	currentMasters := 0
	for _, node := range clusterState {
		if node.Role == "master" && node.NodeID != "" {
			currentMasters++
		}
	}

	// Separate new pods into initial-masters (instance 0) and initial-replicas (instance 1+)
	var initialMasterPods, initialReplicaPods []corev1.Pod
	for _, pod := range newPods {
		_, instanceIdx, err := resource.ParsePodName(kredis.Name, pod.Name)
		if err != nil {
			// Legacy or unparseable - determine role by cluster state
			if currentMasters < int(kredis.Spec.Masters) {
				initialMasterPods = append(initialMasterPods, pod)
			} else {
				initialReplicaPods = append(initialReplicaPods, pod)
			}
			continue
		}
		if instanceIdx == 0 {
			initialMasterPods = append(initialMasterPods, pod)
		} else {
			initialReplicaPods = append(initialReplicaPods, pod)
		}
	}

	var targetPod corev1.Pod
	var isMaster bool

	// Decide actual role based on cluster needs, not pod name
	// This handles failover scenarios where a replica pod may need to become master
	if currentMasters < int(kredis.Spec.Masters) {
		// Phase 1: 마스터가 부족하면 마스터부터 추가
		// Prefer initial-master pods, but use any available pod if needed
		if len(initialMasterPods) > 0 {
			targetPod = initialMasterPods[0]
		} else if len(initialReplicaPods) > 0 {
			targetPod = initialReplicaPods[0]
		} else {
			logger.Info("No new pods to add as master")
			return cm.finalizeScale(ctx, kredis, pods, clusterState, commandPod, delta)
		}
		isMaster = true
		logger.Info("Phase 1: Adding node as master", "pod", targetPod.Name, "currentMasters", currentMasters, "needed", kredis.Spec.Masters)
	} else if len(initialReplicaPods) > 0 || len(initialMasterPods) > 0 {
		// 마스터가 충분함 - 리밸런싱 필요 여부 확인
		allMastersHaveSlots, err := cm.checkAllMastersHaveSlots(ctx, *commandPod, kredis.Spec.BasePort)
		if err != nil {
			logger.Error(err, "Failed to check if all masters have slots")
			return err
		}
		if !allMastersHaveSlots {
			// Phase 2: 리밸런싱 필요 - scale에서 rebalance로 전환
			logger.Info("Phase 2: All masters added, but some have no slots. Triggering rebalance before adding slaves.")
			delta.LastClusterOperation = fmt.Sprintf("rebalance-needed:%d", time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
			return nil
		}

		// Phase 3: 슬레이브 추가 전 리밸런싱 완료 여부 확인
		if isRebalancing {
			logger.Info("Rebalancing in progress, waiting before adding slaves",
				"lastOp", lastOp, "pendingReplicas", len(initialReplicaPods)+len(initialMasterPods))
			return nil
		}

		// Phase 3: Slave 추가 - prefer replica pods, but use master pods if needed (failover recovery)
		if len(initialReplicaPods) > 0 {
			targetPod = initialReplicaPods[0]
		} else {
			targetPod = initialMasterPods[0]
		}
		isMaster = false
		logger.Info("Phase 3: Adding node as slave", "pod", targetPod.Name)
	} else {
		// No new pods to add
		logger.Info("No new pods to add")
		return cm.finalizeScale(ctx, kredis, pods, clusterState, commandPod, delta)
	}

	// 4. 노드가 이미 클러스터에 있는지 확인
	isInCluster, existingNode := cm.isNodeInCluster(targetPod, clusterState)
	if isInCluster {
		logger.Info("Node already in cluster, handling existing node",
			"pod", targetPod.Name,
			"role", existingNode.Role,
			"nodeID", existingNode.NodeID)
		return cm.handleExistingNode(ctx, kredis, targetPod, commandPod, existingNode, isMaster, pods, clusterState, delta)
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
	return cm.addNodeToCluster(ctx, kredis, targetPod, commandPod, isMaster, pods, clusterState, delta)
}

// handleExistingNode handles a node that is already in the cluster.
// This can happen when:
// 1. Previous add-node succeeded but status wasn't updated
// 2. Node was added as master but should be slave (or vice versa)
func (cm *ClusterManager) handleExistingNode(ctx context.Context, kredis *cachev1alpha1.Kredis, targetPod corev1.Pod, commandPod *corev1.Pod,
	existingNode *cachev1alpha1.ClusterNode, shouldBeMaster bool, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode,
	delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	logger.Error(nil, "UNEXPECTED: handleExistingNode called - node already in cluster before adding node",
		"pod", targetPod.Name,
		"nodeID", existingNode.NodeID,
		"currentRole", existingNode.Role,
		"shouldBeMaster", shouldBeMaster,
		"slotCount", existingNode.SlotCount)

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
			// 빈 마스터 → 슬레이브로 변환
			// Find the master in the same shard or the one with fewest slaves
			var targetMasterID string
			shardIdx, _, err := resource.ParsePodName(kredis.Name, targetPod.Name)
			if err == nil {
				// Try to find the current master for this shard
				// In shard-based naming, any instance in the shard can be master after failover
				for _, node := range clusterState {
					if node.Role == "master" && node.NodeID != "" && node.NodeID != existingNode.NodeID {
						// Check if this master is in the same shard
						nodeShardIdx, _, nodeErr := resource.ParsePodName(kredis.Name, node.PodName)
						if nodeErr == nil && nodeShardIdx == shardIdx {
							targetMasterID = node.NodeID
							break
						}
					}
				}
			}

			// If not found, find master with fewest slaves
			if targetMasterID == "" {
				targetMasterID = cm.selectMasterWithFewestSlaves(clusterState, kredis.Spec.Replicas)
			}

			if targetMasterID == "" {
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
			// 슬롯 있는 마스터 → 변환 불가, 유지
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
func (cm *ClusterManager) addNodeToCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, targetPod corev1.Pod, commandPod *corev1.Pod, isMaster bool, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	if isMaster {
		logger.Info("Adding new master node to cluster", "pod", targetPod.Name, "ip", targetPod.Status.PodIP)
		if err := cm.PodExecutor.AddNodeToCluster(ctx, targetPod, *commandPod, kredis.Spec.BasePort, ""); err != nil {
			delta.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
			return fmt.Errorf("failed to add master node %s to cluster: %w", targetPod.Name, err)
		}
	} else {
		// Find target master for this slave
		// In shard-based naming, try to find the master in the same shard first
		var targetMasterID string
		shardIdx, _, err := resource.ParsePodName(kredis.Name, targetPod.Name)
		if err == nil {
			// Find the master in the same shard
			for _, node := range clusterState {
				if node.Role == "master" && node.NodeID != "" && node.SlotCount > 0 {
					nodeShardIdx, _, nodeErr := resource.ParsePodName(kredis.Name, node.PodName)
					if nodeErr == nil && nodeShardIdx == shardIdx {
						targetMasterID = node.NodeID
						break
					}
				}
			}
		}

		// If not found (different shard or legacy naming), find master with fewest slaves
		if targetMasterID == "" {
			targetMasterID = cm.selectMasterWithFewestSlaves(clusterState, kredis.Spec.Replicas)
		}

		if targetMasterID == "" {
			return fmt.Errorf("no master available for slave %s", targetPod.Name)
		}

		logger.Info("Adding new slave node to cluster", "pod", targetPod.Name, "ip", targetPod.Status.PodIP, "masterID", targetMasterID)
		if err := cm.PodExecutor.AddNodeToCluster(ctx, targetPod, *commandPod, kredis.Spec.BasePort, targetMasterID); err != nil {
			delta.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
			delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
			return fmt.Errorf("failed to add slave node %s to cluster: %w", targetPod.Name, err)
		}
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
	commandPod := cm.findMasterPod(pods, clusterState)
	if commandPod == nil {
		return fmt.Errorf("failed to find a pod for scaling command")
	}

	return cm.addNodeToCluster(ctx, kredis, *targetPod, commandPod, isMaster, pods, clusterState, delta)
}

// isNodeReset is now in utils.go as a common function
// resetNode is now in utils.go as a common function

// selectMasterWithFewestSlaves finds the master with the fewest slaves for load balancing.
// Prefers masters with slots over empty masters.
func (cm *ClusterManager) selectMasterWithFewestSlaves(clusterState []cachev1alpha1.ClusterNode, expectedReplicasPerMaster int32) string {
	// Build master -> slave count map
	slaveCount := make(map[string]int)
	masterSlots := make(map[string]int)

	for _, node := range clusterState {
		if node.Role == "master" && node.NodeID != "" {
			slaveCount[node.NodeID] = 0
			masterSlots[node.NodeID] = node.SlotCount
		}
	}
	for _, node := range clusterState {
		if node.Role == "slave" && node.MasterID != "" {
			slaveCount[node.MasterID]++
		}
	}

	// First try to find a master with slots that needs more replicas
	var bestMasterID string
	minSlaves := int(expectedReplicasPerMaster) + 1

	for masterID, count := range slaveCount {
		if masterSlots[masterID] > 0 && count < int(expectedReplicasPerMaster) && count < minSlaves {
			minSlaves = count
			bestMasterID = masterID
		}
	}

	// If all slotted masters have enough replicas, try any master
	if bestMasterID == "" {
		minSlaves = int(expectedReplicasPerMaster) + 1
		for masterID, count := range slaveCount {
			if count < int(expectedReplicasPerMaster) && count < minSlaves {
				minSlaves = count
				bestMasterID = masterID
			}
		}
	}

	return bestMasterID
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

	// 리밸런스 필요 여부 확인 (빈 마스터가 있는지)
	if cm.needsRebalance(ctx, *commandPod, kredis.Spec.BasePort) {
		logger.Info("Rebalance needed, some masters have no slots")
		delta.LastClusterOperation = fmt.Sprintf("rebalance-needed:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRebalancing)
		return nil
	}

	logger.Info("Scale completed successfully")
	delta.LastClusterOperation = fmt.Sprintf("scale-success:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)

	// Update LastScaleTime to enable stabilization window for autoscaling
	now := time.Now()
	delta.LastScaleTime = &now
	delta.LastScaleType = "masters-up" // Scale-up operation
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
