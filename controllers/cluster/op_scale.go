package cluster

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// scaleCluster handles adding or removing nodes from the cluster
func (cm *ClusterManager) scaleCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	logger.Info("Scaling up Redis cluster")

	delta.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())

	commandPod := cm.findMasterPod(pods, kredis, clusterState)
	if commandPod == nil {
		return fmt.Errorf("failed to find a pod for scaling command")
	}
	logger.Info("Using pod for scaling command", "pod", commandPod.Name)

	newPods := filterNewPods(pods, kredis.Status.JoinedPods)
	if len(newPods) == 0 {
		// 새 파드가 없으면 스케일 작업 스킵
		logger.Info("No new pods to add; scale operation skipped")
		delta.LastClusterOperation = fmt.Sprintf("scale-skipped-no-new-pods:%d", time.Now().Unix())
		return nil
	}

	// --- Pre-flight check: Reset all NEW nodes before adding them to the cluster ---
	logger.Info("Resetting new nodes before adding them to the cluster.")
	for _, pod := range newPods {
		logger.Info("Executing FLUSHALL on new pod", "pod", pod.Name)
		if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, kredis.Spec.BasePort, "FLUSHALL"); err != nil {
			logger.Error(err, "Failed to flush new node, but proceeding", "pod", pod.Name)
		}
		logger.Info("Executing CLUSTER RESET on new pod", "pod", pod.Name)
		if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, kredis.Spec.BasePort, "CLUSTER", "RESET"); err != nil {
			return fmt.Errorf("failed to reset new node %s: %w", pod.Name, err)
		}
	}
	// Give nodes a moment to settle after reset
	time.Sleep(2 * time.Second)
	// --- End of pre-flight check ---

	// 1. 현재 JoinedPods에 포함된 마스터 노드 집계
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

	// 2. 새로 추가될 파드 이름 오름차순 정렬
	sort.Slice(newPods, func(i, j int) bool {
		return newPods[i].Name < newPods[j].Name
	})

	// 3. 마스터 부족분만큼 마스터로 추가
	var newMasters []corev1.Pod
	var newSlaves []corev1.Pod
	mastersNeeded := int(kredis.Spec.Masters) - len(masterNodes)
	for idx, pod := range newPods {
		if idx < mastersNeeded {
			newMasters = append(newMasters, pod)
		} else {
			newSlaves = append(newSlaves, pod)
		}
	}

	// 4. 마스터로 추가
	for _, newMaster := range newMasters {
		logger.Info("Adding new master node to cluster", "pod", newMaster.Name, "ip", newMaster.Status.PodIP)
		err := cm.PodExecutor.AddNodeToCluster(ctx, newMaster, *commandPod, kredis.Spec.BasePort, "")
		if err != nil {
			delta.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
			return fmt.Errorf("failed to add master node %s to cluster: %w", newMaster.Name, err)
		}
		time.Sleep(1 * time.Second) // Short sleep to avoid overwhelming the cluster

		// 마스터로 추가된 노드의 NodeID를 알아내기 위해 클러스터 상태 갱신
		refreshedState, err := cm.PodExecutor.GetRedisClusterNodes(ctx, *commandPod, kredis.Spec.BasePort)
		if err != nil {
			return fmt.Errorf("failed to refresh cluster state after adding master: %w", err)
		}
		// NodeID 찾기
		for _, redisNode := range refreshedState {
			if redisNode.IP == newMaster.Status.PodIP {
				masterNodes = append(masterNodes, cachev1alpha1.ClusterNode{
					NodeID:   redisNode.NodeID,
					PodName:  newMaster.Name,
					IP:       redisNode.IP,
					Port:     redisNode.Port,
					Role:     "master",
					MasterID: "",
					Status:   redisNode.Status,
					Joined:   true, // deprecated, 실제 사용은 JoinedPods
				})
				// Join된 파드로 추가
				if !containsString(delta.JoinedPods, newMaster.Name) {
					delta.JoinedPods = append(delta.JoinedPods, newMaster.Name)
				}
				break
			}
		}
	}

	// 5. 각 마스터별 replica 수 집계용 map
	replicaMap := make(map[string]int)
	for _, m := range masterNodes {
		replicaMap[m.NodeID] = 0
	}
	// 기존 slave 집계
	for _, node := range clusterState {
		if _, ok := joinedPodsSet[node.PodName]; ok && node.Role == "slave" && node.MasterID != "" {
			replicaMap[node.MasterID]++
		}
	}

	// 6. 슬레이브로 추가 (각 마스터별로 kredis.Spec.Replicas 만큼만 할당)
	for _, newSlave := range newSlaves {
		// replica가 spec보다 적은 마스터 중 replica가 가장 적은 마스터 선택
		var targetMasterID string
		minReplicas := int(kredis.Spec.Replicas) + 1
		for masterID, count := range replicaMap {
			if count < int(kredis.Spec.Replicas) && count < minReplicas {
				minReplicas = count
				targetMasterID = masterID
			}
		}
		if targetMasterID == "" {
			return fmt.Errorf("no master available for new replica %s", newSlave.Name)
		}
		logger.Info("Adding new slave node to cluster", "pod", newSlave.Name, "ip", newSlave.Status.PodIP, "masterID", targetMasterID)
		err := cm.PodExecutor.AddNodeToCluster(ctx, newSlave, *commandPod, kredis.Spec.BasePort, targetMasterID)
		if err != nil {
			delta.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
			return fmt.Errorf("failed to add slave node %s to cluster: %w", newSlave.Name, err)
		}
		// Join된 파드로 추가
		if !containsString(delta.JoinedPods, newSlave.Name) {
			delta.JoinedPods = append(delta.JoinedPods, newSlave.Name)
		}
		replicaMap[targetMasterID]++
		time.Sleep(1 * time.Second)
	}

	// After adding all nodes, wait for them to be recognized by the cluster.
	logger.Info("Waiting for all new nodes to be known by the cluster...")
	expectedTotalPods := cm.getExpectedPodCount(kredis)
	err := cm.waitForAllNodesToBeKnown(ctx, *commandPod, kredis.Spec.BasePort, int(expectedTotalPods))
	if err != nil {
		delta.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
		return fmt.Errorf("cluster nodes did not report correct count after adding nodes: %w", err)
	}

	// All nodes are now known by the cluster
	known := int(expectedTotalPods)
	delta.KnownClusterNodes = &known

	// 마스터가 추가된 경우에만 리밸런스 필요
	if len(newMasters) > 0 {
		logger.Info("New masters added, rebalance needed", "newMastersCount", len(newMasters))
		delta.LastClusterOperation = fmt.Sprintf("rebalance-needed:%d", time.Now().Unix())
	} else {
		logger.Info("Only replicas added, scale completed successfully")
		delta.LastClusterOperation = fmt.Sprintf("scale-success:%d", time.Now().Unix())
	}

	return nil
}
