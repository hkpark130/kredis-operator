package cluster

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// ClusterManager handles Redis cluster operations
type ClusterManager struct {
	client.Client
	PodExecutor *PodExecutor
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(client client.Client, podExecutor *PodExecutor) *ClusterManager {
	return &ClusterManager{
		Client:      client,
		PodExecutor: podExecutor,
	}
}

// ClusterOperation represents different cluster operations
type ClusterOperation string

const (
	OperationCreate    ClusterOperation = "create"
	OperationScale     ClusterOperation = "scale"
	OperationRebalance ClusterOperation = "rebalance"
	OperationHeal      ClusterOperation = "heal"
)

// ReconcileCluster manages the Redis cluster state and returns whether cluster is ready
func (cm *ClusterManager) ReconcileCluster(ctx context.Context, kredis *cachev1alpha1.Kredis) (bool, error) {
	logger := log.FromContext(ctx)

	// 1. Get all pods for this Kredis instance
	pods, err := cm.getKredisPods(ctx, kredis)
	if err != nil {
		return false, fmt.Errorf("failed to get kredis pods: %w", err)
	}

	// 2. Check if all pods are ready
	if !cm.areAllPodsReady(pods, kredis) {
		logger.V(1).Info("Waiting for all pods to be ready",
			"ready", len(cm.getReadyPods(pods)),
			"expected", cm.getExpectedPodCount(kredis))
		return false, nil
	}

	// 3. Discover current cluster state ONCE and cache it for this reconciliation loop.
	// This avoids multiple redundant calls to Redis.
	clusterState, err := cm.discoverClusterState(ctx, kredis, pods)
	// 4. Update status with discovered nodes
	kredis.Status.ClusterNodes = clusterState

	// 5. Determine what operation is needed based on the discovered state
	operation := cm.determineRequiredOperation(ctx, kredis, clusterState, pods)

	// 6. Execute the operation if needed
	if operation != "" {
		// Pass the discovered state to the execution function to avoid re-querying
		err := cm.executeClusterOperation(ctx, kredis, operation, pods, clusterState)
		if err != nil {
			return false, err
		}
		// After operation, cluster might need time to stabilize
		return false, nil
	}

	// Cluster is stable and no operations needed
	return true, nil
}

// getKredisPods retrieves all pods belonging to this Kredis instance
func (cm *ClusterManager) getKredisPods(ctx context.Context, kredis *cachev1alpha1.Kredis) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	labelSelector := client.MatchingLabels{
		"app":    "redis",
		"kredis": kredis.Name,
	}

	err := cm.List(ctx, podList, client.InNamespace(kredis.Namespace), labelSelector)
	if err != nil {
		return nil, err
	}

	// Sort pods by name to ensure consistent ordering
	pods := podList.Items
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Name < pods[j].Name
	})

	return pods, nil
}

// areAllPodsReady checks if all expected pods are ready
func (cm *ClusterManager) areAllPodsReady(pods []corev1.Pod, kredis *cachev1alpha1.Kredis) bool {
	expectedCount := cm.getExpectedPodCount(kredis)
	readyPods := cm.getReadyPods(pods)

	return len(readyPods) == int(expectedCount) && len(pods) == int(expectedCount)
}

// getReadyPods returns only the ready pods
func (cm *ClusterManager) getReadyPods(pods []corev1.Pod) []corev1.Pod {
	var readyPods []corev1.Pod
	for _, pod := range pods {
		if cm.isPodReady(pod) {
			readyPods = append(readyPods, pod)
		}
	}
	return readyPods
}

// isPodReady checks if a pod is ready
func (cm *ClusterManager) isPodReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// getExpectedPodCount calculates the expected number of pods
func (cm *ClusterManager) getExpectedPodCount(kredis *cachev1alpha1.Kredis) int32 {
	return kredis.Spec.Masters + (kredis.Spec.Masters * kredis.Spec.Replicas)
}

// discoverClusterState queries Redis nodes to understand current cluster state
func (cm *ClusterManager) discoverClusterState(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod) ([]cachev1alpha1.ClusterNode, error) {
	var clusterNodes []cachev1alpha1.ClusterNode
	for _, pod := range pods {
		// Query this pod's Redis for cluster info
		nodeInfo, err := cm.queryRedisNodeInfo(ctx, pod, kredis.Spec.BasePort)
		if err != nil {
			// Pod might not be part of cluster yet
			clusterNodes = append(clusterNodes, cachev1alpha1.ClusterNode{
				PodName: pod.Name,
				IP:      pod.Status.PodIP,
				Port:    kredis.Spec.BasePort,
				Role:    "unknown", // Role is unknown until it joins the cluster
				Status:  "pending",
			})
			continue
		}

		clusterNodes = append(clusterNodes, nodeInfo)
	}

	return clusterNodes, nil
}

// queryRedisNodeInfo queries a Redis pod for its cluster information
func (cm *ClusterManager) queryRedisNodeInfo(ctx context.Context, pod corev1.Pod, port int32) (cachev1alpha1.ClusterNode, error) {
	node := cachev1alpha1.ClusterNode{
		PodName: pod.Name,
		IP:      pod.Status.PodIP,
		Port:    port,
		Status:  "pending",
	}

	// Check if Redis is ready
	if !cm.PodExecutor.IsRedisReady(ctx, pod, port) {
		return node, fmt.Errorf("redis not ready")
	}

	// Get cluster node information
	redisNodes, err := cm.PodExecutor.GetRedisClusterNodes(ctx, pod, port)
	if err != nil {
		return node, err
	}

	// Find this pod's node info
	for _, redisNode := range redisNodes {
		if redisNode.IP == pod.Status.PodIP {
			node.NodeID = redisNode.NodeID
			node.Role = redisNode.Role
			node.MasterID = redisNode.MasterID
			node.Status = redisNode.Status
			break
		}
	}

	return node, nil
}

// determineRequiredOperation analyzes current vs desired state
func (cm *ClusterManager) determineRequiredOperation(ctx context.Context, kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode, pods []corev1.Pod) ClusterOperation {
	logger := log.FromContext(ctx)

	// Check if there's an ongoing operation
	if cm.isOperationInProgress(kredis) {
		lastOp := kredis.Status.LastClusterOperation
		// rebalance-in-progress인 경우 특별 처리
		if strings.Contains(lastOp, "rebalance-in-progress") {
			logger.Info("Rebalance is in progress - waiting for completion",
				"lastOperation", lastOp,
				"currentTime", time.Now().Unix())
		} else {
			logger.Info("Operation already in progress - skipping new operation",
				"lastOperation", lastOp,
				"currentTime", time.Now().Unix())
		}
		return ""
	}

	// --- Analyze the discovered state to distinguish between new pods and cluster members ---
	var masterNodes []cachev1alpha1.ClusterNode
	var clusterMembers []cachev1alpha1.ClusterNode
	podMap := make(map[string]corev1.Pod)
	for _, pod := range pods {
		podMap[pod.Name] = pod
	}

	// kredis.Status.JoinedPods를 사용하여 클러스터 멤버를 식별합니다.
	joinedPodsSet := make(map[string]struct{})
	for _, podName := range kredis.Status.JoinedPods {
		joinedPodsSet[podName] = struct{}{}
	}

	for _, node := range clusterState {
		// A node is considered a member only if its name is in JoinedPods.
		if _, isJoined := joinedPodsSet[node.PodName]; isJoined {
			clusterMembers = append(clusterMembers, node)
			if node.Role == "master" {
				masterNodes = append(masterNodes, node)
			}
		}
	}

	// --- Decision Logic ---

	// Check if a functional cluster already exists.
	clusterExists, err := cm.isClusterAlreadyExists(ctx, kredis, pods)
	if err != nil {
		logger.Error(err, "Failed to check for cluster existence")
		return ""
	}

	// Scenario 1: No functional cluster exists.
	if !clusterExists {
		// All pods must be ready before we can attempt to create the cluster.
		if len(pods) == int(cm.getExpectedPodCount(kredis)) {
			logger.Info("No existing functional cluster found, and all pods are ready. Will create cluster.")
			return OperationCreate
		}
		logger.Info("Waiting for all pods to be created before cluster creation.", "current", len(pods), "expected", cm.getExpectedPodCount(kredis))
		return ""
	}

	// Scenario 2: A cluster exists. Now we check its health and scaling needs.
	// Find a reliable master pod to check cluster health
	var healthCheckPod *corev1.Pod
	if len(masterNodes) > 0 {
		if pod, ok := podMap[masterNodes[0].PodName]; ok {
			healthCheckPod = &pod
		}
	}
	if healthCheckPod == nil {
		logger.Info("Cluster exists, but no master pod is available for a health check. Waiting.")
		return "" // Cannot proceed without a node to query
	}

	// Health is OK (already checked in isClusterAlreadyExists), now check for scaling.
	expectedTotalNodes := cm.getExpectedPodCount(kredis)
	currentJoinedNodes := len(clusterMembers)
	unassignedPodCount := len(pods) - currentJoinedNodes

	logger.Info("Cluster state for scaling check",
		"isHealthy", true, // From isClusterAlreadyExists
		"currentJoinedNodes", currentJoinedNodes,
		"expectedTotalNodes", expectedTotalNodes,
		"unassignedPods", unassignedPodCount,
		"currentMasters", len(masterNodes),
		"expectedMasters", kredis.Spec.Masters)

	// Scale-up condition:
	// 1. The number of nodes currently in the cluster is less than what is expected.
	// 2. There are unassigned (new) pods ready to be added to the cluster.
	if currentJoinedNodes < int(expectedTotalNodes) && unassignedPodCount > 0 {
		logger.Info("Cluster needs scaling. More nodes need to be joined.", "action", "OperationScale")
		return OperationScale
	}

	// Scale-down condition (not fully implemented, but logic placeholder)
	if currentJoinedNodes > int(expectedTotalNodes) {
		logger.Info("Node count higher than expected - scale down not implemented yet")
		// return OperationScaleDown // Future implementation
	}

	// If none of the above, the cluster is stable.
	logger.Info("Cluster is properly formed and scaled - no operation needed")
	return ""
}

// executeClusterOperation performs the required cluster operation
func (cm *ClusterManager) executeClusterOperation(ctx context.Context, kredis *cachev1alpha1.Kredis, operation ClusterOperation, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode) error {
	logger := log.FromContext(ctx)
	logger.Info("Executing cluster operation", "operation", operation)

	switch operation {
	case OperationCreate:
		return cm.createCluster(ctx, kredis, pods)
	case OperationScale:
		// Pass the already discovered state to the scale function
		return cm.scaleCluster(ctx, kredis, pods, clusterState)
	case OperationRebalance:
		// Pass the already discovered state to the rebalance function
		return cm.rebalanceCluster(ctx, kredis, pods, clusterState)
	default:
		return fmt.Errorf("unknown cluster operation: %s", operation)
	}
}

// createCluster initializes a new Redis cluster
func (cm *ClusterManager) createCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod) error {
	logger := log.FromContext(ctx)

	// --- Pre-flight check: Reset all nodes before creating a cluster ---
	logger.Info("Resetting all nodes before cluster creation to ensure a clean state.")
	for _, pod := range pods {
		logger.Info("Executing FLUSHALL on pod", "pod", pod.Name)
		if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, kredis.Spec.BasePort, "FLUSHALL"); err != nil {
			logger.Error(err, "Failed to flush node, but proceeding", "pod", pod.Name)
			// Proceeding might be okay if the node was already empty.
		}
		logger.Info("Executing CLUSTER RESET on pod", "pod", pod.Name)
		if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, kredis.Spec.BasePort, "CLUSTER", "RESET"); err != nil {
			// This is more critical. If reset fails, creation is likely to fail.
			return fmt.Errorf("failed to reset node %s: %w", pod.Name, err)
		}
	}
	// Give nodes a moment to settle after reset
	time.Sleep(2 * time.Second)
	// --- End of pre-flight check ---

	kredis.Status.LastClusterOperation = fmt.Sprintf("create-in-progress:%d", time.Now().Unix())
	if err := cm.Client.Status().Update(ctx, kredis); err != nil {
		logger.Error(err, "Failed to update status to create-in-progress")
		return err
	}

	// Find a pod to execute cluster create command from. Any ready pod will do.
	var commandPod *corev1.Pod
	for i := range pods {
		if cm.isPodReady(pods[i]) {
			commandPod = &pods[i]
			break
		}
	}
	if commandPod == nil {
		return fmt.Errorf("no ready pod found for cluster creation")
	}

	logger.Info("Creating Redis cluster", "commandPod", commandPod.Name)

	// Build the node IP list for the cluster create command
	var nodeIPs []string
	for _, pod := range pods {
		if pod.Status.PodIP != "" {
			nodeIPs = append(nodeIPs, pod.Status.PodIP)
		}
	}

	if len(nodeIPs) != len(pods) {
		return fmt.Errorf("not all pods have an IP address yet")
	}

	logger.Info("Node list for cluster creation", "nodeIPs", strings.Join(nodeIPs, " "), "nodeCount", len(nodeIPs))

	// Execute cluster create command using the executor
	_, err := cm.PodExecutor.CreateCluster(ctx, *commandPod, kredis.Spec.BasePort, nodeIPs, int(kredis.Spec.Replicas))
	if err != nil {
		kredis.Status.LastClusterOperation = fmt.Sprintf("create-failed:%d", time.Now().Unix())
		if err := cm.Client.Status().Update(ctx, kredis); err != nil {
			logger.Error(err, "Failed to update status to create-failed")
		}
		return fmt.Errorf("failed to create cluster: %w", err)
	}

	// After creation, wait for stabilization
	logger.Info("Waiting for cluster to stabilize after creation...")
	err = cm.waitForClusterStabilization(ctx, kredis, commandPod)
	if err != nil {
		kredis.Status.LastClusterOperation = fmt.Sprintf("create-failed:%d", time.Now().Unix())
		if err := cm.Client.Status().Update(ctx, kredis); err != nil {
			logger.Error(err, "Failed to update status to create-failed after stabilization")
		}
		return fmt.Errorf("cluster failed to stabilize after creation: %w", err)
	}

	kredis.Status.LastClusterOperation = fmt.Sprintf("create-success:%d", time.Now().Unix())
	kredis.Status.ClusterState = "created"

	// Wait a moment for the cluster to initialize
	time.Sleep(2 * time.Second)

	logger.Info("Successfully created Redis cluster")

	// After successful creation, update the known node count and joined pods in the status
	kredis.Status.KnownClusterNodes = len(pods)
	// 클러스터 생성에 사용된 파드 이름을 JoinedPods에 저장
	var joinedPodNames []string
	for _, pod := range pods {
		if pod.Status.PodIP != "" {
			joinedPodNames = append(joinedPodNames, pod.Name)
		}
	}
	kredis.Status.JoinedPods = joinedPodNames
	if err := cm.Client.Status().Update(ctx, kredis); err != nil {
		logger.Error(err, "Failed to update Kredis status with known node count after creation")
		return err // Return error but cluster might be created
	}

	return nil
}

// scaleCluster handles adding or removing nodes from the cluster
func (cm *ClusterManager) scaleCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode) error {
	logger := log.FromContext(ctx)
	logger.Info("Scaling up Redis cluster")

	kredis.Status.LastClusterOperation = fmt.Sprintf("scale-in-progress:%d", time.Now().Unix())
	if err := cm.Client.Status().Update(ctx, kredis); err != nil {
		logger.Error(err, "Failed to update status to scale-in-progress")
		return err
	}

	commandPod := cm.findMasterPod(pods, kredis, clusterState)
	if commandPod == nil {
		return fmt.Errorf("failed to find a pod for scaling command")
	}
	logger.Info("Using pod for scaling command", "pod", commandPod.Name)

	newPods := filterNewPods(pods, kredis.Status.JoinedPods)
	if len(newPods) == 0 {
		logger.Info("No new pods to add to the cluster.")
		kredis.Status.LastClusterOperation = fmt.Sprintf("scale-skipped-no-new-pods:%d", time.Now().Unix())
		_ = cm.Client.Status().Update(ctx, kredis)
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
			kredis.Status.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
			if err := cm.Client.Status().Update(ctx, kredis); err != nil {
				logger.Error(err, "Failed to update status to scale-failed")
			}
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
				if !containsString(kredis.Status.JoinedPods, newMaster.Name) {
					kredis.Status.JoinedPods = append(kredis.Status.JoinedPods, newMaster.Name)
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
			kredis.Status.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
			if err := cm.Client.Status().Update(ctx, kredis); err != nil {
				logger.Error(err, "Failed to update status to scale-failed")
			}
			return fmt.Errorf("failed to add slave node %s to cluster: %w", newSlave.Name, err)
		}
		// Join된 파드로 추가
		if !containsString(kredis.Status.JoinedPods, newSlave.Name) {
			kredis.Status.JoinedPods = append(kredis.Status.JoinedPods, newSlave.Name)
		}
		replicaMap[targetMasterID]++
		time.Sleep(1 * time.Second)
	}

	// After adding all nodes, wait for them to be recognized by the cluster.
	logger.Info("Waiting for all new nodes to be known by the cluster...")
	expectedTotalPods := cm.getExpectedPodCount(kredis)
	err := cm.waitForAllNodesToBeKnown(ctx, *commandPod, kredis.Spec.BasePort, int(expectedTotalPods))
	if err != nil {
		kredis.Status.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
		if sErr := cm.Client.Status().Update(ctx, kredis); sErr != nil {
			logger.Error(sErr, "Failed to update status to scale-failed")
		}
		return fmt.Errorf("cluster nodes did not report correct count after adding nodes: %w", err)
	}

	// Wait for the cluster to be generally healthy before rebalancing
	logger.Info("Waiting for cluster to stabilize before rebalancing...")
	err = cm.waitForClusterStabilization(ctx, kredis, commandPod)
	if err != nil {
		kredis.Status.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
		if sErr := cm.Client.Status().Update(ctx, kredis); sErr != nil {
			logger.Error(sErr, "Failed to update status to scale-failed")
		}
		return fmt.Errorf("cluster failed to stabilize before rebalancing: %w", err)
	}

	logger.Info("Starting Redis cluster rebalance after scaling")

	// rebalance 시작 전 상태 설정
	kredis.Status.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
	if err := cm.Client.Status().Update(ctx, kredis); err != nil {
		logger.Error(err, "Failed to update status to rebalance-in-progress")
		return err
	}

	// 클러스터 토폴로지 안정화 대기
	logger.Info("Waiting for cluster topology to stabilize before rebalancing...")
	time.Sleep(3 * time.Second)

	// 단일 rebalance 시도
	logger.Info("Executing cluster rebalance command")
	_, rebalanceErr := cm.PodExecutor.RebalanceCluster(ctx, *commandPod, kredis.Spec.BasePort)
	if rebalanceErr != nil {
		if strings.Contains(rebalanceErr.Error(), "ERR Please use SETSLOT only with masters") {
			logger.Info("Ignoring 'SETSLOT' error during rebalance, as it's expected when a master becomes a replica.")
			rebalanceErr = nil
		} else {
			kredis.Status.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
			if sErr := cm.Client.Status().Update(ctx, kredis); sErr != nil {
				logger.Error(sErr, "Failed to update status to rebalance-failed")
			}
			return fmt.Errorf("rebalance command failed: %w", rebalanceErr)
		}
	}

	// rebalance 명령어 실행 후 결과 검증을 위한 대기 및 재시도 로직
	logger.Info("Waiting for rebalance to complete and verifying results...")
	rebalanceTimeout := time.After(5 * time.Minute)
	verificationTicker := time.NewTicker(10 * time.Second)
	defer verificationTicker.Stop()

	rebalanceCompleted := false
	for !rebalanceCompleted {
		select {
		case <-ctx.Done():
			kredis.Status.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
			_ = cm.Client.Status().Update(ctx, kredis)
			return ctx.Err()
		case <-rebalanceTimeout:
			kredis.Status.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
			_ = cm.Client.Status().Update(ctx, kredis)
			return fmt.Errorf("timed out waiting for rebalance to complete - cluster may still be processing slot migration")
		case <-verificationTicker.C:
			// 클러스터 건강 상태 확인
			isHealthy, healthErr := cm.PodExecutor.IsClusterHealthy(ctx, *commandPod, kredis.Spec.BasePort)
			if healthErr != nil {
				logger.V(1).Info("Cluster health check failed during rebalance verification", "error", healthErr)
				continue
			}
			if !isHealthy {
				logger.Info("Cluster is not healthy yet, waiting for rebalance to complete...")
				continue
			}

			// 모든 마스터가 슬롯을 가지고 있는지 확인
			allMastersHaveSlots, checkErr := cm.checkAllMastersHaveSlots(ctx, *commandPod, kredis.Spec.BasePort)
			if checkErr != nil {
				logger.Error(checkErr, "Failed to check master slots during rebalance verification")
				continue
			}

			if allMastersHaveSlots {
				logger.Info("Rebalance completed successfully - all masters have slots assigned")
				kredis.Status.LastClusterOperation = fmt.Sprintf("rebalance-success:%d", time.Now().Unix())
				if sErr := cm.Client.Status().Update(ctx, kredis); sErr != nil {
					logger.Error(sErr, "Failed to update status to rebalance-success")
				}
				rebalanceCompleted = true
			} else {
				logger.Info("Rebalance still in progress - some masters don't have slots yet")
			}
		}
	}

	// Wait for stabilization again after rebalancing
	logger.Info("Waiting for cluster to stabilize after rebalancing...")
	err = cm.waitForClusterStabilization(ctx, kredis, commandPod)
	if err != nil {
		kredis.Status.LastClusterOperation = fmt.Sprintf("scale-failed:%d", time.Now().Unix())
		if sErr := cm.Client.Status().Update(ctx, kredis); sErr != nil {
			logger.Error(sErr, "Failed to update status to scale-failed")
		}
		return fmt.Errorf("cluster failed to stabilize after rebalancing: %w", err)
	}

	kredis.Status.LastClusterOperation = fmt.Sprintf("scale-successful:%d", time.Now().Unix())
	logger.Info("Successfully scaled up and rebalanced Redis cluster")

	// After scaling and rebalancing, update the known node count to the expected final state
	expectedCount := cm.getExpectedPodCount(kredis)
	kredis.Status.KnownClusterNodes = int(expectedCount)
	if err := cm.Client.Status().Update(ctx, kredis); err != nil {
		logger.Error(err, "Failed to update Kredis status with known node count after scaling")
		// Don't block reconciliation, but log the error
		return err
	}

	return nil
}

// rebalanceCluster rebalances the cluster slots
func (cm *ClusterManager) rebalanceCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode) error {
	logger := log.FromContext(ctx)

	// Find a master pod to execute rebalance command
	masterPod := cm.findMasterPod(pods, kredis, clusterState)
	if masterPod == nil {
		return fmt.Errorf("no master pod found for rebalancing")
	}

	logger.Info("Starting standalone Redis cluster rebalance", "masterPod", masterPod.Name)

	// rebalance 시작 상태 설정
	kredis.Status.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())
	if err := cm.Client.Status().Update(ctx, kredis); err != nil {
		logger.Error(err, "Failed to update status to rebalance-in-progress")
		return err
	}

	// Execute cluster rebalance command
	_, err := cm.PodExecutor.RebalanceCluster(ctx, *masterPod, kredis.Spec.BasePort)
	if err != nil {
		if strings.Contains(err.Error(), "ERR Please use SETSLOT only with masters") {
			logger.Info("Ignoring 'SETSLOT' error during rebalance, as it's expected when a master becomes a replica.")
		} else {
			kredis.Status.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
			if sErr := cm.Client.Status().Update(ctx, kredis); sErr != nil {
				logger.Error(sErr, "Failed to update status to rebalance-failed")
			}
			return fmt.Errorf("failed to rebalance cluster: %w", err)
		}
	}

	kredis.Status.LastClusterOperation = fmt.Sprintf("rebalance-success:%d", time.Now().Unix())
	if err := cm.Client.Status().Update(ctx, kredis); err != nil {
		logger.Error(err, "Failed to update status to rebalance-success")
	}
	return nil
}

// waitForClusterStabilization waits for the cluster to become healthy after an operation.
func (cm *ClusterManager) waitForClusterStabilization(ctx context.Context, kredis *cachev1alpha1.Kredis, queryPod *corev1.Pod) error {
	logger := log.FromContext(ctx)
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for cluster to stabilize")
		case <-ticker.C:
			isHealthy, err := cm.PodExecutor.IsClusterHealthy(ctx, *queryPod, kredis.Spec.BasePort)
			if err != nil {
				logger.V(1).Info("Waiting for cluster stabilization, health check failed", "error", err)
				continue
			}
			if isHealthy {
				logger.Info("Cluster has stabilized.")
				return nil
			}
			logger.Info("Waiting for cluster to stabilize...")
		}
	}
}

// waitForAllNodesToBeKnown waits until the cluster reports a certain number of nodes.
func (cm *ClusterManager) waitForAllNodesToBeKnown(ctx context.Context, commandPod corev1.Pod, port int32, expectedCount int) error {
	logger := log.FromContext(ctx)
	logger.Info("Waiting for all nodes to be known by the cluster", "expectedCount", expectedCount)

	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for all nodes to be known")
		case <-ticker.C:
			nodes, err := cm.PodExecutor.GetRedisClusterNodes(ctx, commandPod, port)
			if err != nil {
				logger.Error(err, "Failed to get cluster nodes, retrying...")
				continue
			}

			// Filter out nodes in handshake state, as they are not fully joined yet.
			knownNodes := 0
			for _, node := range nodes {
				if !strings.Contains(node.Status, "handshake") {
					knownNodes++
				}
			}

			logger.Info("Checking number of known nodes", "current", knownNodes, "expected", expectedCount)
			if knownNodes >= expectedCount {
				logger.Info("All nodes are known by the cluster.")
				return nil
			}
		}
	}
}

// isClusterAlreadyExists checks if a functional Redis cluster is already formed.
func (cm *ClusterManager) isClusterAlreadyExists(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod) (bool, error) {
	logger := log.FromContext(ctx)
	port := kredis.Spec.BasePort

	// Find a ready pod to query for cluster info.
	var queryPod *corev1.Pod
	for i := range pods {
		if cm.isPodReady(pods[i]) {
			queryPod = &pods[i]
			break
		}
	}
	if queryPod == nil {
		// Not an error, just means we can't check yet.
		logger.Info("No ready pods available to check cluster existence")
		return false, nil
	}

	// 1. Check cluster health (cluster_state:ok)
	isHealthy, err := cm.PodExecutor.IsClusterHealthy(ctx, *queryPod, port)
	if err != nil {
		// An error here might mean the cluster isn't up, which is not an "existence" failure.
		logger.V(1).Info("Could not check cluster health, assuming cluster does not exist", "error", err)
		return false, nil
	}
	if !isHealthy {
		logger.Info("Cluster state is not 'ok', assuming cluster does not exist or is not properly formed yet.")
		return false, nil
	}

	// 2. Check the number of nodes in the cluster against the known state.
	nodes, err := cm.PodExecutor.GetRedisClusterNodes(ctx, *queryPod, port)
	if err != nil {
		logger.V(1).Info("Could not get cluster nodes, assuming cluster does not exist", "error", err)
		return false, nil
	}

	// A healthy cluster with a node count matching the last known good state is considered to exist.
	// knownNodes := kredis.Status.KnownClusterNodes
	// if knownNodes > 0 && len(nodes) == knownNodes {
	if isHealthy && len(nodes) > 0 {
		logger.Info("Found existing healthy cluster.")
		return true, nil
	}

	logger.Info("Cluster check completed, but does not meet existence criteria", "isHealthy", isHealthy, "nodeCount", len(nodes))
	return false, nil
}

// findMasterPod finds a pod that is a known master and can execute cluster commands
func (cm *ClusterManager) findMasterPod(pods []corev1.Pod, kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode) *corev1.Pod {
	// Use the provided clusterState to find a master without new queries.
	podMap := make(map[string]corev1.Pod)
	for _, p := range pods {
		podMap[p.Name] = p
	}

	for _, node := range clusterState {
		if node.Role == "master" {
			if pod, ok := podMap[node.PodName]; ok && cm.isPodReady(pod) {
				return &pod // Found a ready master
			}
		}
	}

	// Fallback to any ready pod if no master was found in the state
	for i := range pods {
		if cm.isPodReady(pods[i]) {
			return &pods[i]
		}
	}
	return nil
}

// buildNodeListForClusterCreate builds the node list for cluster create command
func (cm *ClusterManager) buildNodeListForClusterCreate(pods []corev1.Pod, kredis *cachev1alpha1.Kredis) []string {
	var nodes []string
	for _, pod := range pods {
		if cm.isPodReady(pod) {
			nodes = append(nodes, fmt.Sprintf("%s:%d", pod.Status.PodIP, kredis.Spec.BasePort))
		}
	}
	return nodes
}

// executeRedisCommand executes a redis-cli command in a pod
func (cm *ClusterManager) executeRedisCommand(ctx context.Context, pod corev1.Pod, args ...string) error {
	_, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, 6379, args...)
	return err
}

// isOperationInProgress checks if there's an ongoing cluster operation
func (cm *ClusterManager) isOperationInProgress(kredis *cachev1alpha1.Kredis) bool {
	lastOp := kredis.Status.LastClusterOperation
	if lastOp == "" {
		return false
	}

	// Only consider operations with "-in-progress" as ongoing
	if !strings.Contains(lastOp, "-in-progress") {
		return false
	}

	// Parse the timestamp from "operation-in-progress:timestamp" format
	parts := strings.Split(lastOp, ":")
	if len(parts) < 2 {
		// Invalid format, treat as not in progress
		return false
	}

	timestamp, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		// Invalid timestamp, treat as not in progress
		return false
	}

	currentTime := time.Now().Unix()
	timeDiff := currentTime - timestamp

	// rebalance 작업의 경우 더 긴 타임아웃 허용 (10분)
	var timeoutDuration int64 = 300 // 5분 (기본)
	if strings.Contains(lastOp, "rebalance-in-progress") {
		timeoutDuration = 600 // 10분 (rebalance는 시간이 더 오래 걸릴 수 있음)
	}

	// If the operation is "in-progress" but older than timeout, consider it stale and allow a new operation.
	if timeDiff > timeoutDuration {
		// Since we don't have context here, we use the global logger.
		// A proper implementation might pass context to this function.
		log.Log.Info("Stale operation found, allowing new operation",
			"lastOperation", lastOp,
			"timeDiff", timeDiff,
			"timeoutDuration", timeoutDuration)
		return false
	}

	// Otherwise, an operation is in progress.
	return true
}

// getActualMasterCount gets the actual number of master nodes from Redis cluster
func (cm *ClusterManager) getActualMasterCount(ctx context.Context, pods []corev1.Pod, port int32) (int, error) {
	// Find any ready pod to query cluster info
	for _, pod := range pods {
		if cm.isPodReady(pod) && cm.PodExecutor.IsRedisReady(ctx, pod, port) {
			// Execute redis-cli cluster nodes and count masters
			result, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, port, "cluster", "nodes")
			if err != nil {
				continue // Try next pod
			}

			masterCount := 0
			lines := strings.Split(result.Stdout, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				// Check if line contains "master" flag
				if strings.Contains(line, " master ") || strings.Contains(line, ",master,") || strings.Contains(line, ",master ") {
					masterCount++
				}
			}

			return masterCount, nil
		}
	}

	return 0, fmt.Errorf("no ready Redis pods found to query cluster info")
}

// checkAllMastersHaveSlots checks if all master nodes in the cluster have slots assigned.
func (cm *ClusterManager) checkAllMastersHaveSlots(ctx context.Context, commandPod corev1.Pod, port int32) (bool, error) {
	logger := log.FromContext(ctx)
	result, err := cm.PodExecutor.ExecuteRedisCommand(ctx, commandPod, port, "cluster", "nodes")
	if err != nil {
		return false, fmt.Errorf("failed to get cluster nodes for slot check: %w", err)
	}

	lines := strings.Split(result.Stdout, "\n")
	masterCount := 0
	mastersWithSlots := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Check if the line represents a master node that is not in a failed state
		if strings.Contains(line, "master") && !strings.Contains(line, "fail") {
			masterCount++
			// A master line with slots has more than 8 fields.
			// <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> [slot_range ...]
			fields := strings.Fields(line)
			if len(fields) > 8 {
				mastersWithSlots++
			} else {
				logger.Info("Found master with no slots", "nodeLine", line)
			}
		}
	}

	if masterCount == 0 {
		logger.Info("No active masters found during slot check.")
		return false, nil
	}

	return masterCount == mastersWithSlots, nil
}

// filterNewPods는 JoinedPods 기반으로 미가입 파드만 반환합니다.
func filterNewPods(pods []corev1.Pod, joinedPods []string) []corev1.Pod {
	joinedPodsSet := make(map[string]struct{})
	for _, podName := range joinedPods {
		joinedPodsSet[podName] = struct{}{}
	}
	var newPods []corev1.Pod
	for _, pod := range pods {
		if _, ok := joinedPodsSet[pod.Name]; !ok {
			newPods = append(newPods, pod)
		}
	}
	return newPods
}

// containsString returns true if the slice contains the string
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
