package cluster

import (
	"context"
	"fmt"
	"sort"
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
	JobManager  *JobManager
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(c client.Client, podExecutor *PodExecutor) *ClusterManager {
	return &ClusterManager{
		Client:      c,
		PodExecutor: podExecutor,
		JobManager:  NewJobManager(c),
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

// ClusterStatusDelta carries status changes for the controller to persist.
type ClusterStatusDelta struct {
	LastClusterOperation string
	KnownClusterNodes    *int
	ClusterNodes         []cachev1alpha1.ClusterNode
	ClusterState         string
}

// ReconcileCluster manages the Redis cluster state and returns whether cluster is ready
func (cm *ClusterManager) ReconcileCluster(ctx context.Context, kredis *cachev1alpha1.Kredis) (bool, *ClusterStatusDelta, error) {
	logger := log.FromContext(ctx)
	delta := &ClusterStatusDelta{}

	// 1. Get all pods for this Kredis instance
	pods, err := cm.getKredisPods(ctx, kredis)
	if err != nil {
		return false, delta, fmt.Errorf("failed to get kredis pods: %w", err)
	}

	// 2. Check if all pods are ready
	if !cm.areAllPodsReady(pods, kredis) {
		logger.V(1).Info("Waiting for all pods to be ready",
			"ready", len(cm.getReadyPods(pods)),
			"expected", cm.getExpectedPodCount(kredis))
		return false, delta, nil
	}

	// 3. Discover current cluster state ONCE and cache it for this reconciliation loop.
	// This avoids multiple redundant calls to Redis.
	clusterState, _ := cm.discoverClusterState(ctx, kredis, pods)
	// 4. Provide discovered nodes via delta
	delta.ClusterNodes = clusterState

	// 4.1 Sync pod role labels based on discovered cluster state (dynamic update)
	cm.SyncPodRoleLabels(ctx, kredis, clusterState)

	// 5. Determine what operation is needed based on the discovered state
	operation := cm.determineRequiredOperation(ctx, kredis, clusterState, pods)

	// 6. Execute the operation if needed
	if operation != "" {
		// Pass the discovered state to the execution function to avoid re-querying
		err := cm.executeClusterOperation(ctx, kredis, operation, pods, clusterState, delta)
		if err != nil {
			return false, delta, err
		}
		// After operation, cluster might need time to stabilize
		return false, delta, nil
	}

	// Cluster is stable and no operations needed
	return true, delta, nil
}

// getExpectedPodCount calculates the expected number of pods
func (cm *ClusterManager) getExpectedPodCount(kredis *cachev1alpha1.Kredis) int32 {
	return kredis.Spec.Masters + (kredis.Spec.Masters * kredis.Spec.Replicas)
}

// determineRequiredOperation analyzes current vs desired state
func (cm *ClusterManager) determineRequiredOperation(ctx context.Context, kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode, pods []corev1.Pod) ClusterOperation {
	logger := log.FromContext(ctx)

	// Check if there's an ongoing operation and resume verification if needed
	lastOp := kredis.Status.LastClusterOperation
	if strings.Contains(lastOp, "create-reset-pending") {
		// 클러스터 생성 전 노드 리셋 대기 중
		logger.Info("Create reset pending - will check if all nodes are reset",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return OperationCreate
	}
	if strings.Contains(lastOp, "scale-reset-pending") {
		// 스케일 전 노드 리셋 대기 중
		logger.Info("Scale reset pending - will check if node is reset",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return OperationScale
	}
	if strings.Contains(lastOp, "create-in-progress") {
		// 클러스터 생성 진행 중 - Job 상태 확인 및 완료 처리
		logger.Info("Create in progress - will verify and finalize",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return OperationCreate
	}
	if strings.Contains(lastOp, "rebalance-needed") {
		// 스케일 후 리밸런스가 필요한 상태 - 리밸런스 시작 (Phase 1: reshard)
		logger.Info("Rebalance needed after scale - will start reshard phase",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return OperationRebalance
	}
	if strings.Contains(lastOp, "reshard-in-progress") {
		// reshard 진행 중 - 검증 후 rebalance로 진행
		logger.Info("Reshard in progress - will verify and proceed to rebalance",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return OperationRebalance
	}
	if strings.Contains(lastOp, "rebalance-in-progress") {
		// 리밸런스는 검증/완료 마킹을 위해 별도 오퍼레이션으로 처리
		logger.Info("Rebalance in progress - will run verification step",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return OperationRebalance
	}
	if strings.Contains(lastOp, "scale-in-progress") {
		// 스케일 도중 재시작 등의 상황에서 검증/후속 절차 재개
		logger.Info("Scale in progress - will resume scale verification",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return OperationScale
	}
	if strings.Contains(lastOp, "heal-in-progress") {
		// 힐 작업 진행 중이면 검증/완료 단계 재실행
		logger.Info("Heal in progress - will continue heal verification",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return OperationHeal
	}
	if cm.isOperationInProgress(ctx, kredis, pods) {
		logger.Info("Operation already in progress - skipping new operation",
			"lastOperation", lastOp,
			"currentTime", time.Now().Unix())
		return ""
	}

	// --- Analyze the discovered state to distinguish between new pods and cluster members ---
	// Use clusterState directly (real-time Redis cluster state) instead of JoinedPods
	// This prevents race conditions in concurrent reconciles
	var masterNodes []cachev1alpha1.ClusterNode
	var clusterMembers []cachev1alpha1.ClusterNode
	podMap := make(map[string]corev1.Pod)
	ourPodsSet := make(map[string]struct{})
	for _, pod := range pods {
		podMap[pod.Name] = pod
		ourPodsSet[pod.Name] = struct{}{}
	}

	for _, node := range clusterState {
		// A node is considered a cluster member if:
		// 1. It's one of our pods (in the current pod list)
		// 2. It has a valid NodeID (actually joined the cluster)
		// 3. It has a known role (master/slave, not "unknown")
		if _, isOurs := ourPodsSet[node.PodName]; isOurs && node.NodeID != "" && node.Role != "unknown" && node.Role != "" {
			clusterMembers = append(clusterMembers, node)
			if node.Role == "master" {
				masterNodes = append(masterNodes, node)
			}
		}
	}

	// --- Decision Logic ---

	// Check if a functional cluster already exists.
	// Pass clusterState to avoid redundant GetRedisClusterNodes call.
	clusterExists, err := cm.isClusterAlreadyExists(ctx, kredis, pods, clusterState)
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
	// NOTE: This condition only handles scale-up. Scale-down requires different handling:
	// - Need to reshard slots away from nodes being removed
	// - Need to gracefully remove nodes from cluster before pod deletion
	// - Current StatefulSet-based approach doesn't support this well
	if currentJoinedNodes < int(expectedTotalNodes) && unassignedPodCount > 0 {
		logger.Info("Cluster needs scaling. More nodes need to be joined.", "action", "OperationScale")
		return OperationScale
	}

	// Scale-down condition (NOT YET IMPLEMENTED - critical limitations exist)
	// WARNING: Scale-down is complex and requires:
	// 1. Resharding slots from nodes to be removed to remaining nodes
	// 2. Removing nodes from cluster using CLUSTER FORGET
	// 3. Waiting for cluster to stabilize before deleting pods
	// 4. Handling replicas of removed masters
	// Current implementation does NOT support scale-down safely!
	if currentJoinedNodes > int(expectedTotalNodes) {
		logger.Info("WARNING: Node count higher than expected - scale down NOT IMPLEMENTED",
			"currentJoined", currentJoinedNodes,
			"expected", expectedTotalNodes,
			"action", "manual intervention required")
		// TODO: Implement OperationScaleDown
		// For now, we do NOT return OperationScale as that would try to add more nodes
	}

	// Heal condition: Check for failed nodes in the cluster state
	for _, node := range clusterState {
		if strings.Contains(node.Status, "fail") {
			logger.Info("Found failed node, initiating heal operation", "pod", node.PodName, "nodeID", node.NodeID)
			return OperationHeal
		}
	}

	// If none of the above, the cluster is stable.
	logger.Info("Cluster is properly formed and scaled - no operation needed")
	return ""
}

// executeClusterOperation performs the required cluster operation
func (cm *ClusterManager) executeClusterOperation(ctx context.Context, kredis *cachev1alpha1.Kredis, operation ClusterOperation, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	logger.Info("Executing cluster operation", "operation", operation)

	switch operation {
	case OperationCreate:
		return cm.createCluster(ctx, kredis, pods, delta)
	case OperationScale:
		// Pass the already discovered state to the scale function
		return cm.scaleCluster(ctx, kredis, pods, clusterState, delta)
	case OperationRebalance:
		// Pass the already discovered state to the rebalance function
		return cm.rebalanceCluster(ctx, kredis, pods, clusterState, delta)
	case OperationHeal:
		return cm.healCluster(ctx, kredis, pods, clusterState, delta)
	default:
		return fmt.Errorf("unknown cluster operation: %s", operation)
	}
}

// isClusterAlreadyExists checks if a functional Redis cluster is already formed.
// clusterState is passed from discoverClusterState to avoid redundant Redis queries.
func (cm *ClusterManager) isClusterAlreadyExists(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode) (bool, error) {
	logger := log.FromContext(ctx)
	port := kredis.Spec.BasePort

	// Find a ready pod to query for cluster info.
	// Priority: cluster members first (from clusterState), then any ready pod.
	// This prevents new unjoined pods (e.g., pod-0) from being selected over existing cluster members.
	queryPod := cm.findQueryPod(pods, clusterState, port)
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
		return false, err
	}
	if !isHealthy {
		logger.Info("Cluster state is not 'ok', assuming cluster does not exist or is not properly formed yet.")
		return false, nil
	}

	// 2. Use clusterState (already fetched by discoverClusterState) to count nodes.
	// This avoids a redundant GetRedisClusterNodes call.
	connectedNodes := 0
	for _, node := range clusterState {
		if node.Status == "connected" || node.Role == "master" || node.Role == "slave" {
			connectedNodes++
		}
	}

	// A healthy cluster with connected nodes is considered to exist.
	if isHealthy && connectedNodes > 0 {
		logger.Info("Found existing healthy cluster.", "connectedNodes", connectedNodes)
		return true, nil
	}

	logger.Info("Cluster check completed, but does not meet existence criteria", "isHealthy", isHealthy, "connectedNodes", connectedNodes)
	return false, nil
}

// getKredisPods retrieves all pods belonging to this Kredis instance
func (cm *ClusterManager) getKredisPods(ctx context.Context, kredis *cachev1alpha1.Kredis) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	labelSelector := client.MatchingLabels{
		"app":                    "kredis",
		"app.kubernetes.io/name": kredis.Name,
	}

	if err := cm.List(ctx, podList, client.InNamespace(kredis.Namespace), labelSelector); err != nil {
		return nil, err
	}
	pods := podList.Items
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	return pods, nil
}

// discoverClusterState queries Redis cluster state from a single ready pod.
// Optimized: "cluster nodes" returns the entire cluster state from any node.
func (cm *ClusterManager) discoverClusterState(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod) ([]cachev1alpha1.ClusterNode, error) {
	logger := log.FromContext(ctx)
	port := kredis.Spec.BasePort

	// Find a single ready pod to query cluster state.
	// For initial discovery, we don't have clusterState yet, so pass nil.
	// findQueryPod will fall back to any ready pod.
	queryPod := cm.findQueryPod(pods, nil, port)

	// If no ready pod found, return all pods as pending
	if queryPod == nil {
		logger.V(1).Info("No ready pod found to query cluster state")
		var clusterNodes []cachev1alpha1.ClusterNode
		for _, pod := range pods {
			clusterNodes = append(clusterNodes, cachev1alpha1.ClusterNode{
				PodName: pod.Name,
				IP:      pod.Status.PodIP,
				Port:    port,
				Role:    "unknown",
				Status:  "pending",
			})
		}
		return clusterNodes, nil
	}

	// Query cluster nodes from the single ready pod (1 call instead of N calls)
	redisNodes, err := cm.PodExecutor.GetRedisClusterNodes(ctx, *queryPod, port)
	if err != nil {
		logger.V(1).Info("Failed to get cluster nodes", "pod", queryPod.Name, "error", err)
		var clusterNodes []cachev1alpha1.ClusterNode
		for _, pod := range pods {
			clusterNodes = append(clusterNodes, cachev1alpha1.ClusterNode{
				PodName: pod.Name,
				IP:      pod.Status.PodIP,
				Port:    port,
				Role:    "unknown",
				Status:  "pending",
			})
		}
		return clusterNodes, nil
	}

	// Map Redis nodes by IP for quick lookup
	redisNodeByIP := make(map[string]*RedisNodeInfo)
	for i := range redisNodes {
		redisNodeByIP[redisNodes[i].IP] = &redisNodes[i]
	}

	// Build cluster state by matching pods with Redis node info
	var clusterNodes []cachev1alpha1.ClusterNode
	for _, pod := range pods {
		node := cachev1alpha1.ClusterNode{
			PodName: pod.Name,
			IP:      pod.Status.PodIP,
			Port:    port,
			Role:    "unknown",
			Status:  "pending",
		}
		if redisNode, ok := redisNodeByIP[pod.Status.PodIP]; ok {
			node.NodeID = redisNode.NodeID
			node.Role = redisNode.Role
			node.MasterID = redisNode.MasterID
			node.SlotCount = redisNode.SlotCount
			node.Status = redisNode.Status
		}
		clusterNodes = append(clusterNodes, node)
	}

	return clusterNodes, nil
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

// areAllNodesKnown checks if the cluster knows about exactly the expected number of nodes (non-blocking).
// Uses exact match (==) to work correctly for both scale-up and scale-down scenarios.
func (cm *ClusterManager) areAllNodesKnown(ctx context.Context, commandPod corev1.Pod, port int32, expectedCount int) bool {
	logger := log.FromContext(ctx)
	nodes, err := cm.PodExecutor.GetRedisClusterNodes(ctx, commandPod, port)
	if err != nil {
		logger.V(1).Info("Failed to get cluster nodes for node count check", "error", err)
		return false
	}
	knownNodes := 0
	for _, node := range nodes {
		if !strings.Contains(node.Status, "handshake") {
			knownNodes++
		}
	}
	logger.Info("Checking known nodes", "current", knownNodes, "expected", expectedCount)
	return knownNodes == expectedCount
}

// findMasterPod returns a ready master pod from the cluster.
// Priority: cluster masters with slots > cluster masters > any cluster member
// Uses clusterState (real-time Redis state) to ensure we select actual cluster members.
func (cm *ClusterManager) findMasterPod(pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode) *corev1.Pod {
	podMap := make(map[string]corev1.Pod)
	for _, p := range pods {
		podMap[p.Name] = p
	}

	// 1st priority: Masters with slots (most reliable for commands)
	for _, node := range clusterState {
		if node.Role == "master" && node.NodeID != "" && node.SlotCount > 0 {
			if pod, ok := podMap[node.PodName]; ok && cm.isPodReady(pod) {
				return &pod
			}
		}
	}

	// 2nd priority: Any master in the cluster
	for _, node := range clusterState {
		if node.Role == "master" && node.NodeID != "" {
			if pod, ok := podMap[node.PodName]; ok && cm.isPodReady(pod) {
				return &pod
			}
		}
	}

	return nil
}

// findQueryPod selects the best pod to query for cluster state.
// Priority: cluster members (from clusterState) first, then any ready pod.
// This prevents newly created pods (not yet joined) from being selected,
// which would return incomplete cluster info (e.g., cluster_state:fail).
func (cm *ClusterManager) findQueryPod(pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, port int32) *corev1.Pod {
	podMap := make(map[string]corev1.Pod)
	for _, p := range pods {
		podMap[p.Name] = p
	}

	// Build set of pods that are actually in the cluster (have NodeID)
	clusterMemberSet := make(map[string]struct{})
	for _, node := range clusterState {
		if node.NodeID != "" && node.Role != "unknown" && node.Role != "" {
			clusterMemberSet[node.PodName] = struct{}{}
		}
	}

	// First priority: Select from cluster members (masters first)
	for _, node := range clusterState {
		if node.Role == "master" && node.NodeID != "" {
			if pod, ok := podMap[node.PodName]; ok {
				if cm.isPodReady(pod) && cm.PodExecutor.IsRedisReady(context.Background(), pod, port) {
					return &pod
				}
			}
		}
	}

	// Second priority: Any cluster member
	for _, node := range clusterState {
		if node.NodeID != "" {
			if pod, ok := podMap[node.PodName]; ok {
				if cm.isPodReady(pod) && cm.PodExecutor.IsRedisReady(context.Background(), pod, port) {
					return &pod
				}
			}
		}
	}

	// Third priority: Any ready pod (fallback for initial cluster creation)
	for i := range pods {
		if _, isMember := clusterMemberSet[pods[i].Name]; !isMember {
			if cm.isPodReady(pods[i]) && cm.PodExecutor.IsRedisReady(context.Background(), pods[i], port) {
				return &pods[i]
			}
		}
	}

	return nil
}

func (cm *ClusterManager) getActualMasterCount(ctx context.Context, pods []corev1.Pod, port int32) (int, error) {
	for _, pod := range pods {
		if cm.isPodReady(pod) && cm.PodExecutor.IsRedisReady(ctx, pod, port) {
			result, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, port, "cluster", "nodes")
			if err != nil {
				continue
			}
			masterCount := 0
			lines := strings.Split(result.Stdout, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				if strings.Contains(line, " master ") || strings.Contains(line, ",master,") || strings.Contains(line, ",master ") {
					masterCount++
				}
			}
			return masterCount, nil
		}
	}
	return 0, fmt.Errorf("no ready Redis pods found to query cluster info")
}

func (cm *ClusterManager) checkAllMastersHaveSlots(ctx context.Context, commandPod corev1.Pod, port int32) (bool, error) {
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
		if strings.Contains(line, "master") && !strings.Contains(line, "fail") {
			masterCount++
			fields := strings.Fields(line)
			if len(fields) > 8 {
				mastersWithSlots++
			}
		}
	}
	if masterCount == 0 {
		return false, nil
	}
	return masterCount == mastersWithSlots, nil
}

// filterNewPods returns pods that are NOT yet part of the Redis cluster.
// Uses clusterState (actual Redis cluster state) for accurate detection,
// preventing duplicate add-node attempts when multiple reconciles run concurrently.
// A node is considered "in cluster" only if it has a valid NodeID and a known role
// (not "unknown" or empty). This prevents pods that exist but haven't actually
// joined the cluster from being incorrectly filtered out.
func filterNewPods(pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode) []corev1.Pod {
	// Build a set of pod names that are ACTUALLY in the cluster
	// A node is "in cluster" if it has a valid NodeID and a known role
	clusterPodSet := make(map[string]struct{})
	for _, node := range clusterState {
		if node.PodName != "" && node.NodeID != "" && node.Role != "unknown" && node.Role != "" {
			clusterPodSet[node.PodName] = struct{}{}
		}
	}
	var newPods []corev1.Pod
	for _, pod := range pods {
		if _, ok := clusterPodSet[pod.Name]; !ok {
			newPods = append(newPods, pod)
		}
	}
	return newPods
}

// SyncPodRoleLabels: discovered cluster state 를 바탕으로 각 파드의 role 라벨(master/replica/unknown)을 동기화
// StatefulSet selector 에 포함되지 않은 라벨만 수정해야 함 (현재 selector 는 app, app.kubernetes.io/name, app.kubernetes.io/instance, role
// 이므로 role 은 selector 에 포함되어 StatefulSet 자체의 PodTemplate 라벨 변경은 재생성 위험 -> Patch 로 개별 Pod 업데이트)
func (cm *ClusterManager) SyncPodRoleLabels(ctx context.Context, kredis *cachev1alpha1.Kredis, clusterState []cachev1alpha1.ClusterNode) {
	logger := log.FromContext(ctx)
	for _, node := range clusterState {
		if node.PodName == "" {
			continue
		}
		desired := strings.ToLower(node.Role)
		switch desired {
		case "master":
			// ok
		case "slave", "replica":
			desired = "slave" // 서비스 selector 와 일치
		default:
			desired = "unknown"
		}
		var pod corev1.Pod
		if err := cm.Get(ctx, client.ObjectKey{Namespace: kredis.Namespace, Name: node.PodName}, &pod); err != nil {
			logger.V(1).Info("Skip role label sync - cannot get pod", "pod", node.PodName, "err", err)
			continue
		}
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		current := pod.Labels["role"]
		if current == desired {
			continue
		}
		// Patch only the metadata.labels.role field to avoid touching spec for StatefulSet managed pods
		patch := client.MergeFrom(pod.DeepCopy())
		pod.Labels["role"] = desired
		if err := cm.Patch(ctx, &pod, patch); err != nil {
			logger.V(1).Info("Failed to patch role label", "pod", pod.Name, "desired", desired, "err", err)
			continue
		}
		logger.V(1).Info("Patched pod role label", "pod", pod.Name, "role", desired)
	}
}

// isOperationInProgress checks if there's an ongoing cluster operation by checking Job status.
func (cm *ClusterManager) isOperationInProgress(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod) bool {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	if lastOp == "" {
		return false
	}
	if !strings.Contains(lastOp, "-in-progress") {
		return false
	}

	// Check if any Jobs are incomplete (pending, running, or retrying)
	// This correctly handles jobs that are created but Pod not yet scheduled
	hasIncompleteJobs, err := cm.JobManager.HasIncompleteJobs(ctx, kredis)
	if err != nil {
		logger.Error(err, "Failed to check incomplete Jobs")
		// If we can't check, assume operation is still in progress
		return true
	}

	if hasIncompleteJobs {
		logger.V(1).Info("Incomplete Jobs found, operation in progress")
		return true
	}

	// No incomplete jobs - operation has completed (success or failure)
	// The actual status will be updated by the operation handler
	logger.V(1).Info("No incomplete Jobs, operation is no longer in progress", "lastOperation", lastOp)
	return false
}
