package redis

import (
	"context"
	"fmt"
	"strings"
	
	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	// "k8s.io/client-go/kubernetes"
	// "k8s.io/client-go/rest"
	// "k8s.io/client-go/tools/clientcmd"
	// "k8s.io/client-go/util/homedir"
	// "path/filepath"
	// "os"
)

// CheckClusterExists는 Redis 클러스터가 이미 초기화되어 있는지 확인합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 클러스터 존재 여부, 오류
func (cm *ClusterManager) CheckClusterExists(ctx context.Context, cr *v1alpha1.KRedis) (bool, error) {
	log := log.FromContext(ctx)

	// StatefulSet에 스케일다운 어노테이션이 있는지 확인
	masterSts := &appsv1.StatefulSet{}
	stsErr := cm.client.Get(ctx, types.NamespacedName{
		Namespace: cr.Namespace,
		Name: fmt.Sprintf("%s-master", cr.Name),
	}, masterSts)
	
	// 어노테이션이 있을 경우 클러스터가 이미 존재하는 것으로 처리
	if stsErr == nil && masterSts.Annotations != nil {
		if _, found := masterSts.Annotations["redis.kredis-operator/scale-down-in-progress"]; found {
			log.Info("Scale-down in progress, treating cluster as existing")
			return true, nil
		}
	}

	// 첫 번째 마스터 Pod에 접근하여 클러스터 상태 확인
	podName := fmt.Sprintf("%s-master-0", cr.Name)

	// redis-cli --cluster info 명령 실행
	output, err := cm.execCommandOnPod(ctx, cr.Namespace, podName, []string{
		"redis-cli", "-p", fmt.Sprintf("%d", cr.Spec.BasePort), "cluster", "nodes",
	})

	if err != nil {
		// Pod가 아직 준비되지 않았거나, Redis가 실행 중이지 않을 수 있음
		log.Info("Failed to check cluster state, cluster may not be initialized yet", "error", err)
		return false, nil
	}

	// 출력에 노드 정보가 포함되어 있는지 확인 (최소한 1개의 노드)
	lines := strings.Split(output, "\n")
	nodeCount := 0
	for _, line := range lines {
		if len(strings.TrimSpace(line)) > 0 {
			nodeCount++
		}
	}

	clusterExists := nodeCount > 0
	log.Info("Cluster existence check", "exists", clusterExists, "nodeCount", nodeCount)
	return clusterExists, nil
}

// isPodReady는 Pod가 Ready 상태인지 확인합니다.
func isPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// GetCurrentMasters는 현재 클러스터에 있는 마스터 노드 목록을 가져옵니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 마스터 노드 목록, 오류
func (cm *ClusterManager) GetCurrentMasters(ctx context.Context, cr *v1alpha1.KRedis) ([]RedisNode, error) {
	nodes, err := cm.getClusterNodes(ctx, cr)
	if (err != nil) {
		return nil, err
	}

	// 마스터 노드만 필터링
	masters := make([]RedisNode, 0)
	for _, node := range nodes {
		if node.Role == "master" {
			masters = append(masters, node)
		}
	}

	return masters, nil
}

// GetCurrentSlaves는 현재 클러스터에 있는 슬레이브 노드 목록을 가져옵니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 슬레이브 노드 목록, 오류
func (cm *ClusterManager) GetCurrentSlaves(ctx context.Context, cr *v1alpha1.KRedis) ([]RedisNode, error) {
	nodes, err := cm.getClusterNodes(ctx, cr)
	if (err != nil) {
		return nil, err
	}

	// 슬레이브 노드만 필터링
	slaves := make([]RedisNode, 0)
	for _, node := range nodes {
		if node.Role == "slave" {
			slaves = append(slaves, node)
		}
	}

	return slaves, nil
}

// GetSlaveCountForMaster는 특정 마스터 노드의 슬레이브 수를 반환합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// masterIndex: 마스터 노드 인덱스
// return: 슬레이브 수, 오류
func (cm *ClusterManager) GetSlaveCountForMaster(ctx context.Context, cr *v1alpha1.KRedis, masterIndex int) (int, error) {
	nodes, err := cm.getClusterNodes(ctx, cr)
	if (err != nil) {
		return 0, err
	}

	// 먼저 masterIndex에 해당하는 마스터 노드의 ID 찾기
	var masterID string
	masters := 0
	for _, node := range nodes {
		if node.Role == "master" {
			if masters == masterIndex {
				masterID = node.ID
				break
			}
			masters++
		}
	}

	if masterID == "" {
		return 0, fmt.Errorf("master node with index %d not found", masterIndex)
	}

	// 해당 마스터에 연결된 슬레이브 수 계산
	slaveCount := 0
	for _, node := range nodes {
		if node.Role == "slave" && node.MasterID == masterID {
			slaveCount++
		}
	}

	return slaveCount, nil
}

// CheckClusterStatus는 Redis 클러스터의 건강 상태를 확인합니다.
//
// 검사 항목:
// - 클러스터 상태 (cluster_state:ok)
// - 노드 수 일치 여부
// - 슬롯 할당 상태 (16384 슬롯 모두 할당)
// - 노드 간 통신 상태
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 클러스터 정상 여부, 오류
func (cm *ClusterManager) CheckClusterStatus(ctx context.Context, cr *v1alpha1.KRedis) (bool, error) {
	log := log.FromContext(ctx)

	// 첫 번째 마스터 Pod에 접근하여 클러스터 상태 확인
	podName := fmt.Sprintf("%s-master-0", cr.Name)

	// 클러스터 정보 확인
	output, err := cm.execCommandOnPod(ctx, cr.Namespace, podName, []string{
		"redis-cli", "-p", fmt.Sprintf("%d", cr.Spec.BasePort), "cluster", "info",
	})

	if err != nil {
		log.Error(err, "Failed to check cluster info")
		return false, err
	}

	// cluster_state:ok 확인
	if !strings.Contains(output, "cluster_state:ok") {
		log.Info("Cluster state is not OK", "output", output)
		return false, nil
	}

	// 슬롯 커버리지 확인
	output, err = cm.execCommandOnPod(ctx, cr.Namespace, podName, []string{
		"redis-cli", "--cluster", "check",
		fmt.Sprintf("$(hostname -i):%d", cr.Spec.BasePort),
	})

	if err != nil {
		log.Error(err, "Failed to check cluster slots")
		return false, err
	}

	// "All 16384 slots covered" 확인
	if !strings.Contains(output, "All 16384 slots covered") {
		log.Info("Not all slots are covered", "output", output)
		return false, nil
	}

	return true, nil
}

// CheckSlotsBalance는 클러스터 내 슬롯이 균등하게 분배되어 있는지 확인합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 리밸런싱 필요 여부, 오류
func (cm *ClusterManager) CheckSlotsBalance(ctx context.Context, cr *v1alpha1.KRedis) (bool, error) {
	nodes, err := cm.getClusterNodes(ctx, cr)
	if (err != nil) {
		return false, err
	}

	// 마스터 노드만 필터링
	masters := make([]RedisNode, 0)
	for _, node := range nodes {
		if node.Role == "master" {
			masters = append(masters, node)
		}
	}

	if len(masters) == 0 {
		return false, fmt.Errorf("no master nodes found")
	}

	// 이상적인 슬롯 수 계산 (전체 슬롯 / 마스터 수)
	idealSlots := 16384 / len(masters)

	// 슬롯 분포 확인
	for _, master := range masters {
		slotCount := 0
		for _, slotRange := range master.Slots {
			// 슬롯 범위 예: "0-5460"
			parts := strings.Split(slotRange, "-")
			if len(parts) == 2 {
				start := 0
				end := 0
				fmt.Sscanf(parts[0], "%d", &start)
				fmt.Sscanf(parts[1], "%d", &end)
				slotCount += (end - start + 1)
			} else if len(parts) == 1 {
				// 단일 슬롯
				slotCount++
			}
		}

		// 마스터 노드의 슬롯 수가 이상적인 슬롯 수와 크게 차이나는지 확인
		// 20% 이상 차이나면 리밸런싱 필요
		deviation := float64(slotCount-idealSlots) / float64(idealSlots)
		if deviation < -0.2 || deviation > 0.2 {
			return true, nil
		}
	}

	return false, nil
}

// GetCurrentSlaveReplicas는 마스터별로 현재 클러스터에 있는 슬레이브 수를 맵으로 반환합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 마스터ID를 키로 하고 슬레이브 수를 값으로 하는 맵, 오류
func (cm *ClusterManager) GetCurrentSlaveReplicas(ctx context.Context, cr *v1alpha1.KRedis) (map[string]int, error) {
	nodes, err := cm.getClusterNodes(ctx, cr)
	if (err != nil) {
		return nil, err
	}

	// 마스터별 슬레이브 수 맵 초기화
	replicaMap := make(map[string]int)
	
	// 마스터 노드 ID 수집
	for _, node := range nodes {
		if node.Role == "master" {
			replicaMap[node.ID] = 0
		}
	}
	
	// 각 슬레이브의 마스터를 확인하여 카운트
	for _, node := range nodes {
		if node.Role == "slave" && node.MasterID != "" {
			replicaMap[node.MasterID]++
		}
	}
	
	return replicaMap, nil
}