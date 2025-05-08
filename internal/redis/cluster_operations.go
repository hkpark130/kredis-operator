package redis

import (
	"context"
	"fmt"
	"strings"
	
	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// InitializeCluster는 새로운 Redis 클러스터를 초기화합니다.
// 마스터 노드가 모두 준비된 후 클러스터를 생성하고, 슬롯을 균등하게 분배합니다.
//
// 주요 단계:
// 1. 모든 마스터 노드가 준비될 때까지 대기
// 2. 클러스터 생성 명령 실행 (redis-cli --cluster create)
// 3. 16384개 슬롯을 마스터 노드에 균등 분배
// 4. 클러스터 상태 검증
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
func (cm *ClusterManager) InitializeCluster(ctx context.Context, cr *v1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 1. 모든 마스터 노드와 슬레이브 노드가 준비될 때까지 대기
	if err := cm.waitForAllNodes(ctx, cr); err != nil {
		return err
	}

	// 2. 클러스터 생성을 위한 노드 주소 목록 구성
	nodeAddresses := make([]string, 0)

	// 모든 마스터 노드 추가
	for i := 0; i < cr.Spec.Masters; i++ {
		podName := fmt.Sprintf("%s-master-%d", cr.Name, i)
		podIP, err := cm.getPodIP(ctx, cr.Namespace, podName)
		if err != nil {
			return err
		}
		nodeAddresses = append(nodeAddresses, fmt.Sprintf("%s:%d", podIP, cr.Spec.BasePort))
	}

	// 모든 슬레이브 노드 추가
	for i := 0; i < cr.Spec.Masters; i++ {
		for j := 0; j < int(cr.Spec.Replicas); j++ {
			podName := fmt.Sprintf("%s-slave-%d-%d", cr.Name, i, j)
			podIP, err := cm.getPodIP(ctx, cr.Namespace, podName)
			if err != nil {
				return err
			}
			nodeAddresses = append(nodeAddresses, fmt.Sprintf("%s:%d", podIP, cr.Spec.BasePort))
		}
	}

	// 3. redis-cli --cluster create 명령 실행
	cmd := []string{
		"redis-cli", "--cluster", "create",
	}
	cmd = append(cmd, nodeAddresses...)
	cmd = append(cmd, "--cluster-replicas", fmt.Sprintf("%d", cr.Spec.Replicas), "--cluster-yes")

	log.Info("Initializing Redis cluster", "command", strings.Join(cmd, " "))

	output, err := cm.execCommandOnPod(ctx, cr.Namespace, fmt.Sprintf("%s-master-0", cr.Name), cmd)
	if err != nil {
		log.Error(err, "Failed to initialize Redis cluster", "output", output)
		return err
	}

	log.Info("Redis cluster initialized successfully", "output", output)
	return nil
}

// RebalanceSlots는 클러스터 내 슬롯을 균등하게 재분배합니다.
//
// 주요 단계:
// 1. 현재 슬롯 분포 확인
// 2. 목표 분포 계산 (마스터 노드 간 균등 분배)
// 3. 슬롯 마이그레이션 수행
// 4. 마이그레이션 완료 확인
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
func (cm *ClusterManager) RebalanceSlots(ctx context.Context, cr *v1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 첫 번째 마스터 Pod에서 작업 수행
	firstMasterPod := fmt.Sprintf("%s-master-0", cr.Name)

	// redis-cli --cluster rebalance 명령 실행
	cmd := []string{
		"redis-cli", "--cluster", "rebalance",
		fmt.Sprintf("$(hostname -i):%d", cr.Spec.BasePort),
		"--cluster-use-empty-masters",
		"--cluster-yes",
	}

	output, err := cm.execCommandOnPod(ctx, cr.Namespace, firstMasterPod, cmd)
	if err != nil {
		log.Error(err, "Failed to rebalance slots", "output", output)
		return err
	}

	log.Info("Successfully rebalanced slots", "output", output)
	return nil
}

// EnsureClusterHealth는 Redis 클러스터의 건강 상태를 확인하고 필요한 복구 작업을 수행합니다.
//
// 주요 작업:
// 1. 클러스터 상태 확인
// 2. 누락된 슬롯 확인 및 복구
// 3. 노드 간 연결 확인
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
func (cm *ClusterManager) EnsureClusterHealth(ctx context.Context, cr *v1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 클러스터 상태 확인
	healthy, err := cm.CheckClusterStatus(ctx, cr)
	if err != nil {
		return err
	}

	if !healthy {
		log.Info("Cluster is not healthy, trying to fix issues")
		
		// 슬롯 밸런싱 수행
		needsRebalancing, err := cm.CheckSlotsBalance(ctx, cr)
		if err != nil {
			log.Error(err, "Failed to check slots balance")
			return err
		}
		
		if needsRebalancing {
			log.Info("Rebalancing slots...")
			if err := cm.RebalanceSlots(ctx, cr); err != nil {
				return err
			}
		}
		
		// 후속 상태 확인
		healthy, err = cm.CheckClusterStatus(ctx, cr)
		if err != nil {
			return err
		}
		
		if !healthy {
			log.Info("Cluster is still not healthy after fix attempts")
			return fmt.Errorf("cluster is not healthy after fix attempts")
		}
		
		log.Info("Cluster health issues fixed successfully")
	}
	
	return nil
}

// DetectClusterConfigChanges는 KRedis 리소스와 실제 클러스터 설정 간의 차이를 감지합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 마스터 변경 필요 여부, 슬레이브 변경 필요 여부, 오류
func (cm *ClusterManager) DetectClusterConfigChanges(ctx context.Context, cr *v1alpha1.KRedis) (bool, bool, error) {
	// 마스터 노드 변경 확인
	currentMasters, err := cm.GetCurrentMasters(ctx, cr)
	if err != nil {
		return false, false, err
	}
	
	masterChangesNeeded := len(currentMasters) != cr.Spec.Masters
	
	// 슬레이브 구성 변경 확인
	slaveChangesNeeded := false
	
	// 현재 마스터별 슬레이브 수 확인
	replicaMap, err := cm.GetCurrentSlaveReplicas(ctx, cr)
	if err != nil {
		return masterChangesNeeded, false, err
	}
	
	// 각 마스터의 슬레이브 수가 원하는 값과 다른지 확인
	for _, master := range currentMasters {
		if replicaMap[master.ID] != int(cr.Spec.Replicas) {
			slaveChangesNeeded = true
			break
		}
	}
	
	return masterChangesNeeded, slaveChangesNeeded, nil
}