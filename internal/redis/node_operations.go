package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// AddMasterNode는 기존 클러스터에 새로운 마스터 노드를 추가합니다.
// 해시 슬롯을 일부 마이그레이션하여 새 노드에 할당합니다.
//
// 주요 단계:
// 1. 새 노드를 빈 마스터로 클러스터에 추가
// 2. 기존 마스터에서 슬롯 일부를 새 마스터로 리밸런싱
// 3. 슬롯 마이그레이션 모니터링 및 완료 대기
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// nodeIndex: 추가할 마스터 노드의 인덱스
func (cm *ClusterManager) AddMasterNode(ctx context.Context, cr *v1alpha1.KRedis, nodeIndex int) error {
	log := log.FromContext(ctx)

	// 1. 추가할 마스터 노드 Pod 정보 가져오기
	podName := fmt.Sprintf("%s-master-%d", cr.Name, nodeIndex)
	podIP, err := cm.getPodIP(ctx, cr.Namespace, podName)
	if err != nil {
		return err
	}

	// 2. 기존 클러스터에 새 노드 추가 (빈 마스터로)
	firstMasterPod := fmt.Sprintf("%s-master-0", cr.Name)
	cmd := []string{
		"redis-cli", "--cluster", "add-node",
		fmt.Sprintf("%s:%d", podIP, cr.Spec.BasePort),
		// 기존 클러스터의 아무 노드나 사용 (여기서는 첫 번째 마스터 사용)
		fmt.Sprintf("$(hostname -i):%d", cr.Spec.BasePort),
	}

	log.Info("Adding new master node to cluster", "pod", podName, "ip", podIP)
	output, err := cm.execCommandOnPod(ctx, cr.Namespace, firstMasterPod, cmd)
	if err != nil {
		log.Error(err, "Failed to add new master node", "output", output)
		return err
	}

	// 3. 슬롯 리밸런싱 수행
	log.Info("Rebalancing slots to include new master")
	if err := cm.RebalanceSlots(ctx, cr); err != nil {
		log.Error(err, "Failed to rebalance slots")
		return err
	}

	return nil
}

// AddSlaveNode는 기존 클러스터에 슬레이브 노드를 추가합니다.
// 지정된 마스터 노드에 슬레이브 노드를 연결합니다.
//
// 주요 단계:
// 1. 해당 인덱스의 마스터 노드 ID 찾기
// 2. 새 슬레이브 노드를 클러스터에 추가
// 3. 슬레이브 노드를 마스터에 연결(replicaof)
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// masterIndex: 마스터 노드 인덱스
// slaveIndex: 슬레이브 노드 인덱스
func (cm *ClusterManager) AddSlaveNode(ctx context.Context, cr *v1alpha1.KRedis, masterIndex int, slaveIndex int) error {
	log := log.FromContext(ctx)

	// 1. 클러스터 내 모든 노드 정보 가져오기
	nodes, err := cm.getClusterNodes(ctx, cr)
	if err != nil {
		return err
	}

	// 2. 해당 마스터 노드 ID 및 주소 찾기
	var masterID string
	var masterIP string
	masterCount := 0
	for _, node := range nodes {
		if node.Role == "master" {
			if masterCount == masterIndex {
				masterID = node.ID
				masterIP = node.IP
				break
			}
			masterCount++
		}
	}

	if masterID == "" {
		return fmt.Errorf("master node with index %d not found", masterIndex)
	}

	// 3. 추가할 슬레이브 노드 Pod 정보 가져오기
	podName := fmt.Sprintf("%s-slave-%d-%d", cr.Name, masterIndex, slaveIndex)
	podIP, err := cm.getPodIP(ctx, cr.Namespace, podName)
	if err != nil {
		return err
	}

	// 4. 첫 번째 마스터 Pod에서 작업 수행
	firstMasterPod := fmt.Sprintf("%s-master-0", cr.Name)

	// 5. 새 노드를 클러스터에 추가 (슬레이브로)
	log.Info("Adding new slave node to cluster", "pod", podName, "ip", podIP, "masterID", masterID)
	cmd := []string{
		"redis-cli", "--cluster", "add-node",
		fmt.Sprintf("%s:%d", podIP, cr.Spec.BasePort),
		// 기존 클러스터의 아무 노드나 사용 (여기서는 첫 번째 마스터 사용)
		fmt.Sprintf("$(hostname -i):%d", cr.Spec.BasePort),
		"--cluster-slave",
		"--cluster-master-id", masterID,
	}

	output, err := cm.execCommandOnPod(ctx, cr.Namespace, firstMasterPod, cmd)
	if err != nil {
		log.Error(err, "Failed to add slave node", "output", output)
		return err
	}

	// 6. 레플리케이션 상태 확인
	// 슬레이브가 성공적으로 마스터에 연결되었는지 확인
	time.Sleep(5 * time.Second) // 약간의 지연을 두고 확인

	replicationCheckCmd := []string{
		"redis-cli", "-h", podIP, "-p", fmt.Sprintf("%d", cr.Spec.BasePort),
		"info", "replication",
	}

	output, err = cm.execCommandOnPod(ctx, cr.Namespace, firstMasterPod, replicationCheckCmd)
	if err != nil {
		log.Error(err, "Failed to check replication status", "output", output)
		// 오류를 반환하지 않고 경고만 기록 (성공으로 간주하고 계속 진행)
	}

	// 레플리케이션 상태 확인
	if strings.Contains(output, "role:slave") && strings.Contains(output, fmt.Sprintf("master_host:%s", masterIP)) {
		log.Info("Slave node successfully connected to master",
			"slave", podName,
			"master", fmt.Sprintf("%s-master-%d", cr.Name, masterIndex))
	} else {
		log.Info("Warning: Slave node may not be properly connected to master",
			"slave", podName,
			"master", fmt.Sprintf("%s-master-%d", cr.Name, masterIndex),
			"output", output)
	}

	return nil
}

// RemoveMasterNode는 클러스터에서 마스터 노드의 슬롯을 다른 마스터 노드로 마이그레이션합니다.
// 이 함수는 데이터 재분배(리샤딩)만 담당하며, 클러스터에서 노드를 제거하지 않습니다.
//
// 주요 단계:
// 1. 제거할 노드의 슬롯을 다른 마스터로 마이그레이션
// 2. 슬롯 마이그레이션이 완료될 때까지 대기
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// nodeIndex: 제거할 마스터 노드의 인덱스
func (cm *ClusterManager) RemoveMasterNode(ctx context.Context, cr *v1alpha1.KRedis, nodeIndex int) error {
	log := log.FromContext(ctx)

	// 1. 클러스터 내 모든 노드 정보 가져오기
	nodes, err := cm.getClusterNodes(ctx, cr)
	if err != nil {
		return err
	}

	// 2. 제거할 마스터 노드 찾기
	var targetMaster RedisNode
	masterCount := 0
	for _, node := range nodes {
		if node.Role == "master" {
			if masterCount == nodeIndex {
				targetMaster = node
				break
			}
			masterCount++
		}
	}

	if targetMaster.ID == "" {
		return fmt.Errorf("master node with index %d not found", nodeIndex)
	}

	// 3. 첫 번째 마스터 Pod에서 작업 수행
	firstMasterPod := fmt.Sprintf("%s-master-0", cr.Name)

	// 제거할 마스터가 슬롯을 가지고 있는지 확인
	if len(targetMaster.Slots) > 0 {
		// 4. 다른 마스터 노드들 찾기 (슬롯을 재분배할 대상)
		remainingMasters := make([]RedisNode, 0)
		for _, node := range nodes {
			if node.Role == "master" && node.ID != targetMaster.ID {
				remainingMasters = append(remainingMasters, node)
			}
		}

		if len(remainingMasters) == 0 {
			return fmt.Errorf("cannot remove the last master node")
		}

		// 5. 슬롯 마이그레이션 - redis-cli --cluster reshard 명령 사용
		// 슬롯을 남은 마스터 노드들에게 균등하게 분배
		log.Info("Migrating slots from master being removed",
			"masterID", targetMaster.ID,
			"slots", strings.Join(targetMaster.Slots, ","))

		// 대상 노드 ID - 첫 번째 남은 마스터를 임시로 사용
		recipientID := remainingMasters[0].ID

		// reshard 명령 실행
		cmd := []string{
			"redis-cli", "--cluster", "reshard",
			fmt.Sprintf("$(hostname -i):%d", cr.Spec.BasePort),
			"--cluster-from", targetMaster.ID,
			"--cluster-to", recipientID,
			"--cluster-slots", "16384", // 모든 슬롯 이동 (실제로는 노드가 가진 슬롯만 이동됨)
			"--cluster-yes",
		}

		output, err := cm.execCommandOnPod(ctx, cr.Namespace, firstMasterPod, cmd)
		if err != nil {
			log.Error(err, "Failed to migrate slots", "output", output)
			return err
		}

		log.Info("Slots migration completed", "output", output)
	} else {
		log.Info("Master node has no slots, skipping migration", "masterID", targetMaster.ID)
	}

	return nil
}

// ForgetNode는 클러스터에서 노드를 제거합니다.
// 이 함수는 노드 ID를 찾아 CLUSTER FORGET 명령을 사용하여 클러스터에서 노드를 제거합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// nodeIndex: 노드 인덱스
// role: 노드 역할 ("master" 또는 "slave")
func (cm *ClusterManager) ForgetNode(ctx context.Context, cr *v1alpha1.KRedis, nodeIndex int, role string) error {
	log := log.FromContext(ctx)

	// 1. 클러스터 내 모든 노드 정보 가져오기
	nodes, err := cm.getClusterNodes(ctx, cr)
	if err != nil {
		return err
	}

	// 2. 제거할 노드 찾기
	var targetNode RedisNode
	var count int

	// role에 따라 마스터 또는 슬레이브 노드 찾기
	for _, node := range nodes {
		if node.Role == role {
			if count == nodeIndex {
				targetNode = node
				break
			}
			count++
		}
	}

	if targetNode.ID == "" {
		return fmt.Errorf("node with role %s and index %d not found", role, nodeIndex)
	}

	// 3. 첫 번째 마스터 Pod에서 작업 수행
	firstMasterPod := fmt.Sprintf("%s-master-0", cr.Name)

	// 4. 모든 노드에서 타겟 노드를 잊도록 CLUSTER FORGET 명령 실행
	log.Info("Removing node from cluster", "nodeID", targetNode.ID, "role", role)

	// 클러스터 내 모든 노드에서 타겟 노드 제거
	for _, node := range nodes {
		// 자기 자신은 제외
		if node.ID == targetNode.ID {
			continue
		}

		cmd := []string{
			"redis-cli", "-h", node.IP, "-p", node.Port, "cluster", "forget", targetNode.ID,
		}

		output, err := cm.execCommandOnPod(ctx, cr.Namespace, firstMasterPod, cmd)
		if err != nil {
			log.Error(err, "Failed to forget node", "nodeID", targetNode.ID, "from", node.ID, "output", output)
			// 오류가 발생해도 계속 다른 노드에서 제거 시도
			continue
		}
	}

	// 5. 노드가 성공적으로 제거되었는지 확인
	// 새 노드 목록 가져오기
	newNodes, err := cm.getClusterNodes(ctx, cr)
	if err != nil {
		return err
	}

	// 제거된 노드가 목록에 있는지 확인
	for _, node := range newNodes {
		if node.ID == targetNode.ID {
			log.Info("Warning: Node still exists in cluster", "nodeID", targetNode.ID)
			// 이 경우, 일부 노드에서만 제거되었을 수 있으므로 경고만 기록
			break
		}
	}

	log.Info("Successfully removed node from cluster", "nodeID", targetNode.ID, "role", role)
	return nil
}

// RemoveSlaveNode는 클러스터에서 슬레이브 노드를 제거합니다.
//
// 주요 단계:
// 1. 클러스터에서 슬레이브 노드 제거 (CLUSTER FORGET)
// 2. 제거 확인
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// masterIndex: 마스터 노드 인덱스
// slaveIndex: 슬레이브 노드 인덱스
func (cm *ClusterManager) RemoveSlaveNode(ctx context.Context, cr *v1alpha1.KRedis, masterIndex int, slaveIndex int) error {
	log := log.FromContext(ctx)

	// 1. 클러스터 내 모든 노드 정보 가져오기
	nodes, err := cm.getClusterNodes(ctx, cr)
	if err != nil {
		return err
	}

	// 2. 해당 마스터 노드 ID 찾기
	var masterID string
	masterCount := 0
	for _, node := range nodes {
		if node.Role == "master" {
			if masterCount == masterIndex {
				masterID = node.ID
				break
			}
			masterCount++
		}
	}

	if masterID == "" {
		return fmt.Errorf("master node with index %d not found", masterIndex)
	}

	// 3. 제거할 슬레이브 노드 찾기
	var targetSlave RedisNode
	slaveCount := 0
	for _, node := range nodes {
		if node.Role == "slave" && node.MasterID == masterID {
			if slaveCount == slaveIndex {
				targetSlave = node
				break
			}
			slaveCount++
		}
	}

	if targetSlave.ID == "" {
		return fmt.Errorf("slave node with index %d for master index %d not found", slaveIndex, masterIndex)
	}

	// 4. 첫 번째 마스터 Pod에서 작업 수행
	firstMasterPod := fmt.Sprintf("%s-master-0", cr.Name)

	// 5. 노드를 클러스터에서 제거
	log.Info("Removing slave node from cluster", "slaveID", targetSlave.ID)
	cmd := []string{
		"redis-cli", "--cluster", "del-node",
		fmt.Sprintf("$(hostname -i):%d", cr.Spec.BasePort),
		targetSlave.ID,
	}

	output, err := cm.execCommandOnPod(ctx, cr.Namespace, firstMasterPod, cmd)
	if err != nil {
		log.Error(err, "Failed to remove slave node", "output", output)
		return err
	}

	log.Info("Successfully removed slave node from cluster", "output", output)
	return nil
}

// ReconcileSlavesForMaster는 특정 마스터 노드에 대한 슬레이브 노드 수를 조정합니다.
// 필요에 따라 슬레이브를 추가하거나 제거합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// masterIndex: 마스터 노드 인덱스
// return: 슬레이브 변경 발생 여부, 오류
func (cm *ClusterManager) ReconcileSlavesForMaster(ctx context.Context, cr *v1alpha1.KRedis, masterIndex int) (bool, error) {
	log := log.FromContext(ctx)
	
	// 현재 슬레이브 수 확인
	currentSlaveCount, err := cm.GetSlaveCountForMaster(ctx, cr, masterIndex)
	if err != nil {
		return false, err
	}
	
	desiredSlaveCount := int(cr.Spec.Replicas)
	changesApplied := false
	
	// 부족한 슬레이브 추가
	if currentSlaveCount < desiredSlaveCount {
		log.Info("Adding slaves to master", "masterIndex", masterIndex, 
			"current", currentSlaveCount, "desired", desiredSlaveCount)
			
		for i := currentSlaveCount; i < desiredSlaveCount; i++ {
			if err := cm.AddSlaveNode(ctx, cr, masterIndex, i); err != nil {
				return changesApplied, err
			}
			changesApplied = true
		}
	} else if currentSlaveCount > desiredSlaveCount {
		// 초과 슬레이브 제거
		log.Info("Removing extra slaves from master", "masterIndex", masterIndex,
			"current", currentSlaveCount, "desired", desiredSlaveCount)
			
		// 뒤에서부터 제거 (인덱스가 높은 슬레이브부터)
		for i := currentSlaveCount - 1; i >= desiredSlaveCount; i-- {
			if err := cm.RemoveSlaveNode(ctx, cr, masterIndex, i); err != nil {
				return changesApplied, err
			}
			changesApplied = true
		}
	}
	
	return changesApplied, nil
}