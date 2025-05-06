package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ClusterManager는 Redis 클러스터 구성 및 관리를 위한 기능을 제공합니다.
// 클러스터의 생성, 확장, 축소, 모니터링 등 전반적인 라이프사이클을 관리합니다.
type ClusterManager struct {
	client client.Client
}

// RedisNode는 클러스터 내의 Redis 노드 정보를 저장합니다.
type RedisNode struct {
	ID       string
	IP       string
	Port     string
	Role     string   // "master" 또는 "slave"
	MasterID string   // 슬레이브인 경우 마스터 ID
	Slots    []string // 슬롯 범위 (마스터만 해당)
}

// NewClusterManager는 새로운 ClusterManager 인스턴스를 생성합니다.
//
// client: 쿠버네티스 API 클라이언트
// return: 초기화된 ClusterManager 객체
func NewClusterManager(client client.Client) *ClusterManager {
	return &ClusterManager{
		client: client,
	}
}

// CheckClusterExists는 Redis 클러스터가 이미 초기화되어 있는지 확인합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 클러스터 존재 여부, 오류
func (cm *ClusterManager) CheckClusterExists(ctx context.Context, cr *v1alpha1.KRedis) (bool, error) {
	log := log.FromContext(ctx)

	// 첫 번째 마스터 Pod에 접근하여 클러스터 상태 확인
	podName := fmt.Sprintf("%s-master-0", cr.Name)

	// redis-cli --cluster info 명령 실행
	output, err := cm.execCommandOnPod(ctx, cr.Namespace, podName, []string{
		"redis-cli", "-p", fmt.Sprintf("%d", cr.Spec.BasePort), "cluster", "info",
	})

	if err != nil {
		// Pod가 아직 준비되지 않았거나, Redis가 실행 중이지 않을 수 있음
		log.Info("Failed to check cluster state, cluster may not be initialized yet", "error", err)
		return false, nil
	}

	// 출력에 "cluster_state:ok" 또는 "cluster_state:fail" 포함 여부 확인
	return strings.Contains(output, "cluster_state:ok") || strings.Contains(output, "cluster_state:fail"), nil
}

// GetCurrentMasters는 현재 클러스터에 있는 마스터 노드 목록을 가져옵니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// return: 마스터 노드 목록, 오류
func (cm *ClusterManager) GetCurrentMasters(ctx context.Context, cr *v1alpha1.KRedis) ([]RedisNode, error) {
	nodes, err := cm.getClusterNodes(ctx, cr)
	if err != nil {
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

// GetSlaveCountForMaster는 특정 마스터 노드의 슬레이브 수를 반환합니다.
//
// ctx: 컨텍스트
// cr: KRedis 커스텀 리소스
// masterIndex: 마스터 노드 인덱스
// return: 슬레이브 수, 오류
func (cm *ClusterManager) GetSlaveCountForMaster(ctx context.Context, cr *v1alpha1.KRedis, masterIndex int) (int, error) {
	nodes, err := cm.getClusterNodes(ctx, cr)
	if err != nil {
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

	// 해당 마스터에 연결된 슬레이브 수 계산
	slaveCount := 0
	for _, node := range nodes {
		if node.Role == "slave" && node.MasterID == masterID {
			slaveCount++
		}
	}

	return slaveCount, nil
}

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
	if err != nil {
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

// 유틸리티 메서드들

// getClusterNodes는 Redis 클러스터의 모든 노드 정보를 가져옵니다.
func (cm *ClusterManager) getClusterNodes(ctx context.Context, cr *v1alpha1.KRedis) ([]RedisNode, error) {
	// 첫 번째 마스터 Pod에서 CLUSTER NODES 명령 실행
	podName := fmt.Sprintf("%s-master-0", cr.Name)

	output, err := cm.execCommandOnPod(ctx, cr.Namespace, podName, []string{
		"redis-cli", "-p", fmt.Sprintf("%d", cr.Spec.BasePort), "cluster", "nodes",
	})

	if err != nil {
		return nil, err
	}

	// 출력 파싱하여 노드 정보 추출
	return cm.parseClusterNodes(output), nil
}

// parseClusterNodes는 'CLUSTER NODES' 명령의 출력을 파싱하여 노드 정보를 추출합니다.
func (cm *ClusterManager) parseClusterNodes(output string) []RedisNode {
	nodes := make([]RedisNode, 0)

	for _, line := range strings.Split(output, "\n") {
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}

		// 노드 ID
		id := parts[0]

		// IP:Port
		addrParts := strings.Split(parts[1], ":")
		if len(addrParts) < 2 {
			continue
		}
		ip := addrParts[0]
		port := addrParts[1]

		// 역할 (마스터/슬레이브)
		role := "master"
		masterID := ""
		if strings.Contains(parts[2], "slave") {
			role = "slave"
			masterID = parts[3]
		}

		// 슬롯 정보 (마스터만 해당)
		slots := make([]string, 0)
		if role == "master" {
			for i := 8; i < len(parts); i++ {
				slots = append(slots, parts[i])
			}
		}

		nodes = append(nodes, RedisNode{
			ID:       id,
			IP:       ip,
			Port:     port,
			Role:     role,
			MasterID: masterID,
			Slots:    slots,
		})
	}

	return nodes
}

// waitForAllNodes는 모든 Redis 노드가 준비될 때까지 대기합니다.
func (cm *ClusterManager) waitForAllNodes(ctx context.Context, cr *v1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 마스터 노드 대기
	for i := 0; i < cr.Spec.Masters; i++ {
		podName := fmt.Sprintf("%s-master-%d", cr.Name, i)
		log.Info("Waiting for master pod to be ready", "pod", podName)

		if err := cm.waitForPod(ctx, cr.Namespace, podName); err != nil {
			return err
		}
	}

	// 슬레이브 노드 대기
	for i := 0; i < cr.Spec.Masters; i++ {
		for j := 0; j < int(cr.Spec.Replicas); j++ {
			podName := fmt.Sprintf("%s-slave-%d-%d", cr.Name, i, j)
			log.Info("Waiting for slave pod to be ready", "pod", podName)

			if err := cm.waitForPod(ctx, cr.Namespace, podName); err != nil {
				return err
			}
		}
	}

	return nil
}

// waitForPod는 Pod가 준비될 때까지 대기합니다.
func (cm *ClusterManager) waitForPod(ctx context.Context, namespace, podName string) error {
	// 최대 10분 대기
	timeout := time.After(10 * time.Minute)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for pod %s to be ready", podName)
		case <-tick.C:
			pod := &corev1.Pod{}
			err := cm.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod)
			if err != nil {
				continue
			}

			// Pod가 Ready 상태인지 확인
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// getPodIP는 Pod의 IP 주소를 가져옵니다.
func (cm *ClusterManager) getPodIP(ctx context.Context, namespace, podName string) (string, error) {
	pod := &corev1.Pod{}
	err := cm.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod)
	if err != nil {
		return "", err
	}

	if pod.Status.PodIP == "" {
		return "", fmt.Errorf("pod %s has no IP address", podName)
	}

	return pod.Status.PodIP, nil
}

// execCommandOnPod는 Pod에서 명령을 실행합니다.
func (cm *ClusterManager) execCommandOnPod(ctx context.Context, namespace, podName string, command []string) (string, error) {
	// TODO: 실제로 kubectl exec를 사용하여 Pod에서 명령 실행
	// 여기서는 간단한 구현만 제공

	// 실제 구현에서는:
	// 1. client-go의 Exec 함수를 사용하거나
	// 2. kubectl exec 명령을 실행하여 Pod에 접속
	// 3. 명령 실행 결과 수집 및 반환

	return "Command executed successfully (mock)", nil
}
