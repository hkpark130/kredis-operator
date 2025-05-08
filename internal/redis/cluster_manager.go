package redis

import (
	"context"
	"fmt"
	
	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
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

// SlaveScaleStatus는 슬레이브 노드 스케일 다운 상태를 추적합니다.
type SlaveScaleStatus struct {
	InProgress bool   // 스케일 다운이 진행 중인지 여부
	Completed  bool   // 스케일 다운이 완료되었는지 여부
	MasterIdx  int    // 관련 마스터 인덱스
	Message    string // 상태 메시지
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

// OrchestrateCluster는 Redis 클러스터 구성을 관리합니다.
// 클러스터 초기화, 슬롯 재분배, 노드 추가/제거 등의 작업을 수행합니다.
func (cm *ClusterManager) OrchestrateCluster(ctx context.Context, cr *v1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 1. 클러스터 상태 확인 (초기화 필요 여부)
	clusterExists, err := cm.CheckClusterExists(ctx, cr)
	if err != nil {
		log.Error(err, "Failed to check if cluster exists")
		return err
	}

	// 2. 필요시 클러스터 초기화
	if !clusterExists {
		log.Info("Initializing new Redis cluster")
		if err := cm.InitializeCluster(ctx, cr); err != nil {
			log.Error(err, "Failed to initialize cluster")
			return err
		}
		log.Info("Successfully initialized Redis cluster")
		return nil // 초기화 후 다음 조정에서 나머지 작업 진행
	}

	// 3. 구성 변경이 필요한지 확인
	masterChangesNeeded, slaveChangesNeeded, err := cm.DetectClusterConfigChanges(ctx, cr)
	if err != nil {
		log.Error(err, "Failed to detect cluster configuration changes")
		return err
	}

	// 4. 마스터 노드 변경 처리
	if masterChangesNeeded {
		if err := cm.reconcileMasterNodes(ctx, cr); err != nil {
			log.Error(err, "Failed to reconcile master nodes")
			return err
		}
	}

	// 5. 슬레이브 노드 변경 처리
	if slaveChangesNeeded {
		if err := cm.reconcileSlaveNodes(ctx, cr); err != nil {
			log.Error(err, "Failed to reconcile slave nodes")
			return err
		}
	}

	// 6. 슬롯 밸런싱이 필요한지 확인하고 수행
	needsRebalancing, err := cm.CheckSlotsBalance(ctx, cr)
	if err != nil {
		log.Error(err, "Failed to check slots balance")
		return err
	}

	if needsRebalancing {
		log.Info("Rebalancing slots across cluster")
		if err := cm.RebalanceSlots(ctx, cr); err != nil {
			log.Error(err, "Failed to rebalance slots")
			return err
		}
	}

	// 7. 클러스터 건강 상태 확인
	if err := cm.EnsureClusterHealth(ctx, cr); err != nil {
		log.Error(err, "Cluster health issues detected")
		return err
	}

	log.Info("Cluster orchestration completed successfully")
	return nil
}

// reconcileMasterNodes는 마스터 노드를 원하는 수로 조정합니다.
// 필요에 따라 마스터 노드를 추가하거나 제거합니다.
func (cm *ClusterManager) reconcileMasterNodes(ctx context.Context, cr *v1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 현재 마스터 노드 확인
	currentMasters, err := cm.GetCurrentMasters(ctx, cr)
	if err != nil {
		return err
	}

	// 마스터 노드 수 변경 감지
	desiredMasters := cr.Spec.Masters
	if len(currentMasters) > desiredMasters {
		// 스케일 다운 필요
		log.Info("Master scale down detected", "current", len(currentMasters), "desired", desiredMasters)

		// 제거할 마스터 노드 수
		toRemove := len(currentMasters) - desiredMasters

		for i := 0; i < toRemove; i++ {
			// 현재 마스터 노드 목록에서 마지막 인덱스를 제거 대상으로 선택
			nodeIndex := len(currentMasters) - 1 - i

			log.Info("Removing master node and its slaves", "masterIndex", nodeIndex)

			// 1. 마스터 노드의 데이터 리샤딩 (중요: 데이터 이동 먼저 수행)
			if err := cm.RemoveMasterNode(ctx, cr, nodeIndex); err != nil {
				log.Error(err, "Failed to remove master node (resharding failed)", "index", nodeIndex)
				return err
			}
			log.Info("Successfully resharded data from master node", "masterIndex", nodeIndex)

			// 2. 마스터에 연결된 슬레이브 노드들 제거
			slaveCount, err := cm.GetSlaveCountForMaster(ctx, cr, nodeIndex)
			if err != nil {
				log.Error(err, "Failed to get slave count for master", "masterIndex", nodeIndex)
				return err
			}

			for slaveIdx := 0; slaveIdx < slaveCount; slaveIdx++ {
				if err := cm.RemoveSlaveNode(ctx, cr, nodeIndex, slaveIdx); err != nil {
					log.Error(err, "Failed to remove slave node", "masterIndex", nodeIndex, "slaveIndex", slaveIdx)
					return err
				}
				log.Info("Successfully removed slave node", "masterIndex", nodeIndex, "slaveIndex", slaveIdx)
			}

			// 3. 클러스터에서 마스터 노드도 완전히 제거
			if err := cm.ForgetNode(ctx, cr, nodeIndex, "master"); err != nil {
				log.Error(err, "Failed to forget master node from cluster", "index", nodeIndex)
				return err
			}
			log.Info("Successfully forgot master node from cluster", "masterIndex", nodeIndex)

			// 4. StatefulSet 레플리카 수 조정을 위한 어노테이션 설정
			masterSts := &appsv1.StatefulSet{}
			err = cm.client.Get(ctx, types.NamespacedName{
				Name:      fmt.Sprintf("%s-master", cr.Name),
				Namespace: cr.Namespace,
			}, masterSts)
			if err != nil {
				log.Error(err, "Failed to get master StatefulSet")
				return err
			}

			if masterSts.Annotations == nil {
				masterSts.Annotations = make(map[string]string)
			}
			
			// 완료 어노테이션 설정 및 진행 중 어노테이션 제거
			log.Info("Setting scale-down-complete annotation on master StatefulSet")
			masterSts.Annotations["redis.kredis-operator/scale-down-complete"] = "true"
			delete(masterSts.Annotations, "redis.kredis-operator/scale-down-in-progress")

			if err = cm.client.Update(ctx, masterSts); err != nil {
				log.Error(err, "Failed to update master StatefulSet annotations")
				return err
			}
			
			log.Info("Successfully updated StatefulSet annotations after scale down")
		}
		
		// 처리 완료 후 확인 로그
		log.Info("Master scale down process completed", "removed", toRemove, "remaining", desiredMasters)
		
	} else if len(currentMasters) < desiredMasters {
		// 스케일 업 필요
		log.Info("Master scale up detected", "current", len(currentMasters), "desired", desiredMasters)

		// 추가할 마스터 노드 수
		toAdd := desiredMasters - len(currentMasters)

		for i := 0; i < toAdd; i++ {
			nodeIndex := len(currentMasters) + i

			// 1. 마스터 노드 추가
			if err := cm.AddMasterNode(ctx, cr, nodeIndex); err != nil {
				log.Error(err, "Failed to add master node", "index", nodeIndex)
				return err
			}

			// 2. 필요한 슬레이브 노드 추가
			for slaveIdx := 0; slaveIdx < int(cr.Spec.Replicas); slaveIdx++ {
				if err := cm.AddSlaveNode(ctx, cr, nodeIndex, slaveIdx); err != nil {
					log.Error(err, "Failed to add slave node", "masterIndex", nodeIndex, "slaveIndex", slaveIdx)
					return err
				}
			}

			log.Info("Successfully added master node and its slaves", "masterIndex", nodeIndex)
		}
	}

	return nil
}

// reconcileSlaveNodes는 각 마스터에 대한 슬레이브 노드 수를 조정합니다.
// 필요에 따라 슬레이브를 추가하거나 제거합니다.
func (cm *ClusterManager) reconcileSlaveNodes(ctx context.Context, cr *v1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 현재 마스터 노드 확인
	currentMasters, err := cm.GetCurrentMasters(ctx, cr)
	if err != nil {
		return err
	}

	// 각 마스터에 대해 슬레이브 수 조정
	for i, master := range currentMasters {
		log.Info("Reconciling slaves for master", "masterID", master.ID, "masterIndex", i)
		
		// 현재 슬레이브 수 확인
		currentSlaveCount, err := cm.GetSlaveCountForMaster(ctx, cr, i)
		if err != nil {
			log.Error(err, "Failed to get current slave count", "masterIndex", i)
			return err
		}

		// 슬레이브 수 조정
		if currentSlaveCount != int(cr.Spec.Replicas) {
			log.Info("Adjusting slave count", 
				"masterIndex", i, 
				"currentCount", currentSlaveCount, 
				"desiredCount", cr.Spec.Replicas)
				
			// StatefulSet 업데이트 전에 슬레이브 노드 조정
			changesApplied, err := cm.ReconcileSlavesForMaster(ctx, cr, i)
			if err != nil {
				log.Error(err, "Failed to reconcile slaves for master", "masterIndex", i)
				return err
			}
			
			if changesApplied {
				log.Info("Successfully reconciled slaves for master", "masterIndex", i)
			}
		}
	}

	return nil
}