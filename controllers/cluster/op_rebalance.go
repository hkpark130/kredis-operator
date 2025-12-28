package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// rebalanceCluster rebalances slots across masters and replicates
func (cm *ClusterManager) rebalanceCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	lastOp := kredis.Status.LastClusterOperation

	masterPod := cm.findMasterPod(pods, kredis, clusterState)
	if masterPod == nil {
		return fmt.Errorf("no master pod found for rebalancing")
	}

	// rebalance-needed 상태면 리밸런스 시작, in-progress면 검증만 수행
	if strings.Contains(lastOp, "rebalance-needed") {
		logger.Info("Starting rebalance triggered by scale operation")
		delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())

		// 리밸런스 트리거 (다음 reconcile에서 검증)
		if _, err := cm.PodExecutor.RebalanceCluster(ctx, *masterPod, kredis.Spec.BasePort); err != nil {
			if strings.Contains(err.Error(), "ERR Please use SETSLOT only with masters") {
				logger.Info("Ignoring 'SETSLOT' error during rebalance trigger")
			} else {
				delta.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
				return fmt.Errorf("rebalance failed to start: %w", err)
			}
		}
		// 다음 reconcile에서 검증 수행
		return nil
	}

	// rebalance-in-progress 상태: 검증 단계
	delta.LastClusterOperation = fmt.Sprintf("rebalance-in-progress:%d", time.Now().Unix())

	// 리밸런스가 실제 진행 중인지 확인; 진행 중이 아니면 한번 트리거
	if !cm.checkIfRebalanceInProgress(ctx, *masterPod, kredis.Spec.BasePort) {
		if _, err := cm.PodExecutor.RebalanceCluster(ctx, *masterPod, kredis.Spec.BasePort); err != nil {
			if strings.Contains(err.Error(), "ERR Please use SETSLOT only with masters") {
				logger.Info("Ignoring 'SETSLOT' error during rebalance trigger")
			} else {
				delta.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
				return fmt.Errorf("rebalance failed to start: %w", err)
			}
		}
	}

	logger.Info("Waiting for cluster to stabilize after rebalance...")
	if err := cm.waitForClusterStabilization(ctx, kredis, masterPod); err != nil {
		delta.LastClusterOperation = fmt.Sprintf("rebalance-failed:%d", time.Now().Unix())
		return fmt.Errorf("cluster failed to stabilize after rebalance: %w", err)
	}

	ok, err := cm.checkAllMastersHaveSlots(ctx, *masterPod, kredis.Spec.BasePort)
	if err != nil {
		return fmt.Errorf("failed to verify slots distribution: %w", err)
	}
	if !ok {
		logger.Info("Rebalance might still be ongoing; will verify in next reconcile")
		return nil
	}

	delta.LastClusterOperation = fmt.Sprintf("rebalance-success:%d", time.Now().Unix())
	return nil
}
