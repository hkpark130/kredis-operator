package cluster

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// healCluster attempts to fix a degraded cluster state
func (cm *ClusterManager) healCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, clusterState []cachev1alpha1.ClusterNode, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)
	delta.LastClusterOperation = fmt.Sprintf("heal-in-progress:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateHealing)

	masterPod := cm.findMasterPod(pods, kredis, clusterState)
	if masterPod == nil {
		return fmt.Errorf("failed to find a master pod for heal")
	}

	// 이미 건강한 경우(재개 검증 등)에는 즉시 성공 처리
	if ok, err := cm.PodExecutor.IsClusterHealthy(ctx, *masterPod, kredis.Spec.BasePort); err == nil && ok {
		logger.Info("Cluster already healthy; marking heal as success")
		delta.LastClusterOperation = fmt.Sprintf("heal-success:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)
		return nil
	}

	if err := cm.PodExecutor.RepairCluster(ctx, *masterPod, kredis.Spec.BasePort); err != nil {
		delta.LastClusterOperation = fmt.Sprintf("heal-failed:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
		return fmt.Errorf("heal failed: %w", err)
	}

	logger.Info("Waiting for cluster to stabilize after heal...")
	if err := cm.waitForClusterStabilization(ctx, kredis, masterPod); err != nil {
		delta.LastClusterOperation = fmt.Sprintf("heal-failed:%d", time.Now().Unix())
		delta.ClusterState = string(cachev1alpha1.ClusterStateFailed)
		return fmt.Errorf("cluster failed to stabilize after heal: %w", err)
	}

	delta.LastClusterOperation = fmt.Sprintf("heal-success:%d", time.Now().Unix())
	delta.ClusterState = string(cachev1alpha1.ClusterStateRunning)
	return nil
}
