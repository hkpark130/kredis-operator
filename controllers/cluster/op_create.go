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

// createCluster initializes a new Redis cluster
func (cm *ClusterManager) createCluster(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod, delta *ClusterStatusDelta) error {
	logger := log.FromContext(ctx)

	logger.Info("Resetting all nodes before cluster creation to ensure a clean state.")
	for _, pod := range pods {
		logger.Info("Executing FLUSHALL on pod", "pod", pod.Name)
		if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, kredis.Spec.BasePort, "FLUSHALL"); err != nil {
			logger.Error(err, "Failed to flush node, but proceeding", "pod", pod.Name)
		}
		logger.Info("Executing CLUSTER RESET on pod", "pod", pod.Name)
		if _, err := cm.PodExecutor.ExecuteRedisCommand(ctx, pod, kredis.Spec.BasePort, "CLUSTER", "RESET"); err != nil {
			return fmt.Errorf("failed to reset node %s: %w", pod.Name, err)
		}
	}
	time.Sleep(2 * time.Second)

	delta.LastClusterOperation = fmt.Sprintf("create-in-progress:%d", time.Now().Unix())

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

	_, err := cm.PodExecutor.CreateCluster(ctx, *commandPod, kredis.Spec.BasePort, nodeIPs, int(kredis.Spec.Replicas))
	if err != nil {
		delta.LastClusterOperation = fmt.Sprintf("create-failed:%d", time.Now().Unix())
		return fmt.Errorf("failed to create cluster: %w", err)
	}

	logger.Info("Waiting for cluster to stabilize after creation...")
	err = cm.waitForClusterStabilization(ctx, kredis, commandPod)
	if err != nil {
		delta.LastClusterOperation = fmt.Sprintf("create-failed:%d", time.Now().Unix())
		return fmt.Errorf("cluster failed to stabilize after creation: %w", err)
	}

	delta.LastClusterOperation = fmt.Sprintf("create-success:%d", time.Now().Unix())
	delta.ClusterState = "created"
	time.Sleep(2 * time.Second)
	logger.Info("Successfully created Redis cluster")

	known := len(pods)
	delta.KnownClusterNodes = &known
	var joinedPodNames []string
	for _, pod := range pods {
		if pod.Status.PodIP != "" {
			joinedPodNames = append(joinedPodNames, pod.Name)
		}
	}
	delta.JoinedPods = joinedPodNames

	return nil
}
