package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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
	log := log.FromContext(ctx)
	
	// 재시도 설정
	maxRetries := 5
	retryInterval := 3 * time.Second
	
	var lastErr error
	
	for i := 0; i < maxRetries; i++ {
		// 파드 정보 조회
		pod := &corev1.Pod{}
		err := cm.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, pod)
		if err != nil {
			log.Error(err, "Failed to get pod", "pod", podName)
			lastErr = err
			time.Sleep(retryInterval)
			continue
		}
		
		// Pod가 Running 상태인지 확인
		if pod.Status.Phase != corev1.PodRunning {
			log.Info("Pod is not in Running state yet", 
				"pod", podName, 
				"phase", pod.Status.Phase,
				"retry", i+1)
			lastErr = fmt.Errorf("pod %s is in %s state, not Running", podName, pod.Status.Phase)
			time.Sleep(retryInterval)
			continue
		}
		
		// Pod의 IP 주소 확인
		if pod.Status.PodIP == "" {
			log.Info("Pod has no IP address yet", "pod", podName, "retry", i+1)
			lastErr = fmt.Errorf("pod %s has no IP address", podName)
			time.Sleep(retryInterval)
			continue
		}
		
		// IP 주소가 있으면 반환
		log.Info("Successfully got pod IP", "pod", podName, "ip", pod.Status.PodIP)
		return pod.Status.PodIP, nil
	}
	
	// 모든 재시도 실패 시 마지막 오류 반환
	log.Error(lastErr, "All retries failed to get pod IP", "pod", podName)
	return "", fmt.Errorf("failed to get IP address for pod %s after %d attempts: %v", 
		podName, maxRetries, lastErr)
}

// execCommandOnPod는 Pod에서 명령을 실행합니다.
func (cm *ClusterManager) execCommandOnPod(ctx context.Context, namespace, podName string, command []string) (string, error) {
	// 이 함수는 "kubexec" 또는 기타 방식을 통해 Pod에서 명령을 실행합니다.
	// 실제 구현은 환경에 따라 달라질 수 있으며, 여기서는 간단한 예시만 제공합니다.

	// 참고: 실제 구현에서는 k8s.io/client-go/kubernetes/scheme 등을 사용하여
	// Pod에서 명령을 실행해야 합니다.
	
	// 실제 코드는 생략되었으므로 이 로직은 컨트롤러 구현체에 맞게 수정해야 합니다.
	// 이 함수는 Redis 클러스터 관리 로직의 테스트를 위한 접근점 역할을 합니다.
	
	// 여기서는 간단히 command를 출력하고 성공 결과를 반환합니다.
	log.FromContext(ctx).Info("Executing command on pod", "namespace", namespace, "pod", podName, "command", command)
	
	// 실제 execCommandOnPod 함수 로직을 여기에 구현하세요.
	// 이 함수는 클러스터 테스트를 위한 명령을 실행하기 위해 필요합니다.
	
	return "", nil
}

// GetSlaveScaleStatus는 슬레이브 노드의 스케일 다운 상태를 조회합니다.
func (cm *ClusterManager) GetSlaveScaleStatus(ctx context.Context, cr *v1alpha1.KRedis, masterIndex int) (SlaveScaleStatus, error) {
	result := SlaveScaleStatus{
		MasterIdx: masterIndex,
	}
	
	// 해당 슬레이브 StatefulSet 가져오기
	pod := &corev1.Pod{}
	podName := fmt.Sprintf("%s-slave-%d-0", cr.Name, masterIndex)
	
	err := cm.client.Get(ctx, types.NamespacedName{
		Name: podName,
		Namespace: cr.Namespace,
	}, pod)
	
	// Pod가 존재하지 않거나 다른 오류 발생 시
	if err != nil {
		return result, nil
	}
	
	// Pod의 어노테이션에서 스케일 다운 상태 확인
	if pod.Annotations != nil {
		if _, ok := pod.Annotations["redis.kredis-operator/scale-down-in-progress"]; ok {
			result.InProgress = true
		}
		if _, ok := pod.Annotations["redis.kredis-operator/scale-down-complete"]; ok {
			result.Completed = true
		}
		if msg, ok := pod.Annotations["redis.kredis-operator/scale-down-message"]; ok {
			result.Message = msg
		}
	}
	
	return result, nil
}

// SetSlaveScaleStatus는 슬레이브 노드의 스케일 다운 상태를 설정합니다.
func (cm *ClusterManager) SetSlaveScaleStatus(ctx context.Context, cr *v1alpha1.KRedis, masterIndex int, status SlaveScaleStatus) error {
	// 해당 슬레이브 StatefulSet 가져오기
	pod := &corev1.Pod{}
	podName := fmt.Sprintf("%s-slave-%d-0", cr.Name, masterIndex)
	
	err := cm.client.Get(ctx, types.NamespacedName{
		Name: podName,
		Namespace: cr.Namespace,
	}, pod)
	
	if err != nil {
		return err
	}
	
	// Pod의 어노테이션 업데이트
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	
	if status.InProgress {
		pod.Annotations["redis.kredis-operator/scale-down-in-progress"] = "true"
	} else {
		delete(pod.Annotations, "redis.kredis-operator/scale-down-in-progress")
	}
	
	if status.Completed {
		pod.Annotations["redis.kredis-operator/scale-down-complete"] = "true"
	} else {
		delete(pod.Annotations, "redis.kredis-operator/scale-down-complete")
	}
	
	if status.Message != "" {
		pod.Annotations["redis.kredis-operator/scale-down-message"] = status.Message
	} else {
		delete(pod.Annotations, "redis.kredis-operator/scale-down-message")
	}
	
	// Pod 업데이트
	return cm.client.Update(ctx, pod)
}