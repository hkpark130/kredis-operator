/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redis

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	
	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// GetNodeIPs 함수는 Redis Master 및 Slave 파드의 IP 주소를 수집합니다
func GetNodeIPs(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) ([]string, error) {
	logger := log.FromContext(ctx)
	var allIPs []string

	// 모든 Master Pod에 대해
	for i := 0; i < int(cr.Spec.Masters); i++ {
		// Master IP 가져오기
		masterName := fmt.Sprintf("%s-master-%d", cr.Name, i)
		masterIP, err := getPodIP(ctx, cl, cr.Namespace, map[string]string{
			"app":  "kredis", 
			"role": "master", 
			"master_pod_name": cr.Name + "-master",
		})
		
		if err != nil {
			logger.Error(err, "Failed to get master IP", "master", masterName)
			return nil, err
		}
		allIPs = append(allIPs, masterIP)

		// Slave IP 가져오기
		slaveName := fmt.Sprintf("%s-slave-%d", cr.Name, i)
		slaveIP, err := getPodIP(ctx, cl, cr.Namespace, map[string]string{
			"app":  "kredis", 
			"role": "slave", 
			"master_pod_name": fmt.Sprintf("%s-master-%d", cr.Name, i),
		})
		
		if err != nil {
			logger.Error(err, "Failed to get slave IP", "slave", slaveName)
			return nil, err
		}
		allIPs = append(allIPs, slaveIP)
	}

	if len(allIPs) == 0 {
		return nil, fmt.Errorf("no pods found with IP addresses")
	}
	
	return allIPs, nil
}

// Pod IP 주소 가져오기 (재시도 로직 포함)
func getPodIP(ctx context.Context, cl client.Client, namespace string, labels map[string]string) (string, error) {
	logger := log.FromContext(ctx)

	var podIP string
	err := retry.OnError(
		retry.DefaultBackoff,
		func(err error) bool {
			return strings.Contains(err.Error(), "is not running or does not have an IP address yet")
		},
		func() error {
			podList := &corev1.PodList{}
			selector := &metav1.LabelSelector{MatchLabels: labels}
			labelSelector, err := metav1.LabelSelectorAsSelector(selector)
			if err != nil {
				logger.Error(err, "Failed to convert label selector", "labels", labels)
				return err
			}

			listOps := &client.ListOptions{
				Namespace:     namespace,
				LabelSelector: labelSelector,
			}

			if err := cl.List(ctx, podList, listOps); err != nil {
				logger.Error(err, "Failed to list pods", "labels", labels)
				return err
			}

			if len(podList.Items) == 0 {
				err := fmt.Errorf("no pods found with labels: %v", labels)
				logger.Info(err.Error())
				return err
			}

			pod := podList.Items[0]
			if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
				podIP = pod.Status.PodIP
				return nil
			} else {
				err := fmt.Errorf("pod %s is not running or does not have an IP address yet", pod.Name)
				logger.Info(err.Error())
				return err
			}
		})

	if err != nil {
		logger.Error(err, "Failed to get pod IP after multiple retries", "labels", labels)
		return "", err
	}

	return podIP, nil
}

// CreateCluster 함수는 Redis 클러스터를 생성합니다
func CreateCluster(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis, allIPs []string) error {
	logger := log.FromContext(ctx)

	// Redis 클러스터 생성 명령어 생성
	var command []string
	command = append(command, "redis-cli", "--cluster", "create")

	// Master와 Slave 노드 IP 주소 추가
	for _, ip := range allIPs {
		command = append(command, fmt.Sprintf("%s:%d", ip, cr.Spec.BasePort))
	}

	// Master당 Replica(Slave) 수 지정
	command = append(command, "--cluster-replicas", fmt.Sprintf("%d", cr.Spec.Replicas))
	command = append(command, "--cluster-yes")
	logger.Info("Executing command", "command", command)

	// 첫 번째 마스터 파드 이름 가져오기
	masterStsName := cr.Name + "-master"
	podName := fmt.Sprintf("%s-0", masterStsName)

	// kubectl exec를 사용하여 Redis 클러스터 생성 명령 실행
	cmd := exec.Command("kubectl", "exec", podName, "-n", cr.Namespace, "--")
	cmd.Args = append(cmd.Args, command...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error(err, "Failed to execute redis-cli cluster create", "output", string(output))
		return fmt.Errorf("failed to execute redis-cli cluster create: %v, output: %s", err, output)
	}

	logger.Info("Redis cluster created successfully", "output", string(output))
	return nil
}

// UpdateClusterInitialized 함수는 clusterInitialized 필드를 업데이트합니다
func UpdateClusterInitialized(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)
	
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestKRedis := &stablev1alpha1.KRedis{}
		err := cl.Get(ctx, types.NamespacedName{
			Name:      cr.Name,
			Namespace: cr.Namespace,
		}, latestKRedis)
		
		if err != nil {
			return err
		}

		latestKRedis.Spec.ClusterInitialized = true // 클러스터 초기화 완료로 설정
		err = cl.Update(ctx, latestKRedis)
		return err
	})

	if err != nil {
		logger.Error(err, "Failed to update KRedis CRD with ClusterInitialized=true")
		return err
	}

	logger.Info("KRedis CRD updated with ClusterInitialized=true")
	return nil
}

// HandleDataMigration 함수는 스케일 다운 시 데이터 마이그레이션을 처리합니다
func HandleDataMigration(ctx context.Context, cr *stablev1alpha1.KRedis, replicas int32) error {
	logger := log.FromContext(ctx)
	
	// 마지막 마스터 노드의 데이터 마이그레이션
	podName := fmt.Sprintf("%s-master-%d", cr.Name, replicas-1)
	
	// redis-cli를 통한 리샤딩 명령 실행
	cmd := exec.Command("kubectl", "exec", podName, "-n", cr.Namespace, "--", 
		"redis-cli", "--cluster", "reshard", 
		fmt.Sprintf("%s:6379", podName),
		"--cluster-yes")
	
	if output, err := cmd.CombinedOutput(); err != nil {
		logger.Error(err, "Redis reshard failed", "output", string(output))
		return fmt.Errorf("redis reshard failed: %v, output: %s", err, output)
	}
	
	logger.Info("Data migration completed successfully")
	return nil
}

// AddNodesToCluster 함수는 Redis 클러스터에 새로운 노드를 추가합니다
func AddNodesToCluster(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis, allIPs []string) error {
	logger := log.FromContext(ctx)
	logger.Info("Adding nodes to Redis Cluster")
	
	// 첫 번째 마스터 파드 이름 가져오기
	masterPodName := fmt.Sprintf("%s-master-0", cr.Name)
	
	// 클러스터 노드 정보 가져오기
	cmd := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
		"redis-cli", "cluster", "nodes")
	
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error(err, "Failed to get cluster nodes", "output", string(output))
		return err
	}
	
	// 기존 노드 ID와 IP 매핑
	existingNodes := make(map[string]bool)
	nodeLines := strings.Split(string(output), "\n")
	
	for _, line := range nodeLines {
		if line == "" {
			continue
		}
		
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			// 두 번째 필드는 IP:port 형식
			existingNodes[parts[1]] = true
		}
	}
	
	// 새 노드 추가
	for _, ip := range allIPs {
		nodeAddr := fmt.Sprintf("%s:%d", ip, cr.Spec.BasePort)
		if existingNodes[nodeAddr] {
			logger.Info("Node already exists in cluster, skipping", "node", nodeAddr)
			continue
		}
		
		// 새 노드를 클러스터에 추가
		logger.Info("Adding node to cluster", "node", nodeAddr)
		addCmd := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
			"redis-cli", "--cluster", "add-node", nodeAddr, 
			fmt.Sprintf("%s-master-0.%s-master-headless.%s.svc.cluster.local:%d", 
				cr.Name, cr.Name, cr.Namespace, cr.Spec.BasePort))
		
		addOutput, err := addCmd.CombinedOutput()
		if err != nil {
			logger.Error(err, "Failed to add node to cluster", "output", string(addOutput))
			// 노드 추가 실패는 무시하고 계속 진행
			continue
		}
		
		logger.Info("Node added to cluster", "node", nodeAddr, "output", string(addOutput))
	}
	
	// 리샤딩 필요 여부 확인 및 실행
	reshard := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
		"redis-cli", "--cluster", "rebalance", 
		fmt.Sprintf("%s-master-0.%s-master-headless.%s.svc.cluster.local:%d", 
			cr.Name, cr.Name, cr.Namespace, cr.Spec.BasePort),
		"--cluster-use-empty-masters")
	
	reshardOutput, err := reshard.CombinedOutput()
	if err != nil {
		logger.Error(err, "Failed to rebalance cluster", "output", string(reshardOutput))
		// 리샤딩 실패는 경고로 처리하고 계속 진행
	} else {
		logger.Info("Cluster rebalanced successfully", "output", string(reshardOutput))
	}
	
	return nil
}