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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
	
	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// ClusterStatus 구조체는 Redis 클러스터 상태 정보를 담습니다
type ClusterStatus struct {
	IsHealthy        bool
	ActiveMasters    int
	ActiveReplicas   int
	FailedNodes      []string
	AllSlotsAssigned bool
}

// CheckClusterHealth 함수는 Redis 클러스터의 건강 상태를 확인합니다
func CheckClusterHealth(ctx context.Context, cr *stablev1alpha1.KRedis) (*ClusterStatus, error) {
	logger := log.FromContext(ctx)
	
	// 첫 번째 마스터 파드 이름 가져오기
	masterPodName := fmt.Sprintf("%s-master-0", cr.Name)
	
	// Redis 클러스터 정보 가져오기
	cmd := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
		"redis-cli", "cluster", "info")
	
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error(err, "Failed to execute cluster info command", "output", string(output))
		return nil, err
	}
	
	// 클러스터 노드 정보 가져오기
	nodesCmd := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
		"redis-cli", "cluster", "nodes")
	
	nodesOutput, err := nodesCmd.CombinedOutput()
	if err != nil {
		logger.Error(err, "Failed to execute cluster nodes command", "output", string(nodesOutput))
		return nil, err
	}
	
	// 클러스터 상태 파싱
	status := &ClusterStatus{
		IsHealthy: true, // 기본값은 정상 상태
	}
	
	// 클러스터 상태 정보 분석
	clusterInfoStr := string(output)
	if strings.Contains(clusterInfoStr, "cluster_state:fail") {
		status.IsHealthy = false
	}
	
	// 노드 정보 분석
	nodeLines := strings.Split(string(nodesOutput), "\n")
	status.ActiveMasters = 0
	status.ActiveReplicas = 0
	
	for _, line := range nodeLines {
		if line == "" {
			continue
		}
		
		// 마스터 노드 확인
		if strings.Contains(line, "master") && !strings.Contains(line, "fail") {
			status.ActiveMasters++
		}
		
		// 슬레이브(레플리카) 노드 확인
		if strings.Contains(line, "slave") && !strings.Contains(line, "fail") {
			status.ActiveReplicas++
		}
		
		// 실패한 노드 확인
		if strings.Contains(line, "fail") || strings.Contains(line, "disconnected") {
			parts := strings.Fields(line)
			if len(parts) > 1 {
				status.FailedNodes = append(status.FailedNodes, parts[1])
				status.IsHealthy = false
			}
		}
	}
	
	// 모든 슬롯이 할당되었는지 확인
	slotsCmd := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
		"redis-cli", "cluster", "slots")
	
	slotsOutput, err := slotsCmd.CombinedOutput()
	if err == nil {
		// 간단한 검증: 응답이 비어있지 않으면 슬롯이 할당됨
		status.AllSlotsAssigned = len(slotsOutput) > 0
	} else {
		// 슬롯 정보를 가져오는데 실패했다면 상태는 불량으로 설정
		status.IsHealthy = false
		status.AllSlotsAssigned = false
	}
	
	// 예상되는 노드 수와 비교
	expectedMasters := int(cr.Spec.Masters)
	expectedReplicas := int(cr.Spec.Masters * cr.Spec.Replicas)
	
	if status.ActiveMasters != expectedMasters || status.ActiveReplicas != expectedReplicas {
		status.IsHealthy = false
	}
	
	logger.Info("Cluster health check completed", 
		"isHealthy", status.IsHealthy,
		"activeMasters", status.ActiveMasters, 
		"activeReplicas", status.ActiveReplicas,
		"failedNodes", status.FailedNodes)
	
	return status, nil
}

// RecoverFailedNode 함수는 실패한 Redis 노드 복구를 시도합니다
func RecoverFailedNode(ctx context.Context, cr *stablev1alpha1.KRedis, failedNode string) error {
	logger := log.FromContext(ctx)
	
	logger.Info("Attempting to recover failed node", "nodeID", failedNode)
	
	// 노드가 마스터인지 슬레이브인지 확인
	isMaster := false
	
	// 첫 번째 살아있는 마스터 파드 이름 가져오기
	masterPodName := fmt.Sprintf("%s-master-0", cr.Name)
	
	// 노드 정보 가져오기
	cmd := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
		"redis-cli", "cluster", "nodes")
	
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes: %v", err)
	}
	
	nodeLines := strings.Split(string(output), "\n")
	for _, line := range nodeLines {
		if strings.Contains(line, failedNode) {
			if strings.Contains(line, "master") {
				isMaster = true
			}
			break
		}
	}
	
	// 실패한 노드 삭제
	forgetCmd := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
		"redis-cli", "cluster", "forget", failedNode)
	
	if _, err := forgetCmd.CombinedOutput(); err != nil {
		logger.Error(err, "Failed to forget node", "nodeID", failedNode)
		// 계속 진행 (다른 노드에서는 성공할 수 있음)
	}
	
	// 마스터 노드였다면 새로운 마스터 선정이 필요
	if isMaster {
		// 잠시 대기 (클러스터가 새 마스터를 선출할 시간)
		time.Sleep(3 * time.Second)
		
		// 필요한 경우 슬롯 재할당
		reshardCmd := exec.Command("kubectl", "exec", masterPodName, "-n", cr.Namespace, "--",
			"redis-cli", "--cluster", "fix", fmt.Sprintf("%s:%d", masterPodName, cr.Spec.BasePort))
		
		if output, err := reshardCmd.CombinedOutput(); err != nil {
			logger.Error(err, "Failed to fix cluster slots", "output", string(output))
			return fmt.Errorf("failed to fix cluster: %v", err)
		}
	}
	
	logger.Info("Node recovery process completed", "nodeID", failedNode)
	return nil
}