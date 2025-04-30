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

package affinity

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	
	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// CreatePodAntiAffinity 함수는 Pod가 서로 다른 노드에 분산되도록 Anti-Affinity 설정을 생성합니다
func CreatePodAntiAffinity(role string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			// 강한 제약 조건: 같은 역할의 Pod는 서로 다른 노드에 배치되어야 함
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app":  "kredis",
							"role": role,
						},
					},
					TopologyKey: "kubernetes.io/hostname",
				},
			},
			// 약한 제약 조건: 가능하면 다른 역할의 Pod와 다른 노드에 배치
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 80,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "role",
									Operator: metav1.LabelSelectorOpNotIn,
									Values:   []string{role},
								},
								{
									Key:      "app",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{"kredis"},
								},
							},
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		},
	}
}

// CreatePodAntiAffinityWithOpposite 함수는 Pod가 특정 다른 역할의 Pod와 서로 다른 노드에 배치되도록 설정합니다
func CreatePodAntiAffinityWithOpposite(oppositeRole string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"app":  "kredis",
								"role": oppositeRole,
							},
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		},
	}
}

// CreateZoneAntiAffinity 함수는 Pod가 여러 가용 영역(AZ)에 분산되도록 Affinity 설정을 생성합니다
func CreateZoneAntiAffinity(role string) *corev1.Affinity {
	affinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"app":  "kredis",
								"role": role,
							},
						},
						TopologyKey: "topology.kubernetes.io/zone",
					},
				},
			},
		},
	}
	
	return affinity
}

// MergeAffinities 함수는 여러 Affinity 설정을 하나로 병합합니다
func MergeAffinities(affinities ...*corev1.Affinity) *corev1.Affinity {
	result := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:      []corev1.PodAffinityTerm{},
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{},
		},
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution:      []corev1.PodAffinityTerm{},
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{},
		},
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{},
			},
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{},
		},
	}
	
	for _, aff := range affinities {
		if aff == nil {
			continue
		}
		
		// PodAntiAffinity 병합
		if aff.PodAntiAffinity != nil {
			if aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
				result.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
					result.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
					aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution...,
				)
			}
			if aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
				result.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
					result.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
					aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution...,
				)
			}
		}
		
		// 필요한 경우 PodAffinity와 NodeAffinity도 병합 가능
	}
	
	// 빈 필드 정리
	if len(result.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) == 0 &&
		len(result.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0 {
		result.PodAntiAffinity = nil
	}
	if len(result.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) == 0 &&
		len(result.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0 {
		result.PodAffinity = nil
	}
	if result.NodeAffinity != nil && len(result.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0 &&
		(result.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil ||
			len(result.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0) {
		result.NodeAffinity = nil
	}
	
	return result
}

// GetOptimalAffinityForRole 함수는 주어진 역할에 맞는 최적의 Affinity 설정을 반환합니다
func GetOptimalAffinityForRole(cr *stablev1alpha1.KRedis, role string) *corev1.Affinity {
	// Master와 Slave 역할에 따라 서로 다른 노드에 배치되도록 설정
	var oppositeRole string
	if role == "master" {
		oppositeRole = "slave"
	} else {
		oppositeRole = "master"
	}
	
	// 여러 Affinity 설정 병합
	return MergeAffinities(
		CreatePodAntiAffinity(role),                   // 같은 역할끼리 분산
		CreatePodAntiAffinityWithOpposite(oppositeRole), // 반대 역할과 분산
		CreateZoneAntiAffinity(role),                  // 가용 영역 간 분산
	)
}

// GetAvailableNodes 함수는 현재 클러스터에서 사용 가능한 노드 목록을 반환합니다
func GetAvailableNodes(ctx context.Context, cl client.Client) ([]corev1.Node, error) {
	logger := log.FromContext(ctx)
	
	nodeList := &corev1.NodeList{}
	if err := cl.List(ctx, nodeList); err != nil {
		logger.Error(err, "Failed to list nodes")
		return nil, err
	}
	
	// Ready 상태인 노드만 필터링
	var readyNodes []corev1.Node
	for _, node := range nodeList.Items {
		if isNodeReady(node) {
			readyNodes = append(readyNodes, node)
		}
	}
	
	return readyNodes, nil
}

// isNodeReady 함수는 노드가 Ready 상태인지 확인합니다
func isNodeReady(node corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}