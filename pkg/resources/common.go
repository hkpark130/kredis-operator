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

package resources

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// LabelsForKRedis 함수는 주어진 KRedis 리소스 선택에 사용할 라벨을 반환합니다.
func LabelsForKRedis(name string, role string) map[string]string {
	labels := map[string]string{
		"app":           "kredis",
		"master_pod_name": name,
	}
	
	if strings.Contains(role, "master") {
		labels["role"] = "master" // Master 라벨 추가
	} else {
		labels["role"] = "slave" // Slave 라벨 추가
	}
	
	return labels
}

// ParseResource 함수는 리소스 요청 또는 제한에 대한 수량을 파싱합니다
func ParseResource(resourceMap map[string]string, key string, defaultValue string) resource.Quantity {
	value, ok := resourceMap[key]
	if !ok || value == "" {
		value = defaultValue
	}
	return resource.MustParse(value)
}

// Max 함수는 두 정수 중 큰 값을 반환합니다
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// createResourceRequirements 함수는 컨테이너의 리소스 요구사항을 생성합니다
func createResourceRequirements(cr *stablev1alpha1.KRedis) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			"cpu":    ParseResource(cr.Spec.Resource["limits"], "cpu", "1"),
			"memory": ParseResource(cr.Spec.Resource["limits"], "memory", "1Gi"),
		},
		Requests: corev1.ResourceList{
			"cpu":    ParseResource(cr.Spec.Resource["requests"], "cpu", "500m"),
			"memory": ParseResource(cr.Spec.Resource["requests"], "memory", "512Mi"),
		},
	}
}

// createPodAntiAffinity 함수는 Pod가 다른 역할의 Pod와 다른 노드에 배치되도록 Affinity 설정을 생성합니다
func createPodAntiAffinity(antiAffinityWithRole string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &corev1.LabelSelector{
							MatchLabels: map[string]string{
								"app":  "kredis",
								"role": antiAffinityWithRole, // 반대 역할과 배치 회피
							},
						},
						TopologyKey: "kubernetes.io/hostname", // 같은 노드에 배치되지 않도록 설정
					},
				},
			},
		},
	}
}

// intOrString 함수는 int를 IntOrString으로 변환합니다
func intOrString(val int) intstr.IntOrString {
	return intstr.FromInt(val)
}