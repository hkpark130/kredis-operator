package utils

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ParseResource는 리소스 맵에서 값을 가져와 Quantity 객체로 변환합니다.
// 맵에서 해당 키에 대한 값이 없거나 비어있는 경우 기본값을 사용합니다.
//
// resourceMap: 리소스 값을 포함하는 맵 (CPU, 메모리 등)
// key: 찾을 리소스 키 (예: "cpu", "memory")
// defaultValue: 기본값 (예: "100m", "256Mi")
// return: 변환된 쿠버네티스 리소스 Quantity
func ParseResource(resourceMap map[string]string, key string, defaultValue string) resource.Quantity {
	value, ok := resourceMap[key]
	if !ok || value == "" {
		value = defaultValue
	}
	return resource.MustParse(value)
}

// ResourceMapToResourceList는 문자열 맵을 쿠버네티스 ResourceList로 변환합니다.
// KRedis CRD에서 정의된 리소스 맵을 쿠버네티스 Pod 컨테이너에 사용할 수 있는
// ResourceList 형식으로 변환합니다.
//
// resourceMap: 리소스 값을 담은 문자열 맵 (예: {"cpu": "100m", "memory": "256Mi"})
// return: 쿠버네티스 ResourceList 객체
func ResourceMapToResourceList(resourceMap map[string]string) corev1.ResourceList {
	resources := corev1.ResourceList{}

	// CPU 리소스 설정
	if cpu, ok := resourceMap["cpu"]; ok && cpu != "" {
		resources[corev1.ResourceCPU] = resource.MustParse(cpu)
	}

	// 메모리 리소스 설정
	if memory, ok := resourceMap["memory"]; ok && memory != "" {
		resources[corev1.ResourceMemory] = resource.MustParse(memory)
	}

	return resources
}
