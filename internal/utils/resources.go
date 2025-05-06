package utils

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"strings"
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

// HasVolumeMount는 볼륨 마운트 목록에서 특정 이름과 경로를 가진 마운트가 있는지 확인합니다.
//
// mounts: 볼륨 마운트 목록
// name: 찾을 볼륨 마운트 이름
// path: 찾을 마운트 경로
// return: 해당 이름과 경로를 가진 마운트가 존재하면 true, 그렇지 않으면 false
func HasVolumeMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.MountPath == path {
			return true
		}
	}
	return false
}

// HasVolume은 볼륨 목록에서 특정 이름을 가진 볼륨이 있는지 확인합니다.
//
// volumes: 볼륨 목록
// name: 찾을 볼륨 이름
// return: 해당 이름을 가진 볼륨이 존재하면 true, 그렇지 않으면 false
func HasVolume(volumes []corev1.Volume, name string) bool {
	for _, vol := range volumes {
		if vol.Name == name {
			return true
		}
	}
	return false
}

// CompareCommand는 두 명령어 배열이 동일한지 비교합니다.
//
// expected: 기대하는 명령어 배열
// actual: 실제 명령어 배열
// return: 두 배열이 완전히 동일하면 true, 그렇지 않으면 false
func CompareCommand(expected, actual []string) bool {
	// 배열 길이가 다르면 명령어가 다름
	if len(expected) != len(actual) {
		return false
	}

	// 명령어가 보통 2-3개 요소로 짧기 때문에
	// 간단히 각 요소를 직접 비교하는 것이 효율적
	for i := range expected {
		if expected[i] != actual[i] {
			return false
		}
	}
	return true
}

// CompareCommandSimple은 두 명령어가 동일한지 간단히 비교합니다.
// 명령어가 redis-server와 설정 파일 경로 같이 단순한 경우 사용하기 좋습니다.
//
// expected: 기대하는 명령어 문자열 (예: "redis-server /etc/redis/master.conf")
// actual: 실제 명령어 배열
// return: 명령어가 동일하면 true, 그렇지 않으면 false
func CompareCommandSimple(expected string, actual []string) bool {
	// 기대하는 명령어 문자열을 공백 기준으로 분리
	expectedSlice := strings.Fields(expected)

	// 분리된 배열과 실제 명령어 배열을 비교
	return CompareCommand(expectedSlice, actual)
}

// MergeEnvVars는 현재 환경 변수 목록과 원하는 환경 변수 목록을 병합합니다.
// 원하는 환경 변수가 현재 환경 변수 목록에 없거나 다른 값을 가지고 있으면 변경으로 간주합니다.
//
// current: 현재 환경 변수 목록
// desired: 원하는 환경 변수 목록
// return: 병합된 환경 변수 목록과 변경 여부 (true: 변경됨, false: 변경되지 않음)
func MergeEnvVars(current, desired []corev1.EnvVar) ([]corev1.EnvVar, bool) {
	changed := false
	currentMap := map[string]corev1.EnvVar{}
	for _, env := range current {
		currentMap[env.Name] = env
	}

	result := []corev1.EnvVar{}
	for _, env := range desired {
		if curr, exists := currentMap[env.Name]; !exists || !reflect.DeepEqual(curr, env) {
			result = append(result, env)
			changed = true
		} else {
			result = append(result, curr)
		}
		delete(currentMap, env.Name)
	}

	// 남은 환경 변수 유지
	for _, env := range currentMap {
		result = append(result, env)
	}

	return result, changed
}

// UpdateContainerResources는 컨테이너의 리소스 요청/제한 값을 업데이트합니다.
// cr에서 정의된 리소스와 기존 컨테이너의 리소스를 비교하여 필요한 경우 업데이트합니다.
//
// container: 업데이트할 컨테이너 (포인터로 전달되어 직접 수정됨)
// resources: KRedis CR에서 정의된 리소스 맵
// logName: 로깅 시 사용할 이름 (StatefulSet 이름 등)
// return: 업데이트 여부 (true: 업데이트됨, false: 이미 최신 상태)
func UpdateContainerResources(container *corev1.Container, resources *map[string]map[string]string, logName string) bool {
	needsUpdate := false
	logger := ctrl.Log.WithName("resources")

	if resources == nil {
		return false
	}

	resourcesMap := *resources

	// CPU, 메모리 리소스 리밋 업데이트
	if limits, ok := resourcesMap["limits"]; ok && limits != nil {
		desiredLimits := ResourceMapToResourceList(limits)
		// 기존 리밋이 없거나 다른 경우
		if container.Resources.Limits == nil ||
			!reflect.DeepEqual(container.Resources.Limits, desiredLimits) {
			logger.Info("Updating resource limits", "name", logName)
			if container.Resources.Limits == nil {
				container.Resources.Limits = corev1.ResourceList{}
			}
			container.Resources.Limits = desiredLimits
			needsUpdate = true
		}
	}

	// CPU, 메모리 리소스 요청 업데이트
	if requests, ok := resourcesMap["requests"]; ok && requests != nil {
		desiredRequests := ResourceMapToResourceList(requests)
		// 기존 요청이 없거나 다른 경우
		if container.Resources.Requests == nil ||
			!reflect.DeepEqual(container.Resources.Requests, desiredRequests) {
			logger.Info("Updating resource requests", "name", logName)
			if container.Resources.Requests == nil {
				container.Resources.Requests = corev1.ResourceList{}
			}
			container.Resources.Requests = desiredRequests
			needsUpdate = true
		}
	}

	return needsUpdate
}
