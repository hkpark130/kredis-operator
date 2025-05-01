package resources

import (
	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateMasterStatefulSet는 Redis 마스터 노드를 위한 StatefulSet을 생성합니다.
func CreateMasterStatefulSet(cr *v1alpha1.KRedis) *appsv1.StatefulSet {
	// TODO: 구현 예정
	return &appsv1.StatefulSet{}
}

// EqualStatefulSets는 두 StatefulSet이 동일한지 비교합니다.
func EqualStatefulSets(current, desired *appsv1.StatefulSet) bool {
	// TODO: 구현 예정
	return true
}