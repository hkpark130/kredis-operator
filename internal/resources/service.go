package resources

import (
	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/internal/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// CreateHeadlessService는 Redis 마스터 노드를 위한 헤드리스 서비스를 생성합니다.
func CreateHeadlessService(cr *v1alpha1.KRedis) *corev1.Service {
	// TODO: 구현 예정
	return &corev1.Service{}
}

// CreateClientService는 Redis 클러스터에 접근하기 위한 클라이언트 서비스를 생성합니다.
func CreateClientService(cr *v1alpha1.KRedis) *corev1.Service {
	// TODO: 구현 예정
	return &corev1.Service{}
}