package resources

import (
	"fmt"
	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateSlaveDeployment는 Redis 슬레이브 노드를 위한 Deployment를 생성합니다.
func CreateSlaveDeployment(cr *v1alpha1.KRedis, index int) *appsv1.Deployment {
	// TODO: 구현 예정
	return &appsv1.Deployment{}
}