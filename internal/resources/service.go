package resources

import (
	"fmt"
	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/internal/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// CreateHeadlessService는 Redis 노드 간 내부 통신을 위한 헤드리스 서비스를 생성합니다.
// 이 헤드리스 서비스는 StatefulSet Pod 간의 안정적인 네트워크 ID를 제공합니다.
//
// 헤드리스 서비스 특징:
// - ClusterIP가 "None"으로 설정되어 kube-dns에 개별 Pod의 DNS 레코드 생성
// - StatefulSet Pod의 DNS 엔트리: $(pod-name).$(service-name).$(namespace).svc.cluster.local
// - 안정적인 네트워크 ID를 통해 Redis 노드 간 통신 지원
// - StatefulSet의 serviceName 필드와 연결
//
// Redis 클러스터 통신:
// - 각 노드가 다른 노드를 안정적으로 찾을 수 있도록 함
// - 포트: basePort (redis), bus-port (cluster bus)
//
// cr: KRedis 커스텀 리소스
// return: 구성된 헤드리스 Service 객체
func CreateHeadlessService(cr *v1alpha1.KRedis) *corev1.Service {
	// 서비스 이름 생성 - 헤드리스 서비스임을 나타내는 접미사 추가
	name := fmt.Sprintf("%s-headless", cr.Name)

	// Redis 포트와 클러스터 버스 포트 계산
	// Redis 클러스터는 데이터 포트와 클러스터 버스 포트 두 개가 필요함
	redisPort := cr.Spec.BasePort
	clusterBusPort := redisPort + 10000 // 클러스터 버스 포트는 일반적으로 basePort + 10000

	// 서비스에 적용할 라벨 생성
	labels := utils.LabelsForKRedis(cr.Name, "service")
	labels["headless"] = "true"

	// 서비스 셀렉터 - 마스터와 슬레이브 노드 모두 대상으로 함
	selector := map[string]string{
		"app":       "kredis",
		"kredis_cr": cr.Name,
	}

	// 헤드리스 서비스 객체 생성
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cr.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cr, v1alpha1.GroupVersion.WithKind("KRedis")),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None", // 헤드리스 서비스의 핵심 설정
			Ports: []corev1.ServicePort{
				{
					Name:       "redis",
					Port:       int32(redisPort),
					TargetPort: intstr.FromInt(int(redisPort)),
					Protocol:   "TCP",
				},
				{
					Name:       "cluster-bus",
					Port:       int32(clusterBusPort),
					TargetPort: intstr.FromInt(int(clusterBusPort)),
					Protocol:   "TCP",
				},
			},
			Selector: selector,
			// StatefulSet에서 Pod와 연결할 때 사용됨
			PublishNotReadyAddresses: true, // 초기화 중인 Pod의 주소도 DNS에 등록
		},
	}

	return service
}

// CreateClientService는 Redis 클러스터에 접근하기 위한 클라이언트 서비스를 생성합니다.
// 이 서비스는 애플리케이션이 Redis 클러스터에 접속할 수 있는 단일 엔트리포인트를 제공합니다.
//
// 클라이언트 서비스 특징:
// - 일반 ClusterIP 서비스 타입 (필요에 따라 NodePort 또는 LoadBalancer로 변경 가능)
// - Redis 포트만 노출 (내부 클러스터 버스 포트는 노출하지 않음)
// - 모든 마스터 노드를 대상으로 함
// - 클라이언트 연결 분산을 위한 세션 어피니티 설정 가능
//
// 트래픽 처리:
// - Redis 클라이언트 라이브러리는 일반적으로 초기 연결 후 클러스터 토폴로지를 학습
// - MOVED/ASK 리디렉션을 통해 올바른 노드로 요청 라우팅
//
// cr: KRedis 커스텀 리소스
// return: 구성된 Client Service 객체
func CreateClientService(cr *v1alpha1.KRedis) *corev1.Service {
	// TODO: 클라이언트 서비스 구현
	// - 서비스 이름 설정 (일반적으로 [name]-client)
	// - 표준 ClusterIP 서비스 타입 설정
	// - 클라이언트 접속용 Redis 포트만 노출
	// - 마스터 노드를 대상으로 하는 셀렉터 설정

	return &corev1.Service{}
}
