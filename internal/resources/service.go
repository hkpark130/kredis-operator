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
    // TODO: 헤드리스 서비스 구현
    // - 서비스 이름 설정 (일반적으로 [name]-headless)
    // - ClusterIP: "None" 설정
    // - Redis 통신 포트 및 클러스터 버스 포트 노출
    // - 적절한 라벨 셀렉터 설정 (역할 구분)

    return &corev1.Service{}
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
