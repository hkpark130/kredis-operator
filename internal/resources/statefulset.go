package resources

import (
	"fmt"
	"github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/internal/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// getRedisCommonEnvVars는 Redis 컨테이너에 공통으로 들어가는 환경 변수를 반환합니다.
func getRedisCommonEnvVars(redisPort, clusterBusPort int32, redisMaxMemory string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
		{
			Name:  "REDIS_PORT",
			Value: fmt.Sprintf("%d", redisPort),
		},
		{
			Name:  "CLUSTER_BUS_PORT",
			Value: fmt.Sprintf("%d", clusterBusPort),
		},
		{
			Name:  "REDIS_MAXMEMORY",
			Value: redisMaxMemory,
		},
	}
}

// getRedisCommonVolumeMounts는 Redis 컨테이너에 공통으로 들어가는 볼륨 마운트를 반환합니다.
func getRedisCommonVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      "redis-data",
			MountPath: "/data",
		},
		{
			Name:      "redis-config",
			MountPath: "/etc/redis",
		},
	}
}

// CreateMasterStatefulSet는 Redis 마스터 노드를 위한 StatefulSet을 생성합니다.
// 이 StatefulSet은 지정된 수의 마스터 노드를 관리하며, 각 노드는 안정적인 네트워크 ID를 가집니다.
//
// StatefulSet 주요 특징:
// - 안정적인 네트워크 ID (pod-0, pod-1 등) 제공
// - 순차적인 배포 및 스케일링
// - PersistentVolumeClaim 자동 생성
// - 파드별 DNS 엔드포인트 제공
//
// Redis 마스터 노드 구성:
// - cluster-enabled 모드로 실행
// - 각 노드는 PVC를 통한 영구 스토리지 사용
// - 적절한 probes 설정으로 상태 모니터링
// - 클러스터 통신을 위한 환경 변수 설정
//
// cr: KRedis 커스텀 리소스
// return: 구성된 StatefulSet 객체
func CreateMasterStatefulSet(cr *v1alpha1.KRedis) *appsv1.StatefulSet {
	// StatefulSet 이름 설정
	name := fmt.Sprintf("%s-master", cr.Name)

	// 헤드리스 서비스 이름 (StatefulSet의 serviceName 필드에 사용)
	headlessServiceName := fmt.Sprintf("%s-headless", cr.Name)

	// Redis 포트와 클러스터 버스 포트 계산
	redisPort := cr.Spec.BasePort
	clusterBusPort := redisPort + 10000

	// 라벨 및 셀렉터 설정
	labels := utils.LabelsForKRedis(cr.Name, "master")
	labelSelector := map[string]string{
		"app":       "kredis",
		"kredis_cr": cr.Name,
		"role":      "master",
	}

	// StatefulSet 생성
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cr.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cr, v1alpha1.GroupVersion.WithKind("KRedis")),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			// cr.Spec.Masters 수만큼의 복제본을 관리
			Replicas: func() *int32 { i := int32(cr.Spec.Masters); return &i }(),

			// Pod 이름 접두사 설정
			ServiceName: headlessServiceName,

			// Pod 셀렉터 설정
			Selector: &metav1.LabelSelector{
				MatchLabels: labelSelector,
			},

			// Pod 업데이트 전략 설정
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				// 업데이트 순서 제어 (고급 조정 시 사용)
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					Partition: nil, // 모든 Pod 업데이트
				},
			},

			// Pod 템플릿 설정
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labelSelector,
				},
				Spec: corev1.PodSpec{
					// 초기화 컨테이너 - 설정 파일 복사
					InitContainers: []corev1.Container{
						{
							Name:  "init-redis-config",
							Image: "busybox:1.28",
							Command: []string{
								"sh",
								"-c",
								"mkdir -p /redis-writable && cp /etc/redis/* /redis-writable/ && chmod +x /redis-writable/*.sh",
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "redis-config-readonly",
									MountPath: "/etc/redis",
								},
								{
									Name:      "redis-config-writable",
									MountPath: "/redis-writable",
								},
							},
						},
					},
					// 컨테이너 설정
					Containers: []corev1.Container{
						{
							Name:  "redis",
							Image: cr.Spec.Image,
							Ports: []corev1.ContainerPort{
								{
									Name:          "redis",
									ContainerPort: int32(redisPort),
									Protocol:      "TCP",
								},
								{
									Name:          "cluster-bus",
									ContainerPort: int32(clusterBusPort),
									Protocol:      "TCP",
								},
							},
							// 시작 스크립트 경로 수정
							Command: []string{
								"sh",
								"/redis-writable/start-master.sh",
							},
							// 환경 변수 설정 - 공통 함수 사용
							Env: getRedisCommonEnvVars(int32(redisPort), int32(clusterBusPort), cr.Spec.Memory),
							// 리소스 제한 설정
							Resources: corev1.ResourceRequirements{
								Requests: utils.ResourceMapToResourceList(cr.Spec.Resource["requests"]),
								Limits:   utils.ResourceMapToResourceList(cr.Spec.Resource["limits"]),
							},
							// Liveness 프로브 - Redis 서버 응답 여부 확인
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(int(redisPort)),
									},
								},
								InitialDelaySeconds: 15,
								PeriodSeconds:       20,
								TimeoutSeconds:      5,
								FailureThreshold:    6,
							},
							// Readiness 프로브 - Redis 명령 처리 가능 여부 확인
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"sh",
											"-c",
											fmt.Sprintf("redis-cli -h $(hostname) -p %d ping", redisPort),
										},
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								TimeoutSeconds:      5,
								SuccessThreshold:    1,
								FailureThreshold:    3,
							},
							// 볼륨 마운트 설정 - 쓰기 가능한 디렉토리로 수정
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "redis-data",
									MountPath: "/data",
								},
								{
									Name:      "redis-config-writable",
									MountPath: "/redis-writable",
								},
							},
						},
					},
					// Docker 이미지 풀 시크릿 설정
					ImagePullSecrets: []corev1.LocalObjectReference{
						{
							Name: cr.Spec.SecretName,
						},
					},
					// ConfigMap 볼륨과 emptyDir 볼륨 추가
					Volumes: []corev1.Volume{
						{
							Name: "redis-config-readonly",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "kredis-operator-redis-config", // Kustomize 접두사를 고려한 이름
									},
								},
							},
						},
						{
							Name: "redis-config-writable",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
			// PVC 템플릿 설정 - 각 Pod에 대한 영구 볼륨 생성
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "redis-data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		},
	}

	return statefulSet
}

// CreateSlaveStatefulSet은 특정 마스터 노드에 종속된 Redis 슬레이브 노드를 위한 StatefulSet을 생성합니다.
// 각 마스터 노드마다 하나의 슬레이브 StatefulSet이 생성되며, 이는 해당 마스터 노드의 데이터를 복제합니다.
//
// 슬레이브 StatefulSet 특징:
// - 각 StatefulSet의 이름은 마스터 인덱스를 포함 (예: [name]-slave-[masterIndex])
// - 레플리카 수는 cr.Spec.Replicas로 설정
// - 슬레이브 Pod는 특정 마스터를 복제하도록 구성
// - 마스터-슬레이브 관계를 나타내는 라벨링
//
// cr: KRedis 커스텀 리소스
// masterIndex: 복제 대상 마스터 노드의 인덱스
// return: 구성된 StatefulSet 객체
func CreateSlaveStatefulSet(cr *v1alpha1.KRedis, masterIndex int) *appsv1.StatefulSet {
	// StatefulSet 이름 설정 - 마스터 인덱스를 포함
	name := fmt.Sprintf("%s-slave-%d", cr.Name, masterIndex)

	// 헤드리스 서비스 이름 (StatefulSet의 serviceName 필드에 사용)
	headlessServiceName := fmt.Sprintf("%s-headless", cr.Name)

	// 해당 마스터 Pod의 이름 (redis-cli --cluster help-node 명령에 사용)
	masterPodName := fmt.Sprintf("%s-master-%d", cr.Name, masterIndex)

	// Redis 포트와 클러스터 버스 포트 계산
	redisPort := cr.Spec.BasePort
	clusterBusPort := redisPort + 10000

	// 라벨 및 셀렉터 설정 - 마스터 인덱스 포함
	labels := utils.LabelsForKRedis(cr.Name, "slave")
	labels["master-index"] = fmt.Sprintf("%d", masterIndex)
	labelSelector := map[string]string{
		"app":          "kredis",
		"kredis_cr":    cr.Name,
		"role":         "slave",
		"master-index": fmt.Sprintf("%d", masterIndex),
	}

	// StatefulSet 생성
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cr.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cr, v1alpha1.GroupVersion.WithKind("KRedis")),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			// cr.Spec.Replicas 수만큼의 복제본을 관리
			Replicas: func() *int32 { i := int32(cr.Spec.Replicas); return &i }(),

			// Pod 이름 접두사 설정
			ServiceName: headlessServiceName,

			// Pod 셀렉터 설정
			Selector: &metav1.LabelSelector{
				MatchLabels: labelSelector,
			},

			// Pod 업데이트 전략 설정
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},

			// Pod 템플릿 설정
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labelSelector,
				},
				Spec: corev1.PodSpec{
					// Pod 초기화 컨테이너
					InitContainers: []corev1.Container{
						{
							Name:  "wait-for-master",
							Image: "busybox:1.28",
							Command: []string{
								"sh",
								"-c",
								fmt.Sprintf("until nslookup %s.%s.%s.svc.cluster.local; do echo waiting for master; sleep 2; done;",
									masterPodName, headlessServiceName, cr.Namespace),
							},
						},
						{
							Name:  "init-redis-config",
							Image: "busybox:1.28",
							Command: []string{
								"sh",
								"-c",
								"mkdir -p /redis-writable && cp /etc/redis/* /redis-writable/ && chmod +x /redis-writable/*.sh",
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "redis-config-readonly",
									MountPath: "/etc/redis",
								},
								{
									Name:      "redis-config-writable",
									MountPath: "/redis-writable",
								},
							},
						},
					},
					// 메인 컨테이너 설정
					Containers: []corev1.Container{
						{
							Name:  "redis",
							Image: cr.Spec.Image,
							Ports: []corev1.ContainerPort{
								{
									Name:          "redis",
									ContainerPort: int32(redisPort),
									Protocol:      "TCP",
								},
								{
									Name:          "cluster-bus",
									ContainerPort: int32(clusterBusPort),
									Protocol:      "TCP",
								},
							},
							// 시작 스크립트 경로 수정
							Command: []string{
								"sh",
								"/redis-writable/start-slave.sh",
							},
							// 환경 변수 설정 - 공통 함수 사용
							Env: getRedisCommonEnvVars(int32(redisPort), int32(clusterBusPort), cr.Spec.Memory),
							// 여기에 마스터 특정 환경 변수를 추가로 설정할 수 있습니다

							// 리소스 제한 설정
							Resources: corev1.ResourceRequirements{
								Requests: utils.ResourceMapToResourceList(cr.Spec.Resource["requests"]),
								Limits:   utils.ResourceMapToResourceList(cr.Spec.Resource["limits"]),
							},
							// Liveness 프로브 - Redis 서버 응답 여부 확인
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									TCPSocket: &corev1.TCPSocketAction{
										Port: intstr.FromInt(int(redisPort)),
									},
								},
								InitialDelaySeconds: 15,
								PeriodSeconds:       20,
								TimeoutSeconds:      5,
								FailureThreshold:    6,
							},
							// Readiness 프로브 - Redis 명령 처리 가능 여부 확인
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"sh",
											"-c",
											fmt.Sprintf("redis-cli -h $(hostname) -p %d ping", redisPort),
										},
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       10,
								TimeoutSeconds:      5,
								SuccessThreshold:    1,
								FailureThreshold:    3,
							},
							// 볼륨 마운트 설정 - 쓰기 가능한 디렉토리로 수정
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "redis-data",
									MountPath: "/data",
								},
								{
									Name:      "redis-config-writable",
									MountPath: "/redis-writable",
								},
							},
						},
					},
					// Docker 이미지 풀 시크릿 설정
					ImagePullSecrets: []corev1.LocalObjectReference{
						{
							Name: cr.Spec.SecretName,
						},
					},
					// ConfigMap 볼륨과 emptyDir 볼륨 추가
					Volumes: []corev1.Volume{
						{
							Name: "redis-config-readonly",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "kredis-operator-redis-config", // Kustomize 접두사를 고려한 이름
									},
								},
							},
						},
						{
							Name: "redis-config-writable",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
			// PVC 템플릿 설정 - 각 Pod에 대한 영구 볼륨 생성
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "redis-data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		},
	}

	return statefulSet
}
