package resource

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// CreateRedisStatefulSet creates a unified StatefulSet for all Redis nodes
func CreateRedisStatefulSet(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *appsv1.StatefulSet {
	// 총 레플리카 수 계산: 마스터 + (마스터 * 슬레이브)
	totalReplicas := k.Spec.Masters + (k.Spec.Masters * k.Spec.Replicas)

	// StatefulSet 자체에는 role 라벨 불필요 (Pod에만 동적으로 부여됨)
	labels := BaseLabelsForKredis(k.Name)
	selectorLabels := labels // role 없으므로 그대로 사용 가능

	// 환경 변수 설정 - 컨트롤러가 클러스터 관리를 담당하므로 최소한만 설정
	env := []corev1.EnvVar{
		{Name: "REDIS_PORT", Value: fmt.Sprintf("%d", k.Spec.BasePort)},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}},
	}
	if k.Spec.MaxMemory != "" {
		env = append(env, corev1.EnvVar{Name: "REDIS_MAXMEMORY", Value: k.Spec.MaxMemory})
	}

	// 리소스 요구사항
	resources := corev1.ResourceRequirements{}
	if k.Spec.Resources.Limits != nil {
		resources.Limits = k.Spec.Resources.Limits.DeepCopy()
	}
	if k.Spec.Resources.Requests != nil {
		resources.Requests = k.Spec.Resources.Requests.DeepCopy()
	}

	// 프로브 설정
	// Liveness/Readiness Probe는 사용하지 않음 - Redis Cluster는 자체 장애 감지/페일오버 메커니즘이 있음
	// - 리밸런싱 등 무거운 작업 중 Probe 실패로 인한 파드 재시작/unready 상태 방지
	// - Redis Cluster 클라이언트는 MOVED/ASK 리다이렉션을 통해 올바른 노드를 찾음
	// - Startup Probe만 유지하여 초기 시작 시간 확보

	startupProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"redis-cli", "-p", fmt.Sprintf("%d", k.Spec.BasePort), "ping"},
			},
		},
		InitialDelaySeconds: 0,
		PeriodSeconds:       5,
		TimeoutSeconds:      5,
		FailureThreshold:    30, // ~2.5 minutes max
	}

	// Redis 컨테이너
	redisContainer := corev1.Container{
		Name:            "redis",
		Image:           k.Spec.Image,
		ImagePullPolicy: corev1.PullAlways,
		Command:         []string{"sh", "-c", "/entrypoint.sh"},
		Env:             env,
		Ports: []corev1.ContainerPort{
			{ContainerPort: k.Spec.BasePort, Name: "redis"},
			{ContainerPort: k.Spec.BasePort + 10000, Name: "cluster-bus"},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "redis-data", MountPath: "/data"},
			{Name: "redis-logs", MountPath: "/logs"},
		},
		Resources:    resources,
		StartupProbe: startupProbe,
		// Liveness/Readiness Probe는 사용하지 않음 - Redis Cluster 자체 HA 메커니즘 활용
	}

	// 볼륨 클레임 템플릿
	volumeClaimTemplates := []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "redis-data"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "redis-logs"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("500Mi"),
					},
				},
			},
		},
	}

	// StatefulSet 생성
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name,
			Namespace: k.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &totalReplicas,
			ServiceName: k.Name,
			Selector: &metav1.LabelSelector{ // role 제외
				MatchLabels: selectorLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{redisContainer},
				},
			},
			VolumeClaimTemplates: volumeClaimTemplates,
			PodManagementPolicy:  appsv1.OrderedReadyPodManagement,
		},
	}

	// Build imagePullSecret name with optional prefix from CR labels
	secretName := "docker-secret"
	if k != nil && k.Labels != nil {
		if prefix, ok := k.Labels["app.kubernetes.io/name-prefix"]; ok && prefix != "" {
			if !strings.HasSuffix(prefix, "-") {
				prefix = prefix + "-"
			}
			secretName = prefix + secretName
		}
	}
	ss.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: secretName}}

	controllerutil.SetControllerReference(k, ss, scheme)
	return ss
}
