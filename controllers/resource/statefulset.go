package resource

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// CreateRedisStatefulSet creates a unified StatefulSet for all Redis nodes
func CreateRedisStatefulSet(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *appsv1.StatefulSet {
	// 총 레플리카 수 계산: 마스터 + (마스터 * 슬레이브)
	totalReplicas := k.Spec.Masters + (k.Spec.Masters * k.Spec.Replicas)

	labels := LabelsForKredis(k.Name, "redis")

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

	// 리소스 요구사항
	resources := corev1.ResourceRequirements{}
	if k.Spec.Resources != nil {
		if limitMap, ok := k.Spec.Resources["limits"]; ok {
			resources.Limits = corev1.ResourceList{}
			for key, value := range limitMap {
				resources.Limits[corev1.ResourceName(key)] = resource.MustParse(value)
			}
		}
		if requestMap, ok := k.Spec.Resources["requests"]; ok {
			resources.Requests = corev1.ResourceList{}
			for key, value := range requestMap {
				resources.Requests[corev1.ResourceName(key)] = resource.MustParse(value)
			}
		}
	}

	// 프로브 설정
	livenessProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(int(k.Spec.BasePort))},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       10,
	}

	readinessProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"redis-cli", "-p", fmt.Sprintf("%d", k.Spec.BasePort), "ping"},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
		TimeoutSeconds:      3,
		SuccessThreshold:    1,
		FailureThreshold:    3,
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
		Resources:      resources,
		LivenessProbe:  livenessProbe,
		ReadinessProbe: readinessProbe,
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
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers:       []corev1.Container{redisContainer},
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: "docker-secret"}},
				},
			},
			VolumeClaimTemplates: volumeClaimTemplates,
			PodManagementPolicy:  appsv1.OrderedReadyPodManagement,
		},
	}

	controllerutil.SetControllerReference(k, ss, scheme)
	return ss
}

// CreateRedisService creates a headless service for the Redis StatefulSet
func CreateRedisService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	labels := LabelsForKredis(k.Name, "redis")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name,
			Namespace: k.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector:  labels,
			ClusterIP: "None", // Headless service
			Type:      corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: k.Spec.BasePort, TargetPort: intstr.FromInt(int(k.Spec.BasePort))},
				{Name: "cluster-bus", Port: k.Spec.BasePort + 10000, TargetPort: intstr.FromInt(int(k.Spec.BasePort + 10000))},
			},
		},
	}

	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}
