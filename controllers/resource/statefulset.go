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

// Helper: Create resource requirements from Spec
func createResourceRequirements(resourcesMap map[string]map[string]string) corev1.ResourceRequirements {
	reqs := corev1.ResourceRequirements{
		Limits:   corev1.ResourceList{},
		Requests: corev1.ResourceList{},
	}
	if limitMap, ok := resourcesMap["limits"]; ok {
		for k, v := range limitMap {
			reqs.Limits[corev1.ResourceName(k)] = resource.MustParse(v)
		}
	}
	if requestMap, ok := resourcesMap["requests"]; ok {
		for k, v := range requestMap {
			reqs.Requests[corev1.ResourceName(k)] = resource.MustParse(v)
		}
	}
	return reqs
}

// Helper: Shared volume claims
func volumeClaims() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "redis-data",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "redis-logs",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("500Mi"),
					},
				},
			},
		},
	}
}

// Helper: Probes
func redisProbes(basePort int32) (*corev1.Probe, *corev1.Probe) {
	liveness := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(int(basePort))},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       10,
	}
	readiness := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"redis-cli", "ping"},
			},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       10,
		TimeoutSeconds:      3,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}
	return liveness, readiness
}

// Helper: Base Redis container
func redisContainer(k *cachev1alpha1.Kredis, env []corev1.EnvVar) corev1.Container {
	liveness, readiness := redisProbes(k.Spec.BasePort)
	return corev1.Container{
		Image:           k.Spec.Image,
		Name:            "redis",
		ImagePullPolicy: corev1.PullAlways,
		Command:         []string{"sh", "-c", "/entrypoint.sh"},
		Env:             env,
		VolumeMounts: []corev1.VolumeMount{
			{Name: "redis-data", MountPath: "/data"},
			{Name: "redis-logs", MountPath: "/logs"},
		},
		Resources:      createResourceRequirements(k.Spec.Resources),
		Ports:          []corev1.ContainerPort{{ContainerPort: k.Spec.BasePort, Name: "redis"}},
		LivenessProbe:  liveness,
		ReadinessProbe: readiness,
	}
}

// Helper: Create StatefulSet
func createRedisStatefulSet(k *cachev1alpha1.Kredis, scheme *runtime.Scheme, name, serviceName string, role string, replicas int32, labels map[string]string, env []corev1.EnvVar) *appsv1.StatefulSet {
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: k.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: serviceName,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers:       []corev1.Container{redisContainer(k, env)},
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: "docker-secret"}},
				},
			},
			VolumeClaimTemplates: volumeClaims(),
		},
	}
	controllerutil.SetControllerReference(k, ss, scheme)
	return ss
}

// 마스터용 StatefulSet
func CreateMasterStatefulSet(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *appsv1.StatefulSet {
	masterName := fmt.Sprintf("%s-master", k.Name)
	env := []corev1.EnvVar{{Name: "REDIS_MODE", Value: "master"}}
	labels := LabelsForKredis(k.Name, "master")
	return createRedisStatefulSet(k, scheme, masterName, masterName, "master", k.Spec.Masters, labels, env)
}

// 일반 슬레이브
func CreateSlaveStatefulSet(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *appsv1.StatefulSet {
	slaveName := fmt.Sprintf("%s-slave", k.Name)
	masterSvc := fmt.Sprintf("%s-master.%s.svc.cluster.local", k.Name, k.Namespace)
	env := []corev1.EnvVar{
		{Name: "REDIS_MODE", Value: "slave"},
		{Name: "MASTER_HOST", Value: masterSvc},
		{Name: "MASTER_PORT", Value: fmt.Sprintf("%d", k.Spec.BasePort)},
	}
	labels := LabelsForKredis(k.Name, "slave")
	return createRedisStatefulSet(k, scheme, slaveName, slaveName, "slave", k.Spec.Replicas, labels, env)
}

// 슬레이브-per-master
func CreateSlaveStatefulSetForMaster(k *cachev1alpha1.Kredis, scheme *runtime.Scheme, masterIndex int) (*appsv1.StatefulSet, error) {
	masterFQDN := fmt.Sprintf("%s-master-%d.%s-master.%s.svc.cluster.local", k.Name, masterIndex, k.Name, k.Namespace)
	slaveName := fmt.Sprintf("%s-slave-%d", k.Name, masterIndex)
	env := []corev1.EnvVar{
		{Name: "REDIS_MODE", Value: "slave"},
		{Name: "MASTER_HOST", Value: masterFQDN},
		{Name: "MASTER_PORT", Value: fmt.Sprintf("%d", k.Spec.BasePort)},
		{Name: "MASTER_INDEX", Value: fmt.Sprintf("%d", masterIndex)},
		{Name: "TOTAL_MASTERS", Value: fmt.Sprintf("%d", k.Spec.Masters)},
	}
	labels := LabelsForKredis(k.Name, fmt.Sprintf("slave-%d", masterIndex))
	return createRedisStatefulSet(k, scheme, slaveName, slaveName, "slave", k.Spec.Replicas, labels, env), nil
}

// Unified 슬레이브
func CreateUnifiedSlaveStatefulSet(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *appsv1.StatefulSet {
	totalSlaves := k.Spec.Masters * k.Spec.Replicas
	slaveName := fmt.Sprintf("%s-slave", k.Name)
	masterSvc := fmt.Sprintf("%s-master.%s.svc.cluster.local", k.Name, k.Namespace)
	env := []corev1.EnvVar{
		{Name: "REDIS_MODE", Value: "slave"},
		{Name: "MASTER_HOST", Value: masterSvc},
		{Name: "MASTER_PORT", Value: fmt.Sprintf("%d", k.Spec.BasePort)},
		{Name: "TOTAL_MASTERS", Value: fmt.Sprintf("%d", k.Spec.Masters)},
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
	}
	labels := LabelsForKredis(k.Name, "slave")
	return createRedisStatefulSet(k, scheme, slaveName, slaveName, "slave", totalSlaves, labels, env)
}

// 슬레이브 서비스 정의 (per master)
func CreateSlaveServiceForMaster(k *cachev1alpha1.Kredis, scheme *runtime.Scheme, masterIndex int) *corev1.Service {
	slaveName := fmt.Sprintf("%s-slave-%d", k.Name, masterIndex)
	labels := LabelsForKredis(k.Name, fmt.Sprintf("slave-%d", masterIndex))

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      slaveName,
			Namespace: k.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector:  labels,
			Ports:     []corev1.ServicePort{{Name: "redis", Port: k.Spec.BasePort, TargetPort: intstr.FromInt(int(k.Spec.BasePort))}},
			ClusterIP: "None",
			Type:      corev1.ServiceTypeClusterIP,
		},
	}
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}
