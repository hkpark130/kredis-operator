/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

// createResourceRequirements converts map[string]map[string]string to corev1.ResourceRequirements
func createResourceRequirements(resourcesMap map[string]map[string]string) corev1.ResourceRequirements {
	requirements := corev1.ResourceRequirements{
		Limits:   corev1.ResourceList{},
		Requests: corev1.ResourceList{},
	}

	// 리소스 맵에서 limits와 requests 추출
	if limitMap, exists := resourcesMap["limits"]; exists {
		for key, val := range limitMap {
			requirements.Limits[corev1.ResourceName(key)] = resource.MustParse(val)
		}
	}

	if requestsMap, exists := resourcesMap["requests"]; exists {
		for key, val := range requestsMap {
			requirements.Requests[corev1.ResourceName(key)] = resource.MustParse(val)
		}
	}

	return requirements
}

// CreateMasterStatefulSet 함수는 Redis 마스터를 위한 StatefulSet을 생성합니다.
func CreateMasterStatefulSet(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *appsv1.StatefulSet {
	masterName := fmt.Sprintf("%s-master", k.Name)
	ls := LabelsForKredis(k.Name, "master")
	replicas := k.Spec.Masters // Use the masters value from KredisSpec

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      masterName,
			Namespace: k.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			ServiceName: masterName,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: []corev1.LocalObjectReference{
						{
							Name: "docker-secret",
						},
					},
					Containers: []corev1.Container{{
						Image:           k.Spec.Image,
						Name:            "redis",
						ImagePullPolicy: corev1.PullAlways, // 항상 이미지를 풀도록 설정
						Command: []string{
							"sh",
							"-c",
							"/entrypoint.sh",
						},
						Env: []corev1.EnvVar{
							{
								Name:  "REDIS_MODE",
								Value: "master",
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "redis-data",
								MountPath: "/data",
							},
							{
								Name:      "redis-logs",
								MountPath: "/logs",
							},
						},
						Ports: []corev1.ContainerPort{{
							ContainerPort: k.Spec.BasePort,
							Name:          "redis",
						}},
						Resources: createResourceRequirements(k.Spec.Resources),
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.IntOrString{Type: intstr.Int, IntVal: k.Spec.BasePort},
								},
							},
							InitialDelaySeconds: 30,
							PeriodSeconds:       10,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.IntOrString{Type: intstr.Int, IntVal: k.Spec.BasePort},
								},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       10,
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
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
			},
		},
	}

	// Set Kredis instance as the owner and controller
	controllerutil.SetControllerReference(k, sts, scheme)
	return sts
}

// CreateSlaveStatefulSet 함수는 Redis 슬레이브를 위한 StatefulSet을 생성합니다.
func CreateSlaveStatefulSet(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *appsv1.StatefulSet {
	ls := LabelsForKredis(k.Name, "slave")

	// Use the number of replicas specified in the spec
	replicas := k.Spec.Replicas

	masterName := fmt.Sprintf("%s-master", k.Name)
	slaveName := fmt.Sprintf("%s-slave", k.Name)
	masterService := fmt.Sprintf("%s.%s.svc.cluster.local", masterName, k.Namespace)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      slaveName,
			Namespace: k.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			ServiceName: slaveName,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: []corev1.LocalObjectReference{
						{
							Name: "docker-secret",
						},
					},
					Containers: []corev1.Container{{
						Image:           k.Spec.Image,
						Name:            "redis",
						ImagePullPolicy: corev1.PullAlways, // 항상 이미지를 풀도록 설정
						Command: []string{
							"sh",
							"-c",
							"/entrypoint.sh",
						},
						Env: []corev1.EnvVar{
							{
								Name:  "REDIS_MODE",
								Value: "slave",
							},
							{
								Name:  "MASTER_HOST",
								Value: masterService,
							},
							{
								Name:  "MASTER_PORT",
								Value: fmt.Sprintf("%d", k.Spec.BasePort),
							},
						},
						Ports: []corev1.ContainerPort{{
							ContainerPort: k.Spec.BasePort,
							Name:          "redis",
						}},
						Resources: createResourceRequirements(k.Spec.Resources),
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.IntOrString{Type: intstr.Int, IntVal: k.Spec.BasePort},
								},
							},
							InitialDelaySeconds: 30,
							PeriodSeconds:       10,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.IntOrString{Type: intstr.Int, IntVal: k.Spec.BasePort},
								},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       10,
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
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
			},
		},
	}
	// Set Kredis instance as the owner and controller
	controllerutil.SetControllerReference(k, sts, scheme)
	return sts
}

// CreateSlaveStatefulSetForMaster 함수는 특정 마스터 파드를 위한 Redis 슬레이브 StatefulSet을 생성합니다.
func CreateSlaveStatefulSetForMaster(k *cachev1alpha1.Kredis, scheme *runtime.Scheme, masterIndex int) (*appsv1.StatefulSet, error) {
	// 특정 마스터 파드 이름 생성
	specificMasterPodName := fmt.Sprintf("%s-master-%d", k.Name, masterIndex)
	// 특정 마스터 파드에 대한 슬레이브 StatefulSet 이름 생성
	slaveName := fmt.Sprintf("%s-slave-%d", k.Name, masterIndex)

	ls := LabelsForKredis(k.Name, fmt.Sprintf("slave-%d", masterIndex))

	// 슬레이브 복제본 수 설정 (각 마스터에 대해 사용자가 지정한 수만큼)
	replicas := int32(1) // 기본값은 1개
	if k.Spec.Replicas > 0 {
		replicas = k.Spec.Replicas
	}

	// 특정 마스터 파드의 FQDN (headless 서비스를 통해)
	masterHeadlessService := fmt.Sprintf("%s-master.%s.svc.cluster.local", k.Name, k.Namespace)
	specificMasterPod := fmt.Sprintf("%s.%s", specificMasterPodName, masterHeadlessService)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      slaveName,
			Namespace: k.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			ServiceName: slaveName,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: []corev1.LocalObjectReference{
						{
							Name: "docker-secret",
						},
					},
					Containers: []corev1.Container{{
						Image:           k.Spec.Image,
						Name:            "redis",
						ImagePullPolicy: corev1.PullAlways, // 항상 이미지를 풀도록 설정
						Command: []string{
							"sh",
							"-c",
							"/entrypoint.sh",
						},
						Env: []corev1.EnvVar{
							{
								Name:  "REDIS_MODE",
								Value: "slave",
							},
							{
								Name:  "MASTER_HOST",
								Value: specificMasterPod,
							},
							{
								Name:  "MASTER_PORT",
								Value: fmt.Sprintf("%d", k.Spec.BasePort),
							},
							{
								Name:  "MASTER_INDEX",
								Value: fmt.Sprintf("%d", masterIndex),
							},
							{
								Name:  "TOTAL_MASTERS",
								Value: fmt.Sprintf("%d", k.Spec.Masters),
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "redis-data",
								MountPath: "/data",
							},
							{
								Name:      "redis-logs",
								MountPath: "/logs",
							},
						},
						Ports: []corev1.ContainerPort{{
							ContainerPort: k.Spec.BasePort,
							Name:          "redis",
						}},
						Resources: createResourceRequirements(k.Spec.Resources),
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.IntOrString{Type: intstr.Int, IntVal: k.Spec.BasePort},
								},
							},
							InitialDelaySeconds: 30,
							PeriodSeconds:       10,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.IntOrString{Type: intstr.Int, IntVal: k.Spec.BasePort},
								},
							},
							InitialDelaySeconds: 5,
							PeriodSeconds:       10,
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
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
			},
		},
	}

	// Set Kredis instance as the owner and controller
	controllerutil.SetControllerReference(k, sts, scheme)
	return sts, nil
}

// CreateSlaveServiceForMaster 함수는 특정 마스터 파드를 위한 슬레이브 서비스를 생성합니다.
func CreateSlaveServiceForMaster(k *cachev1alpha1.Kredis, scheme *runtime.Scheme, masterIndex int) *corev1.Service {
	slaveName := fmt.Sprintf("%s-slave-%d", k.Name, masterIndex)
	ls := LabelsForKredis(k.Name, fmt.Sprintf("slave-%d", masterIndex))

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      slaveName,
			Namespace: k.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Port:       k.Spec.BasePort,
				TargetPort: intstr.FromInt(int(k.Spec.BasePort)),
				Name:       "redis",
			}},
			Selector: ls,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}

	// Headless 서비스 설정 (StatefulSet에서 사용하기 위한 DNS 엔트리)
	svc.Spec.ClusterIP = "None"

	// Set Kredis instance as the owner and controller
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}
