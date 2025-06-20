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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// CreateMasterService 함수는 Redis 마스터를 위한 Service를 생성합니다
func CreateMasterService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	ls := LabelsForKredis(k.Name, "master")
	name := fmt.Sprintf("%s-master", k.Name)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: k.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: ls,
			Ports: []corev1.ServicePort{{
				Port:       k.Spec.BasePort,
				TargetPort: intstr.FromString("redis"),
				Name:       "redis",
			}},
			// Headless 서비스로 설정하여 각 포드의 DNS 주소를 개별적으로 접근 가능하게 함
			ClusterIP: "None",
		},
	}

	// Set Kredis instance as the owner and controller
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}

// CreateSlaveService 함수는 Redis 슬레이브를 위한 Service를 생성합니다
func CreateSlaveService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	ls := LabelsForKredis(k.Name, "slave")
	name := fmt.Sprintf("%s-slave", k.Name)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: k.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: ls,
			Ports: []corev1.ServicePort{{
				Port:       k.Spec.BasePort,
				TargetPort: intstr.FromString("redis"),
				Name:       "redis",
			}},
		},
	}

	// Set Kredis instance as the owner and controller
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}

// CreateGeneralService 함수는 전체 Redis 클러스터를 위한 일반 Service를 생성합니다
func CreateGeneralService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	// 일반 서비스는 모든 Redis 인스턴스를 대상으로 함
	ls := map[string]string{
		"app":                        "kredis",
		"app.kubernetes.io/name":     k.Name,
		"app.kubernetes.io/instance": k.Name,
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name,
			Namespace: k.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: ls,
			Ports: []corev1.ServicePort{{
				Port:       k.Spec.BasePort,
				TargetPort: intstr.FromString("redis"),
				Name:       "redis",
			}},
		},
	}

	// Set Kredis instance as the owner and controller
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}
