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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// CreateRedisService: 통합 Headless Service (cluster-bus 포함)
// - Pod DNS 접근 및 클러스터 버스 포트 모두 제공
func CreateRedisService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	// 전체 파드를 대상으로 하는 Headless Service.
	// selector 에 role 제외 (role 은 동적으로 master/slave 로 변경됨)
	podLabels := LabelsForKredis(k.Name, "redis")
	selector := map[string]string{
		"app":                        podLabels["app"],
		"app.kubernetes.io/name":     podLabels["app.kubernetes.io/name"],
		"app.kubernetes.io/instance": podLabels["app.kubernetes.io/instance"],
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name,
			Namespace: k.Namespace,
			Labels:    selector,
		},
		Spec: corev1.ServiceSpec{
			Selector:  selector,
			ClusterIP: "None", // Headless -> per-pod DNS
			Type:      corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: k.Spec.BasePort, TargetPort: intstr.FromString("redis")},
				{Name: "cluster-bus", Port: k.Spec.BasePort + 10000, TargetPort: intstr.FromString("cluster-bus")},
			},
		},
	}
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}

// CreateRedisNodePortService: 외부 접근용 NodePort Service
func CreateRedisNodePortService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	podLabels := LabelsForKredis(k.Name, "redis")
	selector := map[string]string{
		"app":                        podLabels["app"],
		"app.kubernetes.io/name":     podLabels["app.kubernetes.io/name"],
		"app.kubernetes.io/instance": podLabels["app.kubernetes.io/instance"],
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name + "-nodeport",
			Namespace: k.Namespace,
			Labels:    selector,
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Type:     corev1.ServiceTypeNodePort,
			Ports: []corev1.ServicePort{
				{
					Name:       "redis",
					Port:       k.Spec.BasePort,
					TargetPort: intstr.FromString("redis"),
					NodePort:   30379, // 30000-32767 범위에서 지정 (또는 생략하면 자동 할당)
				},
			},
		},
	}
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}
