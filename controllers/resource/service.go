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

// CreateRedisMasterNodePortService: 외부 쓰기용 Master 전용 NodePort Service
/*
func CreateRedisMasterNodePortService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	baseLabels := BaseLabelsForKredis(k.Name)
	selector := map[string]string{
		"app":                        baseLabels["app"],
		"app.kubernetes.io/name":     baseLabels["app.kubernetes.io/name"],
		"app.kubernetes.io/instance": baseLabels["app.kubernetes.io/instance"],
		"role":                       "master", // Master Pod만 선택
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name + "-master-nodeport",
			Namespace: k.Namespace,
			Labels:    baseLabels,
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
*/

// CreateRedisSlaveNodePortService: 외부 읽기용 Slave 전용 NodePort Service
/*
func CreateRedisSlaveNodePortService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	baseLabels := BaseLabelsForKredis(k.Name)
	selector := map[string]string{
		"app":                        baseLabels["app"],
		"app.kubernetes.io/name":     baseLabels["app.kubernetes.io/name"],
		"app.kubernetes.io/instance": baseLabels["app.kubernetes.io/instance"],
		"role":                       "slave", // Slave Pod만 선택
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name + "-slave-nodeport",
			Namespace: k.Namespace,
			Labels:    baseLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Type:     corev1.ServiceTypeNodePort,
			Ports: []corev1.ServicePort{
				{
					Name:       "redis",
					Port:       k.Spec.BasePort,
					TargetPort: intstr.FromString("redis"),
					NodePort:   30380, // 30000-32767 범위에서 지정 (또는 생략하면 자동 할당)
				},
			},
		},
	}
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}
*/

// CreateRedisHeadlessService creates a headless service for pod DNS resolution
// Each pod gets DNS: {podName}.{serviceName}.{namespace}.svc.cluster.local
func CreateRedisHeadlessService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	baseLabels := BaseLabelsForKredis(k.Name)

	ports := []corev1.ServicePort{
		{Name: "redis", Port: k.Spec.BasePort, TargetPort: intstr.FromString("redis")},
		{Name: "cluster-bus", Port: k.Spec.BasePort + 10000, TargetPort: intstr.FromString("cluster-bus")},
	}

	// Add exporter port if enabled
	if k.Spec.Exporter.Enabled {
		exporterPort := k.Spec.Exporter.Port
		if exporterPort == 0 {
			exporterPort = 9121
		}
		ports = append(ports, corev1.ServicePort{
			Name: "exporter", Port: exporterPort, TargetPort: intstr.FromString("exporter"),
		})
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name, // kredis-sample
			Namespace: k.Namespace,
			Labels:    baseLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector:  baseLabels,
			ClusterIP: "None", // Headless
			Ports:     ports,
			// PublishNotReadyAddresses allows DNS resolution even for not-ready pods
			PublishNotReadyAddresses: true,
		},
	}
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}

// CreateRedisMetricsService creates a service for Prometheus to scrape Redis exporter metrics
// This is a regular ClusterIP service (not headless) for easier Prometheus discovery
func CreateRedisMetricsService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	if !k.Spec.Exporter.Enabled {
		return nil
	}

	baseLabels := BaseLabelsForKredis(k.Name)
	// Add prometheus scrape annotations for auto-discovery
	annotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "9121",
		"prometheus.io/path":   "/metrics",
	}

	exporterPort := k.Spec.Exporter.Port
	if exporterPort == 0 {
		exporterPort = 9121
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        k.Name + "-metrics",
			Namespace:   k.Namespace,
			Labels:      baseLabels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Selector: baseLabels,
			Type:     corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "exporter", Port: exporterPort, TargetPort: intstr.FromString("exporter")},
			},
		},
	}
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}

// CreateRedisMasterService: 쓰기용 Master 전용 서비스
func CreateRedisMasterService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	baseLabels := BaseLabelsForKredis(k.Name)
	selector := map[string]string{
		"app":                        baseLabels["app"],
		"app.kubernetes.io/name":     baseLabels["app.kubernetes.io/name"],
		"app.kubernetes.io/instance": baseLabels["app.kubernetes.io/instance"],
		"role":                       "master", // Master Pod만 선택
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name + "-master",
			Namespace: k.Namespace,
			Labels:    baseLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector:  selector,
			ClusterIP: "None", // Headless for direct pod access
			Type:      corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: k.Spec.BasePort, TargetPort: intstr.FromString("redis")},
			},
		},
	}
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}

// CreateRedisSlaveService: 읽기용 Slave 전용 서비스
func CreateRedisSlaveService(k *cachev1alpha1.Kredis, scheme *runtime.Scheme) *corev1.Service {
	baseLabels := BaseLabelsForKredis(k.Name)
	selector := map[string]string{
		"app":                        baseLabels["app"],
		"app.kubernetes.io/name":     baseLabels["app.kubernetes.io/name"],
		"app.kubernetes.io/instance": baseLabels["app.kubernetes.io/instance"],
		"role":                       "slave", // Slave Pod만 선택
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Name + "-slave",
			Namespace: k.Namespace,
			Labels:    baseLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector:  selector,
			ClusterIP: "None", // Headless for direct pod access
			Type:      corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: k.Spec.BasePort, TargetPort: intstr.FromString("redis")},
			},
		},
	}
	controllerutil.SetControllerReference(k, svc, scheme)
	return svc
}
