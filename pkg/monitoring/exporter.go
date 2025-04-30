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

package monitoring

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	
	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

const (
	// Redis Exporter 이미지 정보
	RedisExporterImage = "oliver006/redis_exporter:v1.44.0"
	ExporterPort       = 9121
)

// ExporterDeploymentForKRedis 함수는 Redis 모니터링을 위한 Redis Exporter를 배포합니다
func ExporterDeploymentForKRedis(cr *stablev1alpha1.KRedis) *appsv1.Deployment {
	replicas := int32(1)
	labels := map[string]string{
		"app":        cr.Name + "-exporter",
		"managed-by": "kredis-operator",
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-exporter",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "9121",
					},
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: []corev1.LocalObjectReference{
						{
							Name: cr.Spec.SecretName,
						},
					},
					Containers: []corev1.Container{{
						Name:  "redis-exporter",
						Image: RedisExporterImage,
						Env: []corev1.EnvVar{
							{
								Name:  "REDIS_ADDR",
								Value: "redis://" + cr.Name + "-client:6379",
							},
						},
						Ports: []corev1.ContainerPort{{
							Name:          "exporter",
							ContainerPort: int32(ExporterPort),
							Protocol:      corev1.ProtocolTCP,
						}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								"cpu":    parseResourceQuantity("100m"),
								"memory": parseResourceQuantity("128Mi"),
							},
							Limits: corev1.ResourceList{
								"cpu":    parseResourceQuantity("200m"),
								"memory": parseResourceQuantity("256Mi"),
							},
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt(ExporterPort),
								},
							},
							InitialDelaySeconds: 10,
							PeriodSeconds:       10,
						},
					}},
				},
			},
		},
	}
}

// ExporterServiceForKRedis 함수는 Redis Exporter를 위한 서비스를 생성합니다
func ExporterServiceForKRedis(cr *stablev1alpha1.KRedis) *corev1.Service {
	labels := map[string]string{
		"app":        cr.Name + "-exporter",
		"managed-by": "kredis-operator",
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-exporter",
			Namespace: cr.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "9121",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "exporter",
				Port:       int32(ExporterPort),
				TargetPort: intstr.FromInt(ExporterPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

// ReconcileExporter 함수는 Redis Exporter를 생성하거나 업데이트합니다
func ReconcileExporter(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)

	// Exporter Deployment 생성/업데이트
	if err := reconcileExporterDeployment(ctx, cl, cr); err != nil {
		return err
	}

	// Exporter Service 생성/업데이트
	if err := reconcileExporterService(ctx, cl, cr); err != nil {
		return err
	}

	logger.Info("Redis Exporter successfully reconciled")
	return nil
}

// reconcileExporterDeployment 함수는 Exporter Deployment를 생성하거나 업데이트합니다
func reconcileExporterDeployment(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)
	
	// 원하는 Exporter Deployment 생성
	deployment := ExporterDeploymentForKRedis(cr)
	
	// 현재 Deployment 조회
	found := &appsv1.Deployment{}
	err := cl.Get(ctx, types.NamespacedName{
		Name:      deployment.Name, 
		Namespace: deployment.Namespace,
	}, found)

	if err != nil && errors.IsNotFound(err) {
		// Deployment가 없으면 생성
		logger.Info("Creating Redis Exporter Deployment", "Namespace", deployment.Namespace, "Name", deployment.Name)
		if err = cl.Create(ctx, deployment); err != nil {
			logger.Error(err, "Failed to create Redis Exporter Deployment")
			return err
		}
	} else if err != nil {
		logger.Error(err, "Failed to get Redis Exporter Deployment")
		return err
	} else {
		// Deployment가 존재하면 업데이트
		logger.Info("Updating Redis Exporter Deployment", "Namespace", found.Namespace, "Name", found.Name)
		
		// 사양 업데이트
		found.Spec.Template = deployment.Spec.Template
		
		if err = cl.Update(ctx, found); err != nil {
			logger.Error(err, "Failed to update Redis Exporter Deployment")
			return err
		}
	}
	
	return nil
}

// reconcileExporterService 함수는 Exporter Service를 생성하거나 업데이트합니다
func reconcileExporterService(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)
	
	// 원하는 Exporter Service 생성
	service := ExporterServiceForKRedis(cr)
	
	// 현재 Service 조회
	found := &corev1.Service{}
	err := cl.Get(ctx, types.NamespacedName{
		Name:      service.Name,
		Namespace: service.Namespace,
	}, found)

	if err != nil && errors.IsNotFound(err) {
		// Service가 없으면 생성
		logger.Info("Creating Redis Exporter Service", "Namespace", service.Namespace, "Name", service.Name)
		if err = cl.Create(ctx, service); err != nil {
			logger.Error(err, "Failed to create Redis Exporter Service")
			return err
		}
	} else if err != nil {
		logger.Error(err, "Failed to get Redis Exporter Service")
		return err
	} else {
		// Service가 존재하면 업데이트
		logger.Info("Updating Redis Exporter Service", "Namespace", found.Namespace, "Name", found.Name)
		
		// 불변 필드를 유지하고 업데이트 가능한 필드만 업데이트
		found.Spec.Ports = service.Spec.Ports
		found.Spec.Selector = service.Spec.Selector
		found.Labels = service.Labels
		found.Annotations = service.Annotations
		
		if err = cl.Update(ctx, found); err != nil {
			logger.Error(err, "Failed to update Redis Exporter Service")
			return err
		}
	}
	
	return nil
}

// parseResourceQuantity 함수는 문자열에서 자원 수량을 파싱합니다
func parseResourceQuantity(value string) corev1.ResourceQuantity {
	quantity, _ := corev1.ParseQuantity(value)
	return quantity
}