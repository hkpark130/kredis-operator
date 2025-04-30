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

package resources

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	
	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// DeploymentForSlave 함수는 Redis 슬레이브 노드를 위한 Deployment 생성
func DeploymentForSlave(cr *stablev1alpha1.KRedis, index int) *appsv1.Deployment {
	labels := LabelsForKRedis(fmt.Sprintf("%s-master-%d", cr.Name, index), "slave")
	replicas := cr.Spec.Replicas
	
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-slave-%d", cr.Name, index),
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
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: []corev1.LocalObjectReference{
						{
							Name: cr.Spec.SecretName,
						},
					},
					Containers: []corev1.Container{{
						Image: cr.Spec.Image,
						Name:  "redis-slave",
						Ports: []corev1.ContainerPort{{
							ContainerPort: cr.Spec.BasePort,
						}},
						Resources: createResourceRequirements(cr),
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intOrString(int(cr.Spec.BasePort)),
								},
							},
							InitialDelaySeconds: 30,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"redis-cli", "ping"},
								},
							},
							InitialDelaySeconds: 5,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
					}},
					Affinity: createPodAntiAffinity("master"),
				},
			},
		},
	}
}

// ReconcileSlaveDeployments 함수는 슬레이브 Deployment를 관리합니다
func ReconcileSlaveDeployments(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)

	// 현재 슬레이브 Deployment 목록 조회
	slaveDeployments := &appsv1.DeploymentList{}
	if err := cl.List(ctx, slaveDeployments, 
		client.InNamespace(cr.Namespace),
		client.MatchingLabels(map[string]string{"app": "kredis", "role": "slave"})); err != nil {
		return err
	}

	// 필요한 슬레이브 수 계산
	currentCount := len(slaveDeployments.Items)
	desiredCount := int(cr.Spec.Masters)
	logger.Info("Reconciling slave deployments", "currentCount", currentCount, "desiredCount", desiredCount)

	// 슬레이브 Deployment 조정
	for i := 0; i < Max(currentCount, desiredCount); i++ {
		if i >= desiredCount {
			// 불필요한 슬레이브 삭제
			if err := DeleteRelatedSlaves(ctx, cl, cr, desiredCount); err != nil {
				return err
			}
		} else {
			// 슬레이브 생성 또는 업데이트
			slaveDep := DeploymentForSlave(cr, i)
			if err := ReconcileDeployment(ctx, cl, cr, slaveDep); err != nil {
				return err
			}
		}
	}

	return nil
}

// ReconcileDeployment 함수는 Deployment를 생성 또는 업데이트합니다
func ReconcileDeployment(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis, deployment *appsv1.Deployment) error {
	logger := log.FromContext(ctx)
	
	found := &appsv1.Deployment{}
	err := cl.Get(ctx, types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}, found)
	
	if err != nil && client.IgnoreNotFound(err) == nil {
		logger.Info("Creating Deployment", "Namespace", deployment.Namespace, "Name", deployment.Name)
		if err = cl.Create(ctx, deployment); err != nil {
			logger.Error(err, "Failed to create Deployment", "Namespace", deployment.Namespace, "Name", deployment.Name)
			return err
		}
	} else if err == nil {
		// Deployment가 존재하면 업데이트
		logger.Info("Updating Deployment", "Namespace", deployment.Namespace, "Name", deployment.Name)
		
		// 메타데이터와 사양만 업데이트
		found.Spec = deployment.Spec
		
		if err = cl.Update(ctx, found); err != nil {
			logger.Error(err, "Failed to update Deployment", "Namespace", deployment.Namespace, "Name", deployment.Name)
			return err
		}
	} else {
		return err
	}
	
	return nil
}

// DeleteRelatedSlaves 함수는 불필요한 슬레이브 Deployment를 삭제합니다
func DeleteRelatedSlaves(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis, masterCount int) error {
	logger := log.FromContext(ctx)

	// masterCount 이후의 슬레이브 Deployment 삭제
	for i := masterCount; i < int(cr.Spec.Masters); i++ {
		slaveName := fmt.Sprintf("%s-slave-%d", cr.Name, i)
		slave := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      slaveName,
				Namespace: cr.Namespace,
			},
		}
		
		if err := cl.Delete(ctx, slave); client.IgnoreNotFound(err) != nil {
			logger.Error(err, "Failed to delete slave deployment", "name", slaveName)
			return err
		}
		logger.Info("Successfully deleted slave deployment", "name", slaveName)
	}
	return nil
}