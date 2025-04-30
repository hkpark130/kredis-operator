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

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// StatefulSetForMaster 함수는 Master 노드를 위한 StatefulSet을 생성합니다
func StatefulSetForMaster(cr *stablev1alpha1.KRedis) *appsv1.StatefulSet {
	labels := LabelsForKRedis(cr.Name+"-master", "master")
	replicas := cr.Spec.Masters

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-master",
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: cr.Name + "-master-headless",
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: cr.Spec.SecretName}},
					Containers: []corev1.Container{{
						Name:  "redis-master",
						Image: cr.Spec.Image,
						Resources: createResourceRequirements(cr),
						Env: []corev1.EnvVar{
							{Name: "MASTER", Value: "true"},
							{Name: "REDIS_MAXMEMORY", Value: cr.Spec.Memory},
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							}},
						},
						Ports: []corev1.ContainerPort{{
							Name:          "redis",
							ContainerPort: cr.Spec.BasePort,
						}},
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
					Affinity: createPodAntiAffinity("slave"),
				},
			},
		},
	}
}

// ReconcileStatefulSet 함수는 StatefulSet을 조정(생성 또는 업데이트)합니다
func ReconcileStatefulSet(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) (bool, error) {
	logger := log.FromContext(ctx)
	changed := false

	// 원하는 StatefulSet 생성
	masterSts := StatefulSetForMaster(cr)
	
	// 현재 StatefulSet 조회
	currentSts := &appsv1.StatefulSet{}
	err := cl.Get(ctx, types.NamespacedName{
		Name:      masterSts.Name, 
		Namespace: masterSts.Namespace,
	}, currentSts)

	if err != nil && errors.IsNotFound(err) {
		// StatefulSet이 없으면 생성
		logger.Info("Creating Master StatefulSet", "Namespace", masterSts.Namespace, "Name", masterSts.Name)
		if err = cl.Create(ctx, masterSts); err != nil {
			logger.Error(err, "Failed to create Master StatefulSet")
			return false, err
		}
		return true, nil
	} else if err != nil {
		logger.Error(err, "Failed to get Master StatefulSet")
		return false, err
	}

	// StatefulSet이 존재하면 업데이트 여부 확인
	if !EqualStatefulSets(currentSts, masterSts) {
		// 스케일 다운 시나리오 체크
		if currentSts.Spec.Replicas != nil && masterSts.Spec.Replicas != nil &&
			*currentSts.Spec.Replicas > *masterSts.Spec.Replicas {
			
			logger.Info("Scaling down StatefulSet", 
				"from", *currentSts.Spec.Replicas, 
				"to", *masterSts.Spec.Replicas)

			// StatefulSet 업데이트
			err := UpdateStatefulSet(ctx, cl, currentSts, masterSts)
			if err != nil {
				return false, err
			}
			
			changed = true
		} else {
			// 일반 업데이트
			err := UpdateStatefulSet(ctx, cl, currentSts, masterSts)
			if err != nil {
				return false, err
			}
			changed = true
		}
	}

	return changed, nil
}

// UpdateStatefulSet 함수는 StatefulSet을 업데이트합니다
func UpdateStatefulSet(ctx context.Context, cl client.Client, currentSts, desiredSts *appsv1.StatefulSet) error {
	logger := log.FromContext(ctx)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// 최신 버전의 StatefulSet 가져오기
		latestSts := &appsv1.StatefulSet{}
		err := cl.Get(ctx, types.NamespacedName{
			Name:      currentSts.Name,
			Namespace: currentSts.Namespace,
		}, latestSts)
		if err != nil {
			return err
		}

		// 필요한 사양만 업데이트
		latestSts.Spec.Replicas = desiredSts.Spec.Replicas
		latestSts.Spec.Template = desiredSts.Spec.Template

		err = cl.Update(ctx, latestSts)
		return err
	})

	if err != nil {
		logger.Error(err, "Failed to update StatefulSet", "name", currentSts.Name)
		return err
	}

	logger.Info("Successfully updated StatefulSet", "name", currentSts.Name)
	return nil
}

// EqualStatefulSets 함수는 두 StatefulSet이 동일한지 확인합니다
func EqualStatefulSets(current, desired *appsv1.StatefulSet) bool {
	// 레플리카 수 비교
	if current.Spec.Replicas != nil && desired.Spec.Replicas != nil {
		if *current.Spec.Replicas != *desired.Spec.Replicas {
			return false
		}
	}
	
	// 컨테이너 이미지 비교
	if len(current.Spec.Template.Spec.Containers) > 0 && len(desired.Spec.Template.Spec.Containers) > 0 {
		if current.Spec.Template.Spec.Containers[0].Image != desired.Spec.Template.Spec.Containers[0].Image {
			return false
		}
	}
	
	return true
}