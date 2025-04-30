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

package scaling

import (
	"context"
	"fmt"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/pkg/redis"
)

// HPAForRedisSlaves 함수는 Redis 슬레이브 노드를 위한 HPA를 생성합니다
func HPAForRedisSlaves(cr *stablev1alpha1.KRedis, index int) *autoscalingv2.HorizontalPodAutoscaler {
	targetName := fmt.Sprintf("%s-slave-%d", cr.Name, index)

	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-hpa", targetName),
			Namespace: cr.Namespace,
			Labels: map[string]string{
				"app":        cr.Name,
				"managed-by": "kredis-operator",
			},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       targetName,
			},
			MinReplicas: &cr.Spec.Replicas,
			MaxReplicas: maxReplicas(cr),
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "cpu",
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: int32Ptr(70),
						},
					},
				},
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "memory",
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: int32Ptr(80),
						},
					},
				},
			},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: int32Ptr(60),
					Policies: []autoscalingv2.HPAScalingPolicy{
						{
							Type:          autoscalingv2.PodsScalingPolicy,
							Value:         1,
							PeriodSeconds: 60,
						},
					},
				},
				ScaleDown: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: int32Ptr(300),
					Policies: []autoscalingv2.HPAScalingPolicy{
						{
							Type:          autoscalingv2.PodsScalingPolicy,
							Value:         1,
							PeriodSeconds: 120,
						},
					},
				},
			},
		},
	}
}

// ReconcileHPA 함수는 HPA 리소스를 조정합니다
func ReconcileHPA(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)

	for i := 0; i < int(cr.Spec.Masters); i++ {
		// 각 슬레이브 노드 그룹에 대한 HPA 생성
		hpa := HPAForRedisSlaves(cr, i)

		// 현재 HPA 조회
		found := &autoscalingv2.HorizontalPodAutoscaler{}
		err := cl.Get(ctx, types.NamespacedName{
			Name:      hpa.Name,
			Namespace: hpa.Namespace,
		}, found)

		if err != nil && errors.IsNotFound(err) {
			// HPA가 없으면 생성
			logger.Info("Creating HPA for Redis slave", "Name", hpa.Name)
			if err = cl.Create(ctx, hpa); err != nil {
				logger.Error(err, "Failed to create HPA for Redis slave")
				return err
			}
		} else if err != nil {
			logger.Error(err, "Failed to get HPA")
			return err
		} else {
			// HPA가 존재하면 업데이트
			logger.Info("Updating HPA for Redis slave", "Name", found.Name)

			found.Spec = hpa.Spec

			if err = cl.Update(ctx, found); err != nil {
				logger.Error(err, "Failed to update HPA")
				return err
			}
		}
	}

	return nil
}

// HandleScaleUp 함수는 스케일 업 시 필요한 작업을 수행합니다
func HandleScaleUp(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)
	logger.Info("Processing scale up event")

	// 현재 클러스터 노드 정보 가져오기
	allIPs, err := redis.GetNodeIPs(ctx, cl, cr)
	if err != nil {
		return err
	}

	// 클러스터에 새 노드 추가
	err = redis.AddNodesToCluster(ctx, cl, cr, allIPs)
	if err != nil {
		return err
	}

	return nil
}

// HandleScaleDown 함수는 스케일 다운 시 필요한 작업을 수행합니다
func HandleScaleDown(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis, oldReplicas int32) error {
	logger := log.FromContext(ctx)
	logger.Info("Processing scale down event", "from", oldReplicas, "to", cr.Spec.Masters)

	// 데이터 마이그레이션 수행
	if err := redis.HandleDataMigration(ctx, cr, cr.Spec.Masters); err != nil {
		return err
	}

	return nil
}

// helper functions
func int32Ptr(val int32) *int32 {
	return &val
}

func maxReplicas(cr *stablev1alpha1.KRedis) int32 {
	// 기본값으로 현재 레플리카 * 3 설정, 최소 10으로 제한
	max := cr.Spec.Replicas * 3
	if max < 10 {
		max = 10
	}
	return max
}