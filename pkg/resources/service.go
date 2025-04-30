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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// HeadlessServiceForMaster 함수는 Redis 마스터 노드를 위한 Headless 서비스를 생성합니다
func HeadlessServiceForMaster(cr *stablev1alpha1.KRedis) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-master-headless",
			Namespace: cr.Namespace,
			Labels: map[string]string{
				"app": cr.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  LabelsForKRedis(cr.Name + "-master", "master"),
			Ports: []corev1.ServicePort{{
				Name:       "redis",
				Port:       cr.Spec.BasePort,
				TargetPort: intstr.FromInt(int(cr.Spec.BasePort)),
			}},
		},
	}
}

// ClientServiceForKRedis 함수는 Redis 클러스터 접근을 위한 클라이언트 서비스를 생성합니다
func ClientServiceForKRedis(cr *stablev1alpha1.KRedis) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-client",
			Namespace: cr.Namespace,
			Labels: map[string]string{
				"app": cr.Name,
			},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "redis",
					Port:       6379,
					TargetPort: intstr.FromInt(int(cr.Spec.BasePort)),
				},
			},
			Selector: map[string]string{
				"app":  "kredis",
				"role": "master",
			},
		},
	}
}

// ReconcileHeadlessService 함수는 Headless 서비스를 조정합니다
func ReconcileHeadlessService(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)
	
	// Headless 서비스 생성
	svc := HeadlessServiceForMaster(cr)

	// 현재 서비스 조회
	found := &corev1.Service{}
	err := cl.Get(ctx, types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, found)

	if err != nil && errors.IsNotFound(err) {
		// 서비스가 없으면 생성
		logger.Info("Creating headless service", "Name", svc.Name)
		if err = cl.Create(ctx, svc); err != nil {
			logger.Error(err, "Failed to create headless service")
			return err
		}
	} else if err != nil {
		logger.Error(err, "Failed to get headless service")
		return err
	} else {
		// 서비스가 존재하면 업데이트
		found.Spec.Ports = svc.Spec.Ports
		found.Spec.Selector = svc.Spec.Selector

		if err = cl.Update(ctx, found); err != nil {
			logger.Error(err, "Failed to update headless service")
			return err
		}
	}

	return nil
}

// ReconcileClientService 함수는 클라이언트 서비스를 조정합니다
func ReconcileClientService(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis) error {
	logger := log.FromContext(ctx)
	
	// 클라이언트 서비스 생성
	svc := ClientServiceForKRedis(cr)

	// 현재 서비스 조회
	found := &corev1.Service{}
	err := cl.Get(ctx, types.NamespacedName{
		Name:      svc.Name,
		Namespace: svc.Namespace,
	}, found)

	if err != nil && errors.IsNotFound(err) {
		// 서비스가 없으면 생성
		logger.Info("Creating client service", "Name", svc.Name)
		if err = cl.Create(ctx, svc); err != nil {
			logger.Error(err, "Failed to create client service")
			return err
		}
	} else if err != nil {
		logger.Error(err, "Failed to get client service")
		return err
	} else {
		// 서비스가 존재하면 업데이트
		found.Spec.Ports = svc.Spec.Ports
		found.Spec.Selector = svc.Spec.Selector

		if err = cl.Update(ctx, found); err != nil {
			logger.Error(err, "Failed to update client service")
			return err
		}
	}

	return nil
}