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

package utils

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// WaitForPodsRunning 함수는 특정 레이블이 있는 모든 파드가 실행 중이 될 때까지 대기합니다
func WaitForPodsRunning(ctx context.Context, cl client.Client, namespace string, labels map[string]string, expectedCount int) error {
	logger := log.FromContext(ctx)
	
	// 타임아웃 설정 (5분)
	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for pods to be running")
		case <-ticker.C:
			// 파드 목록 조회
			podList := &corev1.PodList{}
			selector := &metav1.LabelSelector{MatchLabels: labels}
			labelSelector, err := metav1.LabelSelectorAsSelector(selector)
			if err != nil {
				logger.Error(err, "Failed to convert label selector")
				continue
			}

			listOps := &client.ListOptions{
				Namespace:     namespace,
				LabelSelector: labelSelector,
			}

			if err := cl.List(ctx, podList, listOps); err != nil {
				logger.Error(err, "Failed to list pods")
				continue
			}

			// 실행 중인 파드 수 확인
			runningCount := 0
			for _, pod := range podList.Items {
				if pod.Status.Phase == corev1.PodRunning {
					runningCount++
				}
			}

			logger.Info("Waiting for pods to be running", 
				"running", runningCount, 
				"expected", expectedCount)

			// 모든 파드가 실행 중이면 반환
			if runningCount == expectedCount {
				return nil
			}
		}
	}
}

// UpdateStatus 함수는 KRedis CR의 상태 필드를 업데이트합니다
func UpdateStatus(ctx context.Context, cl client.Client, cr *stablev1alpha1.KRedis, status *stablev1alpha1.KRedisStatus) error {
	logger := log.FromContext(ctx)
	
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &stablev1alpha1.KRedis{}
		err := cl.Get(ctx, types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, latest)
		if err != nil {
			return err
		}
		
		latest.Status = *status
		return cl.Status().Update(ctx, latest)
	})
	
	if err != nil {
		logger.Error(err, "Failed to update KRedis status")
		return err
	}
	
	return nil
}

// PrintValidationErrors 함수는 검증 오류를 로그에 출력합니다
func PrintValidationErrors(ctx context.Context, errors []string) {
	logger := log.FromContext(ctx)
	
	for _, err := range errors {
		logger.Info("Validation error", "error", err)
	}
}

// GetRandomStringWithLength 함수는 지정된 길이의 무작위 문자열을 생성합니다
func GetRandomStringWithLength(length int) string {
	// 암호에 사용 가능한 문자 집합
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-_=+[]{}|;:,.<>?"
	
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
		// 호출 사이에 약간의 시간 간격이 필요
		time.Sleep(1 * time.Nanosecond)
	}
	
	return string(b)
}

// BuildLabelsForComponent 함수는 컴포넌트에 대한 레이블을 생성합니다
func BuildLabelsForComponent(cr *stablev1alpha1.KRedis, component string) map[string]string {
	return map[string]string{
		"app":           "kredis",
		"component":     component,
		"instance":      cr.Name,
		"managed-by":    "kredis-operator",
	}
}

// BuildLabelsForService 함수는 서비스에 대한 레이블을 생성합니다
func BuildLabelsForService(cr *stablev1alpha1.KRedis, component string) map[string]string {
	return map[string]string{
		"app":       "kredis",
		"component": component,
		"instance":  cr.Name,
	}
}