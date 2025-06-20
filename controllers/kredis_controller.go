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

package controllers

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/controllers/resource"
)

// KredisReconciler reconciles a Kredis object
type KredisReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=cache.docker.direa.synology.me,resources=kredis/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets;deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods;services;configmaps;secrets;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *KredisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Kredis instance
	kredis := &cachev1alpha1.Kredis{}
	err := r.Get(ctx, req.NamespacedName, kredis)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Return and don't requeue
			logger.Info("Kredis resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		logger.Error(err, "Failed to get Kredis")
		return ctrl.Result{}, err
	}

	// Create a general service for the Redis cluster
	generalServiceName := kredis.Name
	foundGeneralService := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: generalServiceName, Namespace: kredis.Namespace}, foundGeneralService)
	if err != nil && errors.IsNotFound(err) {
		// Create general service for the Redis cluster
		generalSvc := resource.CreateGeneralService(kredis, r.Scheme)
		logger.Info("Creating Redis General Service", "Service.Namespace", generalSvc.Namespace, "Service.Name", generalSvc.Name)
		err = r.Create(ctx, generalSvc)
		if err != nil {
			logger.Error(err, "Failed to create General Service", "Service.Namespace", generalSvc.Namespace, "Service.Name", generalSvc.Name)
			return ctrl.Result{}, err
		}
	} else if err != nil {
		logger.Error(err, "Failed to get General Service")
		return ctrl.Result{}, err
	}

	// Check for and create single Redis Master StatefulSet
	totalReadyReplicas := int32(0)
	totalReplicas := int32(0)
	allReady := true

	masterName := fmt.Sprintf("%s-master", kredis.Name)
	foundMaster := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: masterName, Namespace: kredis.Namespace}, foundMaster)
	if err != nil && errors.IsNotFound(err) {
		// Define a new Master StatefulSet
		masterSts := resource.CreateMasterStatefulSet(kredis, r.Scheme)
		logger.Info("Creating Redis Master StatefulSet", "StatefulSet.Namespace", masterSts.Namespace, "StatefulSet.Name", masterSts.Name)
		err = r.Create(ctx, masterSts)
		if err != nil {
			logger.Error(err, "Failed to create Master StatefulSet", "StatefulSet.Namespace", masterSts.Namespace, "StatefulSet.Name", masterSts.Name)
			return ctrl.Result{}, err
		}

		// Create Master Service
		masterSvc := resource.CreateMasterService(kredis, r.Scheme)
		logger.Info("Creating Redis Master Service", "Service.Namespace", masterSvc.Namespace, "Service.Name", masterSvc.Name)
		err = r.Create(ctx, masterSvc)
		if err != nil {
			logger.Error(err, "Failed to create Master Service", "Service.Namespace", masterSvc.Namespace, "Service.Name", masterSvc.Name)
			return ctrl.Result{}, err
		}

		// Requeue to continue with the next step after creation
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		logger.Error(err, "Failed to get Master StatefulSet")
		return ctrl.Result{}, err
	}

	// 각 마스터 노드에 대한 슬레이브 StatefulSet 생성
	totalSlaveReadyReplicas := int32(0)
	totalSlaveReplicas := int32(0)

	// 마스터가 준비될 때까지 대기
	if foundMaster.Status.ReadyReplicas < 1 {
		logger.Info("마스터 파드가 준비될 때까지 대기 중...")
		return ctrl.Result{Requeue: true}, nil
	}

	// 마스터 파드 수 확인
	masterCount := int(kredis.Spec.Masters)
	logger.Info(fmt.Sprintf("마스터 노드 수: %d", masterCount))

	// 각 마스터 노드에 대한 슬레이브 StatefulSet 생성
	for i := 0; i < masterCount; i++ {
		// 해당 마스터 노드에 대한 슬레이브 StatefulSet 확인
		slaveName := fmt.Sprintf("%s-slave-%d", kredis.Name, i)
		foundSlave := &appsv1.StatefulSet{}
		err = r.Get(ctx, types.NamespacedName{Name: slaveName, Namespace: kredis.Namespace}, foundSlave)

		if err != nil && errors.IsNotFound(err) {
			// 슬레이브 StatefulSet 생성
			logger.Info(fmt.Sprintf("마스터 %d에 대한 슬레이브 StatefulSet 생성", i),
				"MasterIndex", i,
				"Replicas", kredis.Spec.Replicas)

			slaveSts, createErr := resource.CreateSlaveStatefulSetForMaster(kredis, r.Scheme, i)
			if createErr != nil {
				logger.Error(createErr, "슬레이브 StatefulSet 생성 오류", "MasterIndex", i)
				return ctrl.Result{}, createErr
			}

			logger.Info("Redis Slave StatefulSet 생성",
				"StatefulSet.Namespace", slaveSts.Namespace,
				"StatefulSet.Name", slaveSts.Name,
				"ExpectedPodNameFormat", fmt.Sprintf("%s-{0..%d}", slaveSts.Name, kredis.Spec.Replicas-1))

			err = r.Create(ctx, slaveSts)
			if err != nil {
				logger.Error(err, "Slave StatefulSet 생성 실패", "StatefulSet.Namespace", slaveSts.Namespace, "StatefulSet.Name", slaveSts.Name)
				return ctrl.Result{}, err
			}

			// 슬레이브 서비스 생성
			slaveSvc := resource.CreateSlaveServiceForMaster(kredis, r.Scheme, i)
			logger.Info("Redis Slave Service 생성", "Service.Namespace", slaveSvc.Namespace, "Service.Name", slaveSvc.Name)
			err = r.Create(ctx, slaveSvc)
			if err != nil {
				logger.Error(err, "Slave Service 생성 실패", "Service.Namespace", slaveSvc.Namespace, "Service.Name", slaveSvc.Name)
				return ctrl.Result{}, err
			}

			// 하나씩 생성 후 다음을 위해 리큐
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			logger.Error(err, "Slave StatefulSet 조회 실패", "SlaveName", slaveName)
			return ctrl.Result{}, err
		} else {
			// StatefulSet 발견, 통계 업데이트
			totalSlaveReplicas += foundSlave.Status.Replicas
			totalSlaveReadyReplicas += foundSlave.Status.ReadyReplicas
		}
	}

	// Update metrics with master and all slave metrics
	totalReplicas = foundMaster.Status.Replicas + totalSlaveReplicas
	totalReadyReplicas = foundMaster.Status.ReadyReplicas + totalSlaveReadyReplicas

	// Check if all replicas are ready
	if foundMaster.Status.ReadyReplicas != foundMaster.Status.Replicas ||
		totalSlaveReadyReplicas != totalSlaveReplicas {
		allReady = false
	}

	// 로그에 슬레이브 현황 기록
	logger.Info("슬레이브 현황",
		"총 슬레이브 수", totalSlaveReplicas,
		"준비된 슬레이브 수", totalSlaveReadyReplicas)

	// Update the Kredis status
	kredis.Status.Phase = "Running"
	kredis.Status.ReadyReplicas = totalReadyReplicas
	kredis.Status.Replicas = totalReplicas

	// Add conditions if all masters and slaves are ready
	if allReady {
		condition := metav1.Condition{
			Type:               "Available",
			Status:             metav1.ConditionTrue,
			Reason:             "AllReplicasReady",
			Message:            fmt.Sprintf("All Redis instances (%d master(s) and %d slave(s)) are ready", kredis.Spec.Masters, kredis.Spec.Replicas),
			LastTransitionTime: metav1.Now(),
		}
		kredis.Status.Conditions = []metav1.Condition{condition}
	} else {
		condition := metav1.Condition{
			Type:               "Progressing",
			Status:             metav1.ConditionTrue,
			Reason:             "NotAllReplicasReady",
			Message:            "Some Redis instances are not ready yet",
			LastTransitionTime: metav1.Now(),
		}
		kredis.Status.Conditions = []metav1.Condition{condition}
	}

	if err := r.Status().Update(ctx, kredis); err != nil {
		logger.Error(err, "Failed to update Kredis status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KredisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.Kredis{}).
		Owns(&appsv1.StatefulSet{}). // Watch StatefulSets owned by this controller
		Owns(&corev1.Service{}).     // Watch Services owned by this controller
		Complete(r)
}
