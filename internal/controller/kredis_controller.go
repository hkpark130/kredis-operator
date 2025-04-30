package controller

import (
	"context"
	"fmt"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"

	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/pkg/monitoring"
	"github.com/hkpark130/kredis-operator/pkg/redis"
	"github.com/hkpark130/kredis-operator/pkg/resources"
	"github.com/hkpark130/kredis-operator/pkg/scaling"
	"github.com/hkpark130/kredis-operator/pkg/utils"
)

type KRedisReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=*
// +kubebuilder:rbac:groups=core,resources=pods,verbs=*
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=*
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=*
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=*

func (r *KRedisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// KRedis 객체 조회
	kRedis := &stablev1alpha1.KRedis{}
	if err := r.Client.Get(ctx, req.NamespacedName, kRedis); err != nil {
		if kerrors.IsNotFound(err) {
			// 객체가 삭제되었을 때는 정상 처리
			log.Info("KRedis resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get KRedis")
		return ctrl.Result{}, err
	}

	// 최소 요구사항 검증
	if kRedis.Spec.Masters <= 0 || kRedis.Spec.Replicas <= 0 {
		log.Info("Invalid spec: Masters and Replicas must be greater than zero")
		return ctrl.Result{}, fmt.Errorf("invalid spec: Masters and Replicas must be greater than zero")
	}

	// 1. StatefulSet 관리 (Master 노드)
	stsChanged, err := resources.ReconcileStatefulSet(ctx, r.Client, kRedis)
	if err != nil {
		return ctrl.Result{}, err
	}

	// StatefulSet 생성 이후 바로 재조정 필요
	if stsChanged {
		log.Info("StatefulSet changed, requeueing")
		return ctrl.Result{Requeue: true}, nil
	}

	// 2. Deployment 관리 (Slave 노드)
	if err := resources.ReconcileSlaveDeployments(ctx, r.Client, kRedis); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Headless Service 관리
	if err := resources.ReconcileHeadlessService(ctx, r.Client, kRedis); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Client Service 관리
	if err := resources.ReconcileClientService(ctx, r.Client, kRedis); err != nil {
		return ctrl.Result{}, err
	}

	// 5. Redis Exporter 관리
	if err := monitoring.ReconcileExporter(ctx, r.Client, kRedis); err != nil {
		log.Error(err, "Failed to reconcile Redis Exporter")
		// Redis Exporter 오류는 심각한 오류로 간주하지 않음
	}

	// 6. HPA 관리
	if err := scaling.ReconcileHPA(ctx, r.Client, kRedis); err != nil {
		log.Error(err, "Failed to reconcile HPA")
		// HPA 오류는 심각한 오류로 간주하지 않음
	}

	// 7. Redis 클러스터 초기화 여부 확인 및 처리
	if !kRedis.Spec.ClusterInitialized {
		log.Info("Initializing Redis Cluster")

		// 모든 파드가 실행 중인지 확인
		masterCount := int(kRedis.Spec.Masters)
		slaveCount := int(kRedis.Spec.Masters * kRedis.Spec.Replicas)
		
		// Master 파드 대기
		if err := utils.WaitForPodsRunning(ctx, r.Client, kRedis.Namespace, 
			map[string]string{"app": "kredis", "role": "master"}, masterCount); err != nil {
			log.Error(err, "Failed to wait for master pods to be ready")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		
		// Slave 파드 대기
		if err := utils.WaitForPodsRunning(ctx, r.Client, kRedis.Namespace, 
			map[string]string{"app": "kredis", "role": "slave"}, slaveCount); err != nil {
			log.Error(err, "Failed to wait for slave pods to be ready")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// Master와 Slave 파드의 IP 주소 수집
		allIPs, err := redis.GetNodeIPs(ctx, r.Client, kRedis)
		if err != nil {
			log.Error(err, "Failed to get Redis node IPs")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// Redis 클러스터 생성
		if err := redis.CreateCluster(ctx, r.Client, kRedis, allIPs); err != nil {
			log.Error(err, "Failed to create Redis Cluster")
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		// clusterInitialized 필드 업데이트
		if err := redis.UpdateClusterInitialized(ctx, r.Client, kRedis); err != nil {
			log.Error(err, "Failed to update clusterInitialized field")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		log.Info("Redis Cluster initialized successfully")
		return ctrl.Result{Requeue: true}, nil
	}

	// 8. 클러스터 상태 모니터링
	status, err := redis.CheckClusterHealth(ctx, kRedis)
	if err != nil {
		log.Error(err, "Failed to check cluster health")
	} else {
		// 현재 시간을 ISO8601 형식으로 변환
		currentTime := time.Now().UTC().Format(time.RFC3339)
		
		// 상태 업데이트
		kredisStatus := &stablev1alpha1.KRedisStatus{
			ClusterStatus: "Healthy",
			ActiveNodes:   int32(status.ActiveMasters + status.ActiveReplicas),
			MasterNodes:   int32(status.ActiveMasters),
			SlaveNodes:    int32(status.ActiveReplicas),
			LastChecked:   currentTime,
		}

		if !status.IsHealthy {
			kredisStatus.ClusterStatus = "Unhealthy"
			
			// 실패한 노드 복구 시도
			for _, nodeID := range status.FailedNodes {
				if err := redis.RecoverFailedNode(ctx, kRedis, nodeID); err != nil {
					log.Error(err, "Failed to recover node", "nodeID", nodeID)
				}
			}
		}

		if err := utils.UpdateStatus(ctx, r.Client, kRedis, kredisStatus); err != nil {
			log.Error(err, "Failed to update KRedis status")
		}
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *KRedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&stablev1alpha1.KRedis{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Complete(r)
}
