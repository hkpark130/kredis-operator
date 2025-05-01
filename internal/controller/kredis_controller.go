package controller

import (
	"context"
	"os/exec"
	"fmt"
	"strings"
	"os/exec"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry" // 내용: https://alenkacz.medium.com/kubernetes-operators-best-practices-understanding-conflict-errors-d05353dff421
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	stabilev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/internal/health"
	"github.com/hkpark130/kredis-operator/internal/monitoring"
	"github.com/hkpark130/kredis-operator/internal/reconcilers"
	"github.com/hkpark130/kredis-operator/internal/redis"
	"github.com/hkpark130/kredis-operator/internal/resources"
)

// KRedisReconciler reconciles a KRedis object
type KRedisReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *KRedisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 객체 존재 확인
	kredis := &stabilev1alpha1.KRedis{}
	err := r.Get(ctx, req.NamespacedName, kredis)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// 객체가 삭제됨
			log.Info("KRedis resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// 에러 발생
		log.Error(err, "Failed to get KRedis")
		return ctrl.Result{}, err
	}

	log.Info("Starting reconciliation for KRedis", "name", kredis.Name)

	// 여기에 기존 코드를 하위 모듈로 분리한 후 호출하는 부분만 남기고
	// 실제 구현은 각 패키지에서 진행할 예정입니다.

	log.Info("Reconciliation completed successfully", "name", kredis.Name)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KRedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&stabilev1alpha1.KRedis{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
