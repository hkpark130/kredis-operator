package controller

import (
	"context"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	// "k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	// "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	stabilev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/internal/redis"
	"github.com/hkpark130/kredis-operator/internal/resources"
	"github.com/hkpark130/kredis-operator/internal/utils"
)

// KRedisReconciler는 KRedis 커스텀 리소스를 조정하는 컨트롤러입니다.
// 이 컨트롤러는 쿠버네티스 API 서버에서 KRedis 리소스의 변경 사항을 감시하고,
// 실제 클러스터 상태를 원하는 상태로 조정합니다.
type KRedisReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete

// Reconcile는 쿠버네티스 클러스터의 상태를 KRedis 리소스에 정의된 희망하는 상태에 가깝게 조정합니다.
// 이 함수는 KRedis 리소스가 생성, 수정, 삭제될 때마다 호출됩니다.
//
// 주요 조정 프로세스:
// 1. 헤드리스 서비스 생성/업데이트 (클러스터 내부 통신용)
// 2. 마스터 노드 StatefulSet 관리
// 3. 슬레이브 노드 StatefulSet 관리
// 4. 클라이언트 서비스 생성/업데이트
// 5. Redis 클러스터 구성 오케스트레이션
//
// req: 요청 객체 (조정 대상 리소스의 네임스페이스와 이름 포함)
// return: 조정 결과와 오류 (있는 경우)
func (r *KRedisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// KRedis 리소스 조회
	kredis := &stabilev1alpha1.KRedis{}
	err := r.Get(ctx, req.NamespacedName, kredis)
	if err != nil {
		if kerrors.IsNotFound(err) {
			// 객체가 삭제됨 - 추가 조치 필요 없음
			log.Info("KRedis resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// 다른 오류 발생
		log.Error(err, "Failed to get KRedis")
		return ctrl.Result{}, err
	}

	log.Info("Starting reconciliation for KRedis", "name", kredis.Name)

	// 1. 헤드리스 서비스 생성/업데이트 (클러스터 내부 통신용)
	if err := r.ensureHeadlessService(ctx, kredis); err != nil {
		log.Error(err, "Failed to ensure headless service")
		return ctrl.Result{}, err
	}

	// 2. 마스터 노드 StatefulSet 관리
	// TODO: reconcileMasters 구현
	if err := r.reconcileMasters(ctx, kredis); err != nil {
		log.Error(err, "Failed to reconcile master nodes")
		return ctrl.Result{}, err
	}

	// 3. 슬레이브 노드 StatefulSet 관리
	// TODO: reconcileSlaves 구현
	if err := r.reconcileSlaves(ctx, kredis); err != nil {
		log.Error(err, "Failed to reconcile slave nodes")
		return ctrl.Result{}, err
	}

	// 4. 클라이언트 서비스 생성/업데이트
	// TODO: ensureClientService 구현
	if err := r.ensureClientService(ctx, kredis); err != nil {
		log.Error(err, "Failed to ensure client service")
		return ctrl.Result{}, err
	}

	// 5. Redis 클러스터 구성 오케스트레이션
	// TODO: orchestrateCluster 구현
	if err := r.orchestrateCluster(ctx, kredis); err != nil {
		log.Error(err, "Failed to orchestrate Redis cluster")
		return ctrl.Result{}, err
	}

	log.Info("Reconciliation completed successfully", "name", kredis.Name)
	return ctrl.Result{}, nil
}

// ensureHeadlessService는 Redis 클러스터 내부 통신을 위한 헤드리스 서비스를 생성하거나 업데이트합니다.
// 헤드리스 서비스는 StatefulSet Pod 간 안정적인 네트워크 통신을 제공합니다.
func (r *KRedisReconciler) ensureHeadlessService(ctx context.Context, cr *stabilev1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 헤드리스 서비스 객체 생성
	headlessService := resources.CreateHeadlessService(cr)

	// 기존 서비스가 있는지 확인
	found := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: headlessService.Name, Namespace: headlessService.Namespace}, found)

	if err != nil {
		if kerrors.IsNotFound(err) {
			// 서비스가 없으면 새로 생성
			log.Info("Creating headless service", "name", headlessService.Name)
			return r.Create(ctx, headlessService)
		}
		return err
	}

	// 서비스가 존재하면 필요에 따라 업데이트
	needsUpdate := false

	// 포트 설정이 다른지 확인 (개수 및 설정)
	if len(found.Spec.Ports) != len(headlessService.Spec.Ports) {
		needsUpdate = true
	} else {
		// 포트 설정 상세 비교
		for i, port := range headlessService.Spec.Ports {
			if found.Spec.Ports[i].Port != port.Port ||
				found.Spec.Ports[i].TargetPort != port.TargetPort ||
				found.Spec.Ports[i].Name != port.Name {
				needsUpdate = true
				break
			}
		}
	}

	// 셀렉터가 다른지 확인 (reflect.DeepEqual 활용)
	if !reflect.DeepEqual(found.Spec.Selector, headlessService.Spec.Selector) {
		needsUpdate = true
	}

	// ClusterIP가 None인지 확인 (헤드리스 서비스 필수 설정)
	if found.Spec.ClusterIP != "None" {
		needsUpdate = true
	}

	// PublishNotReadyAddresses 설정이 다른지 확인 (아직 준비되지 않은 Pod도 DNS에 등록해줌)
	if found.Spec.PublishNotReadyAddresses != headlessService.Spec.PublishNotReadyAddresses {
		needsUpdate = true
	}

	if needsUpdate {
		log.Info("Updating headless service", "name", found.Name)

		// 업데이트할 필드 복사
		found.Spec.Ports = headlessService.Spec.Ports
		found.Spec.Selector = headlessService.Spec.Selector
		found.Spec.ClusterIP = "None"
		found.Spec.PublishNotReadyAddresses = true

		return r.Update(ctx, found)
	}

	log.Info("Headless service already exists and up-to-date", "name", headlessService.Name)
	return nil
}

// reconcileMasters는 KRedis 리소스에 정의된 수의 마스터 노드를 관리합니다.
// StatefulSet을 사용하여 마스터 노드를 생성, 업데이트 또는 삭제합니다.
func (r *KRedisReconciler) reconcileMasters(ctx context.Context, cr *stabilev1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 마스터 StatefulSet 객체 생성
	masterSts := resources.CreateMasterStatefulSet(cr)

	// StatefulSet이 이미 존재하는지 확인
	found := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: masterSts.Name, Namespace: masterSts.Namespace}, found)

	if err != nil {
		if kerrors.IsNotFound(err) {
			// StatefulSet이 없으면 새로 생성
			log.Info("Creating master StatefulSet", "name", masterSts.Name)
			return r.Create(ctx, masterSts)
		}
		return err
	}

	// StatefulSet이 존재하면 필요에 따라 업데이트
	needsUpdate := false

	// 레플리카 수가 다른지 확인
	// 스케일 다운의 경우 Redis 클러스터에서 안전하게 노드를 제거해야 함
	if *found.Spec.Replicas != int32(cr.Spec.Masters) {
		// 스케일 다운(마스터 수 감소)인 경우 클러스터 오케스트레이션에서 처리
		if *found.Spec.Replicas > int32(cr.Spec.Masters) {
			log.Info("Master scale down detected - StatefulSet will be updated after cluster orchestration",
				"name", found.Name,
				"current", *found.Spec.Replicas,
				"desired", cr.Spec.Masters)

			// StatefulSet 업데이트는 orchestrateCluster에서 Redis 클러스터 오케스트레이션이
			// 완료된 후에 개별적으로 진행됩니다.
			// 따라서 여기서는 업데이트하지 않습니다.

			// orchestrateCluster에서 마스터 노드 제거 후 StatefulSet 업데이트를 위해
			// 별도 처리가 필요 없음 (개별 마스터 노드의 스케일 다운은 아래 케이스에서 처리)

			// 하지만 스케일 다운 작업이 진행 중인지 확인하기 위한 어노테이션을 설정
			if found.Annotations == nil {
				found.Annotations = make(map[string]string)
			}
			found.Annotations["redis.kredis-operator/scale-down-in-progress"] = "true"
			needsUpdate = true
		} else {
			// 스케일 업(마스터 수 증가)이거나 어노테이션에 스케일 다운 완료 표시가 있는 경우
			// 바로 StatefulSet 업데이트 진행
			scaleDownComplete := false
			if found.Annotations != nil {
				_, inProgress := found.Annotations["redis.kredis-operator/scale-down-in-progress"]
				complete, hasComplete := found.Annotations["redis.kredis-operator/scale-down-complete"]

				if !inProgress && hasComplete && complete == "true" {
					scaleDownComplete = true
					// 완료 표시를 제거
					delete(found.Annotations, "redis.kredis-operator/scale-down-complete")
					needsUpdate = true
				}
			}

			if *found.Spec.Replicas < int32(cr.Spec.Masters) || scaleDownComplete {
				log.Info("Updating master StatefulSet replicas",
					"name", found.Name,
					"current", *found.Spec.Replicas,
					"desired", cr.Spec.Masters)
				*found.Spec.Replicas = int32(cr.Spec.Masters)
				needsUpdate = true
			}
		}
	}

	// 이미지가 변경되었는지 확인
	if found.Spec.Template.Spec.Containers[0].Image != cr.Spec.Image {
		log.Info("Updating master StatefulSet image",
			"name", found.Name,
			"current", found.Spec.Template.Spec.Containers[0].Image,
			"desired", cr.Spec.Image)
		found.Spec.Template.Spec.Containers[0].Image = cr.Spec.Image
		needsUpdate = true
	}

	// 환경 변수 업데이트 - utils.MergeEnvVars 사용
	mergedEnvs, envChanged := utils.MergeEnvVars(
		found.Spec.Template.Spec.Containers[0].Env,
		masterSts.Spec.Template.Spec.Containers[0].Env)

	if envChanged {
		log.Info("Environment variables changed", "name", found.Name)
		found.Spec.Template.Spec.Containers[0].Env = mergedEnvs
		needsUpdate = true
	}

	// 볼륨 마운트 확인 - Redis 설정 파일이 마운트되었는지 확인 (utils.HasVolumeMount 사용)
	configMountPath := "/etc/redis"
	configMountName := "redis-config"
	if !utils.HasVolumeMount(found.Spec.Template.Spec.Containers[0].VolumeMounts, configMountName, configMountPath) {
		log.Info("Adding Redis config volume mount", "name", found.Name)
		found.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			found.Spec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      configMountName,
				MountPath: configMountPath,
			})
		needsUpdate = true
	}

	// ConfigMap 볼륨이 추가되었는지 확인 (utils.HasVolume 사용)
	if !utils.HasVolume(found.Spec.Template.Spec.Volumes, configMountName) {
		log.Info("Adding Redis config volume", "name", found.Name)
		found.Spec.Template.Spec.Volumes = append(
			found.Spec.Template.Spec.Volumes,
			corev1.Volume{
				Name: configMountName,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "kredis-operator-redis-config",
						},
					},
				},
			})
		needsUpdate = true
	}

	// 명령어 변경 확인 (utils.CompareCommandSimple 사용)
	expectedCommand := "sh /redis-writable/start-master.sh"
	if !utils.CompareCommandSimple(expectedCommand, found.Spec.Template.Spec.Containers[0].Command) {
		log.Info("Updating master StatefulSet Redis command", "name", found.Name)
		found.Spec.Template.Spec.Containers[0].Command = []string{"sh", "/redis-writable/start-master.sh"}
		needsUpdate = true
	}

	// 리소스 요청/제한이 변경되었는지 확인 - utils.UpdateContainerResources 활용
	if cr.Spec.Resource != nil {
		if utils.UpdateContainerResources(&found.Spec.Template.Spec.Containers[0], &cr.Spec.Resource, found.Name) {
			needsUpdate = true
		}
	}

	// 필요한 경우 StatefulSet 업데이트
	if needsUpdate {
		log.Info("Updating master StatefulSet", "name", found.Name)
		err = r.Update(ctx, found)
		if err != nil {
			log.Error(err, "Failed to update master StatefulSet", "name", found.Name)
			return err
		}
	} else {
		log.Info("Master StatefulSet already exists and up-to-date", "name", found.Name)
	}

	return nil
}

// reconcileSlaves는 각 마스터에 대해 지정된 수의 슬레이브 노드를 관리합니다.
// StatefulSet을 사용하여 슬레이브 노드를 생성, 업데이트 또는 삭제합니다.
func (r *KRedisReconciler) reconcileSlaves(ctx context.Context, cr *stabilev1alpha1.KRedis) error {
	log := log.FromContext(ctx)
	clusterManager := redis.NewClusterManager(r.Client)

	// 마스터 수만큼 순회하며 각 마스터에 대한 슬레이브 StatefulSet을 처리
	for masterIdx := int(0); masterIdx < cr.Spec.Masters; masterIdx++ {
		// 마스터에 대한 슬레이브 StatefulSet 생성
		slaveSts := resources.CreateSlaveStatefulSet(cr, masterIdx)

		// StatefulSet이 이미 존재하는지 확인
		found := &appsv1.StatefulSet{}
		err := r.Get(ctx, types.NamespacedName{Name: slaveSts.Name, Namespace: slaveSts.Namespace}, found)

		if err != nil {
			if kerrors.IsNotFound(err) {
				// StatefulSet이 없으면 새로 생성
				log.Info("Creating slave StatefulSet", "name", slaveSts.Name, "for master", masterIdx)
				err = r.Create(ctx, slaveSts)
				if err != nil {
					return err
				}
				continue
			}
			return err
		}

		// StatefulSet이 존재하면 필요에 따라 업데이트
		needsUpdate := false

		// 레플리카 수 변경 감지 및 처리 (스케일 업/다운)
		if *found.Spec.Replicas != int32(cr.Spec.Replicas) {
			// 스케일 다운(슬레이브 수 감소) 케이스 처리
			if *found.Spec.Replicas > int32(cr.Spec.Replicas) {
				log.Info("Slave scale down detected",
					"name", found.Name,
					"current", *found.Spec.Replicas,
					"desired", cr.Spec.Replicas)

				// 슬레이브 스케일 다운 상태 확인
				scaleStatus, err := clusterManager.GetSlaveScaleStatus(ctx, cr, masterIdx)
				if err != nil {
					log.Error(err, "Failed to get slave scale status", "masterIndex", masterIdx)
					return err
				}

				// 진행 중이면 완료 여부 확인
				if scaleStatus.InProgress {
					if scaleStatus.Completed {
						log.Info("Slave scale down completed, updating StatefulSet", "name", found.Name)
						*found.Spec.Replicas = int32(cr.Spec.Replicas)
						needsUpdate = true
						
						// 상태 초기화
						scaleStatus.InProgress = false
						scaleStatus.Completed = false
						if err := clusterManager.SetSlaveScaleStatus(ctx, cr, masterIdx, scaleStatus); err != nil {
							log.Error(err, "Failed to reset slave scale status", "masterIndex", masterIdx)
							return err
						}
					} else {
						log.Info("Slave scale down in progress", "name", found.Name, "message", scaleStatus.Message)
					}
				} else {
					// 아직 시작하지 않았으면 스케일 다운 시작
					log.Info("Starting slave scale down process", "name", found.Name)
					
					// 스케일 다운 상태 설정
					scaleStatus.InProgress = true
					scaleStatus.Message = "Scale down initiated"
					if err := clusterManager.SetSlaveScaleStatus(ctx, cr, masterIdx, scaleStatus); err != nil {
						log.Error(err, "Failed to set slave scale status", "masterIndex", masterIdx)
						return err
					}
					
					// 이 시점에서는 StatefulSet을 업데이트하지 않고 다음 조정에서 처리
				}
			} else {
				// 스케일 업(슬레이브 수 증가)인 경우 바로 업데이트
				log.Info("Updating slave StatefulSet replicas (scale up)",
					"name", found.Name,
					"current", *found.Spec.Replicas,
					"desired", cr.Spec.Replicas)
				*found.Spec.Replicas = int32(cr.Spec.Replicas)
				needsUpdate = true
			}
		}

		// 이미지가 변경되었는지 확인
		if found.Spec.Template.Spec.Containers[0].Image != cr.Spec.Image {
			log.Info("Updating slave StatefulSet image",
				"name", found.Name,
				"current", found.Spec.Template.Spec.Containers[0].Image,
				"desired", cr.Spec.Image)
			found.Spec.Template.Spec.Containers[0].Image = cr.Spec.Image
			needsUpdate = true
		}

		// 환경 변수 업데이트 - utils.MergeEnvVars 사용
		mergedEnvs, envChanged := utils.MergeEnvVars(
			found.Spec.Template.Spec.Containers[0].Env,
			slaveSts.Spec.Template.Spec.Containers[0].Env)

		if envChanged {
			log.Info("Environment variables changed", "name", found.Name)
			found.Spec.Template.Spec.Containers[0].Env = mergedEnvs
			needsUpdate = true
		}

		// 볼륨 마운트 확인 - Redis 설정 파일이 마운트되었는지 확인 (utils.HasVolumeMount 사용)
		configMountPath := "/etc/redis"
		configMountName := "redis-config"
		if !utils.HasVolumeMount(found.Spec.Template.Spec.Containers[0].VolumeMounts, configMountName, configMountPath) {
			log.Info("Adding Redis config volume mount", "name", found.Name)
			found.Spec.Template.Spec.Containers[0].VolumeMounts = append(
				found.Spec.Template.Spec.Containers[0].VolumeMounts,
				corev1.VolumeMount{
					Name:      configMountName,
					MountPath: configMountPath,
				})
			needsUpdate = true
		}

		// ConfigMap 볼륨이 추가되었는지 확인 (utils.HasVolume 사용)
		if !utils.HasVolume(found.Spec.Template.Spec.Volumes, configMountName) {
			log.Info("Adding Redis config volume", "name", found.Name)
			found.Spec.Template.Spec.Volumes = append(
				found.Spec.Template.Spec.Volumes,
				corev1.Volume{
					Name: configMountName,
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "kredis-operator-redis-config",
							},
						},
					},
				})
			needsUpdate = true
		}

		// 명령어 변경 확인 (utils.CompareCommandSimple 사용)
		expectedCommand := "sh /redis-writable/start-slave.sh"
		if !utils.CompareCommandSimple(expectedCommand, found.Spec.Template.Spec.Containers[0].Command) {
			log.Info("Updating slave StatefulSet Redis command", "name", found.Name)
			found.Spec.Template.Spec.Containers[0].Command = []string{"sh", "/redis-writable/start-slave.sh"}
			needsUpdate = true
		}

		// 리소스 요청/제한이 변경되었는지 확인 - utils.UpdateContainerResources 활용
		if cr.Spec.Resource != nil {
			if utils.UpdateContainerResources(&found.Spec.Template.Spec.Containers[0], &cr.Spec.Resource, found.Name) {
				needsUpdate = true
			}
		}

		// 필요한 경우 StatefulSet 업데이트
		if needsUpdate {
			log.Info("Updating slave StatefulSet", "name", found.Name)
			err = r.Update(ctx, found)
			if err != nil {
				log.Error(err, "Failed to update slave StatefulSet", "name", found.Name)
				return err
			}
		} else {
			log.Info("Slave StatefulSet already exists and up-to-date", "name", found.Name, "for master", masterIdx)
		}
	}

	return nil
}

// ensureClientService는 Redis 클러스터에 접근하기 위한 클라이언트 서비스를 생성하거나 업데이트합니다.
// 이 서비스는 애플리케이션에서 Redis 클러스터에 접속하는 엔트리포인트 역할을 합니다.
func (r *KRedisReconciler) ensureClientService(ctx context.Context, cr *stabilev1alpha1.KRedis) error {
	// TODO: 클라이언트 서비스 생성 또는 업데이트 로직 구현
	// 1. 서비스 생성 또는 기존 서비스 조회
	// 2. 필요시 서비스 업데이트
	return nil
}

// orchestrateCluster는 Redis 클러스터 구성을 관리합니다.
// 클러스터 초기화, 슬롯 재분배, 노드 추가/제거 등의 작업을 수행합니다.
func (r *KRedisReconciler) orchestrateCluster(ctx context.Context, cr *stabilev1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// Redis 클러스터 매니저 인스턴스 생성
	clusterManager := redis.NewClusterManager(r.Client)

	// 클러스터 오케스트레이션을 ClusterManager에게 위임
	if err := clusterManager.OrchestrateCluster(ctx, cr); err != nil {
		log.Error(err, "Failed to orchestrate Redis cluster")
		return err
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KRedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&stabilev1alpha1.KRedis{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
