package controller

import (
	"context"
	"fmt"
	"strings"
	"os/exec"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
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

func (r *KRedisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// KRedis 객체 조회
	reqKRedis := &stablev1alpha1.KRedis{}
	if err := r.Client.Get(ctx, req.NamespacedName, reqKRedis); err != nil {
		if kerrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 최소 요구사항 검증
	if reqKRedis.Spec.Masters <= 0 || reqKRedis.Spec.Replicas <= 0 {
		return ctrl.Result{}, fmt.Errorf("invalid spec: Masters and Replicas must be greater than zero")
	}

	// Master StatefulSet 관리
	masterSts := r.statefulSetForMaster(reqKRedis)
	currentSts := &appsv1.StatefulSet{}
	err := r.Client.Get(ctx, client.ObjectKey{Name: masterSts.Name, Namespace: masterSts.Namespace}, currentSts)

	if err != nil && kerrors.IsNotFound(err) {
		// StatefulSet이 없으면 생성
		log.Info("Creating Master StatefulSet", "Namespace", reqKRedis.Namespace, "Name", masterSts.Name)
		if err = r.Client.Create(ctx, masterSts); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	} else {
		// StatefulSet이 존재하면 업데이트 검토
		if !equalStatefulSets(currentSts, masterSts) {
			// 스케일 다운 시나리오 체크
			if currentSts.Spec.Replicas != nil && masterSts.Spec.Replicas != nil &&
				*currentSts.Spec.Replicas > *masterSts.Spec.Replicas {
				
				// 데이터 마이그레이션 수행
				if err := r.handleDataMigration(ctx, reqKRedis, currentSts); err != nil {
					log.Error(err, "Failed to migrate data")
					return ctrl.Result{}, err
				}

				// 관련된 슬레이브 Deployment 삭제
				if err := r.deleteRelatedSlaves(ctx, reqKRedis, int(*masterSts.Spec.Replicas)); err != nil {
					log.Error(err, "Failed to delete related slaves")
					return ctrl.Result{}, err
				}
			}

			// StatefulSet 업데이트
			if err := r.updateStatefulSet(ctx, currentSts, masterSts); err != nil {
				log.Error(err, "Failed to update StatefulSet")
				return ctrl.Result{}, err
			}
		}
	}

	// Slave Deployments 관리
	if err := r.reconcileSlaveDeployments(ctx, reqKRedis); err != nil {
		return ctrl.Result{}, err
	}

	// Headless Service 생성/관리
	if err := r.reconcileHeadlessService(ctx, reqKRedis); err != nil {
		return ctrl.Result{}, err
	}

	// Client Service 생성/관리
	if err := r.reconcileClientService(ctx, reqKRedis); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *KRedisReconciler) handleDataMigration(ctx context.Context, cr *stablev1alpha1.KRedis, sts *appsv1.StatefulSet) error {
	log := log.FromContext(ctx)
	
	// 마지막 마스터 노드의 데이터 마이그레이션
	podName := fmt.Sprintf("%s-%d", sts.Name, *sts.Spec.Replicas-1)
	
	// redis-cli를 통한 리샤딩 명령 실행
	cmd := exec.Command("kubectl", "exec", podName, "-n", cr.Namespace, "--", 
		"redis-cli", "--cluster", "reshard", 
		fmt.Sprintf("%s:6379", podName),
		"--cluster-yes")
	
	if output, err := cmd.CombinedOutput(); err != nil {
		log.Error(err, "Redis reshard failed", "output", string(output))
		return fmt.Errorf("redis reshard failed: %v, output: %s", err, output)
	}
	
	return nil
}

func (r *KRedisReconciler) deleteRelatedSlaves(ctx context.Context, cr *stablev1alpha1.KRedis, masterCount int) error {
	log := log.FromContext(ctx)

	// masterCount 이후의 슬레이브 Deployment 삭제
	for i := masterCount; i < int(cr.Spec.Masters); i++ {
		slaveName := fmt.Sprintf("%s-slave-%d", cr.Name, i)
		slave := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      slaveName,
				Namespace: cr.Namespace,
			},
		}
		
		if err := r.Client.Delete(ctx, slave); err != nil && !kerrors.IsNotFound(err) {
			log.Error(err, "Failed to delete slave deployment", "name", slaveName)
			return err
		}
		log.Info("Successfully deleted slave deployment", "name", slaveName)
	}
	return nil
}

func (r *KRedisReconciler) statefulSetForMaster(cr *stablev1alpha1.KRedis) *appsv1.StatefulSet {
	labels := labelsForKRedis(cr.Name + "-master")
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
					ImagePullSecrets: []corev1.LocalObjectReference{{Name: "docker-secret"}},
					Containers: []corev1.Container{{
						Name:  "redis-master",
						Image: cr.Spec.Image,
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								"cpu":    parseResource(cr.Spec.Resource["limits"], "cpu", "1"),
								"memory": parseResource(cr.Spec.Resource["limits"], "memory", "1Gi"),
							},
							Requests: corev1.ResourceList{
								"cpu":    parseResource(cr.Spec.Resource["requests"], "cpu", "500m"),
								"memory": parseResource(cr.Spec.Resource["requests"], "memory", "512Mi"),
							},
						},
						Env: []corev1.EnvVar{
							{Name: "MASTER", Value: "true"},
							{Name: "REDIS_MAXMEMORY", Value: cr.Spec.Resource["limits"]["memory"]},
							{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							}},
						},
						Ports: []corev1.ContainerPort{{
							Name:          "redis",
							ContainerPort: cr.Spec.BasePort,
						}},
					}},
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 1,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"app":  "kredis",
												"role": "slave",
											},
										},
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (r *KRedisReconciler) reconcileHeadlessService(ctx context.Context, cr *stablev1alpha1.KRedis) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-master-headless",
			Namespace: cr.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labelsForKRedis(cr.Name + "-master"),
			Ports: []corev1.ServicePort{{
				Name:       "redis",
				Port:      cr.Spec.BasePort,
				TargetPort: intstr.FromInt(int(cr.Spec.BasePort)),
			}},
		},
	}

	// 서비스 생성 또는 업데이트
	if err := r.Client.Get(ctx, client.ObjectKey{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{}); err != nil {
		if kerrors.IsNotFound(err) {
			return r.Client.Create(ctx, svc)
		}
		return err
	}
	return r.Client.Update(ctx, svc)
}

func (r *KRedisReconciler) reconcileSlaveDeployments(ctx context.Context, cr *stablev1alpha1.KRedis) error {
	log := log.FromContext(ctx)

	// 현재 슬레이브 Deployment 목록 조회
	slaveDeployments := &appsv1.DeploymentList{}
	if err := r.Client.List(ctx, slaveDeployments, 
		client.InNamespace(cr.Namespace),
		client.MatchingLabels(map[string]string{"app": "kredis", "role": "slave"})); err != nil {
		return err
	}

	// 필요한 슬레이브 수 계산
	currentCount := len(slaveDeployments.Items)
	desiredCount := int(cr.Spec.Masters)

	// 슬레이브 Deployment 조정
	for i := 0; i < max(currentCount, desiredCount); i++ {
		if i >= desiredCount {
			// 불필요한 슬레이브 삭제
			if err := r.deleteRelatedSlaves(ctx, cr, desiredCount); err != nil {
				return err
			}
		} else {
			// 슬레이브 생성 또는 업데이트
			slaveDep := r.deploymentForSlave(cr, i)
			if err := r.Client.Get(ctx, client.ObjectKey{Name: slaveDep.Name, Namespace: slaveDep.Namespace}, &appsv1.Deployment{}); err != nil {
				if kerrors.IsNotFound(err) {
					if err := r.Client.Create(ctx, slaveDep); err != nil {
						return err
					}
				} else {
					return err
				}
			} else {
				if err := r.Client.Update(ctx, slaveDep); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// deploymentForSlave: 슬레이브 노드를 위한 Deployment 생성
func (r *KRedisReconciler) deploymentForSlave(cr *stablev1alpha1.KRedis, index int) *appsv1.Deployment {
	labels := labelsForKRedis(fmt.Sprintf("%s-slave-%d", cr.Name, index))
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
							Name: "docker-secret", // Secret 이름
						},
					},
					Containers: []corev1.Container{{
						Image: cr.Spec.Image,
						Name:  "redis-slave",
						Ports: []corev1.ContainerPort{{
							ContainerPort: cr.Spec.BasePort,
						}},
					}},
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
								{
									Weight: 1,
									PodAffinityTerm: corev1.PodAffinityTerm{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{ // Master와 Slave 구분
												"app":  "kredis",
												"role": "master", // Master와의 배치를 회피
											},
										},
										TopologyKey: "kubernetes.io/hostname", // 같은 노드에서 배치되지 않도록 지시
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func equalStatefulSets(current, desired *appsv1.StatefulSet) bool {
	return current.Spec.Replicas != nil &&
		desired.Spec.Replicas != nil &&
		*current.Spec.Replicas == *desired.Spec.Replicas
}

func parseResource(resourceMap map[string]string, key string, defaultValue string) resource.Quantity {
	value, ok := resourceMap[key]
	if !ok || value == "" {
		value = defaultValue
	}
	return resource.MustParse(value)
}

// labelsForKRedis returns the labels for selecting the resources
// belonging to the given kredis CR name.
func labelsForKRedis(name string) map[string]string {
	labels := map[string]string{
		"app":       "kredis",
		"kredis_cr": name,
	}
	if strings.Contains(name, "master") {
		labels["role"] = "master" // Master 라벨 추가
	} else {
		labels["role"] = "slave" // Slave 라벨 추가
	}
	return labels
}

func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

// getPodNames returns the pod names of the array of pods passed in
func getPodNames(pods []corev1.Pod) []string {
	var podNames []string
	for _, pod := range pods {
		podNames = append(podNames, pod.Name)
	}
	return podNames
}

func (r *KRedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&stablev1alpha1.KRedis{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
