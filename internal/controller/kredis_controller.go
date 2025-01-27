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

package controller

import (
	"context"
	"fmt"
	"strings"
	// "time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	kerrors "k8s.io/apimachinery/pkg/api/errors"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry" // 내용: https://alenkacz.medium.com/kubernetes-operators-best-practices-understanding-conflict-errors-d05353dff421
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	stablev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// KRedisReconciler reconciles a KRedis object
type KRedisReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=stable.docker.direa.synology.me,resources=kredis/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=*
// +kubebuilder:rbac:groups=core,resources=pods,verbs=*
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=*

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the KRedis object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *KRedisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. KRedis 객체를 가져옴
	reqKRedis := &stablev1alpha1.KRedis{}
	err := r.Client.Get(ctx, req.NamespacedName, reqKRedis)
	if err != nil {
		if kerrors.IsNotFound(err) {
			log.Info("KRedis resource not found.")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get KRedis resource.")
		return ctrl.Result{}, err
	}

	// 1-1. Masters, Replicas 는 최소 1개 이상!
	if reqKRedis.Spec.Masters <= 0 || reqKRedis.Spec.Replicas <= 0 {
		err := fmt.Errorf("invalid spec: Masters and Replicas must be greater than zero")
		log.Error(err, "Invalid KRedis spec.")
		return ctrl.Result{}, err
	}

	// 2. Redis Master Deployment 생성
	masterDep := r.deploymentForMaster(reqKRedis)
	masterDeployment := &appsv1.Deployment{}
	err = r.Client.Get(ctx, client.ObjectKey{Name: masterDep.Name, Namespace: masterDep.Namespace}, masterDeployment)
	if err != nil && kerrors.IsNotFound(err) {
		log.Info("Creating Master Deployment.", "Namespace", reqKRedis.Namespace, "Name", masterDep.Name)
		err = r.Client.Create(ctx, masterDep)
		if err != nil {
			log.Error(err, "Failed to create Master Deployment.")
			return ctrl.Result{}, err
		}
	} else if err != nil {
		log.Error(err, "Failed to get Master Deployment.")
		return ctrl.Result{}, err
	} else {
		// Deployment가 이미 존재하면 스펙 업데이트
		log.Info("Updating Master Deployment if necessary.", "Namespace", reqKRedis.Namespace, "Name", masterDep.Name)
		if !equalDeployments(masterDeployment, masterDep) {
			// 기존 Deployment와 CRD에서 정의한 Deployment를 비교하여 다를 경우 업데이트
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				// 현재 Deployment를 가져오기 위한 포인터 타입 객체 생성
				currentMasterDeployment := &appsv1.Deployment{}

				// 리소스 가져오기
				err := r.Client.Get(ctx, types.NamespacedName{
					Name:      masterDeployment.Name,
					Namespace: masterDeployment.Namespace,
				}, currentMasterDeployment)
				if err != nil {
					return err
				}
				currentMasterDeployment.Spec = masterDep.Spec

				err = r.Client.Update(ctx, currentMasterDeployment)
				return err
			})

			if err != nil {
				log.Error(err, "Failed to update Master Deployment.")
				return ctrl.Result{}, fmt.Errorf("failed to update Master Deployment: %w", err)
			}
		}
	}

	// 3. Redis Slave Deployments 생성 (마스터 개수만큼 슬레이브 그룹 생성)
	slaveDeployments := &appsv1.DeploymentList{}
	opts := []client.ListOption{
		client.InNamespace(reqKRedis.Namespace),
		client.MatchingLabels(map[string]string{"app": "kredis", "role": "slave"}),
	}
	
	err = r.Client.List(ctx, slaveDeployments, opts...)
	if err != nil {
		log.Error(err, "Failed to list slave deployments")
		return err
	}

	currentSlaveCount := len(slaveDeployments.Items)
	desiredSlaveCount := int(reqKRedis.Spec.Masters)
	log.Info("Slave counts: ", "currentSlaveCount", currentSlaveCount, "desiredSlaveCount", desiredSlaveCount)

	for i := 0; i < max(currentSlaveCount, desiredSlaveCount); i++ {
		slaveDepName := fmt.Sprintf("%s-slave-%d", reqKRedis.Name, i)

		if i >= desiredSlaveCount {
			// 삭제해야 할 슬레이브 처리
			for _, slaveDep := range slaveDeployments.Items {
				if slaveDep.Name == slaveDepName {
					log.Info("Deleting unnecessary Slave Deployment", "Namespace", slaveDep.Namespace, "Name", slaveDep.Name)
					if err := r.Client.Delete(ctx, &slaveDep); err != nil {
						log.Error(err, "Failed to delete Slave Deployment", "Namespace", slaveDep.Namespace, "Name", slaveDep.Name)
					}
				}
			}
		} else if i >= currentSlaveCount {
			// 생성해야 할 슬레이브 처리: 존재 확인 후 생성
			slaveDep := r.deploymentForSlave(reqKRedis, i)
			existingDep := &appsv1.Deployment{}
			err := r.Client.Get(ctx, client.ObjectKey{Name: slaveDepName, Namespace: reqKRedis.Namespace}, existingDep)
			if err != nil {
				if kerrors.IsNotFound(err) {
					log.Info("Creating missing Slave Deployment", "Namespace", reqKRedis.Namespace, "Name", slaveDepName)
					if createErr := r.Client.Create(ctx, slaveDep); createErr != nil {
						log.Error(createErr, "Failed to create Slave Deployment", "Namespace", reqKRedis.Namespace, "Name", slaveDepName)
						return ctrl.Result{}, createErr
					}
				} else {
					log.Error(err, "Failed to check Slave Deployment existence", "Namespace", reqKRedis.Namespace, "Name", slaveDepName)
					return ctrl.Result{}, err
				}
			}
		}
	}

	// 4. Redis Service 생성
	svc := r.serviceForKRedis(reqKRedis)
	service := &corev1.Service{}
	err = r.Client.Get(ctx, client.ObjectKey{Name: svc.Name, Namespace: svc.Namespace}, service)
	if err != nil && kerrors.IsNotFound(err) {
		log.Info("Creating Redis Service.", "Namespace", svc.Namespace, "Name", svc.Name)
		err = r.Client.Create(ctx, svc)
		if err != nil {
			log.Error(err, "Failed to create Redis Service.")
			return ctrl.Result{}, err
		}
	} else if err != nil {
		log.Error(err, "Failed to get Redis Service.")
		return ctrl.Result{}, err
	}

	log.Info("KRedis Reconcile loop completed successfully.")
	return ctrl.Result{}, nil
}

// deploymentForMaster: 마스터 노드를 위한 Deployment 생성
func (r *KRedisReconciler) deploymentForMaster(cr *stablev1alpha1.KRedis) *appsv1.Deployment {
	labels := labelsForKRedis(cr.Name + "-master")
	replicas := int32(cr.Spec.Masters)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-master",
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
						Name:  "redis-master",
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
							{
								Name:  "MASTER",
								Value: "true",
							},
							{
								Name:  "REDIS_MAXMEMORY", // maxmemory 설정
								Value: cr.Spec.Resource["limits"]["memory"],
							},
						},
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
												"role": "slave", // Slave와의 배치를 회피
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

// serviceForKRedis: Redis Cluster를 위한 Service 생성
func (r *KRedisReconciler) serviceForKRedis(cr *stablev1alpha1.KRedis) *corev1.Service {
	labels := labelsForKRedis(cr.Name)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Protocol:   corev1.ProtocolTCP,
				Port:       cr.Spec.BasePort,
				TargetPort: intstr.FromInt(int(cr.Spec.BasePort)),
			}},
		},
	}
}

func equalDeployments(current, desired *appsv1.Deployment) bool {
	// Spec만 비교하고 metadata는 제외 (Replicas 만 비교중임)
	return current.Spec.Replicas != nil &&
		desired.Spec.Replicas != nil &&
		*current.Spec.Replicas == *desired.Spec.Replicas
	// 추가적인 필드 비교가 필요할 경우 여기서 추가적으로 구현
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

// SetupWithManager sets up the controller with the Manager.
func (r *KRedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&stablev1alpha1.KRedis{}).
		Owns(&appsv1.Deployment{}).
		// WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Complete(r)
}
