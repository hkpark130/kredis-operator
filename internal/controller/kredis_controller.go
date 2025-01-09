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
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	_ = log.FromContext(ctx)
	reqKRedis := &stablev1alpha1.KRedis{}
	err := r.Client.Get(ctx, req.NamespacedName, reqKRedis)
	if err != nil { // custom resource를 가져오지 못 함
		if kerrors.IsNotFound(err) {
			log.Log.Info("KRedis resource not found.")
			return ctrl.Result{}, nil
		}
		log.Log.Error(err, "Failed to get KRedis")
		return ctrl.Result{}, err
	}

	deployment := &appsv1.Deployment{}
	err = r.Client.Get(ctx, types.NamespacedName{Name: reqKRedis.Name, Namespace: reqKRedis.Namespace}, deployment)
	if err != nil && kerrors.IsNotFound(err) {
		// Define a new deployment
		dep := r.deploymentForKRedis(reqKRedis)
		log.Log.Info("Creating a new Deployment.", reqKRedis.Name, reqKRedis.Namespace)
		err = r.Client.Create(ctx, dep)
		if err != nil {
			log.Log.Error(err, "Failed to create new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			return ctrl.Result{}, err
		}

		// Define a new clusterIP
		svc := r.clusterIPForKRedis(reqKRedis)
		log.Log.Info("Creating a new ClusterIP.", reqKRedis.Name, reqKRedis.Namespace)
		err = r.Client.Create(ctx, svc)
		if err != nil {
			log.Log.Error(err, "Failed to create new ClusterIP", "ClusterIP.Namespace", svc.Namespace, "ClusterIP.Name", svc.Name)
			return ctrl.Result{}, err
		}

		// Deployment created successfully - return and requeue
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Log.Error(err, "Failed to get Deployment.")
		return ctrl.Result{}, err
	}

	// Ensure the deployment size is the same as the spec
	size := reqKRedis.Spec.Replicas
	if *deployment.Spec.Replicas != size {
		deployment.Spec.Replicas = &size
		err = r.Client.Update(context.TODO(), deployment)
		if err != nil {
			log.Log.Error(err, "Failed to update Deployment.", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
			return ctrl.Result{}, err
		}
		// Ask to requeue after 1 minute in order to give enough time for the
		// pods be created on the cluster side and the operand be able
		// to do the next update step accurately.
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	// Update the KRedis status with the pod names
	// List the pods for this kredis's deployment
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(reqKRedis.Namespace),
		client.MatchingLabels(labelsForKRedis(reqKRedis.Name)),
	}
	if err = r.Client.List(ctx, podList, listOpts...); err != nil {
		log.Log.Error(err, "Failed to list pods", "KRedis.Namespace", reqKRedis.Namespace, "KRedis.Name", reqKRedis.Name)
		return ctrl.Result{}, err
	}

	log.Log.Info("Successed!")
	return ctrl.Result{}, nil
}

func (r *KRedisReconciler) clusterIPForKRedis(m *stablev1alpha1.KRedis) *corev1.Service {
	ls := labelsForKRedis(m.Name)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
			Labels:    ls,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: ls,
			Ports: []corev1.ServicePort{
				{
					Protocol: corev1.ProtocolTCP,
					Port:     6379,
				},
			},
		},
	}
	// Set kredis instance as the owner and controller
	ctrl.SetControllerReference(m, svc, r.Scheme)
	return svc
}


// deploymentForKRedis returns a kredis Deployment object
func (r *KRedisReconciler) deploymentForKRedis(m *stablev1alpha1.KRedis) *appsv1.Deployment {
	ls := labelsForKRedis(m.Name)
	replicas := m.Spec.Replicas

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
								LabelSelector: &metav1.LabelSelector{
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "app",
											Operator: metav1.LabelSelectorOpIn,
											Values:   []string{"kredis"},
										},
									},
								},
								TopologyKey: "kubernetes.io/hostname",
							}},
						},
					},
					Containers: []corev1.Container{{
						Image: "public.ecr.aws/x8r0y3u4/redis-cluster:latest",
						Name:  "kredis",
						Ports: []corev1.ContainerPort{{
							ContainerPort: 6379,
							Name:          "kredis",
						}},
					}},
				},
			},
		},
	}
	// Set kredis instance as the owner and controller
	ctrl.SetControllerReference(m, dep, r.Scheme)
	return dep
}

// labelsForKRedis returns the labels for selecting the resources
// belonging to the given kredis CR name.
func labelsForKRedis(name string) map[string]string {
	return map[string]string{"app": "kredis", "kredis_cr": name}
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
