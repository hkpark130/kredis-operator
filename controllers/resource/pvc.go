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

package resource

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// DataPVCName returns the data PVC name for a pod
func DataPVCName(podName string) string {
	return fmt.Sprintf("data-%s", podName)
}

// LogsPVCName returns the logs PVC name for a pod
func LogsPVCName(podName string) string {
	return fmt.Sprintf("logs-%s", podName)
}

// CreateDataPVC creates a data PVC for a Redis pod
func CreateDataPVC(k *cachev1alpha1.Kredis, podName string, scheme *runtime.Scheme) *corev1.PersistentVolumeClaim {
	labels := BaseLabelsForKredis(k.Name)
	labels["pod-name"] = podName

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DataPVCName(podName),
			Namespace: k.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	controllerutil.SetControllerReference(k, pvc, scheme)
	return pvc
}

// CreateLogsPVC creates a logs PVC for a Redis pod
func CreateLogsPVC(k *cachev1alpha1.Kredis, podName string, scheme *runtime.Scheme) *corev1.PersistentVolumeClaim {
	labels := BaseLabelsForKredis(k.Name)
	labels["pod-name"] = podName

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      LogsPVCName(podName),
			Namespace: k.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("500Mi"),
				},
			},
		},
	}

	controllerutil.SetControllerReference(k, pvc, scheme)
	return pvc
}

// GetExpectedPVCNames returns all expected PVC names for a pod
func GetExpectedPVCNames(podName string) []string {
	return []string{
		DataPVCName(podName),
		LogsPVCName(podName),
	}
}
