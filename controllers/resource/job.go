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
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// Job labels and annotations
const (
	LabelKredisName    = "cache.docker.direa.synology.me/kredis-name"
	LabelJobType       = "cache.docker.direa.synology.me/job-type"
	AnnotationTargetID = "cache.docker.direa.synology.me/target-node-id"

	// Job configuration
	JobTTLSeconds     = int32(300)  // Clean up completed jobs after 5 minutes
	JobBackoffLimit   = int32(3)    // Retry up to 3 times
	JobActiveDeadline = int64(1800) // Timeout after 30 minutes (migration can take long)
)

// BuildClusterOperationJob constructs a Kubernetes Job for Redis cluster operations.
// This function creates a Job resource that will execute redis-cli commands in a Pod.
// The Job is configured with:
//   - TTLSecondsAfterFinished: 300 (auto-cleanup after 5 minutes)
//   - BackoffLimit: 3 (retry up to 3 times on failure)
//   - ActiveDeadlineSeconds: 600 (timeout after 10 minutes)
//   - OwnerReference: Set to the Kredis resource (cascade delete)
func BuildClusterOperationJob(kredis *cachev1alpha1.Kredis, jobName string, jobType string, targetNodeID string, command []string) *batchv1.Job {
	ttl := JobTTLSeconds
	backoffLimit := JobBackoffLimit
	activeDeadline := JobActiveDeadline

	// Join command for shell execution
	cmdStr := strings.Join(command, " ")

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: kredis.Namespace,
			Labels: map[string]string{
				LabelKredisName: kredis.Name,
				LabelJobType:    jobType,
			},
			Annotations: map[string]string{
				AnnotationTargetID: targetNodeID,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: kredis.APIVersion,
					Kind:       kredis.Kind,
					Name:       kredis.Name,
					UID:        kredis.UID,
				},
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttl,
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   &activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelKredisName: kredis.Name,
						LabelJobType:    jobType,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "redis-cli",
							Image: kredis.Spec.Image,
							Command: []string{
								"/bin/sh",
								"-c",
								cmdStr,
							},
						},
					},
				},
			},
		},
	}

	return job
}
