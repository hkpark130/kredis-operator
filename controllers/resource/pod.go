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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

const (
	// LabelRole is the label key for node role (master/slave) - updated dynamically
	LabelRole = "role"
	// LabelShardIndex is the label key for shard (master group) index
	LabelShardIndex = "shard-index"
	// LabelInstanceIndex is the label key for instance index within shard
	// Instance 0 is initially master, instances 1+ are initially replicas
	LabelInstanceIndex = "instance-index"
)

// ShardPodName returns the pod name for a Redis node in a shard group.
// shardIndex: which shard (0-based, corresponds to master group)
// instanceIndex: which instance in the shard (0 = initial master, 1+ = initial replicas)
// Example: kredis-sample-0-0 (shard 0, instance 0 - initial master)
//
//	kredis-sample-0-1 (shard 0, instance 1 - initial replica)
func ShardPodName(kredisName string, shardIndex, instanceIndex int) string {
	return fmt.Sprintf("%s-%d-%d", kredisName, shardIndex, instanceIndex)
}

// ParsePodName extracts shard and instance indices from pod name.
// Returns: shardIndex, instanceIndex, error
// Note: Role is NOT determined by pod name - it's determined by Redis Cluster state.
func ParsePodName(kredisName, podName string) (int, int, error) {
	prefix := kredisName + "-"
	if !strings.HasPrefix(podName, prefix) {
		return 0, 0, fmt.Errorf("pod name doesn't match kredis name: %s", podName)
	}

	suffix := strings.TrimPrefix(podName, prefix)
	parts := strings.Split(suffix, "-")

	// New format: kredis-sample-0-0 (shard-instance)
	if len(parts) == 2 {
		shardIdx, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid shard index in pod name: %s", podName)
		}
		instanceIdx, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid instance index in pod name: %s", podName)
		}
		return shardIdx, instanceIdx, nil
	}

	// Legacy StatefulSet format: kredis-sample-0 (single index)
	if len(parts) == 1 {
		idx, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid index in legacy pod name: %s", podName)
		}
		// For legacy pods, we can't determine shard/instance from name
		// Return the ordinal as both (will be handled by cluster state)
		return idx, 0, nil
	}

	return 0, 0, fmt.Errorf("unrecognized pod name format: %s", podName)
}

// CreateShardPod creates a Redis pod for a shard group.
// shardIndex: which shard (corresponds to master group)
// instanceIndex: 0 for initial master, 1+ for initial replicas
// initialRole: "master" or "slave" - used for initial label only (can change after failover)
func CreateShardPod(k *cachev1alpha1.Kredis, shardIndex, instanceIndex int, scheme *runtime.Scheme) *corev1.Pod {
	podName := ShardPodName(k.Name, shardIndex, instanceIndex)

	// Initial role: instance 0 starts as master, others as slave
	initialRole := "slave"
	if instanceIndex == 0 {
		initialRole = "master"
	}

	labels := labelsForPod(k.Name, initialRole, shardIndex, instanceIndex)
	pod := createBasePod(k, podName, labels, scheme)
	return pod
}

// labelsForPod returns labels for a pod
func labelsForPod(kredisName, role string, shardIndex, instanceIndex int) map[string]string {
	labels := BaseLabelsForKredis(kredisName)
	labels[LabelRole] = role // This will be updated dynamically based on cluster state
	labels[LabelShardIndex] = strconv.Itoa(shardIndex)
	labels[LabelInstanceIndex] = strconv.Itoa(instanceIndex)
	return labels
}

// createBasePod creates the base pod structure
func createBasePod(k *cachev1alpha1.Kredis, podName string, labels map[string]string, scheme *runtime.Scheme) *corev1.Pod {
	// Environment variables
	env := []corev1.EnvVar{
		{Name: "REDIS_PORT", Value: fmt.Sprintf("%d", k.Spec.BasePort)},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}},
	}
	if k.Spec.MaxMemory != "" {
		env = append(env, corev1.EnvVar{Name: "REDIS_MAXMEMORY", Value: k.Spec.MaxMemory})
	}

	// Resource requirements
	resources := corev1.ResourceRequirements{}
	if k.Spec.Resources.Limits != nil {
		resources.Limits = k.Spec.Resources.Limits.DeepCopy()
	}
	if k.Spec.Resources.Requests != nil {
		resources.Requests = k.Spec.Resources.Requests.DeepCopy()
	}

	// Startup probe
	startupProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"redis-cli", "-p", fmt.Sprintf("%d", k.Spec.BasePort), "ping"},
			},
		},
		InitialDelaySeconds: 0,
		PeriodSeconds:       5,
		TimeoutSeconds:      5,
		FailureThreshold:    30, // ~2.5 minutes max
	}

	// Redis container
	redisContainer := corev1.Container{
		Name:            "redis",
		Image:           k.Spec.Image,
		ImagePullPolicy: corev1.PullAlways,
		Command:         []string{"sh", "-c", "/entrypoint.sh"},
		Env:             env,
		Ports: []corev1.ContainerPort{
			{ContainerPort: k.Spec.BasePort, Name: "redis"},
			{ContainerPort: k.Spec.BasePort + 10000, Name: "cluster-bus"},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "redis-data", MountPath: "/data"},
			{Name: "redis-logs", MountPath: "/logs"},
		},
		Resources:    resources,
		StartupProbe: startupProbe,
	}

	// Build imagePullSecret name
	secretName := "docker-secret"
	if k.Labels != nil {
		if prefix, ok := k.Labels["app.kubernetes.io/name-prefix"]; ok && prefix != "" {
			if !strings.HasSuffix(prefix, "-") {
				prefix = prefix + "-"
			}
			secretName = prefix + secretName
		}
	}

	// Build containers list
	containers := []corev1.Container{redisContainer}

	// Add redis-exporter sidecar if enabled
	if k.Spec.Exporter.Enabled {
		exporterImage := k.Spec.Exporter.Image
		if exporterImage == "" {
			exporterImage = "bitnami/redis-exporter:latest"
		}
		exporterPort := k.Spec.Exporter.Port
		if exporterPort == 0 {
			exporterPort = 9121
		}

		exporterContainer := corev1.Container{
			Name:            "redis-exporter",
			Image:           exporterImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Ports: []corev1.ContainerPort{
				{ContainerPort: exporterPort, Name: "exporter"},
			},
			Env: []corev1.EnvVar{
				{Name: "REDIS_ADDR", Value: fmt.Sprintf("localhost:%d", k.Spec.BasePort)},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("50m"),
					corev1.ResourceMemory: resource.MustParse("64Mi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			},
		}
		containers = append(containers, exporterContainer)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: k.Namespace,
			Labels:    labels,
			// For headless service DNS resolution
			// Pod DNS: {podName}.{serviceName}.{namespace}.svc.cluster.local
		},
		Spec: corev1.PodSpec{
			Hostname:  podName,
			Subdomain: k.Name, // Headless service name for DNS
			Containers: containers,
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: secretName}},
			Volumes: []corev1.Volume{
				{
					Name: "redis-data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: DataPVCName(podName),
						},
					},
				},
				{
					Name: "redis-logs",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: LogsPVCName(podName),
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyAlways,
		},
	}

	controllerutil.SetControllerReference(k, pod, scheme)
	return pod
}

// GetExpectedPodNames returns all expected pod names for a Kredis instance.
// Each shard has 1 + replicas instances (1 initial master + replicas).
// Pod names: kredis-0-0, kredis-0-1, kredis-1-0, kredis-1-1, ...
func GetExpectedPodNames(kredisName string, masters, replicas int32) []string {
	var names []string

	for shardIdx := int32(0); shardIdx < masters; shardIdx++ {
		// Instance 0 is initial master, 1+ are initial replicas
		instanceCount := 1 + replicas
		for instanceIdx := int32(0); instanceIdx < instanceCount; instanceIdx++ {
			names = append(names, ShardPodName(kredisName, int(shardIdx), int(instanceIdx)))
		}
	}

	return names
}
