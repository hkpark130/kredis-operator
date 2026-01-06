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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterState represents the state of Redis cluster
type ClusterState string

const (
	// ClusterStateCreating - 클러스터 생성 중
	ClusterStateCreating ClusterState = "Creating"
	// ClusterStateInitialized - 초기 생성 완료
	ClusterStateInitialized ClusterState = "Initialized"
	// ClusterStateRunning - 정상 동작 중
	ClusterStateRunning ClusterState = "Running"
	// ClusterStateScaling - 스케일링 중
	ClusterStateScaling ClusterState = "Scaling"
	// ClusterStateRebalancing - 리밸런싱 중
	ClusterStateRebalancing ClusterState = "Rebalancing"
	// ClusterStateHealing - 복구 중
	ClusterStateHealing ClusterState = "Healing"
	// ClusterStateDegraded - 일부 노드 장애
	ClusterStateDegraded ClusterState = "Degraded"
	// ClusterStateFailed - 심각한 장애
	ClusterStateFailed ClusterState = "Failed"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ResourceRequirements describes the compute resource requirements.
type ResourceRequirements struct {
	// Limits describes the maximum amount of compute resources allowed.
	// +optional
	Limits corev1.ResourceList `json:"limits,omitempty"`
	// Requests describes the minimum amount of compute resources required.
	// +optional
	Requests corev1.ResourceList `json:"requests,omitempty"`
}

// KredisSpec defines the desired state of Kredis
type KredisSpec struct {
	// Redis Master Node count
	Masters int32 `json:"masters"`

	// Redis Replica Node count
	Replicas int32 `json:"replicas"`

	// MaxMemory: Redis maxmemory 설정 (예: "512mb", "1gb"). 비우면 제한 미적용.
	MaxMemory string `json:"maxMemory,omitempty"`

	// Base Port for Redis
	BasePort int32 `json:"basePort"`

	// Redis container image
	Image string `json:"image"`

	// Resource requirements
	// +optional
	Resources ResourceRequirements `json:"resources,omitempty"`
}

// KredisStatus defines the observed state of Kredis
type KredisStatus struct {
	// Phase represents the current state of the Kredis deployment
	Phase string `json:"phase,omitempty"`

	// Number of ready replicas
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Total number of replicas
	Replicas int32 `json:"replicas,omitempty"`

	// ClusterState represents the Redis cluster initialization state
	ClusterState string `json:"clusterState,omitempty"`

	// ClusterNodes contains information about cluster nodes
	ClusterNodes []ClusterNode `json:"clusterNodes,omitempty"`

	// LastClusterOperation tracks the last cluster operation performed
	LastClusterOperation string `json:"lastClusterOperation,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// KnownClusterNodes represents the number of nodes in the cluster
	KnownClusterNodes int `json:"knownClusterNodes,omitempty"`

	// JoinedPods contains the list of pod names that have joined the cluster
	JoinedPods []string `json:"joinedPods,omitempty"`
}

// ClusterNode represents a Redis node in the cluster
type ClusterNode struct {
	// NodeID is the Redis cluster node ID
	NodeID string `json:"nodeId,omitempty"`

	// PodName is the Kubernetes pod name
	PodName string `json:"podName"`

	// IP is the pod IP address
	IP string `json:"ip"`

	// Port is the Redis port
	Port int32 `json:"port"`

	// Role is either "master" or "slave"
	Role string `json:"role"`

	// MasterID is the master node ID if this is a slave
	MasterID string `json:"masterId,omitempty"`

	// SlotCount is the number of slots assigned to this node (masters only)
	SlotCount int `json:"slotCount,omitempty"`

	// Status is the node status (e.g., "ready", "failed", "pending")
	Status string `json:"status"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Masters",type="integer",JSONPath=".spec.masters",description="Number of master nodes"
//+kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".spec.replicas",description="Number of replicas per master"
//+kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.clusterState",description="Current cluster state"
//+kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.readyReplicas",description="Ready replicas"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Kredis is the Schema for the kredis API
type Kredis struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KredisSpec   `json:"spec,omitempty"`
	Status KredisStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// KredisList contains a list of Kredis
type KredisList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Kredis `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Kredis{}, &KredisList{})
}
