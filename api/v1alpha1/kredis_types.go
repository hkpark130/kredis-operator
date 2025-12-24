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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

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
	Resources map[string]map[string]string `json:"resources"`
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

	// Status is the node status (e.g., "ready", "failed", "pending")
	Status string `json:"status"`

	// Joined indicates if the node has joined the cluster
	Joined bool `json:"joined"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

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
