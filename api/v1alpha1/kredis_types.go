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

	// Redis Memory size
	Memory string `json:"memory"`

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

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
