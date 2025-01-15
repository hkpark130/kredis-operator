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

// KRedisSpec defines the desired state of KRedis
type KRedisSpec struct {
	Image      string                       `json:"image"`
	Masters    int32                        `json:"masters"`
	Replicas   int32                        `json:"replicas"`
	Memory     string                       `json:"memory"`
	BasePort   int32                        `json:"basePort"`
	Resource   map[string]map[string]string `json:"resource"`
	SecretName string                       `json:"secretName"`
}

// KRedisStatus defines the observed state of KRedis
type KRedisStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// KRedis is the Schema for the kredis API
type KRedis struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KRedisSpec   `json:"spec,omitempty"`
	Status KRedisStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KRedisList contains a list of KRedis
type KRedisList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KRedis `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KRedis{}, &KRedisList{})
}
