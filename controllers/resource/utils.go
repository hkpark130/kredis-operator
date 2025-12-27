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
)

// LabelsForKredis returns the labels for selecting the resources
func LabelsForKredis(name string, role string) map[string]string {
	norm := normalizeRole(role)
	return map[string]string{
		// app 은 클러스터 전체 식별용으로 고정값 'kredis' 로 두고 역할은 별도 role 라벨에 표시
		"app":                        "kredis",
		"app.kubernetes.io/name":     name,
		"app.kubernetes.io/instance": name,
		"role":                       norm,
	}
}

// normalizeRole: redis cluster 에서 수집된 role 문자열을 라벨에 안전하게 기록하기 위한 정규화
// master -> master, slave/replica -> replica, 그 외/미정 -> unknown
func normalizeRole(r string) string {
	switch strings.ToLower(r) {
	case "master":
		return "master"
	case "slave", "replica":
		// redis-cli output 은 'slave' 를 사용하므로 라벨도 slave 로 통일 (Service selector 와 매칭)
		return "slave"
	case "redis": // 기존 placeholder
		return "unknown"
	default:
		return "unknown"
	}
}
