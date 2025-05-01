package utils

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// ParseResource는 리소스 맵에서 값을 가져와 Quantity 객체로 변환합니다.
func ParseResource(resourceMap map[string]string, key string, defaultValue string) resource.Quantity {
	value, ok := resourceMap[key]
	if (!ok || value == "") {
		value = defaultValue
	}
	return resource.MustParse(value)
}