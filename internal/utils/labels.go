package utils

// LabelsForKRedis 함수는 주어진 kredis CR 이름에 맞는 라벨을 반환합니다.
func LabelsForKRedis(name string) map[string]string {
	labels := map[string]string{
		"app":       "kredis",
		"kredis_cr": name,
	}
	return labels
}