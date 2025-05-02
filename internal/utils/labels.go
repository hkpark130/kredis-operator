package utils

// LabelsForKRedis 함수는 주어진 kredis CR 이름과 역할에 맞는 라벨을 반환합니다.
// Redis 클러스터의 각 구성 요소(마스터, 슬레이브, 서비스 등)를 식별하는 데 사용됩니다.
//
// 주요 라벨:
// - app: "kredis" - 애플리케이션 식별자
// - kredis_cr: <name> - 소유하는 KRedis 커스텀 리소스 이름
// - role: <role> - 구성 요소 역할 (master/slave/service)
//
// name: KRedis 리소스 이름
// role: 구성 요소 역할 (master/slave/service)
// return: 라벨 맵
func LabelsForKRedis(name string, role string) map[string]string {
	labels := map[string]string{
		"app":       "kredis",
		"kredis_cr": name,
		"role":      role,
	}
	return labels
}
