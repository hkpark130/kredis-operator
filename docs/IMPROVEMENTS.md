# Kredis Operator - 코드 분석 및 개선 보고서

## 요약

이 문서는 kredis-operator 프로젝트의 코드 분석 결과와 수행된 개선 사항, 그리고 남아있는 잠재적 문제점들을 정리합니다.

---

## 수행된 개선 사항

### 1. 클러스터 생성 전 리셋 필요 여부 사전 체크 ✅

**이전 문제:**
- 모든 파드에 대해 무조건 `FLUSHALL` + `CLUSTER RESET` 실행
- 이미 깨끗한 노드도 불필요하게 리셋

**개선:**
- `isNodeResetNeeded()` 함수로 리셋 필요 여부 사전 체크
- `findPodsNeedingReset()` 함수로 리셋이 필요한 파드만 필터링
- 이미 깨끗한 노드는 스킵하여 성능 향상

**수정 파일:** `op_create.go`, `utils.go`

---

### 2. 스케일 다운 시나리오 대응 ⚠️

**현재 상태:**
```go
if currentJoinedNodes < int(expectedTotalNodes) && unassignedPodCount > 0 {
    return OperationScale  // Scale-up only
}
```

**문제점:**
- 이 조건은 스케일 업만 처리
- 스케일 다운 시 `currentJoinedNodes > expectedTotalNodes`가 되지만 처리 안됨
- StatefulSet은 자동으로 파드를 삭제하지만, 클러스터에서 노드를 제거하지 않음

**개선:**
- 경고 로그 및 상세한 주석 추가
- 스케일 다운은 복잡한 작업이므로 별도 구현 필요 (TODO)

**필요한 스케일 다운 구현:**
1. 제거할 노드에서 슬롯을 다른 노드로 리샤딩
2. `CLUSTER FORGET`으로 클러스터에서 노드 제거
3. 클러스터 안정화 대기
4. 파드 삭제 허용

**수정 파일:** `manager.go`

---

### 3. `findMasterPod`를 JoinedPods 기반으로 수정 ✅

**이전 문제:**
- `findMasterPod`는 단순히 clusterState에서 마스터를 찾음
- 새로 추가된 (아직 join 안된) 파드가 선택될 수 있음

**개선:**
- `findQueryPod`처럼 JoinedPods에서 먼저 찾도록 수정
- 우선순위: JoinedPods 마스터 > JoinedPods 멤버 > 아무 ready 파드

**수정 파일:** `manager.go`

---

### 4. 스케일링 시 리셋 사전 체크 ✅

**이전 문제:**
- 스케일링 시 모든 새 파드에 대해 무조건 리셋 실행

**개선:**
- `isNodeResetNeeded()` 체크 후 필요한 경우만 리셋

**수정 파일:** `op_scale.go`

---

### 5. 공통 함수 분리 ✅

**새로 생성된 `utils.go` 파일:**
```go
// 클러스터 주소 생성
func clusterAddr(pod corev1.Pod, port int32) string

// 리셋 필요 여부 체크
func (cm *ClusterManager) isNodeResetNeeded(ctx, pod, port) bool

// 리셋 완료 여부 체크
func (cm *ClusterManager) isNodeReset(ctx, pod, port) bool

// 노드 리셋 실행
func (cm *ClusterManager) resetNode(ctx, pod, port) error

// 필요시 리셋
func (cm *ClusterManager) resetNodeIfNeeded(ctx, pod, port) (bool, error)

// JoinedPods 기반 마스터 파드 찾기
func (cm *ClusterManager) findMasterPodFromJoined(...) *corev1.Pod

// 모든 노드 리셋 여부 체크
func (cm *ClusterManager) areAllNodesReset(ctx, pods, port) bool

// 리셋 필요한 파드 목록
func (cm *ClusterManager) findPodsNeedingReset(ctx, pods, port) []corev1.Pod
```

**중복 제거된 함수:**
- `op_scale.go`에서 `isNodeReset`, `resetNode` 제거
- `op_create.go`에서 `areAllNodesReset` 제거

---

### 6. 슬롯 없는 마스터에 슬레이브 추가 ✅

**분석 결과:**
- Redis 클러스터에서 슬롯이 없는 마스터에도 슬레이브 추가 가능
- 슬레이브는 마스터가 슬롯을 받으면 자동으로 데이터 복제
- 스케일링 중 일시적으로 슬롯 없는 상태가 될 수 있어 현재 로직 유지

**개선:**
- `selectMasterForReplica` 함수에 상세 주석 추가

---

### 7. ClusterState 표시 로직 개선 ✅

**이전 문제:**
- `ClusterState`가 delta에서만 설정됨
- 초기 상태나 안정 상태에서 빈 문자열일 수 있음

**개선:**
- `calculateStatus`에서 ClusterState 초기값 설정
- 성공 후 안정 상태에서 자동으로 "Running" 설정

---

### 8. JobStatusNotFound vs Succeeded 구분 개선 ✅

**이전 문제:**
```go
case JobStatusNotFound:
    // 무조건 새 Job 생성
```

**개선:**
```go
case JobStatusNotFound:
    // 클러스터 상태 확인 - 이미 정상이면 success로 마킹
    if isHealthy && allMastersHaveSlots {
        delta.LastClusterOperation = "rebalance-success:..."
        return nil
    }
    // 아니면 새 Job 생성
```

**수정 파일:** `op_rebalance.go`

---

## 크리티컬 문제점 분석 (9번)

### 1. 동시성 문제 ⚠️

**현상:**
- 여러 reconcile 루프가 동시에 실행될 수 있음
- Job 상태 체크와 Job 생성 사이에 레이스 컨디션

**현재 완화 방법:**
- `isOperationInProgress` 체크로 중복 작업 방지
- Job 이름 해싱으로 중복 Job 생성 방지 (이미 있으면 skip)

**잠재적 위험:**
```go
// GetJobStatus 체크 후 CreateJob 사이에 다른 reconcile이 개입 가능
jobResult, _ := cm.JobManager.GetJobStatus(...)
// <-- 여기서 다른 reconcile이 Job 생성 가능
if jobResult.Status == JobStatusNotFound {
    cm.JobManager.CreateRebalanceJob(...) // 중복 생성 시도
}
```

**현재 보호:**
- `CreateRebalanceJob` 등에서 이미 존재하는 Job 체크
- `errors.IsNotFound(err)` 체크로 중복 방지

---

### 2. 상태 불일치 가능성 ⚠️

**현상:**
- `lastOp`이 `"-in-progress"`이지만 실제 Job이 없는 경우
- Job이 성공했지만 status 업데이트 전에 실패한 경우

**현재 처리:**
- `JobStatusNotFound`에서 클러스터 상태 직접 확인
- `verifyClusterCreation`에서 실제 클러스터 헬스 체크

**개선됨:**
- `executeRebalancePhase`에서 JobNotFound 시 클러스터 상태 확인 후 판단

---

### 3. JoinedPods 동기화 문제 ⚠️

**현상:**
- `JoinedPods`가 실제 클러스터 멤버와 불일치할 수 있음
- 파드가 재시작되거나 삭제된 경우 자동 정리 안됨

**잠재적 시나리오:**
1. 파드 A가 클러스터에 join
2. JoinedPods에 A 추가
3. 파드 A 삭제 (StatefulSet에 의해)
4. JoinedPods에는 A가 남아있음

**현재 완화:**
- `discoverClusterState`에서 실제 클러스터 상태 확인
- `buildJoinedPodsSet`과 실제 pod 비교

**권장 개선:**
```go
// 추후 구현 필요
func (cm *ClusterManager) cleanupStaleJoinedPods(kredis, pods, clusterState) {
    // clusterState에 없는 JoinedPods 제거
}
```

---

### 4. 클러스터 분할 (Split Brain) ⚠️

**현상:**
- 네트워크 파티션 시 여러 파드가 각자 클러스터라고 생각할 수 있음

**현재 처리:**
- `findQueryPod`에서 JoinedPods 우선 사용
- 단일 파드에서 클러스터 상태 조회

**권장 개선:**
- 다수결 기반 헬스 체크
- 여러 파드에서 클러스터 상태 교차 검증

---

### 5. Reshard 동시 실행 방지 ✅

**현재 처리:**
- 한 번에 하나의 reshard Job만 실행
- `checkReshardJobsAndProceed`에서 순차 처리

**코드:**
```go
// IMPORTANT: Create only ONE reshard Job at a time to avoid conflicts
firstEmptyMaster := emptyMasters[0]
if err := cm.JobManager.CreateReshardJob(..., firstEmptyMaster, ...); err != nil {
```

---

### 6. 우연히 작동하는 경우 분석

**왜 현재 잘 작동하는가:**

1. **타이밍 운:**
   - Reconcile 간격이 충분히 길어서 레이스 컨디션 발생 확률 낮음
   - 단일 노드/적은 수의 마스터로 테스트 시 동시성 이슈 드러나지 않음

2. **Job 중복 방지:**
   - Job 이름 해싱 + 존재 체크로 중복 생성 방지됨

3. **Reconcile 재시도:**
   - 실패해도 다음 reconcile에서 재시도
   - 결국 올바른 상태에 수렴

4. **Redis 클러스터 자체 안정성:**
   - Redis 클러스터가 gossip 프로토콜로 자체 복구
   - 일시적 불일치도 시간이 지나면 해결

---

## 권장 추가 개선 사항

### 높은 우선순위

1. **스케일 다운 구현** (추후 구현 예정)
   - 현재 지원 안됨
   - 슬롯 리샤딩 + 노드 제거 로직 필요

2. ~~**JoinedPods 정리 로직**~~ ✅ 구현 완료
   - `CleanupStaleJoinedPods()` 함수 추가
   - 삭제된 파드 자동 정리
   - 클러스터에서 실제로 빠진 노드 제거

3. ~~**Finalizer 추가**~~ ✅ 구현 완료
   - Kredis 리소스 삭제 시 관련 Job 정리
   - 고아 리소스 방지

### 중간 우선순위

4. **메트릭 및 이벤트** (추후 Prometheus 연동 시 구현)
   - Prometheus 메트릭 추가
   - Kubernetes 이벤트 생성

---

## 파일 변경 요약

| 파일 | 변경 내용 |
|------|-----------|
| `utils.go` | 새로 생성 - 공통 함수 |
| `op_create.go` | 리셋 사전 체크, 중복 함수 제거 |
| `op_scale.go` | 리셋 사전 체크, 중복 함수 제거, 주석 개선 |
| `op_rebalance.go` | JobNotFound 처리 개선 |
| `manager.go` | findMasterPod 개선, 스케일다운 경고 추가 |
| `kredis_controller.go` | ClusterState 초기화 로직 추가 |
