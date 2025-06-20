#!/bin/bash
# set -e 옵션은 일단 제거하여 스크립트가 중간에 멈추지 않도록 합니다
# set -e

# 디버깅을 위한 기본 로깅
echo "===== 스크립트 시작 =====" >&2
echo "현재 작업 디렉토리: $(pwd)" >&2
echo "현재 사용자: $(id)" >&2
echo "디스크 사용량: $(df -h)" >&2

# 볼륨 마운트 대기 (최대 30초)
MAX_WAIT=30
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
  if mount | grep "/logs" > /dev/null || mount | grep "/data" > /dev/null; then
    echo "볼륨 마운트가 확인되었습니다." >&2
    break
  fi
  echo "볼륨 마운트 대기 중... ($WAIT_COUNT/$MAX_WAIT)" >&2
  sleep 1
  WAIT_COUNT=$((WAIT_COUNT+1))
done

# 로그 디렉토리 생성 시도
LOG_DIR="/data/logs"  # 데이터 볼륨 내에 로그 저장
mkdir -p $LOG_DIR
if [ $? -ne 0 ]; then
  echo "데이터 볼륨 내 로그 디렉토리 생성 실패, 대체 경로 시도" >&2
  # /logs 볼륨이 있는지 확인
  if [ -d "/logs" ] && [ -w "/logs" ]; then
    LOG_DIR="/logs"
    echo "/logs 디렉토리 사용" >&2
  else
    LOG_DIR="/tmp"
    echo "임시 디렉토리 사용" >&2
  fi
fi

# 디렉토리 권한 확인 및 테스트 파일 생성
echo "로그 디렉토리 $LOG_DIR 권한 확인 중..." >&2
ls -la $LOG_DIR >&2
touch "$LOG_DIR/test_write" 2>/dev/null
if [ $? -eq 0 ]; then
  echo "로그 디렉토리 쓰기 권한 확인: 성공" >&2
  rm "$LOG_DIR/test_write" 2>/dev/null
else
  echo "로그 디렉토리 쓰기 권한 확인: 실패, 임시 디렉토리로 변경" >&2
  LOG_DIR="/tmp"
fi

# 로그 파일 생성
LOG_FILE="$LOG_DIR/redis_$(date +%Y%m%d_%H%M%S).log"
touch $LOG_FILE
if [ $? -ne 0 ]; then
  echo "로그 파일 생성 실패! 임시 로그 파일 사용" >&2
  LOG_FILE="/tmp/redis_$(date +%Y%m%d_%H%M%S).log"
  touch $LOG_FILE
fi

echo "로그 파일 경로: $LOG_FILE" >&2

# 로그 기록 함수
log_message() {
  echo "[$(date)] $1" | tee -a $LOG_FILE
  echo "[$(date)] $1" >&2  # stderr로도 출력
}

log_message "===== Redis 컨테이너 시작 ====="
log_message "환경 변수:"
log_message "REDIS_MODE: $REDIS_MODE"
log_message "MASTER_HOST: $MASTER_HOST"
log_message "MASTER_PORT: $MASTER_PORT"
log_message "호스트명: $(hostname)"

# 호스트명에서 파드 번호 추출
POD_NAME=$(hostname)
log_message "파드 이름: $POD_NAME"
POD_INDEX=$(echo $POD_NAME | grep -o '[0-9]\+$')
log_message "파드 인덱스: $POD_INDEX"

# Redis Exporter를 백그라운드로 시작
/usr/local/bin/redis_exporter &
log_message "Redis Exporter가 시작되었습니다."

# 설정 파일 준비
cp /usr/local/etc/redis/redis.conf /tmp/redis.conf
log_message "Redis 설정 파일을 /tmp/redis.conf로 복사했습니다."

# 환경 변수로 Redis 모드 확인
if [ "$REDIS_MODE" = "slave" ]; then
  log_message "Redis를 Replica 모드로 시작합니다 (클러스터 모드 사용)..."
  
  # 마스터 호스트와 포트 설정 (환경 변수에서 가져옴)
  MASTER_H=${MASTER_HOST:-"localhost"}
  MASTER_P=${MASTER_PORT:-6379}
  
  # 전체 마스터 수와 인덱스 파싱
  TOTAL_MASTERS=${TOTAL_MASTERS:-3}
  MASTER_INDEX=${MASTER_INDEX:-0}
  
  # POD_INDEX를 기반으로 접속할 마스터 노드 결정 (환경변수가 없으면)
  if [ -n "$POD_INDEX" ] && [ -z "$MASTER_INDEX" ]; then
    MASTER_INDEX=$((POD_INDEX % TOTAL_MASTERS))
  fi
  
  # 마스터 헤드리스 서비스 접두어 추출 (kredis-sample-master)
  MASTER_SVC_PREFIX=$(echo "$MASTER_H" | grep -o "^[^\.]*" | sed 's/-master$//')
  
  # 도메인 접미어 추출
  DOMAIN_SUFFIX=$(echo "$MASTER_H" | sed "s/^[^\.]*//")
  
  # kredis-sample-master-<인덱스> 형식으로 마스터 파드 이름 구성
  SPECIFIC_MASTER="${MASTER_SVC_PREFIX}-master-${MASTER_INDEX}${DOMAIN_SUFFIX}"
  
  log_message "마스터 파드에 연결 예정: $SPECIFIC_MASTER:$MASTER_P"
  log_message "파싱 정보: 접두어=$MASTER_SVC_PREFIX, 인덱스=$MASTER_INDEX, 도메인=$DOMAIN_SUFFIX"
  
  # Redis 서버 설정 (클러스터 모드용)
  log_message "Redis 서버를 먼저 시작하고, 이후 마스터에 연결합니다..."
else
  log_message "Redis를 Master 모드로 시작합니다..."
fi

# Redis 서버 시작 시 명령줄 인자를 설정으로 해석하지 않는 옵션 추가
log_message "설정 파일 내용:"
cat /tmp/redis.conf | tee -a $LOG_FILE
log_message "Redis 서버를 시작합니다..."

# 서버 시작 전 마지막 로그 확인을 위해 로그 파일 접근성 확인
if [ -f "$LOG_FILE" ]; then
  log_message "로그 파일이 정상적으로 생성되었습니다."
  ls -la $LOG_FILE | tee -a $LOG_FILE
else
  echo "로그 파일 생성 실패!" >&2
fi

# 로그 파일 위치를 표시하는 파일 생성
echo "$LOG_FILE" > /tmp/redis_log_path

# 슬레이브 모드일 경우 백그라운드로 레디스 시작 후 클러스터 연결
if [ "$REDIS_MODE" = "slave" ]; then
  # Redis 서버 백그라운드로 시작
  redis-server /tmp/redis.conf --daemonize yes
  
  # 마스터 노드가 준비될 때까지 대기
  log_message "마스터 노드가 준비될 때까지 대기 중..."
  MAX_RETRY=30
  RETRY_COUNT=0
  while [ $RETRY_COUNT -lt $MAX_RETRY ]; do
    if redis-cli -h $SPECIFIC_MASTER -p $MASTER_P ping >/dev/null 2>&1; then
      log_message "마스터 노드 접속 성공: $SPECIFIC_MASTER:$MASTER_P"
      break
    fi
    log_message "마스터 노드 접속 시도 중... ($RETRY_COUNT/$MAX_RETRY) ($SPECIFIC_MASTER:$MASTER_P)"
    sleep 3
    RETRY_COUNT=$((RETRY_COUNT+1))
  done
  
  # 마스터 접속 성공 여부와 상관없이 계속 클러스터 모드로 진행
  # 마스터 노드의 클러스터 ID 가져오기
  MASTER_ID=$(redis-cli -h $SPECIFIC_MASTER -p $MASTER_P cluster myid 2>/dev/null)
  if [ -z "$MASTER_ID" ]; then
    log_message "마스터 노드의 ID를 가져올 수 없습니다. 계속 시도합니다."
    
    # 일정 시간 후에도 계속 시도
    for i in {1..5}; do
      sleep 5
      log_message "마스터 노드 ID 재시도 #$i... ($SPECIFIC_MASTER:$MASTER_P)"
      MASTER_ID=$(redis-cli -h $SPECIFIC_MASTER -p $MASTER_P cluster myid 2>/dev/null)
      if [ -n "$MASTER_ID" ]; then
        log_message "마스터 노드 ID 확인: $MASTER_ID"
        break
      fi
    done
  fi

  # 현재 노드 클러스터 초기화
  redis-cli cluster reset
  log_message "현재 노드 클러스터 초기화 완료"
  
  # 마스터 노드에 현재 노드를 클러스터의 일부로 추가
  log_message "클러스터에 현재 노드 추가 중..."
  CURRENT_IP=$(hostname -i || echo "127.0.0.1")
  redis-cli -h $SPECIFIC_MASTER -p $MASTER_P cluster meet $CURRENT_IP 6379
  
  # 현재 노드 ID 가져오기
  sleep 2
  MY_ID=$(redis-cli cluster myid)
  
  if [ -n "$MASTER_ID" ]; then
    # 현재 노드를 마스터의 레플리카로 설정
    log_message "현재 노드를 마스터 $MASTER_ID의 레플리카로 설정 중..."
    redis-cli cluster replicate $MASTER_ID
  else
    # 마스터 ID를 가져오지 못했지만 계속 시도
    log_message "마스터 ID를 얻지 못했지만 계속 시도..."
    
    # 클러스터 노드 목록에서 마스터 찾기 시도
    NODES_INFO=$(redis-cli -h $SPECIFIC_MASTER -p $MASTER_P cluster nodes 2>/dev/null)
    if [ -n "$NODES_INFO" ]; then
      # 마스터 노드 ID 추출 시도
      MASTER_ID=$(echo "$NODES_INFO" | grep "master" | head -n 1 | cut -d' ' -f1)
      if [ -n "$MASTER_ID" ]; then
        log_message "클러스터 노드 목록에서 마스터 ID를 찾았습니다: $MASTER_ID"
        # 현재 노드를 마스터의 레플리카로 설정
        redis-cli cluster replicate $MASTER_ID
      fi
    fi
  fi
  
  # 결과 확인
  sleep 1
  log_message "클러스터 상태 확인:"
  redis-cli cluster info | tee -a $LOG_FILE
  redis-cli cluster nodes | tee -a $LOG_FILE
  
  # 클러스터 상태 지속적 모니터링을 위한 백그라운드 작업
  (
    # 백그라운드에서 주기적으로 클러스터 상태 확인
    while true; do
      sleep 30
      echo "[$(date)] 클러스터 상태 주기적 확인..." >> $LOG_FILE
      
      # 레플리카가 마스터에 연결되었는지 확인
      ROLE=$(redis-cli info replication | grep role | cut -d: -f2 | tr -d '[:space:]')
      
      if [ "$ROLE" = "slave" ]; then
        echo "[$(date)] 현재 노드는 정상적으로 슬레이브 역할을 수행 중입니다." >> $LOG_FILE
      else
        echo "[$(date)] 현재 노드가 슬레이브 역할을 수행하지 않고 있습니다. 재시도합니다." >> $LOG_FILE
        
        # 마스터 ID를 다시 확인
        MASTER_ID=$(redis-cli -h $SPECIFIC_MASTER -p $MASTER_P cluster myid 2>/dev/null)
        
        if [ -n "$MASTER_ID" ]; then
          echo "[$(date)] 마스터 ID를 다시 확인: $MASTER_ID" >> $LOG_FILE
          redis-cli cluster replicate $MASTER_ID
        fi
      fi
    done
  ) &
  
  # 마지막으로 레디스 서버를 포그라운드로 다시 시작
  log_message "Redis 서버를 포그라운드로 재시작합니다..."
  redis-cli shutdown save
  exec redis-server /tmp/redis.conf
else
  # 마스터 모드에서는 바로 실행
  exec redis-server /tmp/redis.conf
fi