#!/bin/bash
set -e

# 설정
MAX_WAIT=30
LOG_DIR=""
DEFAULT_LOG_DIR="/data/logs"
FALLBACK_DIRS=("/logs" "/tmp")
LOG_FILE=""

wait_for_mounts() {
  for ((i=0; i<$MAX_WAIT; i++)); do
    mount | grep -qE "/logs|/data" && return 0
    echo "볼륨 마운트 대기 중... ($i/$MAX_WAIT)" >&2
    sleep 1
  done
  return 1
}

prepare_log_dir() {
  local try_dir="$DEFAULT_LOG_DIR"
  mkdir -p "$try_dir" 2>/dev/null || try_dir=""

  for dir in "${FALLBACK_DIRS[@]}"; do
    if [ -z "$try_dir" ] && [ -w "$dir" ]; then
      try_dir="$dir"
      break
    fi
  done

  # 최종 쓰기 테스트
  touch "$try_dir/test_write" 2>/dev/null && rm "$try_dir/test_write" || try_dir="/tmp"
  LOG_DIR="$try_dir"
  LOG_FILE="$LOG_DIR/redis_$(date +%Y%m%d_%H%M%S).log"
  touch "$LOG_FILE" 2>/dev/null || LOG_FILE="/tmp/redis_$(date +%Y%m%d_%H%M%S).log"
  touch "$LOG_FILE"

  echo "$LOG_FILE" > /tmp/redis_log_path
  echo "로그 파일 경로: $LOG_FILE" >&2
}

log_message() {
  local msg="[$(date)] $1"
  echo "$msg" | tee -a "$LOG_FILE" >&2
}

start_redis_exporter() {
  /usr/local/bin/redis_exporter &
  log_message "Redis Exporter 시작됨"
}

configure_slave() {
  local master_host="${MASTER_HOST:-localhost}"
  local master_port="${MASTER_PORT:-6379}"
  # 파드 이름에서 마지막 인덱스 추출 (kredis-sample-slave-0-1에서 1을 가져옴)
  local pod_index="$(hostname | grep -o '[0-9]\+$')"
  # 이미 환경변수에서 마스터 인덱스를 받지만, 없으면 호스트명에서 추출
  local master_index="${MASTER_INDEX:-$(hostname | sed -E 's/.*-slave-([0-9]+)-[0-9]+$/\1/')}"
  # kredis 이름 추출 (kredis-sample-slave-0-0 → kredis-sample)
  local kredis_name="$(hostname | sed -E 's/-slave-[0-9]+-[0-9]+$//')"
  local target_master="${master_host}"

  log_message "슬레이브 모드: 마스터 노드 -> $target_master:$master_port"

  redis-server /tmp/redis.conf --daemonize yes
  for ((i=0; i<30; i++)); do
    redis-cli -h "$target_master" -p "$master_port" ping &>/dev/null && break
    log_message "마스터 노드 접속 시도... ($i/30)"
    sleep 3
  done

  local master_id=$(redis-cli -h "$target_master" -p "$master_port" cluster myid 2>/dev/null)

  if [ -z "$master_id" ]; then
    for i in {1..5}; do
      sleep 5
      master_id=$(redis-cli -h "$target_master" -p "$master_port" cluster myid 2>/dev/null)
      [ -n "$master_id" ] && break
    done
  fi

  redis-cli cluster reset
  redis-cli -h "$target_master" -p "$master_port" cluster meet "$(hostname -i)" 6379

  if [ -z "$master_id" ]; then
    master_id=$(redis-cli -h "$target_master" -p "$master_port" cluster nodes | grep master | awk '{print $1}' | head -n1)
  fi

  if [ -n "$master_id" ]; then
    redis-cli cluster replicate "$master_id"
    log_message "마스터에 복제 연결 완료 ($master_id)"
  fi

  (while true; do
    sleep 30
    local role=$(redis-cli info replication | grep role | cut -d: -f2 | tr -d '[:space:]')
    echo "[$(date)] 슬레이브 상태: $role" >> "$LOG_FILE"
    if [ "$role" != "slave" ]; then
      redis-cli cluster replicate "$master_id"
    fi
  done) &

  redis-cli shutdown save
}

main() {
  wait_for_mounts
  prepare_log_dir

  log_message "===== Redis 컨테이너 시작 ====="
  log_message "REDIS_MODE: $REDIS_MODE"
  log_message "MASTER_HOST: $MASTER_HOST"
  log_message "MASTER_PORT: $MASTER_PORT"
  log_message "파드 이름: $(hostname)"

  start_redis_exporter

  cp /usr/local/etc/redis/redis.conf /tmp/redis.conf
  log_message "설정 파일 복사 완료"
  cat /tmp/redis.conf | tee -a "$LOG_FILE"

  if [ "$REDIS_MODE" = "slave" ]; then
    configure_slave
    log_message "Redis 서버 포그라운드로 재시작"
    exec redis-server /tmp/redis.conf
  else
    log_message "Redis 마스터 모드로 시작"
    exec redis-server /tmp/redis.conf
  fi
}

main
