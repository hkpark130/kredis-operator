#!/bin/bash
set -e

# 기본 설정
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_MASTERS=${REDIS_MASTERS:-3}
REDIS_REPLICAS_PER_MASTER=${REDIS_REPLICAS_PER_MASTER:-1}

# 로그 함수
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

log_info() { log "INFO: $*"; }
log_warn() { log "WARN: $*"; }
log_error() { log "ERROR: $*"; }

# 파드 정보 파싱
parse_pod_info() {
    local hostname=$(hostname)
    POD_INDEX=$(echo "$hostname" | awk -F'-' '{print $NF}')
    KREDIS_NAME=$(echo "$hostname" | sed -E 's/-[0-9]+$//')
    HEADLESS_SERVICE="$KREDIS_NAME"
    SELF_IP=$(hostname -i)
    
    log_info "파드 정보: name=$KREDIS_NAME, index=$POD_INDEX, ip=$SELF_IP"
}

# 노드 역할 결정 (컨트롤러가 클러스터를 관리하므로 단순화)
get_node_role() {
    local pod_index=$1
    local masters=$2
    
    if [ "$pod_index" -lt "$masters" ]; then
        echo "master"
    else
        echo "slave"  
    fi
}

# Redis 설정 파일 생성 (클러스터 모드만 활성화)
create_redis_config() {
    local config_file="/usr/local/etc/redis/redis.conf"
    
    # 기본 Redis 설정
    cat > "$config_file" << EOF
# Redis 기본 설정  
port $REDIS_PORT
bind 0.0.0.0

# 클러스터 설정
cluster-enabled yes
cluster-announce-ip $SELF_IP
cluster-announce-port $REDIS_PORT
cluster-announce-bus-port $((REDIS_PORT + 10000))
cluster-use-empty-masters yes

# 로그 설정
logfile /logs/redis.log
loglevel notice
EOF

    # MaxMemory 설정 (선택)
    if [ -n "$REDIS_MAXMEMORY" ]; then
        echo "maxmemory $REDIS_MAXMEMORY" >> "$config_file"
        # 기본 정책: allkeys-lfu (LFU 는 인기/빈도 기반, 캐시 워크로드에서 더 일관된 hit ratio)
        echo "maxmemory-policy allkeys-lfu" >> "$config_file"
    fi

    log_info "Redis 설정 파일 생성됨: $config_file"
}

# 주 실행 함수 (매우 단순화 - 컨트롤러가 클러스터 관리)
main() {
    log_info "Redis 엔트리포인트 시작"
    
    # 파드 정보 파싱
    parse_pod_info
    
    # 노드 역할 결정
    ROLE=$(get_node_role $POD_INDEX $REDIS_MASTERS)
    log_info "노드 역할: $ROLE (인덱스: $POD_INDEX)"
    
    # Redis 설정 생성
    create_redis_config
    
    # 데이터 디렉토리 생성
    mkdir -p /data /logs
    chown redis:redis /data /logs
    
    # Redis 시작 (클러스터 관리는 컨트롤러가 담당)
    log_info "Redis 서버 시작..."
    exec redis-server /usr/local/etc/redis/redis.conf
}

# 스크립트 실행
main "$@"
