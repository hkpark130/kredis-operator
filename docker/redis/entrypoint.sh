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

# Kubernetes 메모리 형식을 바이트로 변환
# 지원 형식: 100, 100k, 100Ki, 100M, 100Mi, 100G, 100Gi
convert_k8s_memory_to_bytes() {
    local mem="$1"
    local value
    local unit
    
    # 숫자와 단위 분리
    value=$(echo "$mem" | sed 's/[^0-9]//g')
    unit=$(echo "$mem" | sed 's/[0-9]//g')
    
    if [ -z "$value" ]; then
        echo "0"
        return
    fi
    
    case "$unit" in
        "")     echo "$value" ;;
        "k"|"K") echo $((value * 1000)) ;;
        "Ki")   echo $((value * 1024)) ;;
        "m"|"M") echo $((value * 1000 * 1000)) ;;
        "Mi")   echo $((value * 1024 * 1024)) ;;
        "g"|"G") echo $((value * 1000 * 1000 * 1000)) ;;
        "Gi")   echo $((value * 1024 * 1024 * 1024)) ;;
        *)      echo "$value" ;;  # 알 수 없는 단위는 그대로 반환
    esac
}

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

# 로그 설정
logfile /logs/redis.log
loglevel notice

# 캐시 모드 설정 - MISCONF 에러 방지
# RDB 저장 완전 비활성화 (캐시 용도이므로 persistence 불필요)
save ""

# AOF 비활성화 (캐시 용도)
appendonly no

# 디스크 저장 실패 시에도 쓰기 허용 (MISCONF stop-writes-on-bgsave-error 방지)
stop-writes-on-bgsave-error no
EOF

    # MaxMemory 설정 (선택)
    # Kubernetes 메모리 형식(예: 700Mi, 1Gi)을 Redis 바이트로 변환
    if [ -n "$REDIS_MAXMEMORY" ]; then
        MAXMEM_BYTES=$(convert_k8s_memory_to_bytes "$REDIS_MAXMEMORY")
        if [ "$MAXMEM_BYTES" -gt 0 ] 2>/dev/null; then
            echo "maxmemory $MAXMEM_BYTES" >> "$config_file"
            # 기본 정책: allkeys-lfu (LFU 는 인기/빈도 기반, 캐시 워크로드에서 더 일관된 hit ratio)
            echo "maxmemory-policy allkeys-lfu" >> "$config_file"
            log_info "MaxMemory 설정: $REDIS_MAXMEMORY -> ${MAXMEM_BYTES} bytes"
        else
            log_warn "MaxMemory 변환 실패: $REDIS_MAXMEMORY"
        fi
    fi

    log_info "Redis 설정 파일 생성됨: $config_file"
}

# 주 실행 함수 (매우 단순화 - 컨트롤러가 클러스터 관리)
main() {
    log_info "Redis 엔트리포인트 시작"
    
    # 데이터/로그 디렉토리 먼저 생성 (설정 파일에서 참조하기 전에)
    mkdir -p /data /logs /usr/local/etc/redis
    chown -R redis:redis /data /logs 2>/dev/null || true
    
    # 파드 정보 파싱
    parse_pod_info
    
    # 노드 역할 결정
    ROLE=$(get_node_role $POD_INDEX $REDIS_MASTERS)
    log_info "노드 역할: $ROLE (인덱스: $POD_INDEX)"
    
    # Redis 설정 생성
    create_redis_config
    
    # Redis 시작 (클러스터 관리는 컨트롤러가 담당)
    log_info "Redis 서버 시작..."
    exec redis-server /usr/local/etc/redis/redis.conf
}

# 스크립트 실행
main "$@"
