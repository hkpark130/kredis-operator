FROM redis:8.2-rc1

# 환경 변수 설정
ENV REDIS_EXPORTER_WEB_LISTEN_ADDRESS="0.0.0.0:9121"
ENV REDIS_EXPORTER_EXPORT_CLIENT_LIST="true"
ENV REDIS_EXPORTER_INCL_SYSTEM_METRICS="true"

# Redis 설정 파일 복사
COPY redis.conf /usr/local/etc/redis/redis.conf

# Redis Exporter 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && wget https://github.com/oliver006/redis_exporter/releases/download/v1.44.0/redis_exporter-v1.44.0.linux-amd64.tar.gz \
    && tar xzf redis_exporter-v1.44.0.linux-amd64.tar.gz \
    && mv redis_exporter-v1.44.0.linux-amd64/redis_exporter /usr/local/bin/ \
    && rm -rf redis_exporter-v1.44.0.linux-amd64 redis_exporter-v1.44.0.linux-amd64.tar.gz

# 스크립트 복사
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 6379 9121

ENTRYPOINT ["/entrypoint.sh"]
