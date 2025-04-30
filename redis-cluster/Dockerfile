FROM redis:7.0.5

RUN apt-get update && apt-get install -y curl jq

COPY conf/redis.conf /usr/local/etc/redis/redis.conf
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]