# redis-cluster

```
docker build -t redis-cluster:latest .
# docker login docker.direa.synology.me
docker tag redis-cluster:latest docker.direa.synology.me/redis-cluster:latest
docker push docker.direa.synology.me/redis-cluster:latest
```

```
redis-cli --cluster create \
{IP}:6379 \
...
{IP}:6379 --cluster-replicas 2
```

```
redis-cli -c
set test 123
get test
```
