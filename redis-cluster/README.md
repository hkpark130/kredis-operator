# redis-cluster

```
docker build 
push :latest
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
