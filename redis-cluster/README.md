# redis-cluster

```
docker build -t redis-cluster:latest .
# docker login docker.direa.synology.me
docker tag redis-cluster:latest docker.direa.synology.me/redis-cluster:latest
docker push docker.direa.synology.me/redis-cluster:latest
```

```
# redis-cli --cluster create \
            10.42.5.31:6379 \
            10.42.4.192:6379 \
            10.42.5.32:6379 \
            10.42.6.12:6379 \
            10.42.6.13:6379 \
            10.42.6.11:6379 \
            --cluster-replicas 1
>>> Performing hash slots allocation on 6 nodes...
Master[0] -> Slots 0 - 5460
Master[1] -> Slots 5461 - 10922
Master[2] -> Slots 10923 - 16383
Adding replica 10.42.6.13:6379 to 10.42.5.31:6379
Adding replica 10.42.6.11:6379 to 10.42.4.192:6379
Adding replica 10.42.6.12:6379 to 10.42.5.32:6379
M: a56b197f53884fdf0c60df5c01606f1fe2c09c49 10.42.5.31:6379
   slots:[0-5460] (5461 slots) master
M: 1cf11adcd747d99ebcd1b316815a1f9676d0ffd0 10.42.4.192:6379
   slots:[5461-10922] (5462 slots) master
M: 95085cc6a4df5e15258a68d5e0f158247d2f93f7 10.42.5.32:6379
   slots:[10923-16383] (5461 slots) master
S: b28a106fae90cd03a6fc7b3f20dbc02e49f9a788 10.42.6.12:6379
   replicates 95085cc6a4df5e15258a68d5e0f158247d2f93f7
S: 28f358ee0c954be8471f4a0706a4d28cf20f2181 10.42.6.13:6379
   replicates a56b197f53884fdf0c60df5c01606f1fe2c09c49
S: 3619037c6f66e2f27b742073b58dc54d63770dc1 10.42.6.11:6379
   replicates 1cf11adcd747d99ebcd1b316815a1f9676d0ffd0
Can I set the above configuration? (type 'yes' to accept): yes
>>> Nodes configuration updated
>>> Assign a different config epoch to each node
>>> Sending CLUSTER MEET messages to join the cluster
Waiting for the cluster to join

>>> Performing Cluster Check (using node 10.42.5.31:6379)
M: a56b197f53884fdf0c60df5c01606f1fe2c09c49 10.42.5.31:6379
   slots:[0-5460] (5461 slots) master
   1 additional replica(s)
S: 3619037c6f66e2f27b742073b58dc54d63770dc1 10.42.6.11:6379
   slots: (0 slots) slave
   replicates 1cf11adcd747d99ebcd1b316815a1f9676d0ffd0
M: 95085cc6a4df5e15258a68d5e0f158247d2f93f7 10.42.5.32:6379
   slots:[10923-16383] (5461 slots) master
   1 additional replica(s)
M: 1cf11adcd747d99ebcd1b316815a1f9676d0ffd0 10.42.4.192:6379
   slots:[5461-10922] (5462 slots) master
   1 additional replica(s)
S: b28a106fae90cd03a6fc7b3f20dbc02e49f9a788 10.42.6.12:6379
   slots: (0 slots) slave
   replicates 95085cc6a4df5e15258a68d5e0f158247d2f93f7
S: 28f358ee0c954be8471f4a0706a4d28cf20f2181 10.42.6.13:6379
   slots: (0 slots) slave
   replicates a56b197f53884fdf0c60df5c01606f1fe2c09c49
[OK] All nodes agree about slots configuration.
>>> Check for open slots...
>>> Check slots coverage...
[OK] All 16384 slots covered.
```

```
redis-cli -c 
# -c (클러스터 모드로 진입)
set test 123
get test
```

