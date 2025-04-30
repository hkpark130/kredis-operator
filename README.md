[operator-sdk go](https://sdk.operatorframework.io/docs/building-operators/golang/tutorial/)

# Operator SDK 설치

```
# 운영체제(OS) 및 아키텍처 설정
export ARCH=$(case $(uname -m) in x86_64) echo -n amd64 ;; aarch64) echo -n arm64 ;; *) echo -n $(uname -m) ;; esac)
export OS=$(uname | awk '{print tolower($0)}')

# Operator SDK 다운로드 URL 설정 (최신 버전 v1.38.0 예시)
export OPERATOR_SDK_DL_URL=https://github.com/operator-framework/operator-sdk/releases/download/v1.38.0

# 바이너리 다운로드
curl -LO ${OPERATOR_SDK_DL_URL}/operator-sdk_${OS}_${ARCH}

# 실행 권한 추가 및 설치
chmod +x operator-sdk_${OS}_${ARCH}
sudo mv operator-sdk_${OS}_${ARCH} /usr/local/bin/operator-sdk
```

# helm 패키징

```

```

# operator 생성
```
operator-sdk init --domain docker.direa.synology.me --repo github.com/hkpark130/kredis-operator
```

# controller 생성
```
operator-sdk create api --group stable --version v1alpha1 --kind KRedis --resource --controller
```

# CRD manifests 생성
```
make manifests
```

# build, push operator docker image to docker repo
```
make docker-build docker-push IMG="docker.direa.synology.me/kredis-operator:latest"
---
# (M1 환경에서)
docker buildx build --platform linux/amd64 -t park-operator .
docker tag park-operator public.ecr.aws/x8r0y3u4/park-operator:latest
docker push public.ecr.aws/x8r0y3u4/park-operator:latest
```

# docker secret 배포 (Optional)
```
kubectl apply -f ~/hkpark/docker-secret.yml -n kredis-operator-system
```

# Deploy the controller to the cluster
```
make deploy IMG=docker.direa.synology.me/kredis-operator:latest
```

# Define & Install CRD on K8S Cluster
```
make generate install
```

------------------------------------------------

# To delete the CRDs from the cluster:
```
make uninstall
```

# UnDeploy the controller to the cluster:
```
make undeploy
```

# 오퍼레이터 수정후 반영까지
```
make docker-build docker-push IMG="docker.direa.synology.me/kredis-operator:latest"
make generate install
make deploy IMG=docker.direa.synology.me/kredis-operator:latest
```

------------------------------------------------

# Metrics-server

```
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

# Prometheus

```
kubectl apply -f ./prometheus/prometheus-config.yaml
kubectl apply -f ./prometheus/prometheus-deployment.yaml
kubectl apply -f prometheus/grafana.yaml
```

------------------------------------------------

# 에러 사항

인스턴스 타입마다 pod에 할당할 IP 개수 제한이 있음
[AWS EKS "0/3 nodes are available: 3 Too many pods" Error](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html#AvailableIpPerENI)

