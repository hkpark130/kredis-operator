# kredis-operator
// TODO(user): Add simple overview of use/purpose

## Description
// TODO(user): An in-depth paragraph about your project and overview of use

## Getting Started

### Prerequisites
- go version v1.22.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/kredis-operator:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/kredis-operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following are the steps to build the installer and distribute this project to users.

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/kredis-operator:tag
```

NOTE: The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without
its dependencies.

2. Using the installer

Users can just run kubectl apply -f <URL for YAML BUNDLE> to install the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/kredis-operator/<tag or branch>/dist/install.yaml
```

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

---

# operator

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

