#!/bin/bash

# CRD 업데이트
# make manifests install

# 이미지 빌드 및 푸시:
make docker-build docker-push IMG="docker.direa.synology.me/kredis-operator:latest"

# 컨트롤러 배포:
make deploy IMG="docker.direa.synology.me/kredis-operator:latest"

