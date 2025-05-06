#!/bin/bash

# CRD 업데이트
# make manifests install

# 이미지 빌드 및 푸시:
make docker-build docker-push IMG="192.168.0.33:9083/kredis-operator:latest"

# 컨트롤러 배포:
make deploy IMG="192.168.0.33:9083/kredis-operator:latest"

