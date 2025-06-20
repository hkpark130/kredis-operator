# Redis Docker 빌드 및 넥서스 푸시 스크립트

# 변수 설정
REGISTRY="docker.direa.synology.me"
IMAGE_NAME="redis-cluster"
TAG="6.2"
FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}:${TAG}"

# 로그인 (시스템에 이미 도커 로그인이 되어있다면 생략 가능)
# docker login ${REGISTRY} -u 100 -p jiin

# 이미지 빌드
echo "Redis 이미지 빌드 중..."
docker build -t ${FULL_IMAGE_NAME} ./docker/redis

# 이미지 푸시
echo "넥서스 레지스트리에 이미지 푸시 중..."
docker push ${FULL_IMAGE_NAME}

echo "완료! 이미지가 ${FULL_IMAGE_NAME}에 푸시되었습니다."
