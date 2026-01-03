#!/usr/bin/env bash
set -euo pipefail

# 启用 Docker BuildKit 以加速构建
export DOCKER_BUILDKIT=1

# 配置 Docker daemon 使用国内镜像加速（如果需要）
# echo '{"registry-mirrors": ["https://registry.cn-hangzhou.aliyuncs.com"]}' | sudo tee /etc/docker/daemon.json > /dev/null
# sudo systemctl restart docker

# 检查参数
MODE="${1:-public}"
TAG="${2:-latest}"

# 镜像名称
IMAGE_NAME="quant_data_engine"

# 根据模式设置构建参数
if [ "$MODE" = "private" ]; then
    # 私有环境参数（例如内网环境）
    BUILD_ARGS=(
        --build-arg BASE_IMAGE=ubuntu:22.04
        --build-arg UBUNTU_SUITE=jammy
        --build-arg UBUNTU_MIRROR=http://internal-mirror.local/ubuntu
        --build-arg UBUNTU_SECURITY=http://internal-mirror.local/ubuntu-security
        --build-arg PIP_INDEX_URL=http://internal-pypi.local/simple
        --build-arg PIP_TRUSTED_HOST=internal-pypi.local
    )
    echo "Using private environment configuration"
else
    # 公共环境参数（默认）
    BUILD_ARGS=(
        --build-arg BASE_IMAGE=ubuntu:22.04
        --build-arg UBUNTU_SUITE=jammy
        --build-arg UBUNTU_MIRROR=http://mirrors.volces.com/ubuntu
        --build-arg UBUNTU_SECURITY=http://mirrors.volces.com/ubuntu-security
        --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
        --build-arg PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn
    )
    echo "Using public environment configuration"
fi

# 可选：使用缓存从之前的构建（如果有）
CACHE_FROM="--cache-from ${IMAGE_NAME}:latest"

echo "Building Docker image: ${IMAGE_NAME}:${TAG} for ${MODE} mode"
docker build \
    "${BUILD_ARGS[@]}" \
    -t "${IMAGE_NAME}:${TAG}" \
    .

echo "Build completed successfully!"