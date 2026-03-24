#!/usr/bin/env bash
# 本地构建镜像的快速脚本，避免在命令行反复输入构建参数。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
IMAGE_TAG="${IMAGE_TAG:-sub2api:local}"

BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
COMMIT_VALUE="${COMMIT_VALUE:-$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || printf 'docker-local')}"
VERSION_VALUE="${VERSION_VALUE:-}"

DOCKER_BUILDKIT=${DOCKER_BUILDKIT:-1} docker build -t "${IMAGE_TAG}" \
    --build-arg GOPROXY="${GOPROXY:-https://goproxy.cn,direct}" \
    --build-arg GOSUMDB="${GOSUMDB:-sum.golang.google.cn}" \
    --build-arg COMMIT="${COMMIT_VALUE}" \
    --build-arg DATE="${BUILD_DATE}" \
    --build-arg VERSION="${VERSION_VALUE}" \
    -f "${REPO_ROOT}/Dockerfile" \
    "${REPO_ROOT}"

printf 'Built image: %s\n' "${IMAGE_TAG}"
