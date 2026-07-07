#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-smg-crd-auth-test}"
RELEASE="${RELEASE:-smg-crd-auth-test}"
IMAGE_REPO="${IMAGE_REPO:-smg}"
IMAGE_TAG="${IMAGE_TAG:-crd-auth-test}"
AUTH_URL="${AUTH_URL:-http://regional-auth-backend.${NAMESPACE}.svc.cluster.local:8080/ext-auth}"
MODEL_SELECTOR_HEADER="${MODEL_SELECTOR_HEADER:-x-ai-eg-model}"
CHART_DIR="${CHART_DIR:-deploy/helm/smg}"
DOCKERFILE="${DOCKERFILE:-docker/Dockerfile.fast}"
DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-1}"
BUILD_CACHE_DIR="${BUILD_CACHE_DIR:-.cache/docker-buildx/smg-crd-auth-test}"
USE_BUILDX_CACHE="${USE_BUILDX_CACHE:-auto}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

build_image() {
  local image="${IMAGE_REPO}:${IMAGE_TAG}"

  if [[ "${USE_BUILDX_CACHE}" != "0" ]] && docker buildx version >/dev/null 2>&1; then
    mkdir -p "${BUILD_CACHE_DIR}"
    local next_cache="${BUILD_CACHE_DIR}.next"
    rm -rf "${next_cache}"

    DOCKER_BUILDKIT="${DOCKER_BUILDKIT}" docker buildx build \
      --load \
      --cache-from "type=local,src=${BUILD_CACHE_DIR}" \
      --cache-to "type=local,dest=${next_cache},mode=max" \
      -t "${image}" \
      -f "${DOCKERFILE}" \
      .

    rm -rf "${BUILD_CACHE_DIR}"
    mv "${next_cache}" "${BUILD_CACHE_DIR}"
    return
  fi

  DOCKER_BUILDKIT="${DOCKER_BUILDKIT}" docker build -t "${image}" -f "${DOCKERFILE}" .
}

build_image

if command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -qx "$(kubectl config current-context 2>/dev/null | sed 's/^kind-//')"; then
  kind load docker-image "${IMAGE_REPO}:${IMAGE_TAG}" --name "$(kubectl config current-context | sed 's/^kind-//')"
fi

kubectl apply --server-side -f "${CHART_DIR}/crds/smgworkers.yaml"
kubectl apply --server-side -f "${CHART_DIR}/crds/smggateways.yaml"
kubectl apply --server-side -f "${CHART_DIR}/crds/smgsecuritypolicies.yaml"

if [[ "${APPLY_EXAMPLE_RESOURCES:-0}" == "1" ]]; then
  kubectl apply --server-side -f "${CHART_DIR}/examples/aip-selfheal-test-cr.yaml"
fi

helm upgrade --install "${RELEASE}" "${CHART_DIR}" \
  -n "${NAMESPACE}" \
  --create-namespace \
  --set global.image.registry="" \
  --set global.image.repository="${IMAGE_REPO}" \
  --set global.image.tag="${IMAGE_TAG}" \
  --set global.image.pullPolicy=IfNotPresent \
  --set router.policy=weighted_sticky \
  --set router.serviceDiscovery.crds.enabled=true \
  --set router.serviceDiscovery.namespace="${NAMESPACE}" \
  --set router.securityPolicies.enabled=true \
  --set router.securityPolicies.gatewayName="${SMG_GATEWAY_NAME:-selfheal-test}" \
  --set router.extAuth.url="${AUTH_URL}" \
  --set router.extAuth.timeoutMs=500 \
  --set router.extAuth.failOpenOnTransportError=false \
  --set router.modelSelector.headerName="${MODEL_SELECTOR_HEADER}" \
  --set router.service.port=30000

kubectl -n "${NAMESPACE}" rollout status deploy/"${RELEASE}-router" --timeout=180s
kubectl -n "${NAMESPACE}" get pods,svc -l app.kubernetes.io/instance="${RELEASE}"
