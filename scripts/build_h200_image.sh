#!/usr/bin/env bash
set -euo pipefail

# Build an SMG engine image intended for H200 benchmark/runtime lanes.
#
# This uses docker/engine.Dockerfile, which installs SMG into an engine base
# image. The Dockerfile clones SMG by repo/ref, so set SMG_REPO and SMG_COMMIT
# explicitly when building feature branches.
#
# Examples:
#   LOCAL_SOURCE=1 scripts/build_h200_image.sh
#
#   SMG_REPO=https://github.com/thuanpham582002/smg \
#   SMG_COMMIT=my-branch \
#     scripts/build_h200_image.sh
#
#   ENGINE=vllm \
#   SMG_REPO=https://github.com/thuanpham582002/smg \
#   SMG_COMMIT=my-branch \
#     scripts/build_h200_image.sh

ENGINE="${ENGINE:-sglang}"
BACKEND="${BACKEND:-${ENGINE}}"
SMG_REPO="${SMG_REPO:-}"
SMG_COMMIT="${SMG_COMMIT:-}"
ENGINE_REPO="${ENGINE_REPO:-}"
ENGINE_COMMIT="${ENGINE_COMMIT:-latest}"
TAG="${TAG:-}"
PUSH="${PUSH:-0}"
LOCAL_SOURCE="${LOCAL_SOURCE:-0}"

case "${ENGINE}" in
  sglang)
    BASE_IMAGE_REF="${BASE_IMAGE_REF:-lmsysorg/sglang:v0.5.10}"
    ;;
  vllm)
    BASE_IMAGE_REF="${BASE_IMAGE_REF:-vllm/vllm-openai:v0.19.0}"
    ;;
  trtllm)
    BASE_IMAGE_REF="${BASE_IMAGE_REF:-}"
    ;;
  tgl)
    BASE_IMAGE_REF="${BASE_IMAGE_REF:-}"
    BACKEND="${BACKEND:-sglang}"
    ;;
  *)
    echo "ERROR: ENGINE must be one of: sglang, vllm, trtllm, tgl" >&2
    exit 1
    ;;
esac

if [[ -z "${BASE_IMAGE_REF}" ]]; then
  echo "ERROR: BASE_IMAGE_REF is required for ENGINE=${ENGINE}" >&2
  exit 1
fi

if [[ "${LOCAL_SOURCE}" != "1" && ( -z "${SMG_REPO}" || -z "${SMG_COMMIT}" ) ]]; then
  echo "ERROR: SMG_REPO and SMG_COMMIT are required because docker/engine.Dockerfile clones SMG." >&2
  echo "For local working tree builds, use: LOCAL_SOURCE=1 $0" >&2
  echo "Example: SMG_REPO=https://github.com/thuanpham582002/smg SMG_COMMIT=my-branch $0" >&2
  exit 1
fi

if [[ -z "${TAG}" ]]; then
  safe_base="${BASE_IMAGE_REF##*:}"
  safe_base="${safe_base//\//-}"
  if [[ "${LOCAL_SOURCE}" == "1" ]]; then
    safe_commit="local"
  else
    safe_commit="${SMG_COMMIT//\//-}"
  fi
  TAG="smg-h200-${ENGINE}-${safe_base}-${safe_commit}"
fi

echo "Building H200 image:"
echo "  tag:            ${TAG}"
echo "  engine:         ${ENGINE}"
echo "  backend:        ${BACKEND}"
echo "  base_image_ref: ${BASE_IMAGE_REF}"
echo "  local_source:   ${LOCAL_SOURCE}"
echo "  smg_repo:       ${SMG_REPO:-<local working tree>}"
echo "  smg_commit:     ${SMG_COMMIT:-<local working tree>}"
echo "  engine_repo:    ${ENGINE_REPO:-<base image>}"
echo "  engine_commit:  ${ENGINE_COMMIT}"

if [[ "${LOCAL_SOURCE}" == "1" ]]; then
  tmp_dockerfile="$(mktemp)"
  trap 'rm -f "${tmp_dockerfile}"' EXIT
  cat > "${tmp_dockerfile}" <<'DOCKERFILE'
ARG BASE_IMAGE_REF
FROM ${BASE_IMAGE_REF}

ARG ENGINE=sglang
ARG BACKEND
ARG ENGINE_REPO
ARG ENGINE_COMMIT=latest

ENV SMG_DEFAULT_BACKEND=${BACKEND:-${ENGINE}}

COPY . /opt/smg-src
COPY scripts/installation/ /tmp/scripts/

RUN bash /tmp/scripts/install-smg.sh /opt/smg-src

RUN case "${ENGINE}" in \
      vllm|sglang|trtllm|tgl) ;; \
      *) echo "ERROR: Unknown ENGINE '${ENGINE}'" >&2; exit 1 ;; \
    esac \
    && if [ -n "${ENGINE_REPO}" ]; then \
         git clone "${ENGINE_REPO}" /opt/engine-src \
         && if [ "${ENGINE_COMMIT}" != "latest" ]; then \
              ( cd /opt/engine-src && git checkout "${ENGINE_COMMIT}" ); \
            fi \
         && bash /tmp/scripts/install-${ENGINE}.sh /opt/engine-src; \
       fi

ENTRYPOINT ["smg"]
DOCKERFILE
  docker build \
    --build-arg "BASE_IMAGE_REF=${BASE_IMAGE_REF}" \
    --build-arg "ENGINE=${ENGINE}" \
    --build-arg "BACKEND=${BACKEND}" \
    --build-arg "ENGINE_REPO=${ENGINE_REPO}" \
    --build-arg "ENGINE_COMMIT=${ENGINE_COMMIT}" \
    -t "${TAG}" \
    -f "${tmp_dockerfile}" \
    .
else
  docker build \
    --build-arg "BASE_IMAGE_REF=${BASE_IMAGE_REF}" \
    --build-arg "ENGINE=${ENGINE}" \
    --build-arg "BACKEND=${BACKEND}" \
    --build-arg "ENGINE_REPO=${ENGINE_REPO}" \
    --build-arg "ENGINE_COMMIT=${ENGINE_COMMIT}" \
    --build-arg "SMG_REPO=${SMG_REPO}" \
    --build-arg "SMG_COMMIT=${SMG_COMMIT}" \
    -t "${TAG}" \
    -f docker/engine.Dockerfile \
    .
fi

if [[ "${PUSH}" == "1" ]]; then
  docker push "${TAG}"
fi
