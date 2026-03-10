#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CUSTOM_ROOT="${REPO_ROOT}/.testdata/custom"
PACKAGE_DIR="${CUSTOM_ROOT}/n8n-nodes-fastmail"

cd "${REPO_ROOT}"
echo "[1/4] Syncing lockfile version..."
npm version "$(node -p "require('./package.json').version")" --no-git-tag-version --allow-same-version >/dev/null

echo "[2/4] Building node package..."
npm run build

rm -rf "${PACKAGE_DIR}"
mkdir -p "${PACKAGE_DIR}"
cp "${REPO_ROOT}/package.json" "${PACKAGE_DIR}/package.json"
cp -R "${REPO_ROOT}/dist" "${PACKAGE_DIR}/dist"
echo "[3/4] Prepared custom package at ${PACKAGE_DIR}"

DOCKER_TTY_FLAGS=()
if [[ -t 0 && -t 1 ]]; then
  DOCKER_TTY_FLAGS=(-it)
fi

echo "[4/4] Starting n8n..."
docker run --rm "${DOCKER_TTY_FLAGS[@]}" \
  -p 5678:5678 \
  -e N8N_SECURE_COOKIE=false \
  -e N8N_CUSTOM_EXTENSIONS=/home/node/.n8n/custom \
  -v "${CUSTOM_ROOT}:/home/node/.n8n/custom" \
  n8nio/n8n:latest
