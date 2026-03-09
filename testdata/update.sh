#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
N8N_CUSTOM_DIR="${N8N_CUSTOM_EXTENSIONS:-/home/node/.n8n/custom}"
PACKAGE_DIR="${N8N_CUSTOM_DIR}/n8n-nodes-fastmail"
N8N_VERSION="${N8N_VERSION:-latest}"
N8N_RUNTIME_DIR="${N8N_RUNTIME_DIR:-/home/node/.n8n-runtime}"
SKIP_NPM_INSTALL="${SKIP_NPM_INSTALL:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"
FULL_BUILD="${FULL_BUILD:-0}"
FORCE_N8N_INSTALL="${FORCE_N8N_INSTALL:-0}"

mkdir -p "${N8N_CUSTOM_DIR}"

cd "${REPO_ROOT}"

if [[ "${SKIP_NPM_INSTALL}" != "1" ]]; then
  if [[ ! -d "${REPO_ROOT}/node_modules" ]]; then
    npm install --no-audit --fund=false
  fi
fi

if [[ "${SKIP_BUILD}" != "1" ]]; then
  if [[ "${FULL_BUILD}" == "1" ]]; then
    npm run build
  else
    npm run build:dev
  fi
fi

# Create a clean custom package directory for n8n loader (no repo node_modules).
if [[ -d "${PACKAGE_DIR}" ]]; then
  rm -rf "${PACKAGE_DIR}"
fi
mkdir -p "${PACKAGE_DIR}"
cp "${REPO_ROOT}/package.json" "${PACKAGE_DIR}/package.json"
cp -R "${REPO_ROOT}/dist" "${PACKAGE_DIR}/dist"

mkdir -p "${N8N_RUNTIME_DIR}"
if [[ ! -f "${N8N_RUNTIME_DIR}/package.json" ]]; then
  npm --prefix "${N8N_RUNTIME_DIR}" init -y >/dev/null 2>&1
fi

if [[ "${FORCE_N8N_INSTALL}" == "1" || ! -x "${N8N_RUNTIME_DIR}/node_modules/.bin/n8n" ]]; then
  npm --prefix "${N8N_RUNTIME_DIR}" install --no-audit --fund=false "n8n@${N8N_VERSION}"
fi

"${N8N_RUNTIME_DIR}/node_modules/.bin/n8n" start
