#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
N8N_CUSTOM_DIR="${N8N_CUSTOM_EXTENSIONS:-/home/node/.n8n/custom}"
PACKAGE_LINK="${N8N_CUSTOM_DIR}/n8n-nodes-fastmail"
N8N_VERSION="${N8N_VERSION:-latest}"

mkdir -p "${N8N_CUSTOM_DIR}"
if [[ -L "${PACKAGE_LINK}" || -d "${PACKAGE_LINK}" ]]; then
  rm -rf "${PACKAGE_LINK}"
fi
ln -sfn "${REPO_ROOT}" "${PACKAGE_LINK}"

cd "${REPO_ROOT}"
npm install
npm run build

npm install -g "n8n@${N8N_VERSION}"
n8n start
