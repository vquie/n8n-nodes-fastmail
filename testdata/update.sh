#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
N8N_CUSTOM_DIR="/root/.n8n/custom"

mkdir -p "${N8N_CUSTOM_DIR}"
ln -sfn "${REPO_ROOT}" "${N8N_CUSTOM_DIR}/n8n-nodes-fastmail"

cd "${REPO_ROOT}"
npm install
npm run build

npm install -g n8n
n8n start
