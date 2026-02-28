#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

docker run --rm -it \
	-p 5678:5678 \
	-e N8N_SECURE_COOKIE=false \
	-v "${REPO_ROOT}:/workspace/n8n-nodes-fastmail" \
	node:22-slim \
	/bin/bash -lc "cd /workspace/n8n-nodes-fastmail/testdata && ./update.sh"
