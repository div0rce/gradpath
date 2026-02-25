#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export DATABASE_URL="${DATABASE_URL:-sqlite:///${BACKEND_DIR}/gradpath.db}"
PORT="${PORT:-8000}"

cd "${BACKEND_DIR}"
uvicorn app.main:app --reload --port "${PORT}"
