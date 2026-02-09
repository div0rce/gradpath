#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f ".venv/bin/activate" ]]; then
  echo "Missing backend/.venv. Run setup first:"
  echo "  cd backend && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

source .venv/bin/activate

API="${API:-http://localhost:8000}"

if ! curl -fsS "$API/health" >/dev/null; then
  echo "API is not reachable at $API. Start it first:"
  echo "  cd backend && source .venv/bin/activate && uvicorn app.main:app --reload --port 8000"
  exit 1
fi

eval "$(python scripts/dev_seed.py)"

PLAN_ID=$(curl -fsS -X POST "$API/v1/plans" \
  -H "Content-Type: application/json" \
  -d "{\"user_id\":\"$USER_ID\",\"program_version_id\":\"$PROGRAM_VERSION_ID\",\"name\":\"Dev Plan\"}" \
  | python -c 'import sys,json; print(json.load(sys.stdin)["plan_id"])')

ITEM1=$(uuidgen | tr '[:upper:]' '[:lower:]')
ITEM2=$(uuidgen | tr '[:upper:]' '[:lower:]')

echo "Using IDs:"
echo "  USER_ID=$USER_ID"
echo "  PROGRAM_VERSION_ID=$PROGRAM_VERSION_ID"
echo "  TERM_ID=$TERM_ID"
echo "  PLAN_ID=$PLAN_ID"

echo
echo "PUT item 1..."
curl -fsS -X PUT "$API/v1/plans/$PLAN_ID/items/$ITEM1" \
  -H "Content-Type: application/json" \
  -d "{\"term_id\":\"$TERM_ID\",\"position\":1,\"raw_input\":\"14:540:100\",\"completion_status\":\"YES\"}" | python -m json.tool

echo
echo "PUT item 2..."
curl -fsS -X PUT "$API/v1/plans/$PLAN_ID/items/$ITEM2" \
  -H "Content-Type: application/json" \
  -d "{\"term_id\":\"$TERM_ID\",\"position\":2,\"raw_input\":\"14:540:200\",\"completion_status\":\"YES\"}" | python -m json.tool

echo
echo "POST :ready..."
curl -fsS -X POST "$API/v1/plans/$PLAN_ID:ready" | python -m json.tool

echo
echo "POST /finalize..."
FINALIZE=$(curl -fsS -X POST "$API/v1/plans/$PLAN_ID/finalize")
echo "$FINALIZE" | python -m json.tool

echo
echo "Done:"
echo "  PLAN_ID=$PLAN_ID"
echo "  API=$API"
