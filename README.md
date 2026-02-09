# GradPath

Implementation of the Rutgers Degree Planner migration plan with:

- FastAPI backend (`backend/`)
- Next.js frontend scaffold (`apps/web/`)
- Prisma schema contract (`prisma/schema.prisma`)

## What is implemented

### Backend

- Snapshot lifecycle APIs:
  - `POST /v1/catalog/snapshots:stage`
  - `POST /v1/catalog/snapshots:stage-from-csv`
  - `POST /v1/catalog/snapshots/{snapshotId}:promote`
  - `GET /v1/catalog/snapshots/active`
- Course search:
  - `GET /v1/catalog/courses/search`
- Plan APIs:
  - `POST /v1/plans`
  - `POST /v1/plans/{planId}/items:validate`
  - `PUT /v1/plans/{planId}/items/{itemId}`
  - `POST /v1/plans/{planId}/recompute-audit`
  - `GET /v1/plans/{planId}/audit/latest`
  - `POST /v1/plans/{planId}/finalize`
- Core rules:
  - course canonicalization via regex `\d{2}:\d{3}:\d{3}`
  - snapshot-pinned validation
  - offering checks per term instance
  - v1 prereq execution supports `course` and `all`; `any`/`countAtLeast` become unsupported for validation
  - summer same-term-above rule requires `completion_status == YES`
  - save-invalid allowed, finalize blocked on invalid/unsupported

### Frontend scaffold

- Routes:
  - `/catalog`
  - `/planner/[planId]`
  - `/degree-tracker/[planId]`
  - `/progress/[planId]`
- Backend wiring through `NEXT_PUBLIC_API_BASE`

## Quickstart (One Env, One Flow)

### 1) One-time setup

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run API

```bash
cd backend
source .venv/bin/activate
uvicorn app.main:app --reload --port 8000
```

Health check:

```bash
curl -s http://localhost:8000/health
```

### 3) Run tests

```bash
cd backend
source .venv/bin/activate
pytest -q
```

### 4) Run end-to-end dev flow

In a second terminal (while API is running):

```bash
cd backend
source .venv/bin/activate
chmod +x scripts/dev_flow.sh
scripts/dev_flow.sh
```

Optional env overrides:

```bash
API=http://localhost:8000 DEV_NETID=dev123 DEV_EMAIL=dev123@rutgers.edu scripts/dev_flow.sh
```

### Frontend

```bash
cd apps/web
npm install
NEXT_PUBLIC_API_BASE=http://localhost:8000 npm run dev
```

## Troubleshooting

- `POST /v1/plans/{planId}:ready` returns 404:
  - confirm route is loaded:
    ```bash
    curl -s http://localhost:8000/openapi.json | jq -r '.paths | keys[]' | grep ':ready'
    ```
  - if missing, restart `uvicorn`.

- Local DB reset for clean-room testing:
  ```bash
  cd backend
  rm -f gradpath.db
  source .venv/bin/activate
  python - <<'PY'
  from app.db import Base, engine
  Base.metadata.create_all(bind=engine)
  print("fresh db ready")
  PY
  ```

## Notes

- Default backend DB is SQLite (`backend/gradpath.db`) for local development.
- The ingest adapter interface exists (`RegistrarFeedAdapter`) with placeholders for:
  - `DepartmentCSVAdapter`
  - `SOCExportAdapter`
  - `SISAdapter`
- `prisma/schema.prisma` is included as the canonical relational contract for production PostgreSQL migrations.
