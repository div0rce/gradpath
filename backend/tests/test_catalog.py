from __future__ import annotations

from pathlib import Path

from tests.helpers import stage_payload


def test_stage_promote_and_active_snapshot(client):
    stage = client.post("/v1/catalog/snapshots:stage", json=stage_payload())
    assert stage.status_code == 200, stage.text
    snapshot_id = stage.json()["snapshot_id"]

    promote = client.post(f"/v1/catalog/snapshots/{snapshot_id}:promote")
    assert promote.status_code == 200, promote.text
    assert promote.json()["status"] == "PUBLISHED"

    active = client.get("/v1/catalog/snapshots/active")
    assert active.status_code == 200
    assert active.json()["snapshot_id"] == snapshot_id

    search = client.get("/v1/catalog/courses/search", params={"q": "14:540"})
    assert search.status_code == 200
    assert len(search.json()) >= 3


def test_stage_rejects_empty_payload_with_multi_errors(client):
    payload = {
        "source": "DEPARTMENT_CSV",
        "checksum": "sha256:empty",
        "courses": [],
        "terms": [],
        "offerings": [],
        "rules": [],
        "programs": [],
    }
    res = client.post("/v1/catalog/snapshots:stage", json=payload)
    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["error_code"] == "STAGE_VALIDATION_ERROR"
    fields = {err["field"] for err in detail["errors"]}
    assert {"courses", "terms", "offerings", "programs"}.issubset(fields)


def test_stage_from_csv_returns_row_aware_parse_errors(client, tmp_path: Path):
    bundle = tmp_path
    (bundle / "courses.csv").write_text(
        "code,title,credits,active,category\n14:540:100,Intro,3,true,\n",
        encoding="utf-8",
    )
    (bundle / "terms.csv").write_text(
        "campus,code,year,season,starts_at,ends_at\nNB,2025SU,2025,SUMMER,,\n",
        encoding="utf-8",
    )
    (bundle / "offerings.csv").write_text(
        "course_code,term_code,campus,offered\n14:540:100,2025SU,NB,true\n",
        encoding="utf-8",
    )
    (bundle / "rules.csv").write_text(
        "course_code,kind,rule,notes\n14:540:100,PREREQ,{bad-json},\n",
        encoding="utf-8",
    )
    (bundle / "programs.csv").write_text(
        "code,name,campus,catalog_year,effective_from,effective_to,requirement_set_label\n"
        "ISE-BS,Industrial Engineering,NB,2025-2026,2025-01-01T00:00:00,,ISE-2025\n",
        encoding="utf-8",
    )
    (bundle / "program_requirements.csv").write_text(
        "program_code,requirement_set_label,orderIndex,label,rule\n"
        "ISE-BS,ISE-2025,1,Intro,{bad-json}\n",
        encoding="utf-8",
    )

    res = client.post(
        "/v1/catalog/snapshots:stage-from-csv",
        json={"bundle_dir": str(bundle), "checksum": "sha256:csv"},
    )
    assert res.status_code == 400
    detail = res.json()["detail"]
    assert detail["error_code"] == "CSV_PARSE_ERROR"
    assert any(err["file"] == "rules.csv" for err in detail["errors"])
    assert any(err["file"] == "program_requirements.csv" for err in detail["errors"])
