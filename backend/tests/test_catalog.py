from __future__ import annotations

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
