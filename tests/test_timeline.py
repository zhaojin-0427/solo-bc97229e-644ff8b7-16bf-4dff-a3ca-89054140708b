"""End-to-end tests for the timeline consolidation job API."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.timeline import processor
from app.timeline.config import Settings
from app.timeline.store import get_store, reset_store


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("TIMELINE_DATA_DIR", str(data_dir))
    settings = Settings(
        data_dir=data_dir,
        upload_chunk_bytes=1 << 16,
        max_line_bytes=1 << 20,
        sqlite_cache_bytes=2 * 1024 * 1024,
        worker_count=2,
    )
    reset_store(settings)
    with TestClient(app) as client:
        yield client
    get_store().shutdown()


def _gz_bytes(lines: list[str]) -> bytes:
    return gzip.compress(("\n".join(lines) + ("\n" if lines else "")).encode("utf-8"))


def _upload(client: TestClient, payload: bytes, filename: str = "scans.ndjson.gz"):
    return client.post(
        "/timeline/jobs",
        files={"file": (filename, payload, "application/gzip")},
    )


def _wait(client: TestClient, job_id: str) -> dict:
    for _ in range(500):
        body = client.get(f"/timeline/jobs/{job_id}").json()
        if body["status"] in ("done", "failed"):
            return body
        import time

        time.sleep(0.02)
    raise AssertionError("job did not reach a terminal state")


def _download_timeline(client: TestClient, job_id: str) -> list[dict]:
    resp = client.get(f"/timeline/jobs/{job_id}/download")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == "application/gzip"
    text = gzip.decompress(resp.content).decode("utf-8")
    return [json.loads(line) for line in text.splitlines()]


# ---------------------------------------------------------------------------
# happy path: disordered input -> per-awb sorted timeline
# ---------------------------------------------------------------------------


def test_disordered_scans_become_sorted_per_awb_timeline(client: TestClient):
    records = [
        # awb B first in the file, awb A later -> output must be A then B
        {"awb": "B", "scan_id": "s4", "airport": "LAX", "utc": 200},
        {"awb": "B", "scan_id": "s2", "airport": "ORD", "utc": 100},
        {"awb": "A", "scan_id": "s3", "airport": "JFK", "utc": 150},
        {"awb": "A", "scan_id": "s1", "airport": "PVG", "utc": 50},
        # same instant as s1 -> tie broken by scan_id (s1 before s5)
        {"awb": "A", "scan_id": "s5", "airport": "HKG", "utc": 50},
        # ISO-8601 UTC string form, all fields must be preserved verbatim
        {"awb": "A", "scan_id": "s6", "airport": "SIN", "utc": "2024-01-01T00:00:01Z"},
    ]
    resp = _upload(client, _gz_bytes([json.dumps(r) for r in records]))
    assert resp.status_code == 202, resp.text
    job = resp.json()
    assert job["status"] in ("pending", "running")
    assert job["error"] is None

    final = _wait(client, job["job_id"])
    assert final["status"] == "done", final
    assert final["stats"] == {
        "input_lines": 6,
        "unique_events": 6,
        "duplicate_events": 0,
        "awb_count": 2,
    }

    timeline = _download_timeline(client, job["job_id"])
    assert [row["awb"] for row in timeline] == ["A", "B"]

    a_events = timeline[0]["events"]
    assert [e["scan_id"] for e in a_events] == ["s1", "s5", "s3", "s6"]
    # integer utc preserved as integer; ISO string preserved as string
    assert a_events[0] == {"awb": "A", "scan_id": "s1", "airport": "PVG", "utc": 50}
    assert a_events[-1]["utc"] == "2024-01-01T00:00:01Z"
    assert all(set(e) == {"awb", "scan_id", "airport", "utc"} for e in a_events)

    assert [e["scan_id"] for e in timeline[1]["events"]] == ["s2", "s4"]


def test_exact_duplicate_rows_collapse_to_one(client: TestClient):
    row = {"awb": "A", "scan_id": "dup", "airport": "JFK", "utc": 10}
    resp = _upload(client, _gz_bytes([json.dumps(row), json.dumps(row), json.dumps(row)]))
    job = _wait(client, resp.json()["job_id"])
    assert job["status"] == "done"
    assert job["stats"]["input_lines"] == 3
    assert job["stats"]["unique_events"] == 1
    assert job["stats"]["duplicate_events"] == 2

    timeline = _download_timeline(client, job["job_id"])
    assert len(timeline) == 1
    assert len(timeline[0]["events"]) == 1


def test_same_scan_id_different_content_fails_whole_job(client: TestClient):
    rows = [
        {"awb": "A", "scan_id": "x", "airport": "JFK", "utc": 10},
        {"awb": "A", "scan_id": "x", "airport": "LAX", "utc": 10},  # airport differs
    ]
    resp = _upload(client, _gz_bytes([json.dumps(r) for r in rows]))
    job = _wait(client, resp.json()["job_id"])
    assert job["status"] == "failed"
    assert job["error"]["code"] == "duplicate_conflict"

    # No partial result may ever be downloadable.
    dl = client.get(f"/timeline/jobs/{job['job_id']}/download")
    assert dl.status_code == 409
    from app.timeline.store import get_store

    result = get_store().settings.data_dir / job["job_id"] / "timeline.ndjson.gz"
    assert not result.exists()


# ---------------------------------------------------------------------------
# malformed input -> job fails, result never published
# ---------------------------------------------------------------------------


def test_malformed_json_line_fails_job(client: TestClient):
    payload = gzip.compress(b'{"awb":"A","scan_id":"s1","airport":"JFK","utc":1}\nnot-json\n')
    job = _wait(client, _upload(client, payload).json()["job_id"])
    assert job["status"] == "failed"
    assert job["error"]["code"] == "invalid_input"
    assert "line 2" in job["error"]["message"]
    assert client.get(f"/timeline/jobs/{job['job_id']}/download").status_code == 409


def test_missing_and_extra_fields_rejected(client: TestClient):
    bad_rows = [
        b'{"awb":"A","scan_id":"s1","airport":"JFK"}',  # missing utc
        b'{"awb":"A","scan_id":"s1","airport":"JFK","utc":1,"extra":2}',  # extra field
        b'{"awb":"","scan_id":"s1","airport":"JFK","utc":1}',  # empty string
        b'{"awb":"A","scan_id":"s1","airport":"JFK","utc":1.5}',  # fractional
        b'{"awb":"A","scan_id":"s1","airport":"JFK","utc":"2024-01-01T00:00:00.000Z"}',
        b'{"awb":"A","scan_id":"s1","airport":"JFK","utc":"2024-01-01T00:00:00"}',  # naive
        b'{"awb":"A","scan_id":"s1","airport":"JFK","utc":"2024-01-01T00:00:00+08:00"}',
        b'{"awb":"A","scan_id":"s1","airport":"JFK","utc":true}',  # bool
    ]
    for row in bad_rows:
        job = _wait(client, _upload(client, gzip.compress(row + b"\n")).json()["job_id"])
        assert job["status"] == "failed", row
        assert job["error"]["code"] == "invalid_input", row


def test_corrupt_gzip_fails_job(client: TestClient):
    payload = gzip.compress(b'{"awb":"A","scan_id":"s1","airport":"JFK","utc":1}\n')
    payload = payload[: len(payload) // 2]  # truncated member
    job = _wait(client, _upload(client, payload).json()["job_id"])
    assert job["status"] == "failed"
    assert job["error"]["code"] == "gzip_corrupt"
    assert client.get(f"/timeline/jobs/{job['job_id']}/download").status_code == 409


def test_non_gzip_and_zero_byte_upload_fail_as_gzip(client: TestClient):
    for payload in (b"", b"not a gzip stream at all"):
        job = _wait(client, _upload(client, payload).json()["job_id"])
        assert job["status"] == "failed", payload
        assert job["error"]["code"] == "gzip_corrupt", payload


def test_out_of_range_epoch_rejected(client: TestClient):
    row = json.dumps({"awb": "A", "scan_id": "s1", "airport": "JFK", "utc": 10**30})
    job = _wait(client, _upload(client, gzip.compress(row.encode())).json()["job_id"])
    assert job["status"] == "failed"
    assert job["error"]["code"] == "invalid_input"


def test_empty_upload_produces_empty_gzip_result(client: TestClient):
    job = _wait(client, _upload(client, gzip.compress(b"")).json()["job_id"])
    assert job["status"] == "done"
    assert job["stats"] == {
        "input_lines": 0,
        "unique_events": 0,
        "duplicate_events": 0,
        "awb_count": 0,
    }
    assert _download_timeline(client, job["job_id"]) == []


# ---------------------------------------------------------------------------
# streaming / memory-bound behaviour with one very large awb
# ---------------------------------------------------------------------------


def test_single_huge_awb_streams_without_materializing(client: TestClient, monkeypatch):
    # 20k events all on one awb: the pipeline must not collect that awb's
    # events in memory. We assert ordering and the emitted payload, while the
    # design (lazy SQLite cursor + fixed output buffer) keeps memory bounded.
    n = 20_000
    lines = [
        json.dumps({"awb": "AWB-BIG", "scan_id": f"s{i:06d}", "airport": "X", "utc": n - i})
        for i in range(n)
    ]
    job = _wait(client, _upload(client, _gz_bytes(lines)).json()["job_id"])
    assert job["status"] == "done", job
    timeline = _download_timeline(client, job["job_id"])
    assert len(timeline) == 1
    events = timeline[0]["events"]
    assert len(events) == n
    epochs = [e["utc"] for e in events]
    assert epochs == sorted(epochs)
    assert events[0]["scan_id"] == f"s{n - 1:06d}"
    assert events[-1]["scan_id"] == "s000000"


def test_write_failure_at_publish_marks_job_failed(client: TestClient, monkeypatch):
    # Simulate a disk failure at the final atomic publish step.
    def boom(src, dst):
        raise OSError("simulated ENOSPC")

    monkeypatch.setattr(processor.os, "replace", boom)
    row = {"awb": "A", "scan_id": "s1", "airport": "JFK", "utc": 1}
    job = _wait(client, _upload(client, _gz_bytes([json.dumps(row)])).json()["job_id"])
    assert job["status"] == "failed"
    assert job["error"]["code"] == "write_failed"
    assert client.get(f"/timeline/jobs/{job['job_id']}/download").status_code == 409


# ---------------------------------------------------------------------------
# job listing, unknown ids, upload validation
# ---------------------------------------------------------------------------


def test_jobs_listing_and_unknown_job_404(client: TestClient):
    row = {"awb": "A", "scan_id": "s1", "airport": "JFK", "utc": 1}
    job_id = _upload(client, _gz_bytes([json.dumps(row)])).json()["job_id"]
    _wait(client, job_id)

    listing = client.get("/timeline/jobs")
    assert listing.status_code == 200
    assert any(j["job_id"] == job_id and j["status"] == "done" for j in listing.json()["jobs"])

    assert client.get("/timeline/jobs/nope").status_code == 404
    assert client.get("/timeline/jobs/nope/download").status_code == 404


def test_upload_requires_file_field(client: TestClient):
    resp = client.post("/timeline/jobs", files={"other": ("x.gz", b"", "application/gzip")})
    assert resp.status_code == 422


def test_unsupported_content_type_rejected(client: TestClient):
    resp = client.post(
        "/timeline/jobs",
        files={"file": ("x.txt", b"hello", "text/plain")},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# restart recovery: unfinished jobs fail, finished jobs stay downloadable
# ---------------------------------------------------------------------------


def test_restart_marks_unfinished_failed_and_keeps_finished(
    client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    row = {"awb": "A", "scan_id": "s1", "airport": "JFK", "utc": 1}
    done_id = _wait(client, _upload(client, _gz_bytes([json.dumps(row)])).json()["job_id"])["job_id"]

    # Simulate a job that died mid-flight: registry says running, no output.
    from app.timeline.store import get_store

    store = get_store()
    stuck_id, stuck_input = store.create_job()
    store.mark_running(stuck_id)
    stuck_input.write_bytes(gzip.compress(b""))
    result_file = store.settings.data_dir / done_id / "timeline.ndjson.gz"
    assert result_file.is_file()

    # Rebuild the store over the same directory, exactly like a process restart.
    monkeypatch.setenv("TIMELINE_DATA_DIR", str(store.settings.data_dir))
    reset_store(Settings(
        data_dir=store.settings.data_dir,
        upload_chunk_bytes=1 << 16,
        max_line_bytes=1 << 20,
        sqlite_cache_bytes=2 * 1024 * 1024,
        worker_count=2,
    ))

    stuck = client.get(f"/timeline/jobs/{stuck_id}").json()
    assert stuck["status"] == "failed"
    assert stuck["error"]["code"] == "interrupted"
    assert client.get(f"/timeline/jobs/{stuck_id}/download").status_code == 409

    done = client.get(f"/timeline/jobs/{done_id}").json()
    assert done["status"] == "done"
    assert client.get(f"/timeline/jobs/{done_id}/download").status_code == 200


# ---------------------------------------------------------------------------
# the original endpoint keeps working
# ---------------------------------------------------------------------------


def test_normalize_endpoint_still_available(client: TestClient):
    from app.main import DEMO_DST_FALLBACK

    resp = client.post("/normalize", json=DEMO_DST_FALLBACK)
    assert resp.status_code == 200
    assert resp.json()["summary"]["total"] == 5
