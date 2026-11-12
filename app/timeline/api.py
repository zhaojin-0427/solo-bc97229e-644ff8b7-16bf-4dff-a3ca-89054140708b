"""HTTP routes for timeline consolidation jobs.

``POST /timeline/jobs`` streams the upload straight to disk (only one chunk in
memory), then schedules a background pipeline. Results become downloadable
only after the job reaches ``done`` — a failed or still-running job answers
the download route with **409**, so a partial timeline can never be served.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, UploadFile
from fastapi.responses import FileResponse

from .config import load_settings
from .schemas import JobList, JobOut, JobStats
from .store import get_store

router = APIRouter(prefix="/timeline", tags=["timeline"])

_UPLOAD_MEDIA_TYPES = {
    "application/gzip",
    "application/x-gzip",
    "application/octet-stream",
}


def _to_job_out(row: dict) -> JobOut:
    return JobOut(
        job_id=row["job_id"],
        status=row["status"],
        error=row["error"],
        stats=JobStats(
            input_lines=row["input_lines"],
            unique_events=row["unique_events"],
            duplicate_events=row["duplicate_events"],
            awb_count=row["awb_count"],
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.post(
    "/jobs",
    response_model=JobOut,
    status_code=202,
    summary="Upload a gzip NDJSON scan export and start a timeline job",
)
def create_timeline_job(file: UploadFile) -> JobOut:
    if not file.filename:
        raise HTTPException(status_code=422, detail="multipart field 'file' is required")
    if file.content_type and file.content_type not in _UPLOAD_MEDIA_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"unsupported content type {file.content_type!r}; upload a gzip file",
        )

    store = get_store()
    settings = load_settings()
    job_id, input_path = store.create_job()
    try:
        # Stream chunks to disk: the full upload is never held in memory.
        with open(input_path, "wb") as fh:
            while True:
                chunk = file.file.read(settings.upload_chunk_bytes)
                if not chunk:
                    break
                fh.write(chunk)
    except OSError as exc:
        store.fail(job_id, "write_failed", str(exc))
        raise HTTPException(status_code=500, detail="failed to persist upload") from exc
    finally:
        file.file.close()

    store.submit(job_id)
    row = store.get(job_id)
    assert row is not None
    return _to_job_out(row)


@router.get("/jobs", response_model=JobList, summary="List timeline jobs")
def list_timeline_jobs() -> JobList:
    return JobList(jobs=[_to_job_out(row) for row in get_store().list_jobs()])


@router.get(
    "/jobs/{job_id}",
    response_model=JobOut,
    summary="Get one timeline job's status and error",
    responses={404: {"description": "Unknown job id"}},
)
def get_timeline_job(job_id: str) -> JobOut:
    row = get_store().get(job_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"unknown job {job_id!r}")
    return _to_job_out(row)


@router.get(
    "/jobs/{job_id}/download",
    summary="Download the finished gzip NDJSON timeline",
    response_class=FileResponse,
    responses={
        404: {"description": "Unknown job id"},
        409: {"description": "Job not finished successfully (pending/running/failed)"},
    },
)
def download_timeline(job_id: str) -> FileResponse:
    store = get_store()
    if store.get(job_id) is None:
        raise HTTPException(status_code=404, detail=f"unknown job {job_id!r}")
    path = store.result_path(job_id)
    if path is None:
        raise HTTPException(
            status_code=409,
            detail="timeline is not available: job has not finished successfully",
        )
    return FileResponse(
        path,
        media_type="application/gzip",
        filename="timeline.ndjson.gz",
    )
