"""Request/response models for the timeline consolidation API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class JobStats(BaseModel):
    input_lines: int | None = Field(default=None, description="Records read from the upload.")
    unique_events: int | None = Field(default=None, description="Events kept after global scan_id dedup.")
    duplicate_events: int | None = Field(default=None, description="Identical repeats collapsed into one.")
    awb_count: int | None = Field(default=None, description="Distinct waybills in the timeline output.")


class JobError(BaseModel):
    code: Literal[
        "invalid_input",
        "gzip_corrupt",
        "duplicate_conflict",
        "write_failed",
        "interrupted",
        "internal_error",
    ]
    message: str


class JobOut(BaseModel):
    job_id: str
    status: Literal["pending", "running", "done", "failed"]
    error: JobError | None = None
    stats: JobStats
    created_at: str = Field(description="UTC creation time, ISO-8601.")
    updated_at: str = Field(description="UTC last-status-change time, ISO-8601.")


class JobList(BaseModel):
    jobs: list[JobOut]
