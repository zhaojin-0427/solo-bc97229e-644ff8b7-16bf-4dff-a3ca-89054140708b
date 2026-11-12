"""Runtime configuration for timeline jobs.

Everything is read from environment variables once at import time so the same
container image can be pointed at any writable data volume.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

#: One decoded NDJSON line may not exceed this many bytes.
MAX_LINE_BYTES = 1 << 20  # 1 MiB

#: Uploads are streamed to disk in chunks of this size.
UPLOAD_CHUNK_BYTES = 1 << 20  # 1 MiB

#: Hard cap on the SQLite page cache for a single job's working database.
SQLITE_CACHE_BYTES = 20 * 1024 * 1024  # 20 MiB


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    upload_chunk_bytes: int
    max_line_bytes: int
    sqlite_cache_bytes: int
    worker_count: int


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def load_settings() -> Settings:
    return Settings(
        data_dir=Path(os.environ.get("TIMELINE_DATA_DIR", "/data")).resolve(),
        upload_chunk_bytes=UPLOAD_CHUNK_BYTES,
        max_line_bytes=MAX_LINE_BYTES,
        sqlite_cache_bytes=SQLITE_CACHE_BYTES,
        worker_count=_env_int("TIMELINE_WORKERS", 2),
    )
