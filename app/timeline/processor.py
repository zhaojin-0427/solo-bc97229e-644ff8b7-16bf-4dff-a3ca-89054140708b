"""Streaming scan-to-timeline pipeline.

The whole pipeline is bound by a small, fixed amount of memory regardless of
input size:

1. **Receive** — uploads are streamed to ``input.ndjson.gz`` in fixed chunks.
2. **Decompress / parse** — the gzip stream is read one NDJSON line at a time
   (each line is hard-capped at ``MAX_LINE_BYTES``); no full file is buffered.
3. **Deduplicate** — every parsed event is inserted into a per-job SQLite
   database whose ``scan_id`` column carries a unique index. SQLite keeps the
   index in B-tree pages on disk with a capped page cache, so the full set of
   IDs is never held in Python memory. On a unique-constraint violation the
   stored row is compared against the new one byte-for-byte: identical
   four-field rows are collapsed into one, any mismatch aborts the whole job.
4. **Sort** — a composite index ``(awb, utc_epoch, scan_id)`` lets an
   *incremental* cursor return events already ordered; the cursor is lazily
   evaluated, so a single awb with millions of events is never materialized.
5. **Write** — output gzip is produced row by row into a ``.tmp`` file and
   atomically renamed over the result only after the whole pipeline succeeds.
   A late format error, corrupt gzip member or disk failure therefore can never
   leave a downloadable partial result.
"""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO

from .config import Settings

INPUT_NAME = "input.ndjson.gz"
WORK_DB_NAME = "work.sqlite3"
OUTPUT_TMP_NAME = "timeline.ndjson.gz.tmp"
OUTPUT_NAME = "timeline.ndjson.gz"

REQUIRED_FIELDS = ("awb", "scan_id", "airport", "utc")
MAX_STR_LEN = 64
INSERT_BATCH = 500


# ---------------------------------------------------------------------------
# Errors — every one of these means the job fails, no result is published.
# ---------------------------------------------------------------------------


class PipelineError(Exception):
    """Base class for pipeline failures; ``code`` is exposed on the job."""

    code = "pipeline_error"


class InvalidInput(PipelineError):
    """The uploaded NDJSON does not match the required shape."""

    code = "invalid_input"


class GzipCorrupt(PipelineError):
    """The upload is not a valid gzip stream / cannot be fully decompressed."""

    code = "gzip_corrupt"


class DuplicateConflict(PipelineError):
    """A scan_id appears twice with different content."""

    code = "duplicate_conflict"


class WriteFailed(PipelineError):
    """Writing the result to durable storage failed."""

    code = "write_failed"


@dataclass
class PipelineStats:
    input_lines: int
    unique_events: int
    duplicate_events: int
    awb_count: int


# ---------------------------------------------------------------------------
# Bounded line reader
# ---------------------------------------------------------------------------


class _BoundedLineReader:
    """Yield gzip-decompressed NDJSON lines with a hard per-line byte cap.

    ``gzip.GzipFile.readline`` alone would grow without bound on a pathological
    line; chunks are therefore assembled manually so the cap is always enforced.
    """

    def __init__(self, stream: gzip.GzipFile, max_line_bytes: int):
        self._stream = stream
        self._max = max_line_bytes
        self._carry = b""

    def readline(self) -> bytes:
        parts: list[bytes] = []
        size = len(self._carry)
        chunk = self._carry
        self._carry = b""
        while True:
            nl = chunk.find(b"\n")
            if nl >= 0:
                parts.append(chunk[: nl + 1])
                size += nl + 1
                self._carry = chunk[nl + 1 :]
                break
            parts.append(chunk)
            size += len(chunk)
            if size > self._max:
                raise InvalidInput(
                    f"line exceeds the maximum length of {self._max} bytes"
                )
            chunk = self._stream.read(65536)
            if not chunk:
                break
        return b"".join(parts)


# ---------------------------------------------------------------------------
# Record validation
# ---------------------------------------------------------------------------


def _parse_utc(value: object, line_no: int) -> int:
    """Validate ``utc`` and return the UTC whole-second epoch used for sorting.

    Accepted forms: an integer epoch second, or an ISO-8601 string that carries
    an explicit UTC designator (``Z`` / ``+00:00``) and has no sub-second part.
    The original value is always preserved on the event for output.
    """

    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise InvalidInput(
            f"line {line_no}: utc must be an integer epoch second or an ISO-8601 UTC string"
        )
    if isinstance(value, int):
        return value
    text = value
    # Whole-second instants only: a fractional-second marker is rejected even
    # when the fraction is all zeros (e.g. "2024-01-01T00:00:00.000Z").
    if "." in text or "," in text:
        raise InvalidInput(f"line {line_no}: utc must have whole-second precision")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidInput(f"line {line_no}: utc is not a valid ISO-8601 date-time") from exc
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise InvalidInput(f"line {line_no}: utc must be an explicit UTC instant (Z or +00:00)")
    if parsed.microsecond != 0:
        raise InvalidInput(f"line {line_no}: utc must have whole-second precision")
    return int(parsed.timestamp())


def _parse_line(raw: bytes, line_no: int) -> dict:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidInput(f"line {line_no}: payload is not valid UTF-8") from exc

    text = text.removesuffix("\n").removesuffix("\r")
    if not text:
        raise InvalidInput(f"line {line_no}: empty line is not a valid record")

    try:
        record = json.loads(text)
    except json.JSONDecodeError as exc:
        raise InvalidInput(f"line {line_no}: malformed JSON: {exc.msg}") from exc
    if not isinstance(record, dict):
        raise InvalidInput(f"line {line_no}: each line must be a JSON object")
    if set(record) != set(REQUIRED_FIELDS):
        raise InvalidInput(
            f"line {line_no}: record must contain exactly the fields "
            f"awb, scan_id, airport, utc"
        )

    for field in ("awb", "scan_id", "airport"):
        value = record[field]
        if not isinstance(value, str) or not (1 <= len(value) <= MAX_STR_LEN):
            raise InvalidInput(
                f"line {line_no}: {field} must be a string of 1-{MAX_STR_LEN} characters"
            )

    record["_utc_epoch"] = _parse_utc(record["utc"], line_no)
    return record


# ---------------------------------------------------------------------------
# Ingest: stream -> SQLite
# ---------------------------------------------------------------------------


def _open_work_db(path: Path, cache_bytes: int) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    # Small, fixed cache so dedup/sort pressure spills to disk, not RAM.
    conn.execute(f"PRAGMA cache_size = -{max(cache_bytes // 1024, 2048)}")
    conn.execute("PRAGMA temp_store = FILE")
    conn.execute("PRAGMA journal_mode = DELETE")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute(
        """
        CREATE TABLE events (
            awb       TEXT NOT NULL,
            scan_id   TEXT NOT NULL UNIQUE,
            airport   TEXT NOT NULL,
            utc_raw   TEXT NOT NULL,
            utc_epoch INTEGER NOT NULL
        )
        """
    )
    return conn


def _ingest(
    fh: BinaryIO,
    conn: sqlite3.Connection,
    max_line_bytes: int,
) -> tuple[int, int, int]:
    """Read every event into SQLite. Returns (lines, unique, duplicates)."""
    # Reject anything that is not even a gzip member (including a 0-byte file).
    magic = fh.read(2)
    if magic != b"\x1f\x8b":
        raise GzipCorrupt("upload is not a gzip stream (missing gzip magic bytes)")
    fh.seek(0)

    lines = 0
    unique = 0
    duplicates = 0
    pending = 0
    with gzip.GzipFile(fileobj=fh, mode="rb") as gz_stream:
        reader = _BoundedLineReader(gz_stream, max_line_bytes)
        while True:
            try:
                raw = reader.readline()
            except (OSError, EOFError, gzip.BadGzipFile) as exc:
                raise GzipCorrupt(f"gzip stream is corrupt: {exc}") from exc
            if not raw:
                break
            lines += 1
            record = _parse_line(raw, lines)

            # Global per-file dedup on scan_id. Exact four-field repeat -> keep one;
            # same id with different content -> the entire job fails.
            try:
                conn.execute(
                    "INSERT INTO events (awb, scan_id, airport, utc_raw, utc_epoch) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        record["awb"],
                        record["scan_id"],
                        record["airport"],
                        json.dumps(record["utc"], ensure_ascii=False, separators=(",", ":")),
                        record["_utc_epoch"],
                    ),
                )
            except sqlite3.IntegrityError as exc:
                row = conn.execute(
                    "SELECT awb, airport, utc_raw, utc_epoch FROM events WHERE scan_id = ?",
                    (record["scan_id"],),
                ).fetchone()
                if (
                    row is not None
                    and row[0] == record["awb"]
                    and row[1] == record["airport"]
                    and row[2]
                    == json.dumps(record["utc"], ensure_ascii=False, separators=(",", ":"))
                    and row[3] == record["_utc_epoch"]
                ):
                    duplicates += 1
                else:
                    raise DuplicateConflict(
                        f"scan_id {record['scan_id']!r} appears with different content"
                    ) from exc
            except OverflowError as exc:
                # Epoch second outside SQLite's signed 64-bit INTEGER range.
                raise InvalidInput(
                    f"line {lines}: utc epoch second is out of the supported range"
                ) from exc
            else:
                unique += 1

            pending += 1
            if pending >= INSERT_BATCH:
                conn.commit()
                pending = 0
    conn.commit()
    return lines, unique, duplicates


# ---------------------------------------------------------------------------
# Emit: SQLite -> sorted gzip NDJSON, one row in memory at a time
# ---------------------------------------------------------------------------


def _emit(conn: sqlite3.Connection, output_path: Path) -> int:
    """Stream the sorted timeline; returns the number of distinct awbs."""
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_order "
        "ON events (awb, utc_epoch, scan_id)"
    )
    conn.commit()
    # ``awb, utc_epoch, scan_id`` covers both required orderings: awb lexical
    # order on output and (utc, scan_id) order within one awb's events.
    cursor = conn.execute(
        "SELECT awb, scan_id, airport, utc_raw, utc_epoch FROM events "
        "ORDER BY awb, utc_epoch, scan_id"
    )

    tmp_path = output_path.with_name(OUTPUT_TMP_NAME)
    awb_count = 0
    try:
        with open(tmp_path, "wb") as raw_fh:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_fh, mtime=0) as gz:
                current_awb: str | None = None
                buffer = bytearray()
                for awb, scan_id, airport, utc_raw, _epoch in cursor:
                    if current_awb != awb:
                        if current_awb is not None:
                            buffer += b"]}\n"
                            gz.write(buffer)
                            buffer.clear()
                        current_awb = awb
                        awb_count += 1
                        buffer += (
                            b'{"awb":'
                            + json.dumps(awb, ensure_ascii=False).encode("utf-8")
                            + b',"events":['
                        )
                    else:
                        buffer += b","
                    event = (
                        b"{"
                        + b'"awb":' + json.dumps(awb, ensure_ascii=False).encode("utf-8")
                        + b',"scan_id":' + json.dumps(scan_id, ensure_ascii=False).encode("utf-8")
                        + b',"airport":' + json.dumps(airport, ensure_ascii=False).encode("utf-8")
                        + b',"utc":' + utc_raw.encode("utf-8")
                        + b"}"
                    )
                    buffer += event
                    if len(buffer) >= 65536:
                        gz.write(buffer)
                        buffer.clear()

                if current_awb is not None:
                    buffer += b"]}\n"
                    gz.write(buffer)
                gz.flush()
            raw_fh.flush()
            os.fsync(raw_fh.fileno())
        os.replace(tmp_path, output_path)
    except OSError as exc:
        tmp_path.unlink(missing_ok=True)
        raise WriteFailed(f"failed to write result file: {exc}") from exc
    return awb_count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_pipeline(job_dir: Path, settings: Settings) -> PipelineStats:
    """Run the full consolidate pipeline for one job directory.

    Raises a :class:`PipelineError` subclass on any failure; in that case no
    output file is ever published.
    """

    input_path = job_dir / INPUT_NAME
    work_db = job_dir / WORK_DB_NAME
    output_path = job_dir / OUTPUT_NAME
    tmp_path = job_dir / OUTPUT_TMP_NAME

    # Never reuse leftovers from a previous attempt.
    work_db.unlink(missing_ok=True)
    output_path.unlink(missing_ok=True)
    tmp_path.unlink(missing_ok=True)

    conn = _open_work_db(work_db, settings.sqlite_cache_bytes)
    try:
        with open(input_path, "rb") as fh:
            lines, unique, duplicates = _ingest(fh, conn, settings.max_line_bytes)
        awb_count = _emit(conn, output_path)
    finally:
        conn.close()

    # The working database is an intermediate artifact; the input is retained
    # for audit only while the durable output exists.
    work_db.unlink(missing_ok=True)
    return PipelineStats(
        input_lines=lines,
        unique_events=unique,
        duplicate_events=duplicates,
        awb_count=awb_count,
    )
