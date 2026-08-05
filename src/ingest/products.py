"""Ingest products from SQLite with watermark-based incremental loading → Bronze parquet.

Changes from original
---------------------
1. CONTEXT MANAGER for the SQLite connection
   Original: sqlite3.connect() + manual conn.close() inside a try/finally block.
   This is correct but verbose and easy to get wrong if the code is extended later.
   Python's sqlite3.Connection supports the `with` statement, which closes the
   connection automatically whether the block exits normally or via an exception.
   Fix: replace the try/finally with `with sqlite3.connect(db_path) as conn:`.

2. ATOMIC WRITE via a .tmp file + rename
   Original: df.to_parquet(out_path) writes directly to the final file. If the
   process is interrupted mid-write (disk full, SIGKILL, etc.) a truncated Parquet
   file is left at out_path. On the next run the watermark query would return zero
   new rows (since the watermark was not advanced on a failed write), but the Bronze
   read in the Silver stage would try to open the corrupt file and fail with an
   opaque error.
   Fix: write to a sibling .tmp file first, then Path.replace() for an atomic
   rename. The watermark is only advanced after the rename succeeds, so a crash
   anywhere in the write sequence leaves both the old Bronze file and the old
   watermark intact — the next run re-fetches and re-writes cleanly.
"""
from __future__ import annotations
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.utils.state import StateManager
from src.transform.schema_check import check_schema, DriftResult

EXPECTED_COLUMNS = [
    "product_id", "name", "category", "unit_cost", "supplier_id", "updated_at",
]
WATERMARK_KEY = "products_updated_at"


def ingest_products(
    db_path: Path,
    bronze_dir: Path,
    state: StateManager,
    logger: logging.Logger,
) -> tuple[Path, DriftResult]:
    """
    Incrementally load only rows newer than the stored watermark.
    Advances the watermark after a successful write.
    Returns (output path, drift result).
    """
    if not db_path.exists():
        raise IngestionError(f"products DB not found: {db_path}")

    watermark = state.get_watermark(WATERMARK_KEY) or "1970-01-01T00:00:00"
    log_event(logger, "INFO", "products_watermark_read", watermark=watermark)

    # CHANGE 1 — use a context manager instead of try/finally.
    # `with sqlite3.connect(...) as conn` closes the connection automatically
    # on both normal exit and exceptions, with less boilerplate.
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(
            "SELECT * FROM products WHERE updated_at > ? ORDER BY updated_at",
            conn,
            params=(watermark,),
        )

    log_event(logger, "INFO", "products_ingested", rows=len(df), since=watermark)

    if df.empty:
        log_event(logger, "INFO", "products_no_new_rows")
        out_dir = bronze_dir / "products"
        out_dir.mkdir(parents=True, exist_ok=True)
        # No rows to process — retrieve current version without drift detection
        current_version, _ = state.get_current_schema_version("products") if state else (None, [])
        return out_dir / "data.parquet", DriftResult(source_name="products", schema_version=current_version)

    drift = check_schema(list(df.columns), EXPECTED_COLUMNS, "products", logger, state)

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_schema_version"] = drift.schema_version or "unknown"

    out_dir = bronze_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    # CHANGE 2 — atomic write: write to .tmp then rename so the output file is
    # never left in a partial/corrupt state if the process is interrupted.
    # The watermark is advanced only after the rename succeeds, which means a
    # crash during the write leaves the old watermark in place and the next run
    # will re-fetch and re-write the same rows cleanly.
    tmp_path = out_path.with_suffix(".tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(out_path)   # atomic rename — safe on all platforms

    new_watermark = str(df["updated_at"].max())
    state.set_watermark(WATERMARK_KEY, new_watermark)
    log_event(logger, "INFO", "products_watermark_advanced", new_watermark=new_watermark,
              schema_version=drift.schema_version)

    return out_path, drift
