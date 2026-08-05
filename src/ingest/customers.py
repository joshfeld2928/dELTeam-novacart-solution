"""Ingest customers nested JSON → Bronze parquet.

Changes from original
---------------------
1. UTF-8 ENCODING on read_text()
   Original: src.read_text() uses the OS default encoding, which is cp1252 on
   Windows. Any customer name or address containing non-ASCII characters (é, ü,
   ñ, Chinese characters, etc.) would either be silently mis-decoded or raise a
   UnicodeDecodeError with no helpful message.
   Fix: src.read_text(encoding="utf-8") — JSON is always UTF-8 by spec (RFC 8259).

2. JSON STRUCTURE GUARD before building the DataFrame
   Original: if the JSON file is valid JSON but not the expected shape (e.g. a
   dict at the top level, or a list of strings), pd.DataFrame(rows) would raise
   an opaque ValueError or produce an empty/malformed DataFrame with no clear
   error message pointing back to the source file.
   Fix: an explicit isinstance check raises an IngestionError with a descriptive
   message before any processing begins.

3. NON-MUTATING FLATTEN of the nested address object
   Original: rec.pop("address", {}) removes the "address" key from each dict in
   the raw list in-place. This permanently mutates the parsed JSON objects. If
   anything later in the same call-stack inspected `raw` again, the address data
   would be gone — a hidden side-effect.
   Fix: use rec.get("address") to read without modifying, then build each output
   row as a new dict using {**rec} spread, dropping "address" explicitly.

4. ATOMIC WRITE via a .tmp file + rename
   Original: df.to_parquet(out_path) writes directly to the final file. A
   mid-write crash (disk full, power loss, SIGKILL) leaves a truncated Parquet
   file at out_path. The next run sees the file exists and may either skip it or
   fail on read with a confusing error.
   Fix: write to a sibling .tmp file first, then Path.replace() for an atomic
   rename. The output file is always either the previous complete version or the
   new complete version — never a partial.
"""
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.transform.schema_check import check_schema

EXPECTED_COLUMNS = [
    "customer_id", "first_name", "last_name", "email",
    "city", "country", "signup_date", "tier",
]


def ingest_customers(
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Flatten nested JSON export and write to Bronze. Returns output path."""
    src = landing_dir / "customers.json"
    if not src.exists():
        raise IngestionError(f"customers file not found: {src}")

    # CHANGE 1 — read with explicit UTF-8 encoding.
    # JSON is defined as UTF-8 by RFC 8259; using the OS default encoding on
    # Windows (cp1252) would corrupt or crash on any non-ASCII character.
    raw = json.loads(src.read_text(encoding="utf-8"))

    # CHANGE 2 — guard against malformed JSON structure before doing any work.
    # Catches cases where the file is valid JSON but the wrong shape (e.g. a
    # single dict instead of a list, or a list of plain strings).
    if not isinstance(raw, list) or not all(isinstance(r, dict) for r in raw):
        raise IngestionError(
            f"customers.json must be a JSON array of objects; got: {type(raw).__name__}"
        )

    # CHANGE 3 — non-mutating flatten: build a new dict per record instead of
    # calling rec.pop() which modifies the original parsed objects in place.
    # Each output row copies all keys except "address", then adds flat
    # "city" and "country" keys from the nested address object.
    rows = []
    for rec in raw:
        address = rec.get("address") or {}          # read without modifying rec
        rows.append({
            **{k: v for k, v in rec.items() if k != "address"},  # copy all non-address fields
            "city":    address.get("city", ""),
            "country": address.get("country", ""),
        })

    df = pd.DataFrame(rows)
    log_event(logger, "INFO", "customers_ingested", rows=len(df))

    check_schema(list(df.columns), EXPECTED_COLUMNS, "customers", logger)

    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    out_dir = bronze_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    # CHANGE 4 — atomic write: write to .tmp then rename so the output file is
    # never left in a partial/corrupt state if the process is interrupted.
    tmp_path = out_path.with_suffix(".tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(out_path)

    log_event(logger, "INFO", "customers_bronze_written", path=str(out_path), rows=len(df))
    return out_path
