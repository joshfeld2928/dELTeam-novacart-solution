"""Ingest daily orders CSV → Bronze parquet.

Changes from original
---------------------
1. EXPLICIT DTYPES on pd.read_csv()
   Original: pd.read_csv(src) with no dtype argument — pandas guesses column
   types by scanning the file, which is slow on large files and can silently
   miscast values (e.g. an order_id like "001" becoming the integer 1).
   Fix: a DTYPE_MAP declares every column's type up front. order_date is kept
   as str here because Pydantic's OrderRow validates and parses it properly in
   the Silver layer — casting it to datetime during ingest would duplicate work.

2. ATOMIC WRITE via a .tmp file + rename
   Original: df.to_parquet(out_path) writes directly to the final file. If the
   process is killed mid-write (disk full, SIGKILL, etc.) a corrupt, truncated
   Parquet file is left at out_path. A re-run would find a file already present
   and could either skip it or fail with a confusing read error.
   Fix: write to a sibling .tmp file first, then use Path.replace() which is an
   atomic filesystem rename on all platforms. The final file either contains the
   complete new write or the previous good version — never a partial file.
"""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.transform.schema_check import check_schema

EXPECTED_COLUMNS = [
    "order_id", "customer_id", "product_id",
    "order_date", "quantity", "unit_price", "status",
]

# CHANGE 1 — explicit dtype map so pandas never has to guess column types.
# Using str for order_date because Pydantic parses it to date in Silver.
# Using "Int64" (nullable integer) for quantity so missing values don't
# silently become floats; Pydantic will reject them as invalid anyway.
DTYPE_MAP: dict[str, object] = {
    "order_id":    str,
    "customer_id": str,
    "product_id":  str,
    "order_date":  str,
    "quantity":    "Int64",
    "unit_price":  float,
    "status":      str,
}


def ingest_orders(
    date_str: str,
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Read orders_YYYY-MM-DD.csv and write to Bronze layer. Returns output path."""
    src = landing_dir / f"orders_{date_str}.csv"
    if not src.exists():
        raise IngestionError(f"orders file not found: {src}")

    # CHANGE 1 applied — pass dtype=DTYPE_MAP to avoid type-guessing.
    df = pd.read_csv(src, dtype=DTYPE_MAP)
    log_event(logger, "INFO", "orders_ingested", date=date_str, rows=len(df))

    check_schema(list(df.columns), EXPECTED_COLUMNS, "orders", logger)

    # Add Bronze metadata
    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_partition_date"] = date_str

    out_dir = bronze_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    # CHANGE 2 — atomic write: write to .tmp then rename so the output file is
    # never left in a partial/corrupt state if the process is interrupted.
    tmp_path = out_path.with_suffix(".tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(out_path)

    log_event(logger, "INFO", "orders_bronze_written", path=str(out_path), rows=len(df))
    return out_path
