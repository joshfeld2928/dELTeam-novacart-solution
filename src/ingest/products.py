"""Ingest products from SQLite with watermark-based incremental loading → Bronze parquet."""
# • Import required libraries for database access, logging, and data processing
from __future__ import annotations
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.utils.state import StateManager
from src.transform.schema_check import check_schema

# • Define expected schema columns for validation
# • Set watermark key for tracking incremental loads
EXPECTED_COLUMNS = [
    "product_id", "name", "category", "unit_cost", "supplier_id", "updated_at",
]
WATERMARK_KEY = "products_updated_at"


def ingest_products(
    db_path: Path,
    bronze_dir: Path,
    state: StateManager,
    logger: logging.Logger,
) -> Path:
    """
    Incrementally load only rows newer than the stored watermark.
    Advances the watermark after a successful write.
    """
    # • Validate that the SQLite database file exists
    if not db_path.exists():
        raise IngestionError(f"products DB not found: {db_path}")

    # • Retrieve last processed timestamp (watermark) from state
    # • Default to epoch time if no watermark exists
    watermark = state.get_watermark(WATERMARK_KEY) or "1970-01-01T00:00:00"
    log_event(logger, "INFO", "products_watermark_read", watermark=watermark)

    # • Connect to SQLite database
    # • Query only products updated after the watermark timestamp
    # • Ensure connection is closed even if query fails
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM products WHERE updated_at > ? ORDER BY updated_at",
            conn,
            params=(watermark,),
        )

    finally:
        conn.close()

    log_event(logger, "INFO", "products_ingested", rows=len(df), since=watermark)

    # • Handle case where no new products exist
    # • Create output directory structure
    # • Return expected output path without writing
    if df.empty:
        log_event(logger, "INFO", "products_no_new_rows")
        out_dir = bronze_dir / "products"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / "data.parquet"

    # • Validate dataframe columns match expected schema
    check_schema(list(df.columns), EXPECTED_COLUMNS, "products", logger)

    # • Add ingestion timestamp for audit trail
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    # • Create bronze layer directory structure
    # • Write dataframe to parquet format in bronze layer
    out_dir = bronze_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    # • Update watermark to latest processed timestamp
    # • Persist watermark for next incremental load
    new_watermark = str(df["updated_at"].max())
    state.set_watermark(WATERMARK_KEY, new_watermark)
    log_event(logger, "INFO", "products_watermark_advanced", new_watermark=new_watermark)

    return out_path
