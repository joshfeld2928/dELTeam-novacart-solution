"""Bronze → Silver: validate with Pydantic, dedupe, quarantine bad rows."""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Type

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.utils.logging_setup import log_event
from src.utils.schemas import OrderRow, CustomerRow, ProductRow


def _validate_df(
    df: pd.DataFrame,
    model: Type[BaseModel],
    required_fields: list[str],
    primary_key: str,
    quarantine_path: Path,
    logger: logging.Logger,
    source_name: str,
) -> pd.DataFrame:
    """Validate each row with Pydantic. Good rows → Silver, bad rows → quarantine.

    Only required_fields are passed to Pydantic for data-quality validation.
    All other columns (including new upstream additions) are carried forward
    untouched — schema gatekeeping is handled upstream by check_schema().
    """
    good, bad = [], []
    for _, row in df.iterrows():
        try:
            # Pass only the required fields to Pydantic — data quality check only.
            # Extra columns are preserved in the original row and passed through.
            required_data = {k: row[k] for k in required_fields if k in row.index}
            model(**required_data)
            good.append(row)
        except (ValidationError, Exception) as exc:
            row_dict = row.to_dict()
            row_dict["_quarantine_reason"] = str(exc)
            row_dict["_quarantined_at"] = datetime.now(timezone.utc).isoformat()
            bad.append(row_dict)

    if bad:
        q_dir = quarantine_path / source_name
        q_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        pd.DataFrame(bad).to_parquet(q_dir / f"{ts}.parquet", index=False)
        log_event(logger, "WARNING", f"{source_name}_quarantined", count=len(bad))

    result = pd.DataFrame(good) if good else pd.DataFrame(columns=df.columns)

    # Deduplicate on primary key — keep last occurrence
    if primary_key in result.columns and not result.empty:
        before = len(result)
        result = result.drop_duplicates(subset=[primary_key], keep="last")
        dupes = before - len(result)
        if dupes:
            log_event(logger, "INFO", f"{source_name}_deduped", dropped=dupes)

    return result.reset_index(drop=True)


_ORDER_FIELDS    = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "status"]
_CUSTOMER_FIELDS = ["customer_id", "first_name", "last_name", "email", "city", "country", "signup_date", "tier"]
_PRODUCT_FIELDS  = ["product_id", "name", "category", "unit_cost", "supplier_id", "updated_at"]


def build_silver_orders(
    date_str: str,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "orders" / f"date={date_str}" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_orders_no_bronze", date=date_str)
        return silver_dir / "orders" / f"date={date_str}" / "data.parquet"

    df = pd.read_parquet(src)
    df = _validate_df(df, OrderRow, _ORDER_FIELDS, "order_id", quarantine_dir, logger, "orders")

    out_dir = silver_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_orders_written", rows=len(df), date=date_str)
    return out_path


def build_silver_customers(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "customers" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_customers_no_bronze")
        return silver_dir / "customers" / "data.parquet"

    df = pd.read_parquet(src)
    df = _validate_df(df, CustomerRow, _CUSTOMER_FIELDS, "customer_id", quarantine_dir, logger, "customers")

    out_dir = silver_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_customers_written", rows=len(df))
    return out_path


def build_silver_products(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "products" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_products_no_bronze")
        return silver_dir / "products" / "data.parquet"

    df = pd.read_parquet(src)
    if df.empty:
        return silver_dir / "products" / "data.parquet"

    df = _validate_df(df, ProductRow, _PRODUCT_FIELDS, "product_id", quarantine_dir, logger, "products")

    out_dir = silver_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_products_written", rows=len(df))
    return out_path
