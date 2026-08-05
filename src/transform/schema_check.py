"""Schema drift detection with schema evolution support.

Drift classification:
  - Additive (new columns)    → MINOR  — register new version, pipeline continues.
  - Subtractive (missing cols) → MAJOR  — raises SchemaError, pipeline halts.
  - Both present              → MAJOR  — raises SchemaError, pipeline halts.

Returns a DriftResult so callers can attach drift metadata to run records.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.utils.exceptions import SchemaError

if TYPE_CHECKING:
    from src.utils.state import StateManager


@dataclass
class DriftResult:
    source_name: str
    schema_version: str | None       # version string registered (e.g. "v2"), or None if no change
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.added or self.removed)


def check_schema(
    df_columns: list[str],
    expected_columns: list[str],
    source_name: str,
    logger: logging.Logger,
    state: "StateManager | None" = None,
) -> DriftResult:
    """
    Compare actual DataFrame columns against expected schema.

    - No drift                           → registers v1 on first run, returns cleanly.
    - Extra columns only (additive)      → MINOR — registers new version, logs drift event.
    - Missing columns (subtractive)      → MAJOR — raises SchemaError immediately.
    - Both added and removed             → MAJOR — raises SchemaError immediately.

    Args:
        df_columns:       Columns present in the incoming DataFrame.
        expected_columns: Baseline required columns for this source.
        source_name:      Human-readable source label used for registry keys and logs.
        logger:           Pipeline logger.
        state:            StateManager instance for schema registry persistence.
                          If None, drift is still classified and logged but not persisted.

    Returns:
        DriftResult with the registered schema version and lists of added/removed columns.
    """
    actual = set(df_columns)
    # Strip internal bronze metadata columns from comparison
    actual_data = {c for c in actual if not c.startswith("_")}
    expected = set(expected_columns)

    added = sorted(actual_data - expected)
    missing = sorted(expected - actual_data)

    # ── Subtractive or combined drift → hard halt ────────────────────────────
    if missing:
        raise SchemaError(
            f"[schema_drift] {source_name}: MAJOR — required columns missing {missing}"
        )

    # ── Determine current registered version ────────────────────────────────
    current_version: str | None = None
    if state is not None:
        current_version, _ = state.get_current_schema_version(source_name)

    # ── No drift: register v1 on first encounter, return cleanly ────────────
    if not added:
        if state is not None and current_version is None:
            new_version = state.register_schema_version(
                source=source_name,
                columns=sorted(actual_data),
                added=[],
                removed=[],
                registered_at=datetime.now(timezone.utc).isoformat(),
            )
            logger.info(
                f"[schema_registry] {source_name}: initial schema registered as {new_version}"
            )
            return DriftResult(source_name=source_name, schema_version=new_version)
        return DriftResult(source_name=source_name, schema_version=current_version)

    # ── Additive drift → MINOR — register new version ───────────────────────
    new_version = current_version  # fallback if no state manager
    if state is not None:
        new_version = state.register_schema_version(
            source=source_name,
            columns=sorted(actual_data),
            added=added,
            removed=[],
            registered_at=datetime.now(timezone.utc).isoformat(),
        )

    logger.warning(
        f"[schema_drift] {source_name}: MINOR — new columns {added} detected; "
        f"registered as schema {new_version}. Pipeline continues."
    )

    return DriftResult(source_name=source_name, schema_version=new_version, added=added)
