# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All commands run from the repo root. `python` is not on PATH on this machine — use `python3` (or activate a venv).

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

# End-to-end: generate sample data → run pipeline (Nov 7–10) → pytest
python3 scripts/run_everything.py

# Pipeline: one date, or a date plus the N days before it
python3 -m src.pipeline --date 2025-11-07
python3 -m src.pipeline --date 2025-11-30 --backfill 23   # full range of current sample data

# Tests
python3 -m pytest -v
python3 -m pytest tests/test_pipeline_scenarios.py::test_happy_path_fact_rows -v   # single test
python3 -m pytest tests/test_schemas.py -k quantity                                # by keyword

# Wipe all generated output (bronze/silver/gold/quarantine/state/logs); leaves data/landing intact
python3 scripts/cleanup.py

# Row-count analytics over gold/fact_orders → logs/row_count_plot.png
python3 src/utils/row_count_analytics.py --days 7
```

There is no linter or formatter configured, and no pytest config file — pytest relies on being invoked from the repo root so `src` and `tests` are importable.

## Architecture

Medallion ETL: `data/landing` → Bronze → Silver → Gold, orchestrated by a single CLI.

**`src/pipeline.py` is the only orchestrator.** `run_one_date()` runs nine stages in a fixed order (3 ingest → 3 silver → 3 gold), each wrapped by a local `stage()` helper that records name/status/duration. The first exception aborts the remaining stages for that date, but the run is still appended to `state/run_history.jsonl` with `status: "FAIL"` and the error string. `main()` iterates dates oldest→newest and exits non-zero if any date failed.

**Config path resolution is depth-sensitive.** `Config.load(path)` sets `self.root = Path(path).parent.parent`, so all `paths:` in `config/pipeline.yaml` resolve relative to the *config file's grandparent*. This works only because the config lives at `config/pipeline.yaml`. Moving the config to a different depth silently redirects every data path. Tests work around this by `os.chdir()`-ing into a tmp project root (`tests/conftest.py` `config` fixture).

**Partitioning is per-source, not uniform.** Orders and `fact_orders` are date-partitioned (`<layer>/orders/date=YYYY-MM-DD/data.parquet`); customers and products are single unpartitioned snapshots (`<layer>/customers/data.parquet`).

**Underscore-prefixed columns are pipeline metadata.** Ingest adds `_source_file`, `_ingested_at`, `_partition_date`; quarantine adds `_quarantine_reason`, `_quarantined_at`. Every Gold builder strips all `_`-prefixed columns before writing, then re-adds its own (`_updated_at` for SCD1, `_eff_start`/`_eff_end`/`_current`/`_row_hash` for SCD2). Any new internal column must follow this prefix convention or it will leak into Gold.

### Contracts that must be changed together

Adding or renaming a source column requires touching all three of:

1. `EXPECTED_COLUMNS` in the matching `src/ingest/*.py` module — consumed by `check_schema` (`src/transform/schema_check.py`), which **warns and continues on extra columns (additive drift) but raises `SchemaError` on missing columns (subtractive drift)**. This asymmetry is a tested requirement (`test_additive_drift_*` / `test_subtractive_drift_*`).
2. The Pydantic model in `src/utils/schemas.py` (`OrderRow`, `CustomerRow`, `ProductRow`) — these carry the business rules (quantity > 0, non-negative prices, status enum, email contains `@`) and are the sole gate between Bronze and Silver.
3. Anything downstream in `src/transform/gold.py` that references the column by name.

### Validation and quarantine

`_validate_df` in `src/transform/silver.py` is shared by all three Silver builders. It validates **row by row** with Pydantic (slow but gives per-row reasons), sends good rows to Silver and bad rows to `data/quarantine/<source>/<UTC-timestamp>.parquet`, then deduplicates on the primary key with `keep="last"` (so a later duplicate row wins). Quarantine is **append-only** — every run with bad rows writes a new timestamped file, so the directory accumulates across runs and tests must glob rather than assume one file.

### Idempotency, per table

Re-running a date is safe for orders — Silver orders and `fact_orders` fully replace their date partition. The other tables behave differently:

- **`dim_product` (SCD1)** — full overwrite from the latest Silver snapshot.
- **`dim_customer` (SCD2)** — append-only history keyed on an MD5 hash of the fields in `gold.scd2_track_fields` (config). Unchanged rows are left alone, so a same-day re-run is stable; but a *change* on the same calendar day produces an expired row where `_eff_end == _eff_start`, since effective dates have day granularity.
- **Products ingest** — watermark-driven and therefore *not* replayable. `src/ingest/products.py` queries `updated_at > watermark` and advances `products_updated_at` in `state/watermarks.json` on success. A second run finds zero new rows and writes no Bronze parquet at all, leaving the Silver/Gold product stages operating on the previous file. To force a re-ingest, delete the watermark from `state/watermarks.json` (or run `scripts/cleanup.py`).

### Logging

`logs/pipeline.jsonl` is JSON-lines. The formatter emits `record.getMessage()` verbatim, so **only messages written through `log_event(logger, level, event, **fields)` are valid JSON**. Direct `logger.warning(...)` calls (e.g. the schema-drift warning in `schema_check.py`) land in the same file as bare text and will break naive line-by-line JSON parsing.

`get_logger` caches handlers on the named logger, so within a single pytest process every test after the first keeps handlers pointing at the first test's tmp `logs/` directory. Don't build assertions on log output in the scenario tests.

## Repo-specific gotchas

- **Generated output and `.venv/` are committed to git.** `.gitignore` only covers `.DS_Store`, `.claude/`, and `Technical Docs/`, so `data/bronze|silver|gold|quarantine`, `logs/`, `state/`, and the entire `.venv/` are tracked. Expect large diffs after any pipeline run; run `scripts/cleanup.py` before committing. The committed `.venv` is Python 3.9 and stale — the bytecode caches in `src/` are CPython 3.12; create a fresh venv rather than trusting it.
- **`scripts/run_everything.py` is out of date with the sample data.** It runs `--date 2025-11-10 --backfill 3`, but `generate_sample_data.py` now emits Nov 7–30 (the generator's own closing message suggests `--date 2025-11-30 --backfill 23`).
- **`orders_2025-11-10.csv` is intentionally header-only** — a zero-row day used to exercise the low-volume alerting path in `row_count_analytics.py`.
- **`row_count_analytics.py` has a real-world side effect**: when a day falls below 30% of the mean it calls `webbrowser.open()` on a `mailto:` URL with a hardcoded recipient address. Be careful invoking it, and treat that recipient as something to parameterize rather than extend.
- **`_validate_df` catches `Exception`, not just `ValidationError`** — genuine bugs inside the loop get silently recorded as quarantine reasons rather than surfacing.
- **`run_one_date` uses the deprecated `datetime.utcnow()`** while the rest of the codebase uses `datetime.now(timezone.utc)`; prefer the latter in new code.
- Scratch/exploratory scripts not part of the pipeline: `data/landing/parse_customers.py`, `data/landing/explore_products_db.py`, `state/test_parse.py`.

## Tests

`tests/test_schemas.py` unit-tests the Pydantic contracts directly. `tests/test_pipeline_scenarios.py` runs the real `run_one_date`/`main` end to end against a tmp project root and covers seven acceptance scenarios: happy path, duplicate collapse, quarantine, additive drift, subtractive drift, idempotency, backfill. Build fixtures with the `write_orders_csv` / `write_customers_json` / `make_products_db` helpers in `tests/conftest.py` rather than writing files by hand.
