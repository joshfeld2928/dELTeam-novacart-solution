# Schema Drift Silent Row Drop — Fix Plan

## Top-Level Overview

**Goal:** Prevent upstream schema additions from silently quarantining all rows and reporting a successful pipeline run.

**Root Cause:** `_validate_df()` in `silver.py` passes the full Bronze row (including unknown columns) into the Pydantic model. Pydantic's default behaviour rejects any extra keyword argument with a `ValidationError`. Because every row carries the unknown column, every row is quarantined. The pipeline calls this "data quality quarantine" and returns SUCCESS.

**Scope:** Three targeted changes across two files. No architecture changes.

**Non-goals:**
- No changes to ingestion, Gold layer, or pipeline orchestration beyond surfacing quarantine counts in run metadata.
- No new dependencies.
- No changes to the `check_schema()` additive-drift warning — that behaviour is correct and should be preserved.

---

## Sub-Task 1 — Strip unknown columns before Pydantic validation

**Status:** [ ] pending

### Intent
Before each row is handed to the Pydantic model inside `_validate_df()`, project the row down to only the fields the model declares. An upstream-added column should flow through Bronze intact (the additive-drift warning already fires at ingestion) and then be silently dropped at the Silver boundary — which is the original intent of "additive drift → continue".

### Expected Outcomes
- A Bronze parquet containing an extra column (e.g. `discount_amount`) produces a Silver parquet with that column absent but all valid rows present.
- No rows are quarantined solely because of an unrecognised column.
- The existing additive-drift `WARNING` in `schema_check.py` still fires (no change there).
- All existing tests continue to pass. The `test_additive_drift_data_still_lands` test, which currently only checks that the pipeline does not crash, now also verifies that all rows land in Silver.

### Todo List
1. In `src/transform/silver.py`, inside `_validate_df()`, derive the set of field names the model expects by reading `model.model_fields.keys()` (Pydantic v2 API).
2. When building the dict to pass into the model, restrict it to only those keys: `{k: v for k, v in row.to_dict().items() if k in model_fields}`.
3. Keep the original `row` (with all columns, including extras) in the `good` list so Silver retains the extra column in its output if desired — or drop it; confirm with the team. The safest default is to drop it from Silver (keep Silver schema clean).
4. Update `test_pipeline_scenarios.py` Scenario 4 (`test_additive_drift_data_still_lands`) to assert that the Silver/Gold row count matches the input row count, not just that the run status is OK.

### Relevant Context
- `src/transform/silver.py` line 27: `model(**row.to_dict())` — this is the exact line to change.
- `src/utils/schemas.py`: Pydantic v2 models; `model.model_fields` returns a dict of field name → `FieldInfo`.
- `tests/test_pipeline_scenarios.py` Scenario 4 (tests 7 and 8).

---

## Sub-Task 2 — Enforce a quarantine rate threshold

**Status:** [ ] pending

### Intent
After `_validate_df()` collects the good and bad rows, compare the bad count to the total input count. If the bad row rate exceeds a configurable threshold, raise a `PipelineError` instead of silently continuing. A run where 100% of rows are quarantined is not a data-quality edge case — it is a pipeline failure.

### Expected Outcomes
- When bad rows exceed the threshold (e.g. 50%), `_validate_df()` raises `PipelineError` instead of returning a near-empty DataFrame.
- The pipeline stage is recorded as `FAIL` in run history.
- The threshold is configurable per source in `config/pipeline.yaml` under the `silver:` block.
- A new test covers the threshold breach scenario: ingest rows where all fail validation → pipeline fails, not succeeds.
- Runs where bad rows are below the threshold continue to behave as before (quarantine + continue).

### Todo List
1. Add a `max_quarantine_rate` key to the `silver:` block in `config/pipeline.yaml` with a default of `0.5` (50%).
2. In `src/utils/config.py`, expose `silver_cfg` already exists — no new property needed; callers read `config.silver_cfg.get("max_quarantine_rate", 0.5)`.
3. Thread the threshold value into `_validate_df()` as a new parameter `max_quarantine_rate: float = 0.5`.
4. After the good/bad split, compute `rate = len(bad) / len(df)`. If `rate > max_quarantine_rate` and `len(df) > 0`, raise `PipelineError(f"{source_name}: quarantine rate {rate:.1%} exceeds threshold {max_quarantine_rate:.1%}")`.
5. Update all three callers of `_validate_df()` (`build_silver_orders`, `build_silver_customers`, `build_silver_products`) to pass the threshold from config. The config object is not currently threaded into these functions — pass it as a parameter or pass the float value directly.
6. Add a test: write orders where all rows have `quantity=0` → assert run status is `FAIL` and error message contains "quarantine rate".

### Relevant Context
- `src/transform/silver.py` lines 35–40: quarantine write + WARNING log — threshold check goes here, after quarantine is written.
- `src/utils/exceptions.py`: `PipelineError` already exists — use it.
- `config/pipeline.yaml` `silver:` block already has `min_order_amount` and `max_order_amount` keys as a pattern to follow.
- `src/pipeline.py` `stage()` wrapper already catches all exceptions and marks stage as `FAIL`.

---

## Sub-Task 3 — Surface quarantine counts in run history and raise log level

**Status:** [ ] pending

### Intent
The current quarantine log is a `WARNING` in `pipeline.jsonl` with a row count. The run history entry in `state/run_history.jsonl` has no quarantine information at all. Ops teams and alerting systems have no way to detect a high quarantine run from the run history alone. This sub-task makes quarantine counts a first-class field in run metadata.

### Expected Outcomes
- Each stage entry in run history includes a `quarantined` count (0 if none).
- A quarantine count above zero is logged at `ERROR` level (not `WARNING`) when it represents more than a trivial number of rows (above the threshold from Sub-Task 2 — but below it, since above-threshold now fails).
- The `_validate_df()` function returns the quarantine count alongside the DataFrame so callers can propagate it.
- No change to the quarantine parquet files themselves — they are already correct.

### Todo List
1. Change `_validate_df()` return type from `pd.DataFrame` to `tuple[pd.DataFrame, int]` — return `(result, len(bad))`.
2. Update all three callers in `silver.py` to unpack the tuple.
3. Change the quarantine log in `_validate_df()` from `WARNING` to `ERROR` level when `len(bad) > 0`.
4. In `build_silver_orders`, `build_silver_customers`, `build_silver_products`, return the quarantine count alongside the output path (or store it on the logger context). The simplest approach: have each builder return `(out_path, quarantine_count)`.
5. In `src/pipeline.py`, update the `stage()` wrapper to accept and store a `quarantined` count from each Silver stage, and include it in the stage metadata dict that is written to run history.
6. Update relevant tests to handle the new return types.

### Relevant Context
- `src/transform/silver.py` lines 40, 52: current return value and log call.
- `src/pipeline.py` lines around the `stage()` closure and `metadata` dict construction.
- `state/run_history.jsonl`: append-only JSONL; adding new keys to stage dicts is backward-compatible.

---

## Implementation Order

Sub-tasks must be done in order:
1. **Sub-Task 1 first** — fixes the immediate silent-drop bug. Unblocks the real scenario.
2. **Sub-Task 2 second** — adds the safety net. Depends on Sub-Task 1 being in place (so threshold is not immediately triggered by the extra-column bug).
3. **Sub-Task 3 third** — observability improvement. Depends on Sub-Task 2's quarantine rate logic being stable, and refactors the return type of `_validate_df()`.
