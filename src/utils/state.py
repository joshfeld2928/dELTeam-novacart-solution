"""Watermark and run-state manager. Persists to JSON files in state/."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path


class StateManager:
    def __init__(self, state_dir: Path):
        self._dir = state_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._watermark_file = self._dir / "watermarks.json"
        self._runs_file = self._dir / "run_history.jsonl"
        self._schema_registry_file = self._dir / "schema_registry.json"

    # ── watermarks ──────────────────────────────────────────────────────────
    def get_watermark(self, source: str) -> str | None:
        if not self._watermark_file.exists():
            return None
        data = json.loads(self._watermark_file.read_text())
        return data.get(source)

    def set_watermark(self, source: str, value: str) -> None:
        data: dict = {}
        if self._watermark_file.exists():
            data = json.loads(self._watermark_file.read_text())
        data[source] = value
        self._watermark_file.write_text(json.dumps(data, indent=2))

    # ── run history ─────────────────────────────────────────────────────────
    def record_run(self, metadata: dict) -> None:
        with self._runs_file.open("a") as f:
            f.write(json.dumps(metadata) + "\n")

    # ── schema registry ─────────────────────────────────────────────────────
    def get_schema_versions(self, source: str) -> dict:
        """Return all registered schema versions for a source, keyed by version string."""
        if not self._schema_registry_file.exists():
            return {}
        data = json.loads(self._schema_registry_file.read_text())
        return data.get(source, {})

    def get_current_schema_version(self, source: str) -> tuple[str | None, list[str]]:
        """Return (version_string, columns) for the latest registered schema, or (None, [])."""
        versions = self.get_schema_versions(source)
        if not versions:
            return None, []
        latest = sorted(versions.keys())[-1]
        return latest, versions[latest]["columns"]

    def register_schema_version(
        self,
        source: str,
        columns: list[str],
        added: list[str],
        removed: list[str],
        registered_at: str,
    ) -> str:
        """
        Register a new schema version for a source. Returns the new version string (e.g. 'v2').
        Version is auto-incremented from the current highest version.
        """
        data: dict = {}
        if self._schema_registry_file.exists():
            data = json.loads(self._schema_registry_file.read_text())
        source_versions = data.get(source, {})

        # Auto-increment version number
        if source_versions:
            last_n = max(int(v.lstrip("v")) for v in source_versions.keys())
            new_version = f"v{last_n + 1}"
        else:
            new_version = "v1"

        source_versions[new_version] = {
            "columns": columns,
            "added": added,
            "removed": removed,
            "registered_at": registered_at,
        }
        data[source] = source_versions
        self._schema_registry_file.write_text(json.dumps(data, indent=2))
        return new_version
