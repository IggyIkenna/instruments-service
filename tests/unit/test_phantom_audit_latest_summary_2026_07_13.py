# Epic: observability_master
# Lifecycle: permanent
# Delete-when: never (guards the cockpit phantom-audit visibility contract)
"""The phantom reconciler publishes a stable per-AG summary the cockpit consolidator page reads.

`_write_phantom_audit_latest` writes `_index/phantom_audit_latest.json` (schema v1) to the AG's
manifest bucket — the SAME bucket the consolidator card for that AG reads — so the cockpit can show
"last phantom audit: N phantoms (Xd ago)". Absent object = "no phantom audit yet" (honest); a write
hiccup is swallowed (observability side-channel, never fails the reconcile). See
consolidator_throughput_backlog_monitor plan WS-3 (phantom/reprobe visibility, todo 313).
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path
from types import ModuleType


def _load_reconciler_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "reconcile_phantom_manifest_rows_all.py"
    spec = importlib.util.spec_from_file_location("reconcile_phantom_rows_all_summary_test", script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _StubStorageClient:
    """Captures upload_from_file_obj writes keyed by blob path."""

    def __init__(self) -> None:
        self._storage: dict[str, bytes] = {}

    def upload_from_file_obj(self, _bucket: str, path: str, file_obj: io.BytesIO) -> None:
        self._storage[path] = file_obj.getvalue()


def test_write_phantom_audit_latest_publishes_stable_summary() -> None:
    mod = _load_reconciler_module()
    stub = _StubStorageClient()
    mod._write_phantom_audit_latest(
        stub,
        "market-data-tick-cefi-prd-central-element-323112",
        "cefi",
        3,
        "gs://central-element-323112-phantom-triage/triage_cefi_20260713_101010.jsonl",
    )
    blob = "_index/phantom_audit_latest.json"
    assert blob in stub._storage
    payload = json.loads(stub._storage[blob])
    assert payload["schema_version"] == 1
    assert payload["audit"] == "phantom"
    assert payload["asset_group"] == "cefi"
    assert payload["phantom_count"] == 3
    assert payload["triage_jsonl"].endswith("triage_cefi_20260713_101010.jsonl")
    assert "generated_at" in payload


def test_write_phantom_audit_latest_zero_phantoms_is_honest() -> None:
    """A clean audit publishes phantom_count=0 (not absence) so the cockpit shows a fresh all-clear."""
    mod = _load_reconciler_module()
    stub = _StubStorageClient()
    mod._write_phantom_audit_latest(stub, "market-data-tick-defi-prd", "defi", 0, None)
    payload = json.loads(stub._storage["_index/phantom_audit_latest.json"])
    assert payload["phantom_count"] == 0
    assert payload["triage_jsonl"] is None


def test_write_phantom_audit_latest_never_raises_on_write_error() -> None:
    """A summary-write hiccup must not fail the reconcile — swallowed + logged."""
    mod = _load_reconciler_module()

    class _Boom:
        def upload_from_file_obj(self, *_a: object, **_k: object) -> None:
            raise OSError("gcs down")

    # Must return normally despite the upload raising.
    mod._write_phantom_audit_latest(_Boom(), "b", "sports", 5, None)
