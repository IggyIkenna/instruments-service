"""Unit / smoke tests for ``scripts/reconcile_legacy_blank_to_typed_reason.py``
(Wave 3.X Track C).

The reconciler walks the canonical manifest for ``empty_confirmed`` rows whose
``error_reason`` is a 2026-05-07-sweep default (``SOURCE_RETURNED_ZERO`` /
``EXPECTED_INSTRUMENT_NOT_LISTED``) and upgrades them to a more-specific typed
reason via the UTL ``classify_blank_reason_row`` classifier. These tests cover:

* ``_build_candidate_mask`` — picks exactly the sweep-default ``empty_confirmed``
  rows (not already-specific ``EXPECTED_*`` rows, not ``captured`` rows, not
  blank-reason rows — those are the *other* reconciler's job).
* ``main()`` scan-only — a planted Saturday CME row stamped ``SOURCE_RETURNED_ZERO``
  by the sweep produces a "would-upgrade → EXPECTED_WEEKEND" CSV entry; a regular
  Monday CME row is left alone.
* ``main()`` --apply-flips — the same planted row gets ``error_reason`` rewritten
  to ``EXPECTED_WEEKEND`` in the uploaded per-VM shard; the per-VM-isolation env
  guard aborts cleanly when ``MANIFEST_PER_VM_SHARDS`` / ``VM_NAME`` are absent.

The classifier behaviour exercised here (weekend → ``EXPECTED_WEEKEND``) predates
the Wave 3.X Track A/B classifier extensions, so the test passes against either a
fresh or a pre-Track-A/B UTL — the new half-day / understat / transfer-window /
season branches are covered in the UTL package's own classifier tests.

Plan: wave3x_residual_ssots_2026_05_08.md Track C.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_recon_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "scripts" / "reconcile_legacy_blank_to_typed_reason.py"
    spec = importlib.util.spec_from_file_location("reconcile_legacy_blank_to_typed_reason", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _fixed_download(df: pd.DataFrame, manifest_path: str) -> Callable[[str, str], tuple[pd.DataFrame, str]]:
    def _dl(_bucket: str, _asset_group: str) -> tuple[pd.DataFrame, str]:
        return (df, manifest_path)

    return _dl


def test_recon_module_defines_sweep_default_reasons() -> None:
    mod = _load_recon_module()
    assert frozenset({"SOURCE_RETURNED_ZERO", "EXPECTED_INSTRUMENT_NOT_LISTED"}) == mod.SWEEP_DEFAULT_REASONS
    assert mod.RECONCILER_NAME == "reconcile_legacy_blank_to_typed_reason"


def test_build_candidate_mask_picks_sweep_defaults_only() -> None:
    mod = _load_recon_module()
    df = pd.DataFrame(
        {
            "capture_status": [
                "empty_confirmed",  # 0: SOURCE_RETURNED_ZERO → candidate
                "empty_confirmed",  # 1: EXPECTED_INSTRUMENT_NOT_LISTED → candidate
                "empty_confirmed",  # 2: already-specific EXPECTED_HOLIDAY → NOT a candidate
                "captured",  # 3: not empty_confirmed → NOT a candidate
                "empty_confirmed",  # 4: blank reason → NOT this reconciler's job
                "attempted_failed",  # 5: not empty_confirmed → NOT a candidate
            ],
            "error_reason": [
                "SOURCE_RETURNED_ZERO",
                "EXPECTED_INSTRUMENT_NOT_LISTED",
                "EXPECTED_HOLIDAY",
                "SOURCE_RETURNED_ZERO",
                "",
                "SOURCE_RETURNED_ZERO",
            ],
        }
    )
    mask = mod._build_candidate_mask(df)
    assert list(mask) == [True, True, False, False, False, False]


def test_build_candidate_mask_missing_columns_returns_all_false() -> None:
    mod = _load_recon_module()
    assert list(mod._build_candidate_mask(pd.DataFrame({"date": ["2024-01-01"]}))) == [False]
    assert list(mod._build_candidate_mask(pd.DataFrame({"capture_status": ["empty_confirmed"]}))) == [False]


def _synthetic_tradfi_manifest() -> pd.DataFrame:
    """Manifest with one sweep-mis-stamped Saturday row + one legit-trading-day row."""
    return pd.DataFrame(
        {
            "venue": ["CME", "CME"],
            "date": ["2024-01-06", "2024-01-08"],  # Saturday (should be EXPECTED_WEEKEND), Monday (real trading day)
            "data_type": ["ohlcv_1m", "ohlcv_1m"],
            "instrument_type": ["", ""],
            "instrument_id": ["", ""],
            "league_id": ["", ""],
            "chain": ["", ""],
            "capture_status": ["empty_confirmed", "empty_confirmed"],
            "error_reason": ["SOURCE_RETURNED_ZERO", "SOURCE_RETURNED_ZERO"],
        }
    )


def test_main_scan_only_proposes_weekend_upgrade(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    mod = _load_recon_module()
    df = _synthetic_tradfi_manifest()
    monkeypatch.setattr(mod, "_download_manifest", _fixed_download(df, str(tmp_path / "manifest.parquet")))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "reconcile_legacy_blank_to_typed_reason.py",
            "--asset-group",
            "tradfi",
            "--report-dir",
            str(tmp_path),
        ],
    )
    rc = mod.main()
    assert rc == 0
    csvs = sorted(tmp_path.glob("recon-legacy-typed-tradfi-*.csv"))
    assert len(csvs) == 1, f"expected one CSV report, got {csvs}"
    report = pd.read_csv(csvs[0])
    # Saturday (2024-01-06) → Shape (a): SOURCE_RETURNED_ZERO → EXPECTED_WEEKEND, stays empty_confirmed.
    # Monday (2024-01-08) → Shape (b): SOURCE_RETURNED_ZERO → LegacyBlankErrorReasonError, flips to attempted_failed.
    assert len(report) == 2
    weekend_row = report[report["date"] == "2024-01-06"].iloc[0]
    assert weekend_row["old_reason"] == "SOURCE_RETURNED_ZERO"
    assert weekend_row["new_reason"] == "EXPECTED_WEEKEND"
    assert weekend_row["new_capture_status"] == "empty_confirmed"
    monday_row = report[report["date"] == "2024-01-08"].iloc[0]
    assert monday_row["old_reason"] == "SOURCE_RETURNED_ZERO"
    assert monday_row["new_reason"] == "LegacyBlankErrorReasonError"
    assert monday_row["new_capture_status"] == "attempted_failed"


def test_main_apply_flips_rewrites_error_reason(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    mod = _load_recon_module()
    df = _synthetic_tradfi_manifest()
    monkeypatch.setattr(mod, "_download_manifest", _fixed_download(df, str(tmp_path / "manifest.parquet")))

    uploaded: dict[str, pd.DataFrame] = {}

    class _FakeBlob:
        def upload_from_filename(self, filename: str) -> None:
            # Read NOW — the reconciler unlinks the temp file in its finally block.
            uploaded["shard"] = pd.read_parquet(filename)

    class _FakeBucket:
        def blob(self, _name: str) -> _FakeBlob:
            return _FakeBlob()

    class _FakeClient:
        def __init__(self, *_args: object, **_kwargs: object) -> None: ...

        def bucket(self, _name: str) -> _FakeBucket:
            return _FakeBucket()

    monkeypatch.setattr(mod.storage, "Client", _FakeClient)
    monkeypatch.setenv("MANIFEST_PER_VM_SHARDS", "true")
    monkeypatch.setenv("VM_NAME", "recon-legacy-typed-tradfi-pytest")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "reconcile_legacy_blank_to_typed_reason.py",
            "--asset-group",
            "tradfi",
            "--apply-flips",
            "--report-dir",
            str(tmp_path),
        ],
    )
    rc = mod.main()
    assert rc == 0
    assert "shard" in uploaded, "apply-flips should have uploaded a per-VM shard"
    shard = uploaded["shard"]
    # Shard contains both upgraded rows: Shape (a) weekend + Shape (b) status-flip.
    assert len(shard) == 2
    weekend = shard[shard["date"] == "2024-01-06"].iloc[0]
    assert weekend["error_reason"] == "EXPECTED_WEEKEND"
    assert weekend["capture_status"] == "empty_confirmed"
    monday = shard[shard["date"] == "2024-01-08"].iloc[0]
    assert monday["error_reason"] == "LegacyBlankErrorReasonError"
    assert monday["capture_status"] == "attempted_failed"


def test_main_apply_flips_requires_per_vm_isolation_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    mod = _load_recon_module()
    monkeypatch.setattr(
        mod, "_download_manifest", _fixed_download(_synthetic_tradfi_manifest(), str(tmp_path / "m.parquet"))
    )
    monkeypatch.delenv("MANIFEST_PER_VM_SHARDS", raising=False)
    monkeypatch.delenv("VM_NAME", raising=False)
    monkeypatch.setattr(
        sys, "argv", ["reconcile_legacy_blank_to_typed_reason.py", "--asset-group", "tradfi", "--apply-flips"]
    )
    rc = mod.main()
    assert rc == 4  # missing MANIFEST_PER_VM_SHARDS → abort before touching anything
