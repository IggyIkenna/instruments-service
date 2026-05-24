"""Unit tests for scripts/reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py.

Wave 3 CeFi corrector — re-classifies
``attempted_failed/LegacyBlankErrorReasonError`` rows whose per-instrument catalog
bounds NOW fire a specific ``EXPECTED_*`` reason via the extended
``_classify_cefi`` (``read_instruments_catalog_bounds`` added 2026-05-13).

Three integration scenarios:

1. **Dry-run smoke** — planted catalog-before-listing row produces a
   "would-correct → EXPECTED_INSTRUMENT_NOT_LISTED" CSV entry; an active-instrument
   row produces no correction entry; return code 0.
2. **Apply-flips fixture** — same planted rows pass through the write path; the
   per-VM shard parquet contains the corrected status + reason; rows that remain
   ``attempted_failed`` (active instrument) are NOT included in the shard.
3. **Idempotent re-run** — re-running the corrector on already-corrected rows
   (``capture_status=empty_confirmed``) finds 0 candidate rows and exits cleanly.

Plus two env-guard tests mirroring the pattern in
``test_reconcile_legacy_blank_to_typed_reason.py``:

4. Missing ``MANIFEST_PER_VM_SHARDS`` env → exit 4.
5. Missing ``--confirm`` alongside ``--apply-flips`` → exit 1.

Plan: plans/active/issues/defi_classifier_missing_catalog_crossref_2026_05_13.md
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------


def _load_corrector_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py"
    spec = importlib.util.spec_from_file_location(
        "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13", script_path
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _fixed_download(df: pd.DataFrame, manifest_path: str) -> Callable[[str, str], tuple[pd.DataFrame, str]]:
    def _dl(_bucket: str, _asset_group: str) -> tuple[pd.DataFrame, str]:
        return (df, manifest_path)

    return _dl


# ---------------------------------------------------------------------------
# Fixture manifests
# ---------------------------------------------------------------------------


def _synthetic_cefi_manifest() -> pd.DataFrame:
    """Manifest with two attempted_failed/LegacyBlankErrorReasonError rows.

    Row 0 — BINANCE-FUTURES instrument, date 2020-01-01 (before listing 2021-01-01).
              Classifier with catalog cross-ref should fire EXPECTED_INSTRUMENT_NOT_LISTED.
    Row 1 — BINANCE-FUTURES instrument, date 2023-06-01 (within active window).
              Classifier should return SOURCE_RETURNED_ZERO (no catalog override).
    Row 2 — BINANCE-FUTURES row with capture_status=captured (should never be a candidate).
    """
    return pd.DataFrame(
        {
            "venue": ["BINANCE-FUTURES", "BINANCE-FUTURES", "BINANCE-FUTURES"],
            "date": ["2020-01-01", "2023-06-01", "2023-06-02"],
            "data_type": ["ohlcv_1m", "ohlcv_1m", "ohlcv_1m"],
            "instrument_id": [
                "BINANCE-FUTURES:PERP:BTCUSDT",
                "BINANCE-FUTURES:PERP:BTCUSDT",
                "BINANCE-FUTURES:PERP:BTCUSDT",
            ],
            "instrument_key": [
                "BINANCE-FUTURES:PERP:BTCUSDT",
                "BINANCE-FUTURES:PERP:BTCUSDT",
                "BINANCE-FUTURES:PERP:BTCUSDT",
            ],
            "instrument_type": ["PERP", "PERP", "PERP"],
            "league_id": ["", "", ""],
            "chain": ["", "", ""],
            "capture_status": ["attempted_failed", "attempted_failed", "captured"],
            "error_reason": [
                "LegacyBlankErrorReasonError",
                "LegacyBlankErrorReasonError",
                "",
            ],
        }
    )


def _already_corrected_manifest() -> pd.DataFrame:
    """Manifest where the corrector has already run — rows are empty_confirmed."""
    return pd.DataFrame(
        {
            "venue": ["BINANCE-FUTURES"],
            "date": ["2020-01-01"],
            "data_type": ["ohlcv_1m"],
            "instrument_id": ["BINANCE-FUTURES:PERP:BTCUSDT"],
            "instrument_key": ["BINANCE-FUTURES:PERP:BTCUSDT"],
            "instrument_type": ["PERP"],
            "league_id": [""],
            "chain": [""],
            "capture_status": ["empty_confirmed"],  # already corrected
            "error_reason": ["EXPECTED_INSTRUMENT_NOT_LISTED"],
        }
    )


# ---------------------------------------------------------------------------
# Basic module-level constant tests
# ---------------------------------------------------------------------------


class TestCorrectorModuleConstants:
    def test_reconciler_name(self) -> None:
        mod = _load_corrector_module()
        assert mod.RECONCILER_NAME == "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13"

    def test_legacy_blank_error_class(self) -> None:
        mod = _load_corrector_module()
        assert mod.LEGACY_BLANK_ERROR_CLASS == "LegacyBlankErrorReasonError"

    def test_valid_correction_reasons_are_expected_star(self) -> None:
        mod = _load_corrector_module()
        reasons: frozenset[str] = mod.VALID_CORRECTION_REASONS
        assert len(reasons) > 0
        assert all(r.startswith("EXPECTED_") for r in reasons)
        assert "SOURCE_RETURNED_ZERO" not in reasons

    def test_asset_group_buckets_has_cefi(self) -> None:
        mod = _load_corrector_module()
        assert "cefi" in mod.ASSET_GROUP_BUCKETS
        assert mod.ASSET_GROUP_BUCKETS["cefi"].startswith("market-data-tick-cefi-")


# ---------------------------------------------------------------------------
# _build_candidate_mask tests
# ---------------------------------------------------------------------------


class TestBuildCandidateMask:
    def test_picks_attempted_failed_with_legacy_error_reason(self) -> None:
        mod = _load_corrector_module()
        df = pd.DataFrame(
            {
                "capture_status": [
                    "attempted_failed",  # 0: LegacyBlankErrorReasonError → candidate
                    "attempted_failed",  # 1: LegacyBlankErrorReasonError(msg) → candidate (startswith)
                    "empty_confirmed",  # 2: empty_confirmed → NOT a candidate
                    "attempted_failed",  # 3: different error → NOT a candidate
                    "captured",  # 4: captured → NOT a candidate
                ],
                "error_reason": [
                    "LegacyBlankErrorReasonError",
                    "LegacyBlankErrorReasonError: some message",
                    "LegacyBlankErrorReasonError",
                    "SomeOtherError",
                    "",
                ],
            }
        )
        mask = mod._build_candidate_mask(df)
        assert list(mask) == [True, True, False, False, False]

    def test_missing_columns_returns_all_false(self) -> None:
        mod = _load_corrector_module()
        assert list(mod._build_candidate_mask(pd.DataFrame({"date": ["2024-01-01"]}))) == [False]
        assert list(mod._build_candidate_mask(pd.DataFrame({"capture_status": ["attempted_failed"]}))) == [False]

    def test_empty_dataframe_returns_empty_series(self) -> None:
        mod = _load_corrector_module()
        result = mod._build_candidate_mask(pd.DataFrame({"capture_status": [], "error_reason": []}))
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Dry-run smoke test (Test 1)
# ---------------------------------------------------------------------------


class TestDryRunSmoke:
    def test_dry_run_no_catalog_produces_no_corrections(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Without catalog, BINANCE-FUTURES rows always fall through to SOURCE_RETURNED_ZERO
        → no corrections proposed in dry-run mode."""
        mod = _load_corrector_module()
        df = _synthetic_cefi_manifest()
        monkeypatch.setattr(mod, "_download_manifest", _fixed_download(df, str(tmp_path / "manifest.parquet")))
        # Seed catalog cache with empty sentinel so no GCS download is attempted.
        import unified_trading_library.instruments_catalog_reader as icr

        icr.clear_catalog_cache()
        import pandas as _pd

        icr._cache["cefi"] = (float("inf"), _pd.DataFrame())

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--report-dir",
                str(tmp_path),
            ],
        )
        rc = mod.main()
        assert rc == 0
        # No corrections CSV is written when n_corrections == 0
        csvs = sorted(tmp_path.glob("cefi-corrector-cefi-*.csv"))
        assert len(csvs) == 0

    def test_dry_run_with_catalog_produces_correction_csv(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """With catalog seeded, pre-listing row fires EXPECTED_INSTRUMENT_NOT_LISTED;
        active-window row stays SOURCE_RETURNED_ZERO and is NOT included in the CSV."""
        mod = _load_corrector_module()
        df = _synthetic_cefi_manifest()
        monkeypatch.setattr(mod, "_download_manifest", _fixed_download(df, str(tmp_path / "manifest.parquet")))

        import unified_trading_library.instruments_catalog_reader as icr

        # Seed catalog: BTCUSDT listed from 2021-01-01 (so 2020-01-01 is pre-listing).
        icr.clear_catalog_cache()
        catalog_df = pd.DataFrame(
            {
                "instrument_key": ["BINANCE-FUTURES:PERP:BTCUSDT"],
                "venue": ["BINANCE-FUTURES"],
                "raw_symbol": ["BTCUSDT"],
                "base_asset": ["BTC"],
                "available_from_datetime": [pd.Timestamp("2021-01-01")],
                "available_to_datetime": [pd.NaT],
            }
        )
        import time as _time

        icr._cache["cefi"] = (_time.monotonic() + 99999, catalog_df)

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--report-dir",
                str(tmp_path),
            ],
        )
        rc = mod.main()
        assert rc == 0
        csvs = sorted(tmp_path.glob("cefi-corrector-cefi-*.csv"))
        assert len(csvs) == 1, f"expected one CSV report, got {csvs}"
        report = pd.read_csv(csvs[0])
        # Only the pre-listing row (2020-01-01) should appear.
        assert len(report) == 1
        row = report.iloc[0]
        assert row["date"] == "2020-01-01"
        assert row["new_reason"] == "EXPECTED_INSTRUMENT_NOT_LISTED"
        assert row["new_capture_status"] == "empty_confirmed"
        assert row["old_reason"] == "LegacyBlankErrorReasonError"


# ---------------------------------------------------------------------------
# Apply-flips with fixture (Test 2)
# ---------------------------------------------------------------------------


class TestApplyFlipsFixture:
    def test_apply_flips_corrects_pre_listing_row_only(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """--apply-flips uploads a per-VM shard with corrected rows only.

        The pre-listing row (2020-01-01) should appear in the shard with
        empty_confirmed/EXPECTED_INSTRUMENT_NOT_LISTED.
        The active-window row (2023-06-01) should NOT appear (no correction).
        """
        mod = _load_corrector_module()
        df = _synthetic_cefi_manifest()
        monkeypatch.setattr(mod, "_download_manifest", _fixed_download(df, str(tmp_path / "manifest.parquet")))

        # Seed catalog.
        import time as _time

        import unified_trading_library.instruments_catalog_reader as icr

        icr.clear_catalog_cache()
        catalog_df = pd.DataFrame(
            {
                "instrument_key": ["BINANCE-FUTURES:PERP:BTCUSDT"],
                "venue": ["BINANCE-FUTURES"],
                "raw_symbol": ["BTCUSDT"],
                "base_asset": ["BTC"],
                "available_from_datetime": [pd.Timestamp("2021-01-01")],
                "available_to_datetime": [pd.NaT],
            }
        )
        icr._cache["cefi"] = (_time.monotonic() + 99999, catalog_df)

        uploaded: dict[str, pd.DataFrame] = {}

        class _FakeBlob:
            def upload_from_filename(self, filename: str) -> None:
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
        monkeypatch.setenv("VM_NAME", "ikenna-slot2-corrector-cefi-pytest")
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--apply-flips",
                "--confirm",
                "--report-dir",
                str(tmp_path),
            ],
        )
        rc = mod.main()
        assert rc == 0
        assert "shard" in uploaded, "--apply-flips should have uploaded a per-VM shard"
        shard = uploaded["shard"]
        # Only the pre-listing row should be in the shard.
        assert len(shard) == 1
        corrected_row = shard.iloc[0]
        assert str(corrected_row["date"]) == "2020-01-01"
        assert corrected_row["capture_status"] == "empty_confirmed"
        assert corrected_row["error_reason"] == "EXPECTED_INSTRUMENT_NOT_LISTED"

    def test_apply_flips_handles_delisted_instrument(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Post-delisting row fires EXPECTED_INSTRUMENT_DELISTED."""
        mod = _load_corrector_module()
        df = pd.DataFrame(
            {
                "venue": ["OKX"],
                "date": ["2024-12-01"],
                "data_type": ["ohlcv_1m"],
                "instrument_id": ["OKX:PERP:XYZUSDT"],
                "instrument_key": ["OKX:PERP:XYZUSDT"],
                "instrument_type": ["PERP"],
                "league_id": [""],
                "chain": [""],
                "capture_status": ["attempted_failed"],
                "error_reason": ["LegacyBlankErrorReasonError"],
            }
        )
        monkeypatch.setattr(mod, "_download_manifest", _fixed_download(df, str(tmp_path / "manifest.parquet")))

        import time as _time

        import unified_trading_library.instruments_catalog_reader as icr

        icr.clear_catalog_cache()
        # Instrument was delisted 2024-06-01.
        catalog_df = pd.DataFrame(
            {
                "instrument_key": ["OKX:PERP:XYZUSDT"],
                "venue": ["OKX"],
                "raw_symbol": ["XYZUSDT"],
                "base_asset": ["XYZ"],
                "available_from_datetime": [pd.Timestamp("2022-01-01")],
                "available_to_datetime": [pd.Timestamp("2024-06-01")],
            }
        )
        icr._cache["cefi"] = (_time.monotonic() + 99999, catalog_df)

        uploaded: dict[str, pd.DataFrame] = {}

        class _FakeBlob:
            def upload_from_filename(self, filename: str) -> None:
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
        monkeypatch.setenv("VM_NAME", "ikenna-slot2-corrector-cefi-pytest-delisted")
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--apply-flips",
                "--confirm",
                "--report-dir",
                str(tmp_path),
            ],
        )
        rc = mod.main()
        assert rc == 0
        assert "shard" in uploaded
        shard = uploaded["shard"]
        assert len(shard) == 1
        assert shard.iloc[0]["error_reason"] == "EXPECTED_INSTRUMENT_DELISTED"
        assert shard.iloc[0]["capture_status"] == "empty_confirmed"


# ---------------------------------------------------------------------------
# Idempotency test (Test 3)
# ---------------------------------------------------------------------------


class TestIdempotencyRerun:
    def test_already_corrected_rows_produce_zero_candidates(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Re-running on already-corrected rows (empty_confirmed) is a no-op."""
        mod = _load_corrector_module()
        df = _already_corrected_manifest()
        monkeypatch.setattr(mod, "_download_manifest", _fixed_download(df, str(tmp_path / "manifest.parquet")))
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--report-dir",
                str(tmp_path),
            ],
        )
        rc = mod.main()
        assert rc == 0
        # No CSV report generated.
        csvs = sorted(tmp_path.glob("cefi-corrector-cefi-*.csv"))
        assert len(csvs) == 0

    def test_already_corrected_with_apply_flips_produces_no_upload(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Re-running --apply-flips on a clean manifest exits 0 without uploading."""
        mod = _load_corrector_module()
        df = _already_corrected_manifest()
        monkeypatch.setattr(mod, "_download_manifest", _fixed_download(df, str(tmp_path / "manifest.parquet")))

        upload_called: list[bool] = []

        class _FakeBlob:
            def upload_from_filename(self, _filename: str) -> None:
                upload_called.append(True)

        class _FakeBucket:
            def blob(self, _name: str) -> _FakeBlob:
                return _FakeBlob()

        class _FakeClient:
            def __init__(self, *_args: object, **_kwargs: object) -> None: ...

            def bucket(self, _name: str) -> _FakeBucket:
                return _FakeBucket()

        monkeypatch.setattr(mod.storage, "Client", _FakeClient)
        monkeypatch.setenv("MANIFEST_PER_VM_SHARDS", "true")
        monkeypatch.setenv("VM_NAME", "ikenna-slot2-corrector-cefi-idempotency")
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--apply-flips",
                "--confirm",
                "--report-dir",
                str(tmp_path),
            ],
        )
        rc = mod.main()
        assert rc == 0
        # No per-VM shard upload should have been attempted.
        assert len(upload_called) == 0


# ---------------------------------------------------------------------------
# Env-guard tests (Tests 4 and 5)
# ---------------------------------------------------------------------------


class TestEnvGuards:
    def test_apply_flips_missing_per_vm_shards_returns_exit_4(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """--apply-flips without MANIFEST_PER_VM_SHARDS=true → exit 4."""
        mod = _load_corrector_module()
        monkeypatch.setattr(
            mod,
            "_download_manifest",
            _fixed_download(_synthetic_cefi_manifest(), str(tmp_path / "m.parquet")),
        )
        monkeypatch.delenv("MANIFEST_PER_VM_SHARDS", raising=False)
        monkeypatch.delenv("VM_NAME", raising=False)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--apply-flips",
                "--confirm",
            ],
        )
        rc = mod.main()
        assert rc == 4

    def test_apply_flips_missing_confirm_returns_exit_1(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """--apply-flips without --confirm → exit 1 (intent gate)."""
        mod = _load_corrector_module()
        monkeypatch.setattr(
            mod,
            "_download_manifest",
            _fixed_download(_synthetic_cefi_manifest(), str(tmp_path / "m.parquet")),
        )
        monkeypatch.setenv("MANIFEST_PER_VM_SHARDS", "true")
        monkeypatch.setenv("VM_NAME", "ikenna-slot2-corrector-cefi-no-confirm")
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--apply-flips",
                # --confirm intentionally omitted
            ],
        )
        rc = mod.main()
        assert rc == 1

    def test_apply_flips_missing_vm_name_returns_exit_4(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """MANIFEST_PER_VM_SHARDS set but VM_NAME missing → exit 4."""
        mod = _load_corrector_module()
        monkeypatch.setattr(
            mod,
            "_download_manifest",
            _fixed_download(_synthetic_cefi_manifest(), str(tmp_path / "m.parquet")),
        )
        monkeypatch.setenv("MANIFEST_PER_VM_SHARDS", "true")
        monkeypatch.delenv("VM_NAME", raising=False)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "reconcile_correct_legacy_blank_misflips_cefi_2026_05_13.py",
                "--asset-group",
                "cefi",
                "--apply-flips",
                "--confirm",
            ],
        )
        rc = mod.main()
        assert rc == 4
