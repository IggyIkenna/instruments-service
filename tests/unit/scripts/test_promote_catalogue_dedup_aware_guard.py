"""Dedup-aware cefi monotonic guard (RULED 2026-07-28, was `[OPERATOR]`, retagged
`[DATA]` — ``dp_catalog_not_running_sports_prediction_2026_07_15.md``).

Root cause this fixes: cefi's daily incremental job already runs 3 Phase D dedup
passes (`_dedup_bybit_future_base_asset_parsing`, `_dedup_cefi_expiry_off_by_one`,
`_dedup_cefi_margin_type_mislabel`) on the FRESHLY-ROLLED side before the monotonic
guard compares row counts, but `promote_catalogue` was comparing that
ALREADY-DEDUPED new count against a NOT-equally-deduped current-catalogue count —
so a day whose window happened to touch enough still-ambiguous historical
duplicate pairs could trip `CATALOGUE_SHRINK_BLOCKED` even though zero real
instruments were lost (live-confirmed `dropped_active: 0` on every 2026-07-16
through 07-27 occurrence). The fix re-runs the SAME 3 passes over the CURRENT
catalogue before counting, for cefi only.

Kept in its own file (not appended to the large, frequently-contended
test_build_instrument_catalogue.py) — same convention as
test_shrink_drop_diagnostics.py.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_script_module(filename: str, module_name: str) -> ModuleType:
    """Load a script in instruments-service/scripts/ as a module by path."""
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / filename
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def rollup() -> ModuleType:
    return _load_script_module("build_instrument_catalogue.py", "_bic_dedup_guard_test_module")


class _FakeStorage:
    """Minimal duck-typed StorageClient serving one fixed catalogue blob."""

    def __init__(self, blob_path: str, payload: bytes) -> None:
        self._blob_path = blob_path
        self._payload = payload

    def blob_exists(self, bucket: str, blob_path: str) -> bool:
        return blob_path == self._blob_path

    def download_bytes(self, bucket: str, blob_path: str) -> bytes:
        assert blob_path == self._blob_path
        return self._payload


def _to_parquet_bytes(rows: list[dict[str, object]]) -> bytes:
    import io

    buf = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buf, index=False)
    return buf.getvalue()


def _off_by_one_option_rows() -> list[dict[str, object]]:
    """The real DERIBIT (venue, instrument_type, raw_symbol) 3-tuple that
    `_dedup_cefi_expiry_off_by_one` collapses (measured 2026-07-22, see
    test_build_instrument_catalogue.py::TestDedupCefiExpiryOffByOne for the
    original fixture this mirrors)."""
    base = {
        "venue": "DERIBIT",
        "instrument_type": "OPTION",
        "raw_symbol": "ETH-17JUL26-2200-P",
        "base_asset": "ETH",
        "margin_type": "inverse",
        "available_from": "2026-07-15",
    }
    correct = {
        **base,
        "instrument_id": "DERIBIT:OPTION:ETH-USD@INV-20260717-2200-P",
        "canonical_instrument_id": "DERIBIT:OPTION:ETH-USD@INV-20260717-2200-P",
        "expiry": "2026-07-17",
        "available_to": "2026-07-17",
    }
    off_by_one = {
        **base,
        "instrument_id": "DERIBIT:OPTION:ETH-USD@INV-20260718-2200-P",
        "canonical_instrument_id": "DERIBIT:OPTION:ETH-USD@INV-20260718-2200-P",
        "expiry": "2026-07-18",
        "available_to": "2026-07-18",
    }
    return [correct, off_by_one]


def _distinct_real_rows() -> list[dict[str, object]]:
    """Two genuinely distinct cefi instruments — none of the 3 Phase D dedup
    passes ever collapse this pair (different raw_symbol -> no shared group key
    for any of the 3 patterns; BINANCE is scoped out of all 3 passes anyway)."""
    return [
        {
            "venue": "BINANCE-FUTURES",
            "instrument_type": "PERPETUAL",
            "raw_symbol": "BTCUSDT",
            "base_asset": "BTC",
            "margin_type": "linear",
            "instrument_id": "BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN",
            "canonical_instrument_id": "BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN",
            "available_from": "2024-01-01",
            "expiry": None,
            "available_to": None,
        },
        {
            "venue": "BINANCE-FUTURES",
            "instrument_type": "PERPETUAL",
            "raw_symbol": "ETHUSDT",
            "base_asset": "ETH",
            "margin_type": "linear",
            "instrument_id": "BINANCE-FUTURES:PERPETUAL:ETH-USDT@LIN",
            "canonical_instrument_id": "BINANCE-FUTURES:PERPETUAL:ETH-USDT@LIN",
            "available_from": "2024-01-01",
            "expiry": None,
            "available_to": None,
        },
    ]


class TestApplyCefiPhaseDDedupsHelper:
    def test_extracted_helper_matches_run_rollups_own_pass_order(self, rollup: ModuleType) -> None:
        """The shared helper collapses the known off-by-one pair exactly like the
        3 passes called individually would (proves the extraction is a pure
        refactor, not a behavior change)."""
        df = pd.DataFrame(_off_by_one_option_rows())
        out = rollup._apply_cefi_phase_d_dedups(df)
        assert len(out) == 1
        assert out.to_dict("records")[0]["expiry"] == "2026-07-17"

    def test_distinct_rows_round_trip_unchanged(self, rollup: ModuleType) -> None:
        df = pd.DataFrame(_distinct_real_rows())
        out = rollup._apply_cefi_phase_d_dedups(df)
        assert len(out) == 2


class TestPromoteCatalogueDedupAwareGuard:
    """`promote_catalogue`'s monotonic guard, asset_group="cefi"."""

    def test_dedup_only_shrink_does_not_trip_guard(self, rollup: ModuleType) -> None:
        """Current catalogue holds an un-collapsed off-by-one duplicate pair (2
        rows, 1 real instrument). A freshly-rolled catalogue that already
        collapsed it (1 row) must NOT trip CATALOGUE_SHRINK_BLOCKED — before this
        fix, new=1 < raw current=2 would have blocked it."""
        bucket, env = "instruments-store-cefi-prd-test", "prod"
        canonical_blob = f"{env}/{rollup.CATALOG_FILENAME}"
        current_payload = _to_parquet_bytes(_off_by_one_option_rows())
        storage = _FakeStorage(canonical_blob, current_payload)

        new_df = pd.DataFrame([_off_by_one_option_rows()[0]])  # already-deduped: 1 row

        code = rollup.promote_catalogue(
            storage,
            bucket,
            env,
            new_df,
            asset_group="cefi",
            allow_shrink=False,
            dry_run=True,
        )
        assert code == 0, "dedup-only-driven shrink must be ACCEPTED, not CATALOGUE_SHRINK_BLOCKED"

    def test_genuine_active_row_drop_still_trips_guard(
        self, rollup: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Current catalogue holds 2 genuinely distinct instruments (no dedup
        pass touches this pair). A freshly-rolled catalogue that drops one of
        them for real (1 row) must STILL trip CATALOGUE_SHRINK_BLOCKED — the
        dedup-aware guard must not weaken real data-loss protection."""
        monkeypatch.setattr(rollup, "log_event", lambda *args, **kwargs: None)
        bucket, env = "instruments-store-cefi-prd-test", "prod"
        canonical_blob = f"{env}/{rollup.CATALOG_FILENAME}"
        current_payload = _to_parquet_bytes(_distinct_real_rows())
        storage = _FakeStorage(canonical_blob, current_payload)

        new_df = pd.DataFrame([_distinct_real_rows()[0]])  # genuinely drops the ETHUSDT row

        code = rollup.promote_catalogue(
            storage,
            bucket,
            env,
            new_df,
            asset_group="cefi",
            allow_shrink=False,
            dry_run=True,
        )
        assert code == 1, "a genuine active-row drop must still be BLOCKED"

    def test_non_cefi_asset_group_unaffected_by_dedup_aware_guard(
        self, rollup: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-cefi asset_group must never have Phase D dedup applied to its
        current-catalogue count — the SAME 2-row off-by-one-shaped duplicate
        (irrelevant content for a non-cefi AG, used only as inert row data here)
        stays un-deduped, so an equivalent shrink is still blocked."""
        monkeypatch.setattr(rollup, "log_event", lambda *args, **kwargs: None)
        bucket, env = "instruments-store-defi-prd-test", "prod"
        canonical_blob = f"{env}/{rollup.CATALOG_FILENAME}"
        current_payload = _to_parquet_bytes(_off_by_one_option_rows())
        storage = _FakeStorage(canonical_blob, current_payload)

        new_df = pd.DataFrame([_off_by_one_option_rows()[0]])  # 1 row vs raw current=2

        code = rollup.promote_catalogue(
            storage,
            bucket,
            env,
            new_df,
            asset_group="defi",
            allow_shrink=False,
            dry_run=True,
        )
        assert code == 1, "dedup-aware guard is cefi-only; other asset groups must be unaffected"
