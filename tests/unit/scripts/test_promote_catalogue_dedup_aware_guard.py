"""Dedup-aware monotonic guard (RULED 2026-07-28, was `[OPERATOR]`, retagged
`[DATA]` — ``dp_catalog_not_running_sports_prediction_2026_07_15.md``; generalised
2026-08-02 for the defi ``DP-CATALOG-001`` recurrence).

Root cause (cefi, original fix): cefi's daily incremental job already runs 3
Phase D dedup passes (`_dedup_bybit_future_base_asset_parsing`,
`_dedup_cefi_expiry_off_by_one`, `_dedup_cefi_margin_type_mislabel`) on the
FRESHLY-ROLLED side before the monotonic guard compares row counts, but
`promote_catalogue` was comparing that ALREADY-DEDUPED new count against a
NOT-equally-deduped current-catalogue count — so a day whose window happened to
touch enough still-ambiguous historical duplicate pairs could trip
`CATALOGUE_SHRINK_BLOCKED` even though zero real instruments were lost
(live-confirmed `dropped_active: 0` on every 2026-07-16 through 07-27
occurrence). The fix re-runs the SAME 3 passes over the CURRENT catalogue before
counting, for cefi only.

Root cause (defi, 2026-08-02 follow-up): the SAME class recurs for ANY asset
group via `_merge_incremental`'s own dedup-on-re-observation behaviour — when
the current catalogue holds 2 rows sharing one `_incremental_merge_keys`
identity (e.g. a `pool::<CHAIN>::<addr>` pair left over from pre-canonicalisation
drift) and a fresh by_date window re-observes that instrument, the merge
correctly collapses the pair to 1 row, shrinking `len(df)` by 1 even though
zero active instruments were lost. Confirmed live: `lifecycle-catalogue-regen-defi`
blocked 2 consecutive days (DP-CATALOG-001 CRITICAL, 46h stale) on exactly this
shape (14 duplicate pool keys in the stored catalogue, 8 re-observed by the
window, `dropped_active: 0`). `_dedupe_by_incremental_merge_key` generalises the
fix: dedupe the CURRENT catalogue by its own merge key, for every asset group,
in addition to cefi's Phase D passes.

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


def _duplicate_defi_pool_rows() -> list[dict[str, object]]:
    """Two rows for the SAME on-chain pool (identical chain + pool_address, hence
    identical `pool::<CHAIN>::<addr>` merge key) under different `instrument_id`
    spellings — the confirmed 2026-08-02 defi shape (14 such pairs found live in
    ``gs://instruments-store-defi-prd-.../prod/catalog.parquet``), already-closed
    (``available_to`` set) exactly like the live `dropped_delisted` sample."""
    base = {
        "instrument_type": "pool",
        "venue": "AERODROME_V3",
        "chain": "BASE",
        "pool_address": "0x0652202c4b2d09cb93aedefadc14b36869483a98",
        "available_from": "2025-01-24",
        "available_to": "2026-07-30",
    }
    return [
        {**base, "instrument_id": "AERODROME_V3-BASE:POOL:0x0652202c4b2d09cb93aedefadc14b36869483a98"},
        {**base, "instrument_id": "AERODROME_V3-BASE:POOL:0x0652202C4B2D09CB93AEDEFADC14B36869483A98-legacy"},
    ]


def _distinct_defi_pool_rows() -> list[dict[str, object]]:
    """Two genuinely distinct on-chain pools (different pool_address) — must
    never collapse under any dedup pass."""
    return [
        {
            "instrument_type": "pool",
            "venue": "AERODROME_V3",
            "chain": "BASE",
            "pool_address": "0x0652202c4b2d09cb93aedefadc14b36869483a98",
            "instrument_id": "AERODROME_V3-BASE:POOL:0x0652202c4b2d09cb93aedefadc14b36869483a98",
            "available_from": "2025-01-24",
            "available_to": None,
        },
        {
            "instrument_type": "pool",
            "venue": "UNISWAP_V3",
            "chain": "ETHEREUM",
            "pool_address": "0xa1f8a6807c402e4a15ef4eba36528a3fed24e577",
            "instrument_id": "UNISWAP_V3-ETHEREUM:POOL:0xa1f8a6807c402e4a15ef4eba36528a3fed24e577",
            "available_from": "2024-06-01",
            "available_to": None,
        },
    ]


class TestDedupeByIncrementalMergeKeyHelper:
    def test_collapses_duplicate_pool_key_pair(self, rollup: ModuleType) -> None:
        df = pd.DataFrame(_duplicate_defi_pool_rows())
        out = rollup._dedupe_by_incremental_merge_key(df, asset_group="defi")
        assert len(out) == 1

    def test_distinct_pool_rows_round_trip_unchanged(self, rollup: ModuleType) -> None:
        df = pd.DataFrame(_distinct_defi_pool_rows())
        out = rollup._dedupe_by_incremental_merge_key(df, asset_group="defi")
        assert len(out) == 2

    def test_empty_frame_round_trips(self, rollup: ModuleType) -> None:
        df = pd.DataFrame(_distinct_defi_pool_rows()).iloc[0:0]
        out = rollup._dedupe_by_incremental_merge_key(df, asset_group="defi")
        assert len(out) == 0


class TestPromoteCatalogueDedupAwareGuardDefiPoolKey:
    """The confirmed 2026-08-02 defi ``DP-CATALOG-001`` shape: ``promote_catalogue``
    for ``asset_group="defi"``."""

    def test_pool_key_dedup_only_shrink_does_not_trip_guard(self, rollup: ModuleType) -> None:
        """Current catalogue holds an un-collapsed pool-key duplicate pair (2 rows,
        1 real on-chain pool). A freshly-rolled catalogue that re-observed the pool
        and already collapsed it (1 row, via `_merge_incremental`'s branch 1) must
        NOT trip CATALOGUE_SHRINK_BLOCKED — before this fix, new=1 < raw current=2
        would have blocked it exactly like the live incident."""
        bucket, env = "instruments-store-defi-prd-test", "prod"
        canonical_blob = f"{env}/{rollup.CATALOG_FILENAME}"
        current_payload = _to_parquet_bytes(_duplicate_defi_pool_rows())
        storage = _FakeStorage(canonical_blob, current_payload)

        new_df = pd.DataFrame([_duplicate_defi_pool_rows()[0]])  # already-collapsed: 1 row

        code = rollup.promote_catalogue(
            storage,
            bucket,
            env,
            new_df,
            asset_group="defi",
            allow_shrink=False,
            dry_run=True,
        )
        assert code == 0, "pool-key-dedup-only-driven shrink must be ACCEPTED, not CATALOGUE_SHRINK_BLOCKED"

    def test_genuine_active_pool_drop_still_trips_guard(
        self, rollup: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Current catalogue holds 2 genuinely distinct pools (different
        pool_address, no dedup pass ever touches this pair). A freshly-rolled
        catalogue that drops one of them for real (1 row) must STILL trip
        CATALOGUE_SHRINK_BLOCKED — the generalised dedup-aware guard must not
        weaken real data-loss protection for defi."""
        monkeypatch.setattr(rollup, "log_event", lambda *args, **kwargs: None)
        bucket, env = "instruments-store-defi-prd-test", "prod"
        canonical_blob = f"{env}/{rollup.CATALOG_FILENAME}"
        current_payload = _to_parquet_bytes(_distinct_defi_pool_rows())
        storage = _FakeStorage(canonical_blob, current_payload)

        new_df = pd.DataFrame([_distinct_defi_pool_rows()[0]])  # genuinely drops the UNISWAP_V3 pool

        code = rollup.promote_catalogue(
            storage,
            bucket,
            env,
            new_df,
            asset_group="defi",
            allow_shrink=False,
            dry_run=True,
        )
        assert code == 1, "a genuine active-pool drop must still be BLOCKED"


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
