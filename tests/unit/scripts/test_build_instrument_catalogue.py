"""Unit tests — build_instrument_catalogue.py roll-up + monotonic guard, and the
audit_instrument_definition_completeness.py provisional completeness summary.

Tests cover (no GCS — pure functions + module-by-path load, mirroring the v2 enumerator tests):
  - build_catalogue_dataframe lifecycle math: first/last day windows, available_to=None when
    present on the latest snapshot day, delisted instrument gets available_to stamped, metadata
    follows the most-recent snapshot, instrument_key/instrument_id id-column fallback, empty input.
  - evaluate_monotonic_guard accept (first run / growth / equal) + reject (shrink) + override.
  - The catalogue output is consumable by enumerate_expected_universe._catalog_from_dataframe
    (no schema drift).
  - summarise_completeness: status tabulation, attempted_failed gap surfacing, provisional verdict.

Plan: proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md (Phase 1 + Phase 0 P0 tests).
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
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
    return _load_script_module("build_instrument_catalogue.py", "_build_instrument_catalogue_test_module")


@pytest.fixture(scope="module")
def audit() -> ModuleType:
    return _load_script_module(
        "audit_instrument_definition_completeness.py",
        "_audit_instrument_definition_completeness_test_module",
    )


def _snapshot(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _parquet_bytes(rows: list[dict[str, object]]) -> bytes:
    import io

    buf = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buf, index=False)
    return buf.getvalue()


class _Blob:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeStorage:
    """Minimal duck-typed StorageClient for the by_date walk + dry-run promote path."""

    def __init__(self, blobs: dict[str, bytes]) -> None:
        self._blobs = blobs

    def list_blobs(self, bucket: str, prefix: str = "", **_: object) -> list[_Blob]:
        return [_Blob(name) for name in self._blobs if name.startswith(prefix)]

    def download_bytes(self, bucket: str, blob_path: str) -> bytes:
        return self._blobs[blob_path]

    def blob_exists(self, bucket: str, blob_path: str) -> bool:
        return blob_path in self._blobs


# ---------------------------------------------------------------------------
# build_catalogue_dataframe — lifecycle math
# ---------------------------------------------------------------------------


def test_rollup_first_last_day_window(rollup: ModuleType) -> None:
    """available_from = first day present; available_to = last day when not on latest day."""
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "AAA", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
        (d2, _snapshot([{"instrument_key": "AAA", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
        # AAA absent on d3 → delisted; BBB present on the latest day → still active.
        (d3, _snapshot([{"instrument_key": "BBB", "venue": "V", "instrument_type": "SPOT_PAIR"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}

    assert by_id["AAA"]["available_from"] == "2024-01-01"
    assert by_id["AAA"]["available_to"] == "2024-01-02"  # delisted before the latest day
    assert by_id["BBB"]["available_from"] == "2024-01-03"
    assert by_id["BBB"]["available_to"] is None  # present on the latest day → still active


def test_rollup_active_on_latest_day_has_null_available_to(rollup: ModuleType) -> None:
    """An instrument present on the latest snapshot day has available_to=None."""
    d1, d2 = date(2024, 5, 1), date(2024, 5, 2)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "X", "venue": "V"}])),
        (d2, _snapshot([{"instrument_key": "X", "venue": "V"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    row = df.to_dict("records")[0]
    assert row["available_from"] == "2024-05-01"
    assert row["available_to"] is None


# ---------------------------------------------------------------------------
# §7.3 available_to = venue-truth + per-venue, thin-day-aware (G1.1 false-delisting fix)
# ---------------------------------------------------------------------------


def test_rollup_thin_latest_day_does_not_false_delist_other_venues(rollup: ModuleType) -> None:
    """A thin/partial latest capture day on ONE venue must NOT delist a full venue.

    The G1.1 live bug: a thin BINANCE-FUTURES latest day (678 → 47) + a GLOBAL
    last-seen ``available_to`` stamped every venue's actives delisted off that one
    thin day. The per-venue, thin-day-aware liveness anchor fixes it.
    """
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    # VENUE_A: 10 perps every day INCLUDING the latest → all active.
    a_rows = [{"instrument_key": f"A{i}", "venue": "VENUE_A", "instrument_type": "PERPETUAL"} for i in range(10)]
    snapshots = [
        (d1, _snapshot(a_rows)),
        (d2, _snapshot(a_rows)),
        (d3, _snapshot(a_rows)),  # VENUE_A full on the latest day
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}
    # Every VENUE_A perp present on its own venue's latest full day → active.
    assert all(by_id[f"A{i}"]["available_to"] is None for i in range(10))


def test_rollup_thin_latest_day_keeps_full_prior_day_actives_active(rollup: ModuleType) -> None:
    """A venue whose LATEST day is thin (partial capture) keeps prior-full actives active."""
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    full = [{"instrument_key": f"P{i}", "venue": "VENUE_B", "instrument_type": "PERPETUAL"} for i in range(20)]
    thin = [{"instrument_key": "P0", "venue": "VENUE_B", "instrument_type": "PERPETUAL"}]  # 1 of 20 = thin
    snapshots = [(d1, _snapshot(full)), (d2, _snapshot(full)), (d3, _snapshot(thin))]
    df = rollup.build_catalogue_dataframe(snapshots)
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}
    # d3 is thin (5% of the 20-median) → the last FULL day is d2; all 20 actives
    # present on d2 stay active (NOT delisted off the thin d3).
    assert all(by_id[f"P{i}"]["available_to"] is None for i in range(20))


def test_rollup_dated_instrument_available_to_is_venue_truth_expiry(rollup: ModuleType) -> None:
    """A dated FUTURE/OPTION available_to = its venue-declared ``expiry`` (not last-seen)."""
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    rows = [
        {
            "instrument_key": "DERIBIT:OPTION:BTC-2JAN24",
            "venue": "DERIBIT",
            "instrument_type": "OPTION",
            "expiry": "2024-01-02",
        }
    ]
    # Present on both days incl. the latest, but expiry stamps the real close date.
    df = rollup.build_catalogue_dataframe([(d1, _snapshot(rows)), (d2, _snapshot(rows))])
    row = df.to_dict("records")[0]
    assert row["available_to"] == "2024-01-02"  # venue-truth expiry, not None/last-seen


def test_rollup_delisted_at_takes_priority_over_liveness(rollup: ModuleType) -> None:
    """An explicit ``delisted_at`` (venue-reported removal) wins over last-seen liveness."""
    d1, d2 = date(2024, 6, 1), date(2024, 6, 2)
    rows = [
        {
            "instrument_key": "BINANCE-SPOT:SPOT_PAIR:FOO-USDT",
            "venue": "BINANCE-SPOT",
            "instrument_type": "SPOT_PAIR",
            "delisted_at": "2024-05-15",
        }
    ]
    df = rollup.build_catalogue_dataframe([(d1, _snapshot(rows)), (d2, _snapshot(rows))])
    row = df.to_dict("records")[0]
    assert row["available_to"] == "2024-05-15"  # venue-reported delisting date


def test_rollup_perp_active_on_own_venue_latest_full_day(rollup: ModuleType) -> None:
    """A perp with no expiry/delisting, present on its venue's latest full day → active."""
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    rows = [{"instrument_key": "HYPERLIQUID:PERPETUAL:BTC", "venue": "HYPERLIQUID", "instrument_type": "PERPETUAL"}]
    df = rollup.build_catalogue_dataframe([(d1, _snapshot(rows)), (d2, _snapshot(rows))])
    row = df.to_dict("records")[0]
    assert row["available_to"] is None


def test_rollup_genuine_delisting_still_stamped(rollup: ModuleType) -> None:
    """A perp absent from its venue's recent FULL days (real delisting) keeps last-seen."""
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    base = [{"instrument_key": f"K{i}", "venue": "OKX-SWAP", "instrument_type": "PERPETUAL"} for i in range(10)]
    gone = [*base, {"instrument_key": "DEAD", "venue": "OKX-SWAP", "instrument_type": "PERPETUAL"}]
    # DEAD present only on d1; d2/d3 are FULL (10 each) without it → genuine delisting.
    snapshots = [(d1, _snapshot(gone)), (d2, _snapshot(base)), (d3, _snapshot(base))]
    df = rollup.build_catalogue_dataframe(snapshots)
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}
    assert by_id["DEAD"]["available_to"] == "2024-01-01"  # genuinely delisted, last-seen stamp
    assert all(by_id[f"K{i}"]["available_to"] is None for i in range(10))


def test_rollup_metadata_follows_most_recent_snapshot(rollup: ModuleType) -> None:
    """Metadata (venue/type/chain) is taken from the instrument's most-recent definition."""
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    snapshots = [
        (d2, _snapshot([{"instrument_key": "P", "venue": "NEW_V", "instrument_type": "PERP", "chain": "ARBITRUM"}])),
        (d1, _snapshot([{"instrument_key": "P", "venue": "OLD_V", "instrument_type": "PERP", "chain": "ETHEREUM"}])),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    row = df.to_dict("records")[0]
    assert row["venue"] == "NEW_V"
    assert row["chain"] == "ARBITRUM"


def test_rollup_carries_raw_symbol_and_base_asset(rollup: ModuleType) -> None:
    """raw_symbol + base_asset are carried from the by_date source so the UTL
    catalogue reader's ``venue+raw_symbol`` (unique) / ``venue+base_asset``
    (fallback) lifecycle cross-ref matches CeFi manifest bare symbols (E5)."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "BINANCE-FUTURES:PERPETUAL:ADA-USDT",
                            "venue": "BINANCE-FUTURES",
                            "instrument_type": "PERPETUAL",
                            "raw_symbol": "ADA-PERP",
                            "base_asset": "ADA",
                        }
                    ]
                ),
            )
        ]
    )
    assert "raw_symbol" in df.columns
    assert "base_asset" in df.columns
    row = df.to_dict("records")[0]
    assert row["raw_symbol"] == "ADA-PERP"
    assert row["base_asset"] == "ADA"


def test_rollup_raw_symbol_blank_when_source_absent(rollup: ModuleType) -> None:
    """A source row without raw_symbol/base_asset yields "" (never NaN/fabricated)."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe([(d1, _snapshot([{"instrument_key": "X", "venue": "V"}]))])
    row = df.to_dict("records")[0]
    assert row["raw_symbol"] == ""
    assert row["base_asset"] == ""


def test_rollup_defi_pool_emits_dual_form_ids(rollup: ModuleType) -> None:
    """DeFi POOL row (operator Refinement 1): catalogue instrument_id becomes the
    canonical ``pool_address.lower()``, venue splits to the bare protocol, chain is
    populated, and ``glued_pair_id`` carries the human-readable UI form."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "UNISWAP_V3-POLYGON:POOL:USDC-WETH:500",
                            "venue": "UNISWAP_V3-POLYGON",
                            "instrument_type": "POOL",
                            "raw_symbol": "0x45dda9cb7c25131df268515131f647d726f50608",
                            "pool_address": "0x45dDa9cb7c25131DF268515131f647d726f50608",
                            "base_asset": "USDC",
                            "quote_asset": "WETH",
                            "pool_fee_tier": 5.0,
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    assert row["instrument_id"] == "0x45dda9cb7c25131df268515131f647d726f50608"
    assert row["venue"] == "UNISWAP_V3"
    assert row["chain"] == "POLYGON"
    assert row["glued_pair_id"] == "UNISWAP_V3-POLYGON:POOL:USDC-WETH:500"
    assert row["pool_address"] == "0x45dda9cb7c25131df268515131f647d726f50608"


def test_rollup_defi_pool_dual_form_round_trips_via_converter(rollup: ModuleType) -> None:
    """The emitted glued_pair_id parses back to the same venue/chain/pair/fee (SSOT converter)."""
    from unified_api_contracts import parse_glued_pool_id

    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "UNISWAPV3-ARBITRUM:POOL:AAVE-USDC:100",
                            "venue": "UNISWAPV3-ARBITRUM",
                            "instrument_type": "POOL",
                            "raw_symbol": "0xf9188aff",
                            "pool_address": "0xF9188AFF",
                            "base_asset": "AAVE",
                            "quote_asset": "USDC",
                            "pool_fee_tier": 1.0,
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    parsed = parse_glued_pool_id(str(row["glued_pair_id"]))
    assert parsed is not None
    assert parsed.venue == "UNISWAP_V3"
    assert parsed.chain == "ARBITRUM"
    assert parsed.base_asset == "AAVE"
    assert parsed.quote_asset == "USDC"
    assert parsed.fee == "100"


def test_rollup_defi_pool_spelling_variants_collapse_to_one_open_lifecycle(rollup: ModuleType) -> None:
    """Phase 2 premature-delisting fix: the SAME physical pool seen under two venue
    spellings (``UNISWAPV3`` early, ``UNISWAP_V3`` later — the ~2026-05-08 adapter
    switchover) collapses into ONE lifecycle keyed by pool_address, so the pool is
    present on the latest day → ``available_to`` is None (NOT wrongly DELISTED)."""
    addr = "0x45dda9cb7c25131df268515131f647d726f50608"
    d_old, d_switch, d_now = date(2026, 5, 1), date(2026, 5, 8), date(2026, 6, 20)
    old_spelling = {
        "instrument_key": "UNISWAPV3-POLYGON:POOL:USDC-WETH:500",
        "venue": "UNISWAPV3-POLYGON",
        "instrument_type": "POOL",
        "raw_symbol": addr,
        "pool_address": addr,
        "base_asset": "USDC",
        "quote_asset": "WETH",
        "pool_fee_tier": 5.0,
    }
    new_spelling = {
        **old_spelling,
        "instrument_key": "UNISWAP_V3-POLYGON:POOL:USDC-WETH:500",
        "venue": "UNISWAP_V3-POLYGON",
    }
    df = rollup.build_catalogue_dataframe(
        [
            (d_old, _snapshot([old_spelling])),
            (d_switch, _snapshot([new_spelling])),
            (d_now, _snapshot([new_spelling])),
        ]
    )
    pool_rows = df[df["instrument_type"].astype(str).str.upper() == "POOL"].to_dict("records")
    assert len(pool_rows) == 1
    row = pool_rows[0]
    assert row["instrument_id"] == addr
    assert row["available_to"] is None
    assert row["available_from"] == "2026-05-01"
    assert row["venue"] == "UNISWAP_V3"
    assert row["chain"] == "POLYGON"


def test_rollup_non_pool_row_has_blank_dual_form(rollup: ModuleType) -> None:
    """A CeFi/non-pool row carries blank glued_pair_id + pool_address (no fabrication)."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "BINANCE-FUTURES:PERPETUAL:ADA-USDT",
                            "venue": "BINANCE-FUTURES",
                            "instrument_type": "PERPETUAL",
                            "raw_symbol": "ADA-PERP",
                            "base_asset": "ADA",
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    assert row["instrument_id"] == "BINANCE-FUTURES:PERPETUAL:ADA-USDT"
    assert row["glued_pair_id"] == ""
    assert row["pool_address"] == ""


def test_rollup_legacy_raw_binance_futures_dated_future_id_canonicalized(rollup: ModuleType) -> None:
    """A legacy by_date row captured BEFORE the adapter's 2026-07-09 fix still carries the
    raw wire-form dated-FUTURE id (``BINANCE-FUTURES:FUTURE:ETHUSDT_260626``) — the roll-up
    must rebuild it to the dash-canonical shape every other dated-futures venue in the same
    catalogue produces (cefi_mtds_writer_raw_symbol_vs_canonical_eu_namespace_mismatch_
    2026_07_15.md)."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "BINANCE-FUTURES:FUTURE:ETHUSDT_260626",
                            "venue": "BINANCE-FUTURES",
                            "instrument_type": "FUTURE",
                            "raw_symbol": "ETHUSDT_260626",
                            "base_asset": "ETH",
                            "quote_asset": "USDT",
                            "margin_type": "linear",
                            "expiry": "2026-06-26",
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    assert row["instrument_id"] == "BINANCE-FUTURES:FUTURE:ETH-USDT@LIN-20260626"


def test_rollup_legacy_raw_binance_delivery_inverse_dated_future_id_canonicalized(rollup: ModuleType) -> None:
    """Same defect class, BINANCE-DELIVERY inverse side (``@INV``)."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "BINANCE-DELIVERY:FUTURE:BTCUSD_260925",
                            "venue": "BINANCE-DELIVERY",
                            "instrument_type": "FUTURE",
                            "raw_symbol": "BTCUSD_260925",
                            "base_asset": "BTC",
                            "quote_asset": "USD",
                            "margin_type": "inverse",
                            "expiry": "2026-09-25",
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    assert row["instrument_id"] == "BINANCE-DELIVERY:FUTURE:BTC-USD@INV-20260925"


def test_rollup_already_canonical_dated_future_id_untouched(rollup: ModuleType) -> None:
    """A row already fixed at the adapter (KRAKEN-FUTURES, carries ``@``) is an idempotent
    no-op — the roll-up must never re-derive/mangle an already-canonical id."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "KRAKEN-FUTURES:FUTURE:BTC-USD@LIN-20260626",
                            "venue": "KRAKEN-FUTURES",
                            "instrument_type": "FUTURE",
                            "raw_symbol": "FF_XBTUSD_260626",
                            "base_asset": "BTC",
                            "quote_asset": "USD",
                            "margin_type": "linear",
                            "expiry": "2026-06-26",
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    assert row["instrument_id"] == "KRAKEN-FUTURES:FUTURE:BTC-USD@LIN-20260626"


def test_rollup_raw_dated_future_missing_fields_degrades_unchanged(rollup: ModuleType) -> None:
    """A dated-FUTURE row missing a field the rebuild needs (here: quote_asset) must
    degrade to the raw id unchanged rather than guess or raise."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "BINANCE-FUTURES:FUTURE:ETHUSDT_260626",
                            "venue": "BINANCE-FUTURES",
                            "instrument_type": "FUTURE",
                            "raw_symbol": "ETHUSDT_260626",
                            "base_asset": "ETH",
                            "margin_type": "linear",
                            "expiry": "2026-06-26",
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    assert row["instrument_id"] == "BINANCE-FUTURES:FUTURE:ETHUSDT_260626"


def test_rollup_cefi_row_carries_through_adapter_populated_canonical_instrument_id(rollup: ModuleType) -> None:
    """A CeFi row captured AFTER the adapter fix (canonical_instrument_id_cefi_defi_
    backfill_2026_07_14.md) carries its own value through unchanged."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "DERIBIT:OPTION:BTC@INV-20260713-56000-C",
                            "canonical_instrument_id": "DERIBIT:OPTION:BTC@INV-20260713-56000-C",
                            "venue": "DERIBIT",
                            "instrument_type": "OPTION",
                            "raw_symbol": "BTC-13JUL26-56000-C",
                            "base_asset": "BTC",
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    assert row["canonical_instrument_id"] == "DERIBIT:OPTION:BTC@INV-20260713-56000-C"


def test_rollup_cefi_row_backfills_canonical_instrument_id_from_instrument_key(rollup: ModuleType) -> None:
    """A historical CeFi row captured BEFORE the adapter fix (no canonical_instrument_id
    in the source) is backfilled from instrument_key -- the exact value a fresh capture
    would have produced, since CeFi has no raw-code-to-human-name translation gap."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "BINANCE-FUTURES:PERPETUAL:ADA-USDT",
                            "venue": "BINANCE-FUTURES",
                            "instrument_type": "PERPETUAL",
                            "raw_symbol": "ADA-PERP",
                            "base_asset": "ADA",
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    assert row["canonical_instrument_id"] == "BINANCE-FUTURES:PERPETUAL:ADA-USDT"


def test_rollup_defi_pool_row_backfills_canonical_instrument_id_from_instrument_key(rollup: ModuleType) -> None:
    """A DeFi POOL row backfills canonical_instrument_id from instrument_key -- NOT
    from the pool_address-based DefiPoolIdentity.canonical_instrument_id concept,
    which is a separate, unrelated field."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "UNISWAP_V3-ARBITRUM:POOL:USDC-WETH:3000",
                            "venue": "UNISWAP_V3-ARBITRUM",
                            "instrument_type": "POOL",
                            "pool_address": "0x88E6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
                            "base_asset": "USDC",
                            "quote_asset": "WETH",
                            "pool_fee_tier": "3000",
                        }
                    ]
                ),
            )
        ]
    )
    row = df.to_dict("records")[0]
    # instrument_id is re-keyed to the pool address (DUAL-FORM) -- canonical_instrument_id
    # is NOT that; it mirrors instrument_key instead, per the operator-approved policy.
    assert row["instrument_id"] == "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    assert row["canonical_instrument_id"] == "UNISWAP_V3-ARBITRUM:POOL:USDC-WETH:3000"


def test_rollup_supports_instrument_id_column(rollup: ModuleType) -> None:
    """The id column falls back to instrument_id when instrument_key is absent."""
    d1 = date(2024, 1, 1)
    df = rollup.build_catalogue_dataframe([(d1, _snapshot([{"instrument_id": "ZZZ", "venue": "V"}]))])
    assert df.to_dict("records")[0]["instrument_id"] == "ZZZ"


def test_rollup_on_chain_cefi_perp_venue_kept_glued(rollup: ModuleType) -> None:
    """On-chain CeFi perp CLOBs (LIGHTER-ZKSYNC / EXTENDED-STARKNET)
    stay GLUED in the catalogue — the DeFi PROTOCOL-CHAIN split does NOT apply, they
    are cefi venues per UAC ``VENUE_TO_ASSET_GROUP`` and must match the by_date PATH
    + the ``_index`` writer (writers._canonical_manifest_venue_chain @ 24c0dd5) + the
    ``instrument_key`` prefix. Ref: instruments_foundation_completeness_2026_06_24.md
    §G1.3 follow-up (2026-06-27) — originally 3 venues, one row each; PACIFICA (Solana)
    removed entirely 2026-07-16 (operator ruling: all Solana perp DEXes dropped
    except Jupiter, not integrated).
    """
    d1 = date(2024, 10, 19)
    snapshots = [
        {
            "instrument_key": "LIGHTER-ZKSYNC:PERP:BTC-USDC",
            "venue": "LIGHTER-ZKSYNC",
            "instrument_type": "PERPETUAL",
            "raw_symbol": "BTC-PERP",
            "base_asset": "BTC",
        },
        {
            "instrument_key": "EXTENDED-STARKNET:PERP:ETH-USD",
            "venue": "EXTENDED-STARKNET",
            "instrument_type": "PERPETUAL",
            "raw_symbol": "ETH-PERP",
            "base_asset": "ETH",
        },
    ]
    df = rollup.build_catalogue_dataframe([(d1, _snapshot(snapshots))])
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}
    for full_id in (
        "LIGHTER-ZKSYNC:PERP:BTC-USDC",
        "EXTENDED-STARKNET:PERP:ETH-USD",
    ):
        assert full_id in by_id, f"catalogue dropped {full_id}"
        row = by_id[full_id]
        expected_venue = full_id.split(":", 1)[0]
        assert row["venue"] == expected_venue, (
            f"{full_id}: venue split to {row['venue']!r} (should stay glued {expected_venue!r})"
        )
        assert row["chain"] == "", f"{full_id}: chain leaked to {row['chain']!r} (cefi venue has no chain column)"


def test_rollup_skips_blank_ids_and_empty_frames(rollup: ModuleType) -> None:
    """Rows with no usable id are skipped; an EMPTY latest frame does not false-delist.

    §7.3 (G1.1): a globally-empty latest snapshot day is the canonical thin/partial
    capture — it carries NO venue rows, so venue V's last FULL day is d1 and A
    (present on d1) stays ACTIVE. The old behaviour stamped A delisted off the empty
    day via the GLOBAL last-seen rule — that WAS the false-delisting bug.
    """
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    snapshots = [
        (d1, _snapshot([{"instrument_key": "A", "venue": "V"}, {"instrument_key": "", "venue": "V"}])),
        (d2, pd.DataFrame()),  # empty frame — contributes no venue rows
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    assert list(df["instrument_id"]) == ["A"]
    # A is present on venue V's last full day (d1) → active, NOT delisted off the
    # empty d2 (the §7.3 thin-latest-day fix).
    assert df.to_dict("records")[0]["available_to"] is None


def test_rollup_empty_input_returns_catalog_columns(rollup: ModuleType) -> None:
    df = rollup.build_catalogue_dataframe([])
    assert list(df.columns) == list(rollup.CATALOG_COLUMNS)
    assert df.empty


def test_rollup_output_consumable_by_enumerator(rollup: ModuleType) -> None:
    """The rolled-up catalogue feeds enumerate_expected_universe._catalog_from_dataframe (no drift)."""
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_for_catalogue_rollup_test")
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    df = rollup.build_catalogue_dataframe(
        [
            (d1, _snapshot([{"instrument_key": "BTC-USDT", "venue": "BINANCE", "instrument_type": "SPOT_PAIR"}])),
            (d2, _snapshot([{"instrument_key": "BTC-USDT", "venue": "BINANCE", "instrument_type": "SPOT_PAIR"}])),
        ]
    )
    entries = enumerator._catalog_from_dataframe(df)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.instrument_id == "BTC-USDT"
    assert entry.venue == "BINANCE"
    assert entry.available_from == "2024-01-01"
    assert entry.available_to is None  # active on the latest day
    assert entry.data_type is None  # single-grain AG → no grain-binding (legacy iterate)


# ---------------------------------------------------------------------------
# build_prediction_catalogue_dataframe — multi-grain roll-up (cqg + conditionId)
# ---------------------------------------------------------------------------


def _pred_snap(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_prediction_rollup_emits_cqg_bundle_and_per_cid_grains(rollup: ModuleType) -> None:
    """One cqg row (bundle grain) + per-conditionId rows for trades AND market_lifecycle."""
    d1 = date(2025, 3, 14)
    snapshots = [
        (
            d1,
            "POLYMARKET",
            "BTC_UP_DOWN_DAILY",
            _pred_snap(
                [
                    {"instrument_key": "0xaaa", "venue": "POLYMARKET", "instrument_type": "prediction_market"},
                    {"instrument_key": "0xbbb", "venue": "POLYMARKET", "instrument_type": "prediction_market"},
                ]
            ),
        ),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    recs = df.to_dict("records")
    # cqg bundle: exactly one row, instrument_id=cqg, data_type=prediction_canonical_question_group.
    cqg_rows = [r for r in recs if r["data_type"] == "prediction_canonical_question_group"]
    assert len(cqg_rows) == 1
    assert cqg_rows[0]["instrument_id"] == "BTC_UP_DOWN_DAILY"
    # per-conditionId: each of the 2 cids appears under BOTH trades and market_lifecycle.
    trades = {r["instrument_id"] for r in recs if r["data_type"] == "trades"}
    lifecycle = {r["instrument_id"] for r in recs if r["data_type"] == "market_lifecycle"}
    assert trades == {"0xaaa", "0xbbb"}
    assert lifecycle == {"0xaaa", "0xbbb"}
    # No condition_id leaks into the cqg bundle (the inflation bug this guards).
    assert "0xaaa" not in {r["instrument_id"] for r in cqg_rows}


def test_prediction_rollup_cqg_lifecycle_spans_member_window(rollup: ModuleType) -> None:
    """The cqg available_from/to span the union of its member conditionIds' presence."""
    d1, d2, d3 = date(2025, 3, 14), date(2025, 3, 15), date(2025, 3, 16)
    snapshots = [
        (d1, "POLYMARKET", "G", _pred_snap([{"instrument_key": "c1", "venue": "POLYMARKET"}])),
        (d2, "POLYMARKET", "G", _pred_snap([{"instrument_key": "c2", "venue": "POLYMARKET"}])),
        # d3 has a DIFFERENT cqg present → G's last day is d2 (delisted before latest).
        (d3, "POLYMARKET", "H", _pred_snap([{"instrument_key": "c3", "venue": "POLYMARKET"}])),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    cqg = {
        r["instrument_id"]: r for r in df.to_dict("records") if r["data_type"] == "prediction_canonical_question_group"
    }
    assert cqg["G"]["available_from"] == "2025-03-14"
    assert cqg["G"]["available_to"] == "2025-03-15"  # delisted before the latest day (d3)
    assert cqg["H"]["available_from"] == "2025-03-16"
    assert cqg["H"]["available_to"] is None  # present on the latest day → active


def test_prediction_rollup_blank_cqg_emits_conditionid_grain_no_bundle(rollup: ModuleType) -> None:
    """249-a: a blank cqg (the real venue=/market= writer layout, which emits NO
    canonical_question_group) yields NO cqg-bundle row but DOES emit the
    conditionId grain (trades + market_lifecycle). Pre-fix this skipped the whole
    frame → 0-row catalogue; the bundle/cqg grain stays absent (gated on 338)."""
    d1 = date(2025, 3, 14)
    df = rollup.build_prediction_catalogue_dataframe(
        [(d1, "POLYMARKET", "", _pred_snap([{"instrument_key": "c1", "venue": "POLYMARKET"}]))]
    )
    recs = df.to_dict("records")
    # No cqg bundle row (no empty-string bundle; the cqg grain is 249-b, gated on 338).
    assert not [r for r in recs if r["data_type"] == "prediction_canonical_question_group"]
    # ...but the conditionId grain IS materialised (the 249-a fix — was 0 rows before).
    assert {r["instrument_id"] for r in recs if r["data_type"] == "trades"} == {"c1"}
    assert {r["instrument_id"] for r in recs if r["data_type"] == "market_lifecycle"} == {"c1"}


def test_prediction_rollup_consumable_by_enumerator_grain_bound(rollup: ModuleType) -> None:
    """The cqg row round-trips through _catalog_from_dataframe carrying its data_type binding,
    and the v2 prediction enumerator seeds ONLY that data_type at the cqg grain."""
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_for_pred_catalogue_test")
    d1 = date(2025, 3, 14)
    df = rollup.build_prediction_catalogue_dataframe(
        [(d1, "POLYMARKET", "BTC_UP_DOWN_DAILY", _pred_snap([{"instrument_key": "0xaaa", "venue": "POLYMARKET"}]))]
    )
    entries = enumerator._catalog_from_dataframe(df)
    cqg_entry = next(e for e in entries if e.instrument_id == "BTC_UP_DOWN_DAILY")
    assert cqg_entry.data_type == "prediction_canonical_question_group"
    # Enumerate over a date the cqg is alive, with an EMPTY present_set → expect a single
    # expected_unattempted row for the bundle data_type ONLY (NOT crossed with trades/lifecycle).
    rows = list(
        enumerator._enumerate_v2_prediction(
            [cqg_entry],
            [d1],
            ["trades", "prediction_canonical_question_group", "market_lifecycle"],
            present_set=set(),
            present_cols=["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"],
        )
    )
    assert len(rows) == 1
    assert rows[0].data_type == "prediction_canonical_question_group"
    assert rows[0].instrument_id == "BTC_UP_DOWN_DAILY"
    assert rows[0].capture_status == "expected_unattempted"


# ---------------------------------------------------------------------------
# underlying / canonical_instrument_id threading + cross-venue mapping wiring
# (prediction_canonical_identity_migration_2026_07_08.md todos 1 + 2 + 5)
# ---------------------------------------------------------------------------


def test_prediction_rollup_threads_underlying_from_per_date_row(rollup: ModuleType) -> None:
    """A real, adapter-populated ``underlying`` column on the per-date row survives
    into the catalogue's ``underlying`` column (was hardcoded "" before todo 1)."""
    d1 = date(2026, 6, 24)
    snapshots = [
        (
            d1,
            "KALSHI",
            "",
            _pred_snap(
                [
                    {
                        "instrument_key": "KALSHI:PREDICTION_MARKET:KXBTCD-26JUN24-T95000",
                        "venue": "KALSHI",
                        "instrument_type": "PREDICTION_MARKET",
                        "underlying": "BTC",
                    }
                ]
            ),
        ),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    row = next(r for r in df.to_dict("records") if r["data_type"] == "trades")
    assert row["underlying"] == "BTC"


def test_prediction_rollup_cross_venue_mapping_matches_same_market(rollup: ModuleType) -> None:
    """A real Kalshi<->Polymarket BTC UP_DOWN pair on the SAME settlement date gets
    the SAME canonical_instrument_id on BOTH sides (todo 2's cross-venue join,
    wired into this roll-up as the real, scheduled step)."""
    d1 = date(2026, 6, 24)
    snapshots = [
        (
            d1,
            "KALSHI",
            "",
            _pred_snap(
                [
                    {
                        "instrument_key": "KALSHI:PREDICTION_MARKET:KXBTCD-26JUN24-T95000",
                        "venue": "KALSHI",
                        "instrument_type": "PREDICTION_MARKET",
                        "raw_symbol": "KXBTCD-26JUN24",
                        "end_date_iso": "2026-06-24T00:00:00Z",
                    }
                ]
            ),
        ),
        (
            d1,
            "POLYMARKET",
            "",
            _pred_snap(
                [
                    {
                        "instrument_key": "POLYMARKET:PREDICTION_MARKET:0xabc123",
                        "venue": "POLYMARKET",
                        "instrument_type": "PREDICTION_MARKET",
                        "raw_symbol": "bitcoin-up-or-down-june-24-2026",
                        "end_date_iso": "2026-06-24T00:00:00Z",
                    }
                ]
            ),
        ),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    trades = {
        r["instrument_id"]: r["canonical_instrument_id"] for r in df.to_dict("records") if r["data_type"] == "trades"
    }
    kalshi_id = trades["KALSHI:PREDICTION_MARKET:KXBTCD-26JUN24-T95000"]
    poly_id = trades["POLYMARKET:PREDICTION_MARKET:0xabc123"]
    assert kalshi_id  # non-empty — a real match was found
    assert kalshi_id == poly_id  # SAME canonical_instrument_id on both venues


def test_prediction_rollup_unmatched_instrument_keeps_blank_canonical_instrument_id(rollup: ModuleType) -> None:
    """A Kalshi-only instrument (no Polymarket counterpart present) gets NO
    canonical_instrument_id — honest absence, never a guessed/false pair."""
    d1 = date(2026, 6, 24)
    snapshots = [
        (
            d1,
            "KALSHI",
            "",
            _pred_snap(
                [
                    {
                        "instrument_key": "KALSHI:PREDICTION_MARKET:KXWEIRDTHING-26JUL",
                        "venue": "KALSHI",
                        "instrument_type": "PREDICTION_MARKET",
                        "raw_symbol": "KXWEIRDTHING-26JUL",
                    }
                ]
            ),
        ),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    row = next(r for r in df.to_dict("records") if r["data_type"] == "trades")
    assert row["canonical_instrument_id"] == ""


def test_prediction_rollup_preserves_adapter_populated_sports_fixture_id(rollup: ModuleType) -> None:
    """A Polymarket sports row's adapter-populated canonical_instrument_id (todo 5's
    Sports-asset-group-aligned fixture_id) survives the roll-up even though the
    cross-venue matcher (todo 2, no titles supplied) can never produce a sports
    match on its own — the two mechanisms are complementary, not conflicting."""
    d1 = date(2026, 3, 22)
    snapshots = [
        (
            d1,
            "POLYMARKET",
            "",
            _pred_snap(
                [
                    {
                        "instrument_key": "POLYMARKET:PREDICTION_MARKET:0xsports1",
                        "venue": "POLYMARKET",
                        "instrument_type": "PREDICTION_MARKET",
                        "raw_symbol": "epl-arsenal-vs-chelsea-2026-03-22",
                        "canonical_instrument_id": "EPL:CHELSEA_v_ARSENAL:20260322",
                    }
                ]
            ),
        ),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    row = next(r for r in df.to_dict("records") if r["data_type"] == "trades")
    assert row["canonical_instrument_id"] == "EPL:CHELSEA_v_ARSENAL:20260322"


def test_prediction_rollup_cqg_grain_never_gets_canonical_instrument_id(rollup: ModuleType) -> None:
    """The cqg bundle row (family grain) never carries a per-instance
    canonical_instrument_id — a family has no single per-market identity."""
    d1 = date(2026, 6, 24)
    snapshots = [
        (
            d1,
            "POLYMARKET",
            "BTC_UP_DOWN_DAILY",
            _pred_snap(
                [
                    {
                        "instrument_key": "0xaaa",
                        "venue": "POLYMARKET",
                        "canonical_instrument_id": "SHOULD_NEVER_LEAK",
                    }
                ]
            ),
        ),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    cqg_row = next(r for r in df.to_dict("records") if r["data_type"] == "prediction_canonical_question_group")
    assert cqg_row["canonical_instrument_id"] == ""


# ---------------------------------------------------------------------------
# Settlement-date convention tests (prediction available_to = settlement day)
# SSOT: codex/02-data/prediction-settlement-availability-convention.md
# ---------------------------------------------------------------------------


def test_prediction_rollup_end_date_iso_used_as_available_to(rollup: ModuleType) -> None:
    """POLYMARKET-style raw snapshot: ``end_date_iso`` sets ``available_to`` (settlement
    date), not last-seen snapshot day.

    Convention: available_to = settlement date (inclusive last day).  A market settling
    on Jun 26 has available_to=Jun 26 even if it appeared in an earlier snapshot.
    """
    d1 = date(2026, 6, 24)  # snapshot day (earlier than settlement)
    d2 = date(2026, 6, 26)  # settlement date in the row
    latest = date(2026, 6, 29)  # catalogue's latest snapshot day (far future)

    snapshots = [
        (
            d1,
            "POLYMARKET",
            "",
            _pred_snap(
                [
                    {
                        "instrument_key": "0xdeadbeef",
                        "venue": "POLYMARKET",
                        "end_date_iso": "2026-06-26T00:00:00Z",
                    }
                ]
            ),
        ),
        # The "latest" day is provided via a dummy row to set latest_day = latest.
        (
            latest,
            "POLYMARKET",
            "",
            _pred_snap([{"instrument_key": "0xfuture", "venue": "POLYMARKET"}]),
        ),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    row = next(r for r in df.to_dict("records") if r["instrument_id"] == "0xdeadbeef" and r["data_type"] == "trades")
    # available_to must be the settlement date (Jun 26), NOT the last-seen day (Jun 24).
    assert row["available_to"] == d2.isoformat(), (
        f"Expected available_to={d2.isoformat()!r} (settlement date) but got {row['available_to']!r}. "
        "The settlement-date convention requires available_to to equal the market's settlement day, "
        "so that 'catalogue active on D' == 'manifest captured on D' for the reconciliation check."
    )
    # settlement_time should carry the raw value.
    assert row["settlement_time"] == "2026-06-26T00:00:00Z"


def test_prediction_rollup_available_to_datetime_used_as_settlement(rollup: ModuleType) -> None:
    """KALSHI-style normalised snapshot: ``available_to_datetime`` (a Timestamp) sets
    ``available_to`` as the settlement date.

    KALSHI's IS-normalised by_date parquet carries ``instrument_key`` +
    ``available_to_datetime`` (a tz-aware Timestamp); the raw ``end_date_iso`` /
    ``settlement_time`` columns are absent.  The catalogue must still extract the
    settlement day from ``available_to_datetime``.
    """
    d1 = date(2026, 6, 25)  # snapshot day
    settlement_ts = pd.Timestamp("2026-06-27 05:59:59+00:00")
    latest = date(2026, 6, 29)

    snapshots = [
        (
            d1,
            "KALSHI",
            "BTC_UP_DOWN_DAILY",
            _pred_snap(
                [
                    {
                        "instrument_key": "KXBTCUSD-26JUN27",
                        "venue": "KALSHI",
                        "instrument_type": "prediction_market",
                        "available_to_datetime": settlement_ts,
                    }
                ]
            ),
        ),
        (latest, "KALSHI", "BTC_UP_DOWN_DAILY", _pred_snap([{"instrument_key": "0xfuture", "venue": "KALSHI"}])),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    row = next(
        r for r in df.to_dict("records") if r["instrument_id"] == "KXBTCUSD-26JUN27" and r["data_type"] == "trades"
    )
    # The Timestamp's DATE (Jun 27) should be the available_to.
    assert row["available_to"] == "2026-06-27", (
        f"Expected available_to='2026-06-27' from available_to_datetime but got {row['available_to']!r}."
    )


def test_prediction_rollup_settlement_date_convention_boundary(rollup: ModuleType) -> None:
    """Logical-reconciliation boundary: ``available_to`` equals the settlement date so that
    ``available_from <= D <= available_to`` is TRUE on the settlement day and FALSE on D+1.

    This verifies the ``catalogue-active-on-D == manifest-captured-on-D`` property for the
    same-day-settled market case (the 2,177-market off-by-one the operator observed):
    a market settling Jun 26 must have ``available_to = Jun 26``, making it active on Jun 26
    (matching the manifest captured count) and inactive on Jun 27 (not counted on Jun 27).
    """
    settlement_day = date(2026, 6, 26)
    d_before = date(2026, 6, 25)
    d_after = date(2026, 6, 27)
    latest = date(2026, 6, 29)

    snapshots = [
        (
            d_before,
            "POLYMARKET",
            "MISC_NOVELTY",
            _pred_snap(
                [
                    {
                        "instrument_key": "0xmarket1",
                        "venue": "POLYMARKET",
                        "end_date_iso": f"{settlement_day.isoformat()}T00:00:00Z",
                    }
                ]
            ),
        ),
        (latest, "POLYMARKET", "MISC_NOVELTY", _pred_snap([{"instrument_key": "0xfuture", "venue": "POLYMARKET"}])),
    ]
    df = rollup.build_prediction_catalogue_dataframe(snapshots)
    row = next(r for r in df.to_dict("records") if r["instrument_id"] == "0xmarket1" and r["data_type"] == "trades")

    # The ``available_to`` must be the settlement date string, not the last-seen day.
    assert row["available_to"] == settlement_day.isoformat(), (
        f"Settlement-date convention: available_to must equal {settlement_day!r} "
        f"(settlement date) but got {row['available_to']!r}."
    )

    # Verify the boundary directly using the date-range check the enumerator uses:
    avail_from = pd.Timestamp(row["available_from"])
    avail_to = pd.Timestamp(row["available_to"])
    d_on_ts = pd.Timestamp(settlement_day)
    d_after_ts = pd.Timestamp(d_after)

    # Settlement day inclusive: market IS active on Jun 26 (catalogue active == manifest captured).
    assert avail_from <= d_on_ts <= avail_to, (
        f"Market should be ACTIVE on settlement day {settlement_day} "
        f"(available_from={row['available_from']}, available_to={row['available_to']})"
    )
    # Day after exclusive: market NOT active on Jun 27 (matches manifest no-longer-captured).
    assert d_after_ts > avail_to, (
        f"Market should be INACTIVE on {d_after} (day after settlement), "
        f"but available_to={row['available_to']!r} <= {d_after!r}."
    )


# ---------------------------------------------------------------------------
# build_sports_catalogue_dataframe — LEAGUE-grain roll-up (entity=leagues)
# ---------------------------------------------------------------------------


def _league_snap(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_sports_rollup_one_row_per_league_with_lifecycle(rollup: ModuleType) -> None:
    """One league-grain row per league_id; available_from/to track first/last day seen."""
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    snapshots = [
        (d1, _league_snap([{"league_id": "39", "name": "Premier League", "country": "England"}])),
        (d2, _league_snap([{"league_id": "39", "name": "Premier League", "country": "England"}])),
        # league 39 absent on d3 → delisted; league 140 present on latest → still active.
        (d3, _league_snap([{"league_id": "140", "name": "La Liga", "country": "Spain"}])),
    ]
    df = rollup.build_sports_catalogue_dataframe(snapshots)
    by_id = {row["league_id"]: row for row in df.to_dict("records")}

    assert set(by_id) == {"39", "140"}
    # instrument_id mirrors league_id; instrument_type="league"; venue blank (matches captured atom).
    assert by_id["39"]["instrument_id"] == "39"
    assert by_id["39"]["instrument_type"] == rollup.SPORTS_LEAGUE_INSTRUMENT_TYPE
    assert by_id["39"]["venue"] == ""
    assert by_id["39"]["data_type"] is None  # data_type axis handled by the enumerator
    assert by_id["39"]["available_from"] == "2024-01-01"
    assert by_id["39"]["available_to"] == "2024-01-02"  # delisted before the latest day
    assert by_id["140"]["available_from"] == "2024-01-03"
    assert by_id["140"]["available_to"] is None  # present on latest day → still active


def test_sports_rollup_skips_blank_league_ids_and_empty_frames(rollup: ModuleType) -> None:
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    snapshots = [
        (d1, _league_snap([{"league_id": "39"}, {"league_id": ""}, {"league_id": None}])),
        (d2, pd.DataFrame()),  # empty frame still advances the latest-day axis
    ]
    df = rollup.build_sports_catalogue_dataframe(snapshots)
    assert list(df["league_id"]) == ["39"]
    assert df.to_dict("records")[0]["available_to"] == "2024-01-01"  # delisted vs empty latest day


def test_sports_rollup_empty_input_returns_catalog_columns(rollup: ModuleType) -> None:
    df = rollup.build_sports_catalogue_dataframe([])
    assert list(df.columns) == list(rollup.CATALOG_COLUMNS)
    assert df.empty


# ---------------------------------------------------------------------------
# build_sports_catalogue_from_manifest — the namespace-correct could-exist source
# ---------------------------------------------------------------------------


def test_sports_catalogue_from_manifest_superset_and_excludes_retired(rollup: ModuleType) -> None:
    """One row per distinct CURRENT-data_type canonical league; retired/blank excluded.

    Regression for the CF-14 fix (slot-4 2026-06-07): the could-exist universe is
    the MANIFEST's own canonical leagues (a captured league provably could-exist),
    NOT the raw-numeric entity=leagues roll-up. Retired data_types
    (LEAGUES/TRANSFERMARKT_LEAGUES/SFI_LEAGUES) + blank league_ids are excluded.
    """
    manifest = pd.DataFrame(
        [
            {"league_id": "EPL", "data_type": "FIXTURES", "date": "2024-01-05"},
            {"league_id": "EPL", "data_type": "XG", "date": "2024-02-01"},
            {"league_id": "SERIE_A", "data_type": "MATCHES", "date": "2023-08-01"},
            {"league_id": "99", "data_type": "LEAGUES", "date": "2020-01-01"},  # RETIRED data_type
            {"league_id": "", "data_type": "FIXTURES", "date": "2024-01-01"},  # blank league
        ]
    )
    df = rollup.build_sports_catalogue_from_manifest(manifest)
    by_id = {row["league_id"]: row for row in df.to_dict("records")}
    # ⊇ manifest CURRENT leagues; retired-only numeric league + blank dropped.
    assert set(by_id) == {"EPL", "SERIE_A"}
    assert by_id["EPL"]["instrument_type"] == rollup.SPORTS_LEAGUE_INSTRUMENT_TYPE
    assert by_id["EPL"]["venue"] == ""
    assert by_id["EPL"]["data_type"] is None
    # available_from = earliest captured date across that league's current data_types.
    assert by_id["EPL"]["available_from"] == "2024-01-05"
    assert by_id["EPL"]["available_to"] is None  # active (enumerator applies coverage window)


def test_sports_catalogue_from_manifest_excludes_sentinel_and_unregistered_league_ids(rollup: ModuleType) -> None:
    """Sentinel AND non-LEAGUE_REGISTRY league_ids never roll up into a catalogue row.

    Regression for A1 (2026-07-08/09): an unguarded roll-up minted a real,
    persisted ``instrument_id="UNKNOWN"/league_id="UNKNOWN"`` catalogue row that
    the v2 enumerator then amplified into thousands of manifest
    expected_unattempted/empty_confirmed rows. A case-variant is also excluded
    (defensive — the known writer only ever emits the exact uppercase literal,
    but the filter is a case-insensitive compare).

    2026-07-13 (24-league de-registration ruling — supersedes the 2026-07-09
    keep-the-long-tail convention): league_ids outside UAC ``LEAGUE_REGISTRY``
    (raw numeric long-tail ids like ``15066``, alias strings like ``LA_LIGA_2``/
    ``RFPL``) are de-registered and must ALSO be excluded — their surviving GCS
    data must never re-mint a catalogue row the enumerator would re-amplify.
    """
    manifest = pd.DataFrame(
        [
            {"league_id": "EPL", "data_type": "FIXTURES", "date": "2024-01-05"},
            {"league_id": "UNKNOWN", "data_type": "FIXTURES", "date": "2025-12-15"},
            {"league_id": "unknown", "data_type": "XG", "date": "2025-12-16"},
            # De-registered long-tail / alias league_ids (2026-07-13 ruling) — excluded.
            {"league_id": "15066", "data_type": "MATCHES", "date": "2024-03-01"},
            {"league_id": "LA_LIGA_2", "data_type": "ODDS", "date": "2024-03-02"},
            {"league_id": "RFPL", "data_type": "XG", "date": "2024-03-03"},
        ]
    )
    df = rollup.build_sports_catalogue_from_manifest(manifest)
    by_id = {row["league_id"]: row for row in df.to_dict("records")}
    assert set(by_id) == {"EPL"}
    assert "UNKNOWN" not in by_id
    assert "unknown" not in by_id


def test_sports_catalogue_from_manifest_empty_or_missing_cols(rollup: ModuleType) -> None:
    assert rollup.build_sports_catalogue_from_manifest(pd.DataFrame()).empty
    # missing required columns → empty (never a crash)
    bad = pd.DataFrame([{"league_id": "EPL"}])
    out = rollup.build_sports_catalogue_from_manifest(bad)
    assert out.empty
    assert list(out.columns) == list(rollup.CATALOG_COLUMNS)


def test_sports_enumerator_skips_league_outside_entity_coverage(
    rollup: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """XG (Understat) must NOT seed expected_unattempted for a non-Understat league.

    Regression for the entity-coverage gate: get_entity_league_coverage("XG") is the
    Understat 5-league subset; a league outside it gets no XG seed (the source
    legitimately doesn't cover it — not an honest owed cell).
    """
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enum_v2_sports_cov")
    d = date(2024, 6, 1)
    # Stub out the matchday-aware fixture-index build (Root-cause writer fix, part
    # (b)) so this GCS-free test file stays GCS-free — pretend EPL has a fixture on
    # this day so the seed falls through to the entity-coverage behaviour under
    # test, not the (orthogonal) matchday check.
    monkeypatch.setattr(enumerator, "_build_understat_fixture_index", lambda days: {("EPL", "2024-06-01")})
    # EPL IS in Understat coverage; A_RANDOM_LEAGUE is NOT.
    manifest = pd.DataFrame(
        [
            {"league_id": "EPL", "data_type": "XG", "date": "2024-05-01"},
            {"league_id": "A_RANDOM_LEAGUE", "data_type": "MATCHES", "date": "2024-05-01"},
        ]
    )
    catalog = enumerator._catalog_from_dataframe(rollup.build_sports_catalogue_from_manifest(manifest))
    rows = list(
        enumerator.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=[d],
            data_types=["XG"],
            present_set=set(),
            present_cols=["data_type", "league_id", "date"],
        )
    )
    seeded_leagues = {r.league_id for r in rows if r.capture_status == "expected_unattempted"}
    assert "EPL" in seeded_leagues  # covered by Understat → seeded
    assert "A_RANDOM_LEAGUE" not in seeded_leagues  # NOT covered → no false seed


def test_sports_enumerator_reads_rollup_catalogue_and_emits_expected_unattempted(rollup: ModuleType) -> None:
    """End-to-end: producer → _catalog_from_dataframe → enumerate_v2(sports).

    Seeds expected_unattempted at LEAGUE-grain for missing (league, data_type, date)
    cells, SKIPS captured cells, and SKIPS pre-source-coverage dates (owned by v1).
    """
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_v2_sports_verify")
    # api_football FIXTURES source coverage starts 2018-01-01 → use 2024 dates (in-coverage).
    # league_id must be a UAC LEAGUE_REGISTRY member (2026-07-13 de-registration
    # gate: non-registry leagues never seed expected rows).
    d1, d2, d3 = date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)
    df = rollup.build_sports_catalogue_dataframe(
        [
            (d1, _league_snap([{"league_id": "EPL", "name": "PL"}])),
            (d2, _league_snap([{"league_id": "EPL", "name": "PL"}])),
            (d3, _league_snap([{"league_id": "EPL", "name": "PL"}])),
        ]
    )
    catalog = enumerator._catalog_from_dataframe(df)
    assert len(catalog) == 1
    assert catalog[0].league_id == "EPL"

    # League-grain present_set: league EPL FIXTURES captured on d2 only.
    present_cols = ["data_type", "league_id", "date"]
    present_set = {("FIXTURES", "EPL", "2024-06-02")}

    rows = list(
        enumerator.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=[d1, d2, d3],
            data_types=["FIXTURES"],
            present_set=present_set,
            present_cols=present_cols,
        )
    )
    by_date = {r.date: r for r in rows}
    # d2 captured → NOT emitted; d1 + d3 alive + missing → expected_unattempted, league-grain blanks.
    assert "2024-06-02" not in by_date
    assert by_date["2024-06-01"].capture_status == "expected_unattempted"
    assert by_date["2024-06-01"].league_id == "EPL"
    assert by_date["2024-06-01"].instrument_id == ""  # blanked → matches captured atom grain
    assert by_date["2024-06-01"].venue == ""
    assert by_date["2024-06-03"].capture_status == "expected_unattempted"


def test_sports_enumerator_emits_per_source_pre_coverage_and_skips_per_league(rollup: ModuleType) -> None:
    """Pre-coverage dates: v2 emits ONE per-source sentinel + zero per-league rows.

    Prior behaviour (pre-2026-07-06) skipped pre-coverage dates entirely and
    deferred to v1 ``_enumerate_sports``. After the
    ``_yield_v2_sports_pre_source_coverage_rows`` helper landed
    (``v1_enumerator_dispatch_not_deletable_2026_07_06.md`` task 2), v2 owns
    the per-source pre-coverage slice at ``(source, data_type, day,
    league_id="")`` grain. The per-league branch STILL skips those dates to
    avoid double-counting the ``(data_type, date)`` cell at two grains AND to
    prevent fabricating expected_unattempted for alive leagues on dates the
    source could never have covered.
    """
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_v2_sports_precov")
    # XG → understat, coverage starts 2014-01-01. Use a canonical understat league_id
    # ("EPL" — in UNDERSTAT_NAMES) and a date well before coverage start so the
    # pre-coverage path fires cleanly.  "39" is a FootyStats integer ID and is NOT
    # in the understat league coverage set, so it takes the EXPECTED_NO_PROVIDER_COVERAGE
    # branch instead — wrong scenario for this test.
    d_pre = date(2013, 6, 1)
    df = rollup.build_sports_catalogue_dataframe([(d_pre, _league_snap([{"league_id": "EPL"}]))])
    catalog = enumerator._catalog_from_dataframe(df)
    rows = list(
        enumerator.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=[d_pre],
            data_types=["XG"],
            present_set=set(),
            present_cols=["data_type", "league_id", "date"],
        )
    )
    # Exactly ONE per-source sentinel row: (venue="understat", data_type="XG",
    # league_id="", reason="EXPECTED_PRE_SOURCE_COVERAGE_START"). No per-league
    # rows for the pre-coverage date.
    assert len(rows) == 1
    r = rows[0]
    assert r.venue == "understat"
    assert r.data_type == "XG"
    assert r.league_id == ""
    assert r.reason == "EXPECTED_PRE_SOURCE_COVERAGE_START"
    assert r.date == d_pre.isoformat()


def test_sports_could_exist_denominator_never_shrinks(rollup: ModuleType) -> None:
    """Regression: a captured cell is ALWAYS in the present_set → never re-seeded.

    The could-exist universe (catalogue x data_types x dates) is a SUPERSET of the
    captured manifest cells, so the enumerator never emits for a captured cell —
    the coverage denominator can only grow, never shrink below captured.
    """
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_v2_sports_superset")
    d1, d2 = date(2024, 6, 1), date(2024, 6, 2)
    df = rollup.build_sports_catalogue_dataframe(
        [(d1, _league_snap([{"league_id": "39"}])), (d2, _league_snap([{"league_id": "39"}]))]
    )
    catalog = enumerator._catalog_from_dataframe(df)
    present_cols = ["data_type", "league_id", "date"]
    # Every (FIXTURES, 39, date) is captured → present_set covers the whole could-exist universe.
    present_set = {("FIXTURES", "39", "2024-06-01"), ("FIXTURES", "39", "2024-06-02")}
    rows = list(
        enumerator.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=[d1, d2],
            data_types=["FIXTURES"],
            present_set=present_set,
            present_cols=present_cols,
        )
    )
    # No expected_unattempted for already-captured cells → denominator == captured (no shrink).
    assert [r for r in rows if r.capture_status == "expected_unattempted"] == []


# ---------------------------------------------------------------------------
# build_sports_fixture_team_player_catalogue — FIXTURE/TEAM/PLAYER-grain
# roll-up from REAL captured entity=fixtures/teams/injuries by_date data
# (2026-07-09, extends the sports catalogue past league-grain-only).
# ---------------------------------------------------------------------------


def _sports_blob(day: str, entity: str, league: str, rows: list[dict[str, object]]) -> tuple[str, pd.DataFrame]:
    """Build a ``(blob_path, frame)`` pair matching the real GCS shape."""
    path = (
        f"sports_reference/by_date/day={day}/pipeline_mode=batch_api_football/"
        f"entity={entity}/league={league}/{entity}.parquet"
    )
    return path, pd.DataFrame(rows)


def test_split_full_name_two_and_one_token(rollup: ModuleType) -> None:
    assert rollup._split_full_name("Bukayo Saka") == ("Saka", "Bukayo")
    assert rollup._split_full_name("Neymar") == ("Neymar", "")
    assert rollup._split_full_name("Cristiano Ronaldo dos Santos") == ("Santos", "Cristiano Ronaldo dos")


def test_ftp_rollup_builds_fixture_team_player_rows_from_real_shaped_paths(rollup: ModuleType) -> None:
    """End-to-end via a _FakeStorage populated with real GCS-shaped blob paths.

    Covers: canonical fixture_id construction (LEAGUE:HOME_v_AWAY:DATE), team
    lifecycle (available_to=None when present on the latest scanned day),
    player_id construction from a real injuries row's ``player_name``, sentinel
    league_id exclusion, and that a non-FTP entity (``entity=leagues``) is
    never picked up by this walk.
    """
    d1, d2 = "2026-03-22", "2026-03-23"
    blobs = dict(
        [
            _sports_blob(
                d1,
                "fixtures",
                "EPL",
                [{"af_fixture_id": 1, "date": d1, "af_home_name": "Arsenal", "af_away_name": "Chelsea"}],
            ),
            _sports_blob(
                d1,
                "teams",
                "EPL",
                [
                    {"team_id": "ARSENAL", "name": "Arsenal", "league_id": "EPL"},
                    {"team_id": "CHELSEA", "name": "Chelsea", "league_id": "EPL"},
                ],
            ),
            _sports_blob(
                d1,
                "injuries",
                "EPL",
                [{"player_id": 1, "player_name": "Bukayo Saka", "team_id": 1, "league_id": 39}],
            ),
            # Arsenal present on the LATEST scanned day too → still active.
            _sports_blob(
                d2,
                "teams",
                "EPL",
                [{"team_id": "ARSENAL", "name": "Arsenal", "league_id": "EPL"}],
            ),
            # Sentinel league_id must never roll up into a row.
            _sports_blob(
                d1, "fixtures", "UNKNOWN", [{"af_fixture_id": 2, "date": d1, "af_home_name": "X", "af_away_name": "Y"}]
            ),
            # De-registered / non-LEAGUE_REGISTRY league_ids (2026-07-13 ruling):
            # their GCS data objects remain in place, but the FTP walk must not
            # re-mint catalogue rows for them — raw numeric long-tail id + the
            # SCOTTISH_LEAGUE_CUP_185 alias both excluded.
            _sports_blob(
                d1, "fixtures", "110", [{"af_fixture_id": 3, "date": d1, "af_home_name": "A", "af_away_name": "B"}]
            ),
            _sports_blob(
                d1,
                "injuries",
                "SCOTTISH_LEAGUE_CUP_185",
                [{"player_id": 9, "player_name": "Some Player", "team_id": 9, "league_id": 185}],
            ),
            # entity=leagues is NOT one of the three FTP entities — must be ignored by this walk.
            _sports_blob(d1, "leagues", "", [{"league_id": "39"}]),
        ]
    )
    storage = _FakeStorage({path: _parquet_bytes(frame.to_dict("records")) for path, frame in blobs.items()})

    df = rollup.build_sports_fixture_team_player_catalogue(
        storage, "test-bucket", by_date_prefix=rollup.SPORTS_BY_DATE_PREFIX, since=date(2026, 3, 1)
    )
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}

    assert set(by_id) == {
        "EPL:ARSENAL_v_CHELSEA:20260322",
        "ARSENAL",
        "CHELSEA",
        "SAKA_B",
    }

    fixture_row = by_id["EPL:ARSENAL_v_CHELSEA:20260322"]
    assert fixture_row["instrument_type"] == rollup.SPORTS_FIXTURE_INSTRUMENT_TYPE
    assert fixture_row["league_id"] == "EPL"
    assert fixture_row["venue"] == ""
    assert fixture_row["available_from"] == "2026-03-22"

    assert by_id["ARSENAL"]["instrument_type"] == rollup.SPORTS_TEAM_INSTRUMENT_TYPE
    assert by_id["ARSENAL"]["available_to"] is None  # present on latest scanned day → still active
    assert by_id["CHELSEA"]["available_to"] == "2026-03-22"  # only seen on d1 → delisted vs the latest day

    assert by_id["SAKA_B"]["instrument_type"] == rollup.SPORTS_PLAYER_INSTRUMENT_TYPE
    assert by_id["SAKA_B"]["league_id"] == "EPL"


def test_ftp_rollup_empty_walk_returns_catalog_columns(rollup: ModuleType) -> None:
    storage = _FakeStorage({})
    df = rollup.build_sports_fixture_team_player_catalogue(storage, "test-bucket", since=date(2026, 1, 1))
    assert df.empty
    assert list(df.columns) == list(rollup.CATALOG_COLUMNS)


def test_ftp_rollup_rows_never_treated_as_league_by_v2_enumerator(rollup: ModuleType) -> None:
    """Cross-module regression: a fixture-grain catalogue row concatenated onto
    league-grain rows must be invisible to enumerate_expected_universe.py's
    league-grain loop — see that module's own test for the isolated unit
    version; this proves the REAL producer output (this file's CATALOG_COLUMNS
    shape) round-trips through _catalog_from_dataframe correctly.
    """
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enum_v2_sports_ftp_filter")
    d1 = "2026-03-22"
    fixture_blobs = {
        p: _parquet_bytes(f.to_dict("records"))
        for p, f in [
            _sports_blob(
                d1,
                "fixtures",
                "EPL",
                [{"af_fixture_id": 1, "date": d1, "af_home_name": "Arsenal", "af_away_name": "Chelsea"}],
            )
        ]
    }
    storage = _FakeStorage(fixture_blobs)
    ftp_df = rollup.build_sports_fixture_team_player_catalogue(storage, "test-bucket", since=date(2026, 3, 1))
    manifest = pd.DataFrame([{"league_id": "EPL", "data_type": "FIXTURES", "date": "2026-03-01"}])
    league_df = rollup.build_sports_catalogue_from_manifest(manifest)
    combined = pd.concat([league_df, ftp_df], ignore_index=True)

    catalog = enumerator._catalog_from_dataframe(combined)
    rows = list(
        enumerator.enumerate_v2(
            asset_group="sports",
            catalog=catalog,
            date_axis=[date(2026, 3, 1)],
            data_types=["FIXTURES"],
            present_set=set(),
            present_cols=["data_type", "league_id", "date"],
        )
    )
    # Exactly ONE league-grain expected_unattempted seed (from league_df's "EPL"
    # row) — the fixture-grain row must not ALSO seed one.
    seeded = [r for r in rows if r.capture_status == "expected_unattempted" and r.league_id == "EPL"]
    assert len(seeded) == 1


# ---------------------------------------------------------------------------
# _merge_sports_ftp_with_frozen_tail — 2026-07-15 CATALOGUE_SHRINK_BLOCKED fix.
#
# Regression: build_sports_fixture_team_player_catalogue's trailing
# SPORTS_FTP_WINDOW_DAYS window is a full rebuild every run with NO memory of
# a prior run — an instrument whose only captured day ages off the window's
# bottom edge simply has no blob left in the fresh walk, so its row vanishes
# from the catalogue entirely (confirmed live incident: 9 single-day-only
# fixture/player rows aged off day=2025-06-09 while only 3 new same-day
# fixtures were gained, netting a 27216→27210 shrink that jammed the
# monotonic guard). The frozen-tail merge must carry an aged-off row through
# UNCHANGED rather than silently dropping it.
# ---------------------------------------------------------------------------


def test_sports_ftp_frozen_tail_keeps_row_that_aged_off_the_window(rollup: ModuleType) -> None:
    """An FTP row whose sole captured day is now OUTSIDE the fresh window must
    survive (frozen, unchanged) rather than vanish — the exact 2026-07-15 bug."""
    # OLD_FIXTURE's only observed day (2025-06-09) is now before `since` — a
    # bare full rebuild of the window would never see it again.
    prev_catalogue = (
        pd.DataFrame(
            [
                _cat_row(
                    instrument_id="MLS:LOS_ANGELES_FC_v_SPORTING_KANSAS_CITY:20250609",
                    instrument_type=rollup.SPORTS_FIXTURE_INSTRUMENT_TYPE,
                    venue="",
                    league_id="MLS",
                    available_from="2025-06-09",
                    available_to="2025-06-09",
                ),
                _cat_row(
                    instrument_id="JOHNSON_T",
                    instrument_type=rollup.SPORTS_PLAYER_INSTRUMENT_TYPE,
                    venue="",
                    league_id="MLS",
                    available_from="2025-06-09",
                    available_to="2025-06-09",
                ),
                # A league-grain row in the SAME previous catalogue must be
                # filtered out by the helper — it is not FTP-grain.
                _cat_row(
                    instrument_id="MLS",
                    instrument_type=rollup.SPORTS_LEAGUE_INSTRUMENT_TYPE,
                    venue="",
                    league_id="MLS",
                    available_from="2020-01-01",
                    available_to=None,
                ),
            ]
        ),
        None,
    )
    # Fresh window walk (since=2025-06-10) — a brand-new fixture played today,
    # OLD_FIXTURE/JOHNSON_T have no blob left inside the window at all.
    since = date(2025, 6, 10)
    d = "2026-07-15"
    blobs = dict(
        [
            _sports_blob(
                d,
                "fixtures",
                "USL_CHAMPIONSHIP",
                [{"af_fixture_id": 1, "date": d, "af_home_name": "Miami FC", "af_away_name": "Indy Eleven"}],
            ),
        ]
    )
    storage = _FakeStorage({path: _parquet_bytes(frame.to_dict("records")) for path, frame in blobs.items()})

    merged = rollup._merge_sports_ftp_with_frozen_tail(
        storage,
        "test-bucket",
        by_date_prefix=rollup.SPORTS_BY_DATE_PREFIX,
        since=since,
        max_blobs=None,
        prev_catalogue=prev_catalogue,
    )
    by_id = {row["instrument_id"]: row for row in merged.to_dict("records")}

    # The aged-off rows survive, frozen, with their original lifecycle window —
    # NOT silently dropped (the bug) and NOT re-closed a second time.
    assert "MLS:LOS_ANGELES_FC_v_SPORTING_KANSAS_CITY:20250609" in by_id
    assert by_id["MLS:LOS_ANGELES_FC_v_SPORTING_KANSAS_CITY:20250609"]["available_from"] == "2025-06-09"
    assert by_id["MLS:LOS_ANGELES_FC_v_SPORTING_KANSAS_CITY:20250609"]["available_to"] == "2025-06-09"
    assert "JOHNSON_T" in by_id
    assert by_id["JOHNSON_T"]["available_to"] == "2025-06-09"
    # The league-grain row from the previous catalogue must NOT leak into the
    # FTP-grain merge output (the caller concats league_df separately).
    assert "MLS" not in by_id or by_id["MLS"]["instrument_type"] != rollup.SPORTS_LEAGUE_INSTRUMENT_TYPE
    # The brand-new same-day fixture is also present — net effect is growth,
    # never a shrink, once the frozen tail is applied.
    fresh_id = "USL_CHAMPIONSHIP:MIAMI_FC_v_INDY_ELEVEN:20260715"
    assert fresh_id in by_id
    assert len(merged) == 3  # 2 frozen-tail rows + 1 fresh row — never fewer than prev's FTP rows.


def test_sports_ftp_frozen_tail_no_prev_catalogue_returns_window_only(rollup: ModuleType) -> None:
    """Cold start (no previous catalogue) — no tail to merge, window passes through."""
    storage = _FakeStorage({})
    out = rollup._merge_sports_ftp_with_frozen_tail(
        storage,
        "test-bucket",
        by_date_prefix=rollup.SPORTS_BY_DATE_PREFIX,
        since=date(2025, 6, 10),
        max_blobs=None,
        prev_catalogue=None,
    )
    assert out.empty
    assert list(out.columns) == list(rollup.CATALOG_COLUMNS)


# ---------------------------------------------------------------------------
# evaluate_monotonic_guard
# ---------------------------------------------------------------------------


def test_guard_accepts_first_run(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(10, None, allow_shrink=False)
    assert decision.accept
    assert decision.reason == "no_prior_catalogue"


def test_guard_accepts_growth_and_equal(rollup: ModuleType) -> None:
    assert rollup.evaluate_monotonic_guard(11, 10, allow_shrink=False).accept
    assert rollup.evaluate_monotonic_guard(10, 10, allow_shrink=False).accept


def test_guard_rejects_shrink(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(9, 10, allow_shrink=False)
    assert not decision.accept
    assert decision.reason == "shrink_blocked"


def test_guard_override_allows_shrink(rollup: ModuleType) -> None:
    decision = rollup.evaluate_monotonic_guard(9, 10, allow_shrink=True)
    assert decision.accept
    assert decision.reason == "shrink_overridden"


# ---------------------------------------------------------------------------
# summarise_completeness (audit tool)
# ---------------------------------------------------------------------------


def test_completeness_complete_when_no_failed_cells(audit: ModuleType) -> None:
    index_df = pd.DataFrame(
        [
            {"date": "2024-01-01", "venue": "BINANCE", "data_type": "INSTRUMENTS", "capture_status": "captured"},
            {"date": "2024-01-01", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "empty_confirmed"},
        ]
    )
    report = audit.summarise_completeness(index_df, "cefi")
    assert report.is_complete
    assert report.attempted_failed == 0
    assert report.status_counts["captured"] == 1
    assert report.status_counts["empty_confirmed"] == 1


def test_completeness_surfaces_attempted_failed_gaps(audit: ModuleType) -> None:
    index_df = pd.DataFrame(
        [
            {"date": "2024-01-01", "venue": "BINANCE", "data_type": "INSTRUMENTS", "capture_status": "captured"},
            {"date": "2024-01-02", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "attempted_failed"},
            {"date": "2024-01-03", "venue": "OKX", "data_type": "INSTRUMENTS", "capture_status": "attempted_failed"},
        ]
    )
    report = audit.summarise_completeness(index_df, "cefi")
    assert not report.is_complete
    assert report.attempted_failed == 2
    assert report.failed_by_venue["OKX"] == 2
    assert ("OKX", "2024-01-02", "INSTRUMENTS") in report.gap_sample


def test_completeness_blank_status_coerced_to_captured(audit: ModuleType) -> None:
    """Legacy blank/NaN capture_status mirrors the manifest read path (→ captured), not a gap."""
    index_df = pd.DataFrame([{"date": "2024-01-01", "venue": "V", "data_type": "INSTRUMENTS", "capture_status": None}])
    report = audit.summarise_completeness(index_df, "cefi")
    assert report.is_complete
    assert report.status_counts["captured"] == 1


def test_completeness_empty_index(audit: ModuleType) -> None:
    report = audit.summarise_completeness(pd.DataFrame(), "cefi")
    assert report.total_rows == 0
    assert report.is_complete  # vacuously — no failed cells (provisional)


# ---------------------------------------------------------------------------
# _iter_by_date_snapshots — concurrent walk + day parsing + max_blobs cap
# ---------------------------------------------------------------------------


def test_iter_by_date_walk_parses_day_and_reads_frames(rollup: ModuleType) -> None:
    blobs = {
        "instrument_availability/by_date/day=2024-01-01/venue=BINANCE/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "BTC-USDT", "venue": "BINANCE"}]
        ),
        "instrument_availability/by_date/day=2024-01-02/venue=OKX/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "ETH-USDT", "venue": "OKX"}]
        ),
        "instrument_availability/by_date/day=2024-01-02/venue=OKX/_SUCCESS": b"not-parquet",
    }
    out = list(rollup._iter_by_date_snapshots(_FakeStorage(blobs), "bkt", "instrument_availability/by_date"))
    days = sorted(str(d) for d, _ in out)
    assert days == ["2024-01-01", "2024-01-02"]  # non-parquet skipped
    assert all(not f.empty for _, f in out)


def test_iter_by_date_max_blobs_truncates(rollup: ModuleType) -> None:
    blobs = {
        f"instrument_availability/by_date/day=2024-01-0{i}/venue=V/instruments.parquet": _parquet_bytes(
            [{"instrument_key": f"I{i}", "venue": "V"}]
        )
        for i in range(1, 6)
    }
    out = list(
        rollup._iter_by_date_snapshots(_FakeStorage(blobs), "bkt", "instrument_availability/by_date", max_blobs=2)
    )
    assert len(out) == 2  # truncated (path-sorted → earliest two days)


def test_run_rollup_max_blobs_forces_dry_run(rollup: ModuleType) -> None:
    """--max-blobs must force dry-run (a truncated walk is never promotable)."""
    blobs = {
        "instrument_availability/by_date/day=2024-01-01/venue=V/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "A", "venue": "V"}]
        ),
    }
    code = rollup.run_rollup(
        "cefi",
        allow_shrink=False,
        dry_run=False,  # caller asked to write...
        max_blobs=1,  # ...but the cap forces dry-run
        storage=_FakeStorage(blobs),
    )
    assert code == 0
    # No catalog object was written (fake has only the input blob).
    assert "prod/catalog.parquet" not in _FakeStorage(blobs)._blobs


# ---------------------------------------------------------------------------
# Phase-3 cefi verification — the v2 cefi enumerator reads the rolled-up
# catalogue and emits NOT_LISTED / DELISTED / expected_unattempted correctly.
# ---------------------------------------------------------------------------


def test_cefi_enumerator_reads_rollup_catalogue_and_emits_expected_unattempted(rollup: ModuleType) -> None:
    enumerator = _load_script_module("enumerate_expected_universe.py", "_enumerate_v2_cefi_verify")
    d1, d2, d3, d4 = date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3), date(2024, 6, 4)

    # Producer roll-up: OLDCOIN only on d1 (delisted, latest=d3); BTC on d2,d3 (active).
    # MVP-qualifying fixtures (cefi_universe_capture_rule_2026_06_23): BINANCE-FUTURES
    # PERPETUAL bases (ETH/BTC) self-qualify the perp-gate, so the lifecycle assertions
    # (NOT_LISTED / DELISTED / expected_unattempted) are exercised through the MVP gate.
    catalogue_df = rollup.build_catalogue_dataframe(
        [
            (
                d1,
                _snapshot(
                    [
                        {
                            "instrument_key": "ETH",
                            "venue": "BINANCE-FUTURES",
                            "instrument_type": "PERPETUAL",
                            "base_asset": "ETH",
                        }
                    ]
                ),
            ),
            (
                d2,
                _snapshot(
                    [
                        {
                            "instrument_key": "BTC",
                            "venue": "BINANCE-FUTURES",
                            "instrument_type": "PERPETUAL",
                            "base_asset": "BTC",
                        }
                    ]
                ),
            ),
            (
                d3,
                _snapshot(
                    [
                        {
                            "instrument_key": "BTC",
                            "venue": "BINANCE-FUTURES",
                            "instrument_type": "PERPETUAL",
                            "base_asset": "BTC",
                        }
                    ]
                ),
            ),
        ]
    )
    catalog = enumerator._catalog_from_dataframe(catalogue_df)

    # Manifest says BTC was captured on d3 only.
    # present_cols default (cefi, underlying-aware since
    # cefi_mtds_writer_raw_symbol_vs_canonical_eu_namespace_mismatch_2026_07_15.md):
    # venue, chain, data_type, instrument_type, instrument_id, underlying, league_id, date.
    # This fixture is a PERPETUAL leaf, so underlying="" on both sides — a no-op slot.
    present_set = {("BINANCE-FUTURES", "", "INSTRUMENTS", "PERPETUAL", "BTC", "", "", "2024-06-03")}

    rows = list(
        enumerator.enumerate_v2(
            asset_group="cefi",
            catalog=catalog,
            date_axis=[d1, d2, d3, d4],
            data_types=["INSTRUMENTS"],
            present_set=present_set,
        )
    )
    by_key = {(r.instrument_id, r.date): r for r in rows}

    # BTC before available_from (d2) → NOT_LISTED (honest empty_confirmed)
    assert by_key[("BTC", "2024-06-01")].reason == "EXPECTED_INSTRUMENT_NOT_LISTED"
    assert by_key[("BTC", "2024-06-01")].capture_status == "empty_confirmed"
    # BTC alive on d2, no manifest row → expected_unattempted
    assert by_key[("BTC", "2024-06-02")].capture_status == "expected_unattempted"
    # BTC alive on d3 AND captured (present_set) → NOT emitted
    assert ("BTC", "2024-06-03") not in by_key
    # BTC alive on d4 (available_to=None), no manifest row → expected_unattempted
    assert by_key[("BTC", "2024-06-04")].capture_status == "expected_unattempted"
    # ETH after available_to (d1) → DELISTED
    assert by_key[("ETH", "2024-06-02")].reason == "EXPECTED_INSTRUMENT_DELISTED"


# ---------------------------------------------------------------------------
# _tune_download_pool — enlarge GCS HTTP pool (best-effort, guarded)
# ---------------------------------------------------------------------------


class _FakeHttp:
    def __init__(self) -> None:
        self.mounted: dict[str, object] = {}

    def mount(self, prefix: str, adapter: object) -> None:
        self.mounted[prefix] = adapter


class _FakeNativeClient:
    def __init__(self) -> None:
        self._http = _FakeHttp()


class _FakeGcpStorage:
    provider_name = "gcp"

    def __init__(self) -> None:
        self._client = _FakeNativeClient()


class _FakeAwsStorage:
    provider_name = "aws"


def test_tune_download_pool_mounts_adapter_on_gcp(rollup: ModuleType) -> None:
    storage = _FakeGcpStorage()
    rollup._tune_download_pool(storage, 16)
    assert set(storage._client._http.mounted) == {"https://", "http://"}
    # pool sized to the worker count
    adapter = storage._client._http.mounted["https://"]
    assert getattr(adapter, "_pool_maxsize", 16) == 16


def test_tune_download_pool_noop_on_non_gcp(rollup: ModuleType) -> None:
    # No provider / non-gcp / missing native client must not raise.
    rollup._tune_download_pool(_FakeAwsStorage(), 16)
    rollup._tune_download_pool(object(), 16)  # no provider_name / _client at all


def test_instruments_store_bucket_for_prediction_uses_flat_kind(
    rollup: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prediction resolves the dedicated FLAT kind ``instruments-store-prediction``.

    Regression for the catalogue-rollup crash (slot-5 2026-06-07): the per-AG
    ``instruments-store`` dict has NO ``PREDICTION`` entry, so the prior
    ``get_write_bucket_name("instruments", "prediction")`` raised ``BucketNamingError``
    and the prediction roll-up crashed at bucket resolution before any walk.
    """
    captured: list[tuple[str, str | None]] = []

    def _fake(*, cloud: str, kind: str, asset_group: str | None = None) -> str:
        _ = cloud
        captured.append((kind, asset_group))
        return f"bkt-{kind}-{asset_group or 'flat'}"

    monkeypatch.setattr(rollup, "resolve_bucket_name", _fake)
    assert rollup._instruments_store_bucket_for("prediction") == "bkt-instruments-store-prediction-flat"
    assert ("instruments-store-prediction", None) in captured
    # Every OTHER AG uses the per-AG instruments-store dict (unchanged behaviour).
    captured.clear()
    rollup._instruments_store_bucket_for("cefi")
    assert ("instruments-store", "cefi") in captured


def test_instruments_store_bucket_for_unknown_raises(rollup: ModuleType) -> None:
    with pytest.raises(ValueError, match="Unknown asset_group"):
        rollup._instruments_store_bucket_for("bogus")


# ---------------------------------------------------------------------------
# MVP-tagged catalogue view (mvp_scope_catalogue_tagging_2026_06_08)
# ---------------------------------------------------------------------------


def test_add_mvp_column_tags_mvp_and_non_mvp_cells(rollup: ModuleType) -> None:
    """_add_mvp_column tags an MVP cell True and a non-MVP cell False via UAC is_mvp.

    Uses real UAC MVP_SCOPE rules (no mock): a cefi cell on an MVP venue +
    instrument_type + data_type + base_ccy is in scope; a bogus-venue cell is not.
    Regression guard for the IS MVP-tagged catalogue view.
    """
    df = pd.DataFrame(
        [
            # MVP: BINANCE-FUTURES / PERPETUAL / funding_rate / BTC are all in cefi MVP_SCOPE.
            {
                "instrument_id": "MVP-1",
                "instrument_type": "PERPETUAL",
                "venue": "BINANCE-FUTURES",
                "chain": "",
                "league_id": "",
                "available_from": "2024-01-01",
                "available_to": None,
                "market_created_at": None,
                "settlement_time": None,
                "data_type": "funding_rate",
                "underlying": "BTC",
            },
            # NON-MVP: venue not in the MVP venue set → excluded.
            {
                "instrument_id": "OUT-1",
                "instrument_type": "PERPETUAL",
                "venue": "NOT-A-VENUE",
                "chain": "",
                "league_id": "",
                "available_from": "2024-01-01",
                "available_to": None,
                "market_created_at": None,
                "settlement_time": None,
                "data_type": "funding_rate",
                "underlying": "BTC",
            },
        ],
        columns=[c for c in rollup.CATALOG_COLUMNS if c != "mvp"],
    )

    out = rollup._add_mvp_column(df, "cefi")
    assert "mvp" in out.columns
    assert out["mvp"].dtype == bool
    by_id = {row["instrument_id"]: row for row in out.to_dict("records")}
    assert bool(by_id["MVP-1"]["mvp"]) is True
    assert bool(by_id["OUT-1"]["mvp"]) is False


def test_add_mvp_column_empty_frame_keeps_bool_column(rollup: ModuleType) -> None:
    """An empty catalogue keeps the typed bool ``mvp`` column (stable schema)."""
    empty = rollup.build_catalogue_dataframe([])
    out = rollup._add_mvp_column(empty, "cefi")
    assert "mvp" in out.columns
    assert out["mvp"].dtype == bool
    assert out.empty


# ---------------------------------------------------------------------------
# _bounded_parallel_load — memory-bounded sliding window (OOM root-fix 2026-06-23)
# ---------------------------------------------------------------------------


def test_bounded_parallel_load_yields_every_item(rollup: ModuleType) -> None:
    """All items are processed exactly once (order-independent)."""
    items = list(range(50))
    out = sorted(rollup._bounded_parallel_load(items, lambda x: x * 2, max_workers=4))
    assert out == [x * 2 for x in items]


def test_bounded_parallel_load_empty_input(rollup: ModuleType) -> None:
    """No items → no results, no thread pool churn."""
    assert list(rollup._bounded_parallel_load([], lambda x: x, max_workers=4)) == []


def test_bounded_parallel_load_caps_in_flight_at_max_workers(rollup: ModuleType) -> None:
    """Peak concurrent in-flight tasks never exceeds max_workers — the OOM guard.

    Previously ``pool.map`` submitted ALL items at once (peak = len(items) frames
    in RAM). This proves the sliding window holds peak concurrency at max_workers
    regardless of item count.
    """
    import threading
    import time

    max_workers = 3
    n_items = 30
    in_flight = 0
    peak = 0
    lock = threading.Lock()
    release = threading.Event()

    def _slow(_: int) -> int:
        nonlocal in_flight, peak
        with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        # Hold every worker until the window is provably full, so peak reflects
        # the true concurrency ceiling rather than fast serial completion.
        release.wait(timeout=2.0)
        with lock:
            in_flight -= 1
        return 1

    def _drain() -> None:
        for _ in rollup._bounded_parallel_load(list(range(n_items)), _slow, max_workers=max_workers):
            pass

    t = threading.Thread(target=_drain)
    t.start()
    time.sleep(0.3)
    release.set()
    t.join(timeout=5.0)
    assert not t.is_alive()
    assert peak <= max_workers, f"peak in-flight {peak} exceeded max_workers {max_workers}"


def test_bounded_parallel_load_propagates_exception(rollup: ModuleType) -> None:
    """A per-item failure fails loud (never a silent under-count)."""

    def _boom(x: int) -> int:
        if x == 7:
            raise ValueError("blob 7 unreadable")
        return x

    with pytest.raises(ValueError, match="blob 7 unreadable"):
        list(rollup._bounded_parallel_load(list(range(20)), _boom, max_workers=4))


# ---------------------------------------------------------------------------
# Dual-key ghost collapse — AAVE_V3 / COMPOUND_V3 (+171 / +26 triad fix)
# 2026-06-27: non-pool DeFi lending rows whose instrument_id prefix is a
# no-underscore ghost form (AAVEV3- / COMPOUNDV3-) must collapse onto the
# canonical key (AAVE_V3- / COMPOUND_V3-) via _canonical_instrument_id.
# ---------------------------------------------------------------------------


def test_rollup_non_pool_defi_ghost_lending_collapses_to_one_lifecycle(rollup: ModuleType) -> None:
    """AAVE_V3 lending dual-key ghost fix: the same lending market under the
    old ghost instrument_key prefix (``AAVEV3-ARBITRUM:A_TOKEN:USDC``) and the
    new canonical prefix (``AAVE_V3-ARBITRUM:A_TOKEN:USDC``) collapses to ONE
    catalogue row, NOT two.  Without the fix both keys produced separate rows
    each marked active → +171 AAVE_V3 catalogue over-count vs manifest.
    """
    d_old = date(2026, 5, 1)  # old adapter (ghost prefix AAVEV3-)
    d_switch = date(2026, 5, 8)  # adapter switched to canonical AAVE_V3-
    d_now = date(2026, 6, 20)  # recent snapshot (canonical)

    ghost_row = {
        "instrument_key": "AAVEV3-ARBITRUM:A_TOKEN:USDC",
        "venue": "AAVEV3-ARBITRUM",
        "instrument_type": "lending",
        "chain": "",
    }
    canonical_row = {
        "instrument_key": "AAVE_V3-ARBITRUM:A_TOKEN:USDC",
        "venue": "AAVE_V3-ARBITRUM",
        "instrument_type": "lending",
        "chain": "",
    }
    df = rollup.build_catalogue_dataframe(
        [
            (d_old, _snapshot([ghost_row])),
            (d_switch, _snapshot([canonical_row])),
            (d_now, _snapshot([canonical_row])),
        ]
    )
    lending_rows = df[df["instrument_type"].astype(str).str.lower() == "lending"].to_dict("records")
    # Must collapse to exactly ONE row (not two).
    assert len(lending_rows) == 1, (
        f"Expected 1 lending row (ghost+canonical collapsed) but got {len(lending_rows)}. "
        "The dual-key ghost collapse fix is missing."
    )
    row = lending_rows[0]
    # available_to=None: the market is present on the latest day (d_now).
    assert row["available_to"] is None, (
        f"Expected available_to=None (active on latest day) but got {row['available_to']!r}."
    )
    # available_from spans the first (ghost) day.
    assert row["available_from"] == d_old.isoformat()


def test_rollup_compound_v3_ghost_collapses_like_aave_v3(rollup: ModuleType) -> None:
    """COMPOUND_V3 lending dual-key ghost: ``COMPOUNDV3-BASE:SUPPLY:USDC`` and
    ``COMPOUND_V3-BASE:SUPPLY:USDC`` must collapse to ONE row."""
    d_old = date(2026, 4, 1)
    d_now = date(2026, 6, 20)

    ghost_row = {
        "instrument_key": "COMPOUNDV3-BASE:SUPPLY:USDC",
        "venue": "COMPOUNDV3-BASE",
        "instrument_type": "lending",
        "chain": "",
    }
    canonical_row = {
        "instrument_key": "COMPOUND_V3-BASE:SUPPLY:USDC",
        "venue": "COMPOUND_V3-BASE",
        "instrument_type": "lending",
        "chain": "",
    }
    df = rollup.build_catalogue_dataframe(
        [
            (d_old, _snapshot([ghost_row])),
            (d_now, _snapshot([canonical_row])),
        ]
    )
    lending_rows = df[df["instrument_type"].astype(str).str.lower() == "lending"].to_dict("records")
    assert len(lending_rows) == 1, f"COMPOUND_V3 ghost collapse failed: got {len(lending_rows)} rows."
    assert lending_rows[0]["available_to"] is None  # active on latest day (d_now)


# ---------------------------------------------------------------------------
# PANCAKESWAP_V3-BSC old-format false-actives fix — §7.3 liveness via
# canonical venue_day_counts (+73 triad discrepancy 2026-06-27).
# ---------------------------------------------------------------------------


def test_rollup_ghost_venue_liveness_merges_into_canonical_window(rollup: ModuleType) -> None:
    """A ghost-venue pool stopped May 8; the canonical venue is still active today.

    Without the fix: ghost venue ``PANCAKESWAPV3-BSC`` last full day = May 8 →
    every pool with last_day=May 8 gets ``available_to=None`` (false-active).
    With the fix: venue_day_counts is keyed on the CANONICAL venue, so the ghost
    and canonical forms merge into one window extending to today → pools that
    stopped May 8 are correctly delisted (``available_to=2026-05-08``).
    """
    addr_stopped = "0xdeadpool000000000000000000000000000000000"
    addr_active = "0xlivepool000000000000000000000000000000000"

    d_old = date(2026, 4, 1)
    d_stop = date(2026, 5, 8)  # last day under the ghost venue
    d_now = date(2026, 6, 20)  # captured under canonical venue today

    # A pool that only existed under the OLD ghost venue (stopped May 8).
    ghost_stopped = {
        "instrument_key": "PANCAKESWAPV3-BSC:POOL:CAKE-BNB:500",
        "venue": "PANCAKESWAPV3-BSC",
        "instrument_type": "POOL",
        "raw_symbol": addr_stopped,
        "pool_address": addr_stopped,
        "base_asset": "CAKE",
        "quote_asset": "BNB",
        "pool_fee_tier": 5.0,
    }
    # A pool that is STILL active — appeared under ghost May 8, new canonical today.
    ghost_active = {
        **ghost_stopped,
        "instrument_key": "PANCAKESWAPV3-BSC:POOL:CAKE-USDT:2500",
        "raw_symbol": addr_active,
        "pool_address": addr_active,
        "quote_asset": "USDT",
    }
    # Canonical form of the active pool (same pool_address → merges via pool:: key).
    canonical_active = {
        **ghost_active,
        "instrument_key": "PANCAKESWAP_V3-BSC:POOL:CAKE-USDT:2500",
        "venue": "PANCAKESWAP_V3-BSC",
    }

    # Add many canonical pools on d_old, d_stop, d_now to make it a "full" venue day.
    full_canon = [
        {
            "instrument_key": f"PANCAKESWAP_V3-BSC:POOL:TOK{i}-BNB:500",
            "venue": "PANCAKESWAP_V3-BSC",
            "instrument_type": "POOL",
            "raw_symbol": f"0x{i:040x}",
            "pool_address": f"0x{i:040x}",
            "base_asset": f"TOK{i}",
            "quote_asset": "BNB",
            "pool_fee_tier": 5.0,
        }
        for i in range(1, 21)  # 20 canonical pools — makes d_old/d_stop/d_now full days
    ]

    df = rollup.build_catalogue_dataframe(
        [
            (d_old, _snapshot([ghost_stopped, ghost_active, *full_canon])),
            (d_stop, _snapshot([ghost_stopped, ghost_active, *full_canon])),
            (d_now, _snapshot([canonical_active, *full_canon])),
        ]
    )
    by_addr = {row["instrument_id"]: row for row in df.to_dict("records") if row["instrument_type"] == "POOL"}

    # The pool that stopped May 8 (no canonical equivalent) must be DELISTED.
    assert addr_stopped in by_addr, "Stopped pool should still be in catalogue (just delisted)."
    assert by_addr[addr_stopped]["available_to"] == d_stop.isoformat(), (
        f"Stopped pool should have available_to={d_stop.isoformat()!r} (last seen May 8), "
        f"but got {by_addr[addr_stopped]['available_to']!r}. "
        "The §7.3 ghost-venue liveness-merge fix is missing."
    )

    # The pool that is active under the canonical venue today must be ACTIVE.
    assert addr_active in by_addr
    assert by_addr[addr_active]["available_to"] is None, (
        f"Active pool (still captured today) should be active (available_to=None), "
        f"but got {by_addr[addr_active]['available_to']!r}."
    )


# ---------------------------------------------------------------------------
# CeFi perp-family lineage collapse (HYPERLIQUID / ASTER 2026-07 id-convention
# churn) + crypto-venue equity-identity tags (is_equity_perp / tracks_equity).
# Operator 2026-07-16: instrument_type stays the BROAD mechanics type (PERPETUAL /
# SPOT_PAIR), equity identity rides the two tags — NOT a distinct EQUITY_PERP /
# TOKENIZED_EQUITY type.
# Plans: cefi_completion_program_2026_07_15.md +
#        cryptovenue_equity_perps_and_tokenized_stocks_2026_06_20.md.
# ---------------------------------------------------------------------------


def test_cefi_equity_tags_classifier(rollup: ModuleType) -> None:
    """Pure classifier → (is_equity_perp, tracks_equity). instrument_type is NEVER
    changed (operator 2026-07-16): equity identity rides the two tags."""
    r = rollup._cefi_equity_tags
    # equity single-stock perps → (True, real-equity ticker) via tracks_equity
    assert r("PERPETUAL", "AAPL") == (True, "AAPL")
    assert r("PERPETUAL", "NVDA") == (True, "NVDA")
    assert r("PERPETUAL", "META") == (True, "META")
    # commodity RAW form / index are in the universe → is_equity_perp True, but they
    # have no Databento equity twin in the link map → tracks_equity "".
    assert r("PERPETUAL", "XAU") == (True, "")  # commodity RAW form
    assert r("PERPETUAL", "SPX") == (True, "")  # index
    # standalone / pre-IPO equity perp: in the universe (is_equity_perp True) but no
    # real-equity twin → tracks_equity "".
    assert r("PERPETUAL", "SPCX") == (True, "")  # SpaceX pre-IPO
    # crypto perps → not equity (not in the equity universe)
    assert r("PERPETUAL", "BTC") == (False, "")
    assert r("PERPETUAL", "0G") == (False, "")
    # tokenized-share spot: base <TICKER>X where TICKER in the equity universe →
    # is_equity_perp True (it's an equity instrument), tracks_equity = the ticker.
    assert r("SPOT_PAIR", "AAPLX") == (True, "AAPL")
    assert r("SPOT_PAIR", "TSLAX") == (True, "TSLA")
    # spot that only LOOKS tokenized but strips to a non-equity → not equity
    assert r("SPOT_PAIR", "SPX") == (False, "")  # SPX[:-1]=SP not an equity ticker
    assert r("SPOT_PAIR", "BTC") == (False, "")
    # blank base is a no-op
    assert r("PERPETUAL", "") == (False, "")


def test_cefi_perp_lineage_key_helper(rollup: ModuleType) -> None:
    """The (venue-prefix, raw_symbol, margin) key is STABLE across the id-convention chain."""
    k = rollup._cefi_perp_lineage_key
    # All three id forms of the SAME HL BTC perp collapse to one key.
    k1 = k("HYPERLIQUID:PERP:BTC", "PERPETUAL", "BTC", "linear")
    k2 = k("HYPERLIQUID:PERPETUAL:BTC-USD", "PERPETUAL", "BTC", "linear")
    k3 = k("HYPERLIQUID:PERPETUAL:BTC-USD@LIN", "PERPETUAL", "BTC", "linear")
    assert k1 == k2 == k3 is not None
    # A crypto-venue equity perp is typed PERPETUAL (operator 2026-07-16, no distinct
    # EQUITY_PERP type) so it rides the same family across the id-convention chain.
    assert k("BINANCE-FUTURES:PERPETUAL:AAPL-USDT@LIN", "PERPETUAL", "AAPLUSDT", "linear") == k(
        "BINANCE-FUTURES:PERP:AAPLUSDT", "PERPETUAL", "AAPLUSDT", "linear"
    )
    # Distinct quotes on the SAME venue/base stay DISTINCT (raw_symbol differs) — no over-collapse.
    assert k("BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN", "PERPETUAL", "BTCUSDT", "linear") != k(
        "BINANCE-FUTURES:PERPETUAL:BTC-USDC@LIN", "PERPETUAL", "BTCUSDC", "linear"
    )
    # Different venues never collapse.
    assert k("BYBIT:PERPETUAL:BTC-USDT@LIN", "PERPETUAL", "BTCUSDT", "linear") != k(
        "BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN", "PERPETUAL", "BTCUSDT", "linear"
    )
    # Non-perp types and blank raw_symbol return None (caller falls back to the id key).
    assert k("DERIBIT:COMBO:BTC-X", "COMBO", "BTC-X", "") is None
    assert k("HYPERLIQUID:SPOT_PAIR:BTC-USD", "SPOT_PAIR", "BTCUSD", "") is None
    assert k("HYPERLIQUID:PERPETUAL:BTC-USD@LIN", "PERPETUAL", "", "linear") is None


def _perp_row(iid: str, base: str, raw_symbol: str, genesis: str) -> dict[str, object]:
    """A HYPERLIQUID/ASTER-style perp by_date row (linear, with a declared genesis)."""
    venue = iid.split(":", 1)[0]
    return {
        "instrument_key": iid,
        "venue": venue,
        "instrument_type": "PERPETUAL",
        "base_asset": base,
        "raw_symbol": raw_symbol,
        "margin_type": "linear",
        "available_from_datetime": genesis,
    }


def test_rollup_hyperliquid_perp_convention_chain_collapses_to_one_lineage(rollup: ModuleType) -> None:
    """The PERP:BTC → PERPETUAL:BTC-USD → PERPETUAL:BTC-USD@LIN chain (3 IDs for ONE
    perp across the 2026-07 convention churn) collapses to ONE row — the current live
    ``@LIN`` id, earliest available_from, active. The stale old-form rows do NOT
    survive (the HYPERLIQUID ~176-of-534 stale-dup class)."""
    d_old, d_mid, d_live = date(2026, 6, 20), date(2026, 7, 7), date(2026, 7, 14)
    # Two live bases so no day is a thin-day outlier.
    snapshots = [
        (
            d_old,
            _snapshot(
                [
                    _perp_row("HYPERLIQUID:PERP:BTC", "BTC", "BTC", "2023-05-12"),
                    _perp_row("HYPERLIQUID:PERP:ETH", "ETH", "ETH", "2023-05-12"),
                ]
            ),
        ),
        (
            d_mid,
            _snapshot(
                [
                    _perp_row("HYPERLIQUID:PERPETUAL:BTC-USD", "BTC", "BTC", "2023-05-12"),
                    _perp_row("HYPERLIQUID:PERPETUAL:ETH-USD", "ETH", "ETH", "2023-05-12"),
                ]
            ),
        ),
        (
            d_live,
            _snapshot(
                [
                    _perp_row("HYPERLIQUID:PERPETUAL:BTC-USD@LIN", "BTC", "BTC", "2023-05-12"),
                    _perp_row("HYPERLIQUID:PERPETUAL:ETH-USD@LIN", "ETH", "ETH", "2023-05-12"),
                ]
            ),
        ),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}
    # Exactly one lineage per base — the live @LIN id survives, the stale forms are gone.
    assert set(by_id) == {"HYPERLIQUID:PERPETUAL:BTC-USD@LIN", "HYPERLIQUID:PERPETUAL:ETH-USD@LIN"}, by_id
    btc = by_id["HYPERLIQUID:PERPETUAL:BTC-USD@LIN"]
    assert btc["available_from"] == "2023-05-12"  # earliest per-instrument genesis carried
    assert btc["available_to"] is None  # re-observed on the latest day → active


def test_rollup_aster_legacy_perp_date_folds_to_live_form(rollup: ModuleType) -> None:
    """ASTER dating bug: the dead ``ASTER:PERP:0GUSDT`` form carries a spurious
    uniform venue-launch genesis (2023-07-22, which PREDATES the 0G token). Folded
    into the canonical lineage, available_from must follow the LIVE ``@LIN`` form's
    true listing date (2025-09-24), NOT the spurious earlier old-form date."""
    d_old, d_live = date(2026, 6, 20), date(2026, 7, 14)
    snapshots = [
        (
            d_old,
            _snapshot(
                [
                    _perp_row("ASTER:PERP:0GUSDT", "0G", "0GUSDT", "2023-07-22"),
                    _perp_row("ASTER:PERP:BTCUSDT", "BTC", "BTCUSDT", "2021-08-27"),
                ]
            ),
        ),
        (
            d_live,
            _snapshot(
                [
                    _perp_row("ASTER:PERPETUAL:0G-USDT@LIN", "0G", "0GUSDT", "2025-09-24"),
                    _perp_row("ASTER:PERPETUAL:BTC-USDT@LIN", "BTC", "BTCUSDT", "2021-08-27"),
                ]
            ),
        ),
    ]
    df = rollup.build_catalogue_dataframe(snapshots)
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}
    assert set(by_id) == {"ASTER:PERPETUAL:0G-USDT@LIN", "ASTER:PERPETUAL:BTC-USDT@LIN"}, by_id
    # 0G: spurious 2023-07-22 old-form date DISCARDED for the live form's real listing.
    assert by_id["ASTER:PERPETUAL:0G-USDT@LIN"]["available_from"] == "2025-09-24"
    # BTC: both forms agree on the genuine early date → preserved.
    assert by_id["ASTER:PERPETUAL:BTC-USDT@LIN"]["available_from"] == "2021-08-27"


def test_rollup_perp_collapse_never_merges_distinct_quotes(rollup: ModuleType) -> None:
    """SAFETY: two genuinely-different live perps on the SAME venue/base but different
    quote (Binance BTC-USDT vs BTC-USDC linear) have distinct raw_symbols → stay TWO
    rows. No live instrument is ever lost to the collapse."""
    d1, d2 = date(2026, 7, 10), date(2026, 7, 14)
    rows = [
        _perp_row("BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN", "BTC", "BTCUSDT", "2020-01-01"),
        _perp_row("BINANCE-FUTURES:PERPETUAL:BTC-USDC@LIN", "BTC", "BTCUSDC", "2022-01-01"),
    ]
    df = rollup.build_catalogue_dataframe([(d1, _snapshot(rows)), (d2, _snapshot(rows))])
    ids = {row["instrument_id"] for row in df.to_dict("records")}
    assert ids == {"BINANCE-FUTURES:PERPETUAL:BTC-USDT@LIN", "BINANCE-FUTURES:PERPETUAL:BTC-USDC@LIN"}


def test_rollup_equity_instrument_type_broad_and_tags_stamped(rollup: ModuleType) -> None:
    """Operator 2026-07-16: a tradfi-underlying PERPETUAL STAYS PERPETUAL and a
    tokenized-share SPOT STAYS SPOT_PAIR (instrument_type is the broad mechanics
    type, id unchanged). The equity identity rides the is_equity_perp / tracks_equity
    tags stamped by _add_equity_tags. A crypto perp is untouched + untagged."""
    d1, d2 = date(2026, 7, 10), date(2026, 7, 14)
    rows = [
        _perp_row("BINANCE-FUTURES:PERPETUAL:AAPL-USDT@LIN", "AAPL", "AAPLUSDT", "2026-06-01"),
        _perp_row("BINANCE-FUTURES:PERPETUAL:NVDA-USDT@LIN", "NVDA", "NVDAUSDT", "2026-06-01"),
        _perp_row("HYPERLIQUID:PERPETUAL:BTC-USD@LIN", "BTC", "BTC", "2023-05-12"),
        {
            "instrument_key": "BYBIT:SPOT_PAIR:AAPLX-USDT",
            "venue": "BYBIT-SPOT",
            "instrument_type": "SPOT_PAIR",
            "base_asset": "AAPLX",
            "raw_symbol": "AAPLXUSDT",
        },
    ]
    df = rollup.build_catalogue_dataframe([(d1, _snapshot(rows)), (d2, _snapshot(rows))])
    df = rollup._add_equity_tags(df, "cefi")
    by_id = {row["instrument_id"]: row for row in df.to_dict("records")}
    # instrument_type stays the BROAD mechanics type — NOT EQUITY_PERP / TOKENIZED_EQUITY.
    assert by_id["BINANCE-FUTURES:PERPETUAL:AAPL-USDT@LIN"]["instrument_type"] == "PERPETUAL"
    assert by_id["BINANCE-FUTURES:PERPETUAL:NVDA-USDT@LIN"]["instrument_type"] == "PERPETUAL"
    assert by_id["BYBIT:SPOT_PAIR:AAPLX-USDT"]["instrument_type"] == "SPOT_PAIR"
    assert by_id["HYPERLIQUID:PERPETUAL:BTC-USD@LIN"]["instrument_type"] == "PERPETUAL"
    # equity instruments carry the tags (NVDA → NVDA, AAPL perp/tokenized → AAPL).
    assert bool(by_id["BINANCE-FUTURES:PERPETUAL:NVDA-USDT@LIN"]["is_equity_perp"]) is True
    assert by_id["BINANCE-FUTURES:PERPETUAL:NVDA-USDT@LIN"]["tracks_equity"] == "NVDA"
    assert bool(by_id["BINANCE-FUTURES:PERPETUAL:AAPL-USDT@LIN"]["is_equity_perp"]) is True
    assert by_id["BINANCE-FUTURES:PERPETUAL:AAPL-USDT@LIN"]["tracks_equity"] == "AAPL"
    assert bool(by_id["BYBIT:SPOT_PAIR:AAPLX-USDT"]["is_equity_perp"]) is True
    assert by_id["BYBIT:SPOT_PAIR:AAPLX-USDT"]["tracks_equity"] == "AAPL"
    # a crypto perp is NOT an equity instrument.
    assert bool(by_id["HYPERLIQUID:PERPETUAL:BTC-USD@LIN"]["is_equity_perp"]) is False
    assert by_id["HYPERLIQUID:PERPETUAL:BTC-USD@LIN"]["tracks_equity"] == ""


def test_add_equity_tags_non_cefi_defaults_and_dtype(rollup: ModuleType) -> None:
    """Non-cefi rows carry (is_equity_perp=False, tracks_equity=""); empty frame keeps
    a typed bool column (stable schema)."""
    df = pd.DataFrame(
        [{"instrument_id": "UNI-1", "instrument_type": "POOL", "venue": "UNISWAP_V3-ARBITRUM", "base_asset": ""}],
        columns=[c for c in rollup.CATALOG_COLUMNS if c not in ("mvp", "tracks_equity", "is_equity_perp")],
    )
    out = rollup._add_equity_tags(df, "defi")
    assert bool(out["is_equity_perp"].iloc[0]) is False
    assert out["tracks_equity"].iloc[0] == ""
    empty = rollup.build_catalogue_dataframe([])
    out_empty = rollup._add_equity_tags(empty, "cefi")
    assert out_empty["is_equity_perp"].dtype == bool


# ---------------------------------------------------------------------------
# Incremental (trailing-window + frozen-tail) engine —
# plans/active/instruments_catalogue_incremental_rollup_2026_06_29.md
# Phase 1 (mode selection / windowed walk / merge branches / cold start) +
# Phase 2 (incremental == full-rebuild parity, newly-delisted edge case).
# ---------------------------------------------------------------------------

from datetime import UTC as _UTC
from datetime import datetime as _datetime
from datetime import timedelta as _timedelta


class _RecordingStorage(_FakeStorage):
    """_FakeStorage + write ops, recording every list_blobs prefix (window assertions)."""

    def __init__(self, blobs: dict[str, bytes]) -> None:
        super().__init__(blobs)
        self.listed_prefixes: list[str] = []

    def list_blobs(self, bucket: str, prefix: str = "", **kw: object) -> list[_Blob]:
        self.listed_prefixes.append(prefix)
        return super().list_blobs(bucket, prefix, **kw)

    def upload_bytes(self, bucket: str, blob_path: str, payload: bytes, **kw: object) -> None:
        self._blobs[blob_path] = payload

    def copy_blob(self, src_bucket: str, src_path: str, dst_bucket: str, dst_path: str) -> None:
        self._blobs[dst_path] = self._blobs[src_path]

    def delete_blob(self, bucket: str, blob_path: str) -> None:
        del self._blobs[blob_path]


def _cat_row(**overrides: object) -> dict[str, object]:
    """A prev-catalogue row with every CATALOG column defaulted (mvp included)."""
    row: dict[str, object] = {
        "instrument_id": "X",
        "instrument_type": "SPOT_PAIR",
        "venue": "V",
        "chain": "",
        "league_id": "",
        "available_from": "2024-01-01",
        "available_to": None,
        "market_created_at": None,
        "settlement_time": None,
        "data_type": None,
        "underlying": "",
        "raw_symbol": "",
        "base_asset": "",
        "mvp": False,
        "margin_type": "",
        "glued_pair_id": "",
        "pool_address": "",
    }
    row.update(overrides)
    return row


def test_parse_args_mode_defaults_incremental(rollup: ModuleType) -> None:
    args = rollup._parse_args(["--asset-group", "tradfi"])
    assert args.mode == "incremental"
    args = rollup._parse_args(["--asset-group", "tradfi", "--mode", "full"])
    assert args.mode == "full"


def test_compute_window_start_fresh_and_stale(rollup: ModuleType) -> None:
    """Fresh catalogue → 21-day window; stale catalogue → SELF-WIDENING covers the gap."""
    today = date(2026, 7, 3)
    fresh = _datetime(2026, 7, 2, 1, 0, tzinfo=_UTC)
    assert rollup.compute_window_start(today, fresh) == today - _timedelta(days=21)
    # 35 days stale → window = 35 + 7 margin = 42 days (covers the whole gap).
    stale = _datetime(2026, 5, 29, 1, 0, tzinfo=_UTC)
    assert rollup.compute_window_start(today, stale) == today - _timedelta(days=42)
    # Unknown mtime degrades to the minimum window.
    assert rollup.compute_window_start(today, None) == today - _timedelta(days=21)


def test_iter_by_date_since_lists_only_window_days(rollup: ModuleType) -> None:
    """since= must produce per-day prefix listings (date-floored), never a corpus walk."""
    today = _datetime.now(tz=_UTC).date()
    old_day = (today - _timedelta(days=40)).isoformat()
    in_day = (today - _timedelta(days=2)).isoformat()
    blobs = {
        f"instrument_availability/by_date/day={old_day}/venue=V/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "OLD", "venue": "V"}]
        ),
        f"instrument_availability/by_date/day={in_day}/venue=V/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "IN", "venue": "V"}]
        ),
    }
    storage = _RecordingStorage(blobs)
    out = list(
        rollup._iter_by_date_snapshots(
            storage,
            "bkt",
            "instrument_availability/by_date",
            since=today - _timedelta(days=5),
        )
    )
    # Only the in-window frame is read.
    assert [str(d) for d, _ in out] == [in_day]
    # Every listing is a day= prefix at/after the cutoff — no whole-prefix walk.
    assert storage.listed_prefixes, "expected per-day prefix listings"
    for prefix in storage.listed_prefixes:
        assert "/day=" in prefix, f"whole-corpus walk detected: {prefix!r}"
        day_part = prefix.rsplit("day=", 1)[1].rstrip("/")
        assert day_part >= (today - _timedelta(days=5)).isoformat()


def test_merge_updated_row_carries_available_from_and_refreshes(rollup: ModuleType) -> None:
    """Branch 1: window recompute wins, but available_from is immutable (min of both)."""
    prev = pd.DataFrame([_cat_row(instrument_id="A", available_from="2024-01-01", available_to=None, raw_symbol="old")])
    window = pd.DataFrame(
        [_cat_row(instrument_id="A", available_from="2026-06-20", available_to=None, raw_symbol="new")]
    )
    window = window.drop(columns=["mvp"])
    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 6, 12), asset_group="cefi")
    assert len(merged) == 1
    row = merged.to_dict("records")[0]
    assert row["available_from"] == "2024-01-01"  # carried from prev (true listing day)
    assert row["raw_symbol"] == "new"  # metadata follows the window recompute


def test_merge_new_listing_appended(rollup: ModuleType) -> None:
    """Branch 2: a window-only instrument appends with its own (correct) available_from."""
    prev = pd.DataFrame([_cat_row(instrument_id="A")])
    window = pd.DataFrame(
        [
            _cat_row(instrument_id="A"),
            _cat_row(instrument_id="B", available_from="2026-06-25"),
        ]
    ).drop(columns=["mvp"])
    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 6, 12), asset_group="cefi")
    by_id = {r["instrument_id"]: r for r in merged.to_dict("records")}
    assert set(by_id) == {"A", "B"}
    assert by_id["B"]["available_from"] == "2026-06-25"


def test_merge_newly_delisted_closed_at_window_start_minus_one(rollup: ModuleType) -> None:
    """Branch 3: active-in-prev, absent all window, venue still capturing → closed."""
    prev = pd.DataFrame(
        [
            _cat_row(instrument_id="GONE", venue="V", available_to=None),
            _cat_row(instrument_id="STAYS", venue="V", available_to=None),
        ]
    )
    window = pd.DataFrame([_cat_row(instrument_id="STAYS", venue="V", available_to=None)]).drop(columns=["mvp"])
    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 6, 12), asset_group="cefi")
    by_id = {r["instrument_id"]: r for r in merged.to_dict("records")}
    assert by_id["GONE"]["available_to"] == "2026-06-11"  # window_start - 1
    assert by_id["STAYS"]["available_to"] is None


def test_merge_venue_absent_from_window_preserves_active(rollup: ModuleType) -> None:
    """Branch 4 (§7.3 venue-truth): a venue with NO window presence is a capture
    outage, not a mass delisting — its active instruments stay active, exactly like
    the full rebuild (per-venue last-full-day keeps a stopped venue's frontier)."""
    prev = pd.DataFrame(
        [
            _cat_row(instrument_id="OUTAGE-1", venue="DEAD-VENUE", available_to=None),
            _cat_row(instrument_id="DELISTED-OLD", venue="DEAD-VENUE", available_to="2025-01-01"),
        ]
    )
    window = pd.DataFrame([_cat_row(instrument_id="OTHER", venue="LIVE-VENUE")]).drop(columns=["mvp"])
    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 6, 12), asset_group="cefi")
    by_id = {r["instrument_id"]: r for r in merged.to_dict("records")}
    assert by_id["OUTAGE-1"]["available_to"] is None  # NOT closed
    assert by_id["DELISTED-OLD"]["available_to"] == "2025-01-01"  # frozen tail untouched


def test_merge_ghost_venue_spelling_updates_not_duplicates(rollup: ModuleType) -> None:
    """Regression (2026-07-04, first weekly self-heal): one instrument_id whose venue
    FIELD spelling changed era-to-era (prev row ``DERIBIT-COMBO``, window row
    ``DERIBIT``) must merge into ONE row — the full rebuild aggregates non-pool rows
    on ``instrument_id`` alone, never on the venue field. The old venue-composite
    merge key appended 122 such ghost duplicates to the cefi catalogue, so the weekly
    full rebuild produced FEWER rows and the monotonic guard (correctly) blocked the
    self-heal with ``CATALOGUE_SHRINK_BLOCKED``."""
    prev = pd.DataFrame(
        [
            _cat_row(
                instrument_id="DERIBIT:COMBO:BTC-X",
                venue="DERIBIT-COMBO",
                available_from="2024-01-01",
                available_to="2026-06-11",
            )
        ]
    )
    window = pd.DataFrame(
        [
            _cat_row(
                instrument_id="DERIBIT:COMBO:BTC-X",
                venue="DERIBIT",
                available_from="2026-06-28",
                available_to=None,
            )
        ]
    ).drop(columns=["mvp"])
    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 6, 12), asset_group="cefi")
    assert len(merged) == 1, "ghost venue spelling must UPDATE the known row, not append a duplicate"
    row = merged.to_dict("records")[0]
    assert row["venue"] == "DERIBIT"  # metadata follows the most-recent (window) spelling
    assert row["available_from"] == "2024-01-01"  # lifecycle carried through the spelling change
    assert row["available_to"] is None  # re-observed in the window → active


def test_merge_perp_convention_chain_collapses_to_live_lineage(rollup: ModuleType) -> None:
    """Incremental merge over the EXISTING (unmigrated) catalogue: the 3 stale HL id
    forms in prev collapse onto the single live ``@LIN`` lineage the window rebuild
    emits — one row, live id, earliest available_from carried. This is the D-code HL
    dedup applied through the incremental path (a legitimate corrective SHRINK: the
    prod materialisation needs ``--allow-catalogue-shrink`` / a full rebuild)."""
    prev = pd.DataFrame(
        [
            _cat_row(
                instrument_id="HYPERLIQUID:PERP:BTC",
                instrument_type="PERPETUAL",
                venue="HYPERLIQUID",
                base_asset="BTC",
                raw_symbol="BTC",
                margin_type="linear",
                available_from="2023-05-12",
                available_to="2026-07-06",
            ),
            _cat_row(
                instrument_id="HYPERLIQUID:PERPETUAL:BTC-USD",
                instrument_type="PERPETUAL",
                venue="HYPERLIQUID",
                base_asset="BTC",
                raw_symbol="BTC",
                margin_type="linear",
                available_from="2023-05-12",
                available_to="2026-07-08",
            ),
            _cat_row(
                instrument_id="HYPERLIQUID:PERPETUAL:BTC-USD@LIN",
                instrument_type="PERPETUAL",
                venue="HYPERLIQUID",
                base_asset="BTC",
                raw_symbol="BTC",
                margin_type="linear",
                available_from="2023-05-12",
                available_to=None,
            ),
        ]
    )
    # The window rebuild sees only the live @LIN form (current convention).
    window = pd.DataFrame(
        [
            _cat_row(
                instrument_id="HYPERLIQUID:PERPETUAL:BTC-USD@LIN",
                instrument_type="PERPETUAL",
                venue="HYPERLIQUID",
                base_asset="BTC",
                raw_symbol="BTC",
                margin_type="linear",
                available_from="2023-05-12",
                available_to=None,
            )
        ]
    ).drop(columns=["mvp"])
    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 7, 10), asset_group="cefi")
    assert len(merged) == 1, "the 3 id-convention forms must collapse to ONE live lineage row"
    row = merged.to_dict("records")[0]
    assert row["instrument_id"] == "HYPERLIQUID:PERPETUAL:BTC-USD@LIN"  # the live id survives
    assert row["available_from"] == "2023-05-12"  # earliest carried
    assert row["available_to"] is None


def test_merge_defi_pool_keys_on_dual_form_identity(rollup: ModuleType) -> None:
    """Pool rows merge on pool::<CHAIN>::<addr> — same address on two chains stays two rows."""
    addr = "0xabcdef0000000000000000000000000000000001"
    prev = pd.DataFrame(
        [
            _cat_row(
                instrument_id=addr,
                instrument_type="POOL",
                venue="UNISWAP_V3",
                chain="POLYGON",
                pool_address=addr,
                available_from="2024-03-01",
                available_to=None,
            ),
            _cat_row(
                instrument_id=addr,
                instrument_type="POOL",
                venue="UNISWAP_V3",
                chain="ARBITRUM",
                pool_address=addr,
                available_from="2024-04-01",
                available_to=None,
            ),
        ]
    )
    # Window sees only the POLYGON pool.
    window = pd.DataFrame(
        [
            _cat_row(
                instrument_id=addr,
                instrument_type="POOL",
                venue="UNISWAP_V3",
                chain="POLYGON",
                pool_address=addr,
                available_from="2026-06-15",
                available_to=None,
            )
        ]
    ).drop(columns=["mvp"])
    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 6, 12), asset_group="defi")
    assert len(merged) == 2
    by_chain = {r["chain"]: r for r in merged.to_dict("records")}
    assert by_chain["POLYGON"]["available_from"] == "2024-03-01"  # updated, af carried
    # ARBITRUM pool absent from window but venue present → closed (genuine per-pool absence).
    assert by_chain["ARBITRUM"]["available_to"] == "2026-06-11"


def test_merge_empty_window_preserves_catalogue(rollup: ModuleType) -> None:
    """A 0-row window (download outage) must pass the prev catalogue through unchanged."""
    prev = pd.DataFrame([_cat_row(instrument_id="A"), _cat_row(instrument_id="B", available_to="2025-05-05")])
    window = pd.DataFrame(columns=[c for c in rollup.CATALOG_COLUMNS if c != "mvp"])
    merged = rollup._merge_incremental(prev, window, window_start=date(2026, 6, 12), asset_group="cefi")
    assert len(merged) == 2
    by_id = {r["instrument_id"]: r for r in merged.to_dict("records")}
    assert by_id["A"]["available_to"] is None
    assert by_id["B"]["available_to"] == "2025-05-05"


def test_incremental_cold_start_falls_back_to_full(rollup: ModuleType) -> None:
    """No previous catalogue → --mode incremental runs the full rebuild and promotes."""
    today = _datetime.now(tz=_UTC).date().isoformat()
    blobs = {
        f"instrument_availability/by_date/day={today}/venue=V/instruments.parquet": _parquet_bytes(
            [{"instrument_key": "A", "venue": "V"}]
        ),
    }
    storage = _RecordingStorage(blobs)
    code = rollup.run_rollup(
        "cefi",
        allow_shrink=False,
        dry_run=False,
        mode="incremental",
        storage=storage,
    )
    assert code == 0
    # Full walk (whole-prefix listing) was used — cold start, not a window read...
    assert any("day=" not in p for p in storage.listed_prefixes)
    # ...and the catalogue was written.
    assert any(name.endswith("catalog.parquet") and "_catalogue_staging" not in name for name in storage._blobs)


# ---------------------------------------------------------------------------
# Phase 2 — the correctness ship-gate: incremental(prev, window) == full(all),
# row-for-row, per asset group.
# ---------------------------------------------------------------------------


def _parity_frames(rollup: ModuleType, all_snapshots: list, prev_age_days: int = 3, asset_group: str = "cefi") -> tuple:
    """Return (full_df, incremental_df) for a snapshot corpus.

    prev = full rebuild over every day up to (today - prev_age_days), mtime that
    day; window = self-widening trailing read; incremental = merge.
    """
    today = _datetime.now(tz=_UTC).date()
    prev_cutoff = today - _timedelta(days=prev_age_days)
    prev_df = rollup.build_catalogue_dataframe([(d, f) for d, f in all_snapshots if d <= prev_cutoff])
    prev_mtime = _datetime(prev_cutoff.year, prev_cutoff.month, prev_cutoff.day, 1, 0, tzinfo=_UTC)
    window_start = rollup.compute_window_start(today, prev_mtime)
    window_df = rollup.build_catalogue_dataframe([(d, f) for d, f in all_snapshots if d >= window_start])
    incremental = rollup._merge_incremental(prev_df, window_df, window_start=window_start, asset_group=asset_group)
    full = rollup.build_catalogue_dataframe(all_snapshots)
    return full, incremental


def _assert_frames_match(full: pd.DataFrame, incremental: pd.DataFrame) -> None:
    # Compare only the stable rollup columns — mvp + the equity-identity tags are
    # derived/finalization columns stamped AFTER the merge (build_catalogue_dataframe
    # emits them NaN in the full path; _merge_incremental drops them), so they are
    # not part of the merge-parity invariant here.
    cols = [c for c in full.columns if c not in ("mvp", "tracks_equity", "is_equity_perp")]
    f = full[cols].fillna("").astype(str).sort_values(cols).reset_index(drop=True)
    i = incremental[cols].fillna("").astype(str).sort_values(cols).reset_index(drop=True)
    pd.testing.assert_frame_equal(f, i)


def _cefi_corpus() -> list:
    """40 days of cefi spot/perp history: long-active, old-delisted, mid-window
    delist, new listing — ≥4 instruments/day so no day is a thin-day outlier."""
    today = _datetime.now(tz=_UTC).date()
    days = [today - _timedelta(days=n) for n in range(39, -1, -1)]
    snapshots = []
    for d in days:
        age = (today - d).days
        rows = [
            {
                "instrument_key": "BTC-PERP",
                "venue": "BINANCE-FUTURES",
                "instrument_type": "PERPETUAL",
                "base_asset": "BTC",
            },
            {
                "instrument_key": "ETH-PERP",
                "venue": "BINANCE-FUTURES",
                "instrument_type": "PERPETUAL",
                "base_asset": "ETH",
            },
            {
                "instrument_key": "BTC-USDT",
                "venue": "BINANCE-SPOT",
                "instrument_type": "SPOT_PAIR",
                "base_asset": "BTC",
            },
            {
                "instrument_key": "ETH-USDT",
                "venue": "BINANCE-SPOT",
                "instrument_type": "SPOT_PAIR",
                "base_asset": "ETH",
            },
        ]
        if age >= 30:  # delisted long before the window (frozen tail)
            rows.append(
                {
                    "instrument_key": "OLD-USDT",
                    "venue": "BINANCE-SPOT",
                    "instrument_type": "SPOT_PAIR",
                    "base_asset": "OLD",
                }
            )
        if age >= 8:  # delists mid-window
            rows.append(
                {
                    "instrument_key": "MID-PERP",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "PERPETUAL",
                    "base_asset": "MID",
                }
            )
        if age <= 5:  # brand-new listing inside the window
            rows.append(
                {
                    "instrument_key": "NEW-PERP",
                    "venue": "BINANCE-FUTURES",
                    "instrument_type": "PERPETUAL",
                    "base_asset": "NEW",
                }
            )
        snapshots.append((d, _snapshot(rows)))
    return snapshots


def test_incremental_matches_full_rebuild_cefi(rollup: ModuleType) -> None:
    full, incremental = _parity_frames(rollup, _cefi_corpus())
    _assert_frames_match(full, incremental)
    # The finalization tags (mvp + equity tags) are identical on both paths — the
    # full pipeline is merge → _add_mvp_column → _add_equity_tags (perp-gate + equity
    # tags computed over the whole finalized frame, not the window slice).
    full_final = rollup._add_equity_tags(rollup._add_mvp_column(full, "cefi"), "cefi")
    inc_final = rollup._add_equity_tags(rollup._add_mvp_column(incremental, "cefi"), "cefi")
    cols = list(full_final.columns)
    pd.testing.assert_frame_equal(
        full_final[cols].fillna("").astype(str).sort_values(cols).reset_index(drop=True),
        inc_final[cols].fillna("").astype(str).sort_values(cols).reset_index(drop=True),
    )


def test_incremental_matches_full_rebuild_tradfi(rollup: ModuleType) -> None:
    """Dated FUTURE/OPTION rows: available_to = venue-truth expiry on both paths."""
    today = _datetime.now(tz=_UTC).date()
    days = [today - _timedelta(days=n) for n in range(39, -1, -1)]
    far_expiry = (today + _timedelta(days=90)).isoformat()
    past_expiry = (today - _timedelta(days=30)).isoformat()
    snapshots = []
    for d in days:
        age = (today - d).days
        rows = [
            {
                "instrument_key": "ESZ6",
                "venue": "CME",
                "instrument_type": "FUTURE",
                "expiry": far_expiry,
                "underlying": "ES",
            },
            {
                "instrument_key": "NQZ6",
                "venue": "CME",
                "instrument_type": "FUTURE",
                "expiry": far_expiry,
                "underlying": "NQ",
            },
            {"instrument_key": "SPY", "venue": "ARCA", "instrument_type": "SPOT_PAIR", "base_asset": "SPY"},
            {"instrument_key": "QQQ", "venue": "ARCA", "instrument_type": "SPOT_PAIR", "base_asset": "QQQ"},
        ]
        if age >= 25:  # expired contract that stopped appearing pre-window
            rows.append(
                {
                    "instrument_key": "ESU6",
                    "venue": "CME",
                    "instrument_type": "FUTURE",
                    "expiry": past_expiry,
                    "underlying": "ES",
                }
            )
        if age <= 4:  # new contract series after the roll
            rows.append(
                {
                    "instrument_key": "ESH7",
                    "venue": "CME",
                    "instrument_type": "FUTURE",
                    "expiry": far_expiry,
                    "underlying": "ES",
                }
            )
        snapshots.append((d, _snapshot(rows)))
    full, incremental = _parity_frames(rollup, snapshots, asset_group="tradfi")
    _assert_frames_match(full, incremental)


def test_incremental_matches_full_rebuild_defi(rollup: ModuleType) -> None:
    """DeFi dual-form pool rows: parity incl. canonical pool identity + chain split."""
    today = _datetime.now(tz=_UTC).date()
    days = [today - _timedelta(days=n) for n in range(39, -1, -1)]

    def _pool(i: int) -> dict[str, object]:
        addr = f"0x{i:040x}"
        return {
            "instrument_key": f"UNISWAP_V3-POLYGON:POOL:TK{i}-WETH:500",
            "venue": "UNISWAP_V3-POLYGON",
            "instrument_type": "POOL",
            "raw_symbol": addr,
            "pool_address": addr,
            "base_asset": f"TK{i}",
            "quote_asset": "WETH",
            "pool_fee_tier": 5.0,
        }

    snapshots = []
    for d in days:
        age = (today - d).days
        rows = [_pool(1), _pool(2), _pool(3), _pool(4)]
        if age >= 10:  # pool drained/retired mid-window
            rows.append(_pool(5))
        if age <= 6:  # new pool deployed inside the window
            rows.append(_pool(6))
        snapshots.append((d, _snapshot(rows)))
    full, incremental = _parity_frames(rollup, snapshots, asset_group="defi")
    _assert_frames_match(full, incremental)


def test_incremental_newly_delisted_mid_window_closes_to_true_boundary(rollup: ModuleType) -> None:
    """Phase 2 edge case: an active perp that stops appearing MID-window closes at
    its true last-seen day via the §7.3 window recompute (not a thin-day blip, not
    window_start-1)."""
    today = _datetime.now(tz=_UTC).date()
    stop_age = 8
    corpus = _cefi_corpus()
    full, incremental = _parity_frames(rollup, corpus)
    by_id = {r["instrument_id"]: r for r in incremental.to_dict("records")}
    expected_last = (today - _timedelta(days=stop_age)).isoformat()
    assert by_id["MID-PERP"]["available_to"] == expected_last
    # Identical to the full rebuild's verdict.
    full_by_id = {r["instrument_id"]: r for r in full.to_dict("records")}
    assert full_by_id["MID-PERP"]["available_to"] == expected_last


def test_incremental_matches_full_rebuild_prediction(rollup: ModuleType) -> None:
    """Phase 3: the prediction multi-grain rollup gets the same window+merge —
    parity vs full rebuild (settlement-date convention + per-conditionId grain)."""
    today = _datetime.now(tz=_UTC).date()
    days = [today - _timedelta(days=n) for n in range(39, -1, -1)]
    future_settle = (today + _timedelta(days=30)).isoformat()

    def _mkt(cid: str, settle: str | None = None) -> dict[str, object]:
        row: dict[str, object] = {"instrument_key": cid, "instrument_type": "MARKET", "venue": "POLYMARKET"}
        if settle:
            row["end_date_iso"] = settle
        return row

    snapshots = []
    for d in days:
        age = (today - d).days
        rows = [
            _mkt("0xlong1", future_settle),
            _mkt("0xlong2", future_settle),
            _mkt("0xlong3"),
        ]
        if age >= 12:  # settled mid-window: last snapshot T-12, declared settle T-11
            rows.append(_mkt("0xsettled", (today - _timedelta(days=11)).isoformat()))
        if age >= 30:  # long-gone market (frozen tail)
            rows.append(_mkt("0xold", (today - _timedelta(days=29)).isoformat()))
        if age <= 4:  # new market inside the window
            rows.append(_mkt("0xnew", future_settle))
        snapshots.append((d, "POLYMARKET", "", _snapshot(rows)))

    prev_cutoff = today - _timedelta(days=3)
    prev_df = rollup.build_prediction_catalogue_dataframe(
        [(d, v, c, f) for d, v, c, f in snapshots if d <= prev_cutoff]
    )
    prev_mtime = _datetime(prev_cutoff.year, prev_cutoff.month, prev_cutoff.day, 1, 0, tzinfo=_UTC)
    window_start = rollup.compute_window_start(today, prev_mtime)
    window_df = rollup.build_prediction_catalogue_dataframe(
        [(d, v, c, f) for d, v, c, f in snapshots if d >= window_start]
    )
    incremental = rollup._merge_incremental(prev_df, window_df, window_start=window_start, asset_group="prediction")
    full = rollup.build_prediction_catalogue_dataframe(snapshots)
    _assert_frames_match(full, incremental)


def test_coverage_horizon_warns_on_stale_latest_day(rollup: ModuleType) -> None:
    """CATALOGUE_STALE_BY_DATE fires when the newest by_date day is too old."""
    events: list[tuple[str, dict[str, object]]] = []
    orig = rollup._emit_event
    rollup._emit_event = lambda event, **kw: events.append((event, kw))
    try:
        today = date(2026, 7, 3)
        stale_counts = {date(2026, 6, 25): 100, date(2026, 6, 26): 100}  # newest 7d old
        rollup._warn_coverage_horizon(stale_counts, today, "tradfi")
        assert any(e == "CATALOGUE_STALE_BY_DATE" and kw.get("reason") == "latest_day_too_old" for e, kw in events)
        events.clear()
        fresh_counts = {date(2026, 7, 1): 100, date(2026, 7, 2): 100}
        rollup._warn_coverage_horizon(fresh_counts, today, "tradfi")
        assert not events  # healthy feed → silent
    finally:
        rollup._emit_event = orig


def test_coverage_horizon_clamps_future_days(rollup: ModuleType) -> None:
    """Regression (2026-07-06): the prediction writer emits FUTURE-dated day=
    partitions (settlement-dated dirs out to 2028+), which made ``max(day_counts)``
    land in the future and silence BOTH checks — hiding a 6-day capture outage
    (is-daily-enum-prediction failing since 07-01). Future days must be ignored:
    a window whose only recent PAST day is 7d old warns even when future-dated
    partitions are present."""
    events: list[tuple[str, dict[str, object]]] = []
    orig = rollup._emit_event
    rollup._emit_event = lambda event, **kw: events.append((event, kw))
    try:
        today = date(2026, 7, 6)
        counts = {
            date(2026, 6, 28): 2000,  # last real capture day, 8d old
            date(2026, 7, 31): 400,  # future-dated settlement partitions
            date(2028, 6, 30): 50,
        }
        rollup._warn_coverage_horizon(counts, today, "prediction")
        assert any(e == "CATALOGUE_STALE_BY_DATE" and kw.get("reason") == "latest_day_too_old" for e, kw in events), (
            "future-dated day= partitions must not mask a stale capture feed"
        )
        events.clear()
        # Only-future window (degenerate) → no_window_data, not silence.
        rollup._warn_coverage_horizon({date(2028, 6, 30): 50}, today, "prediction")
        assert any(e == "CATALOGUE_STALE_BY_DATE" and kw.get("reason") == "no_window_data" for e, kw in events)
    finally:
        rollup._emit_event = orig


def test_coverage_horizon_warns_on_sharp_count_drop(rollup: ModuleType) -> None:
    """CATALOGUE_STALE_BY_DATE fires when the newest day's count collapses vs the median."""
    events: list[tuple[str, dict[str, object]]] = []
    orig = rollup._emit_event
    rollup._emit_event = lambda event, **kw: events.append((event, kw))
    try:
        today = date(2026, 7, 3)
        counts = {date(2026, 7, d): 100 for d in range(1, 3)}
        counts[date(2026, 7, 3)] = 10  # 10% of median → partial capture
        rollup._warn_coverage_horizon(counts, today, "cefi")
        assert any(
            e == "CATALOGUE_STALE_BY_DATE" and kw.get("reason") == "latest_day_sharp_count_drop" for e, kw in events
        )
        events.clear()
        rollup._warn_coverage_horizon({}, today, "cefi")  # empty window also warns
        assert any(e == "CATALOGUE_STALE_BY_DATE" and kw.get("reason") == "no_window_data" for e, kw in events)
    finally:
        rollup._emit_event = orig
