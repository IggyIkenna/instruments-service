"""Unit tests for measure_cefi_catalogue_enumeration_gap_2026_07_23.py.

Synthetic-fixture only (no live GCS access) -- proves the pattern-matching /
gap-detection logic itself is correct for each of the 3 investigated cases:

  1. OKX-SPOT fiat/crypto-quote gap (spot-quote-gap, dash-delimited bare wire).
  2. COINBASE-SPOT crypto-quote gap (spot-quote-gap, same mechanism).
  3. BITGET-FUTURES CME-letter-month gap (cme-letter-month-gap, stale rollup).

Plus a negative control per case (a row that must NOT be flagged) so the
detector isn't just "everything is a gap."
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest


def _load_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "measure_cefi_catalogue_enumeration_gap_2026_07_23.py"
    module_name = "_measure_cefi_catalogue_enumeration_gap_test"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def mod() -> ModuleType:
    return _load_module()


def _manifest(rows: list[dict[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# extract_base_quote / extract_quote — pure parsing helpers
# ---------------------------------------------------------------------------


class TestExtractHelpers:
    def test_canonical_id_extracts_base_quote(self, mod: ModuleType) -> None:
        assert mod.extract_base_quote("OKX-SPOT:SPOT_PAIR:BTC-USDT") == "BTC-USDT"

    def test_canonical_id_with_marker_strips_marker(self, mod: ModuleType) -> None:
        assert mod.extract_base_quote("BITGET-FUTURES:PERPETUAL:BTC-USDT@LIN") == "BTC-USDT"

    def test_bare_wire_passes_through(self, mod: ModuleType) -> None:
        """Bare (non-canonical) wire ids -- the actual shape gap rows carry -- pass through unchanged."""
        assert mod.extract_base_quote("MATIC-BTC") == "MATIC-BTC"
        assert mod.extract_base_quote("USDC-TRY") == "USDC-TRY"

    def test_empty_and_malformed_return_none(self, mod: ModuleType) -> None:
        assert mod.extract_base_quote("") is None
        assert mod.extract_base_quote("ONLY:TWO") is None

    def test_quote_is_last_dash_segment(self, mod: ModuleType) -> None:
        assert mod.extract_quote("BTC-USDT") == "USDT"
        assert mod.extract_quote("MATIC-BTC") == "BTC"
        assert mod.extract_quote("USDC-TRY") == "TRY"

    def test_quote_none_for_no_dash_concatenated_wire(self, mod: ModuleType) -> None:
        """No-dash concatenated wires (WEETHUSDT) are a DIFFERENT failure mode, deliberately not decomposed here."""
        assert mod.extract_quote("WEETHUSDT") is None
        assert mod.extract_quote(None) is None

    def test_cme_month_shape_matches_known_examples(self, mod: ModuleType) -> None:
        assert mod.is_cme_month_shape("BTCUSDH26")
        assert mod.is_cme_month_shape("ETHUSDZ24")
        assert not mod.is_cme_month_shape("BTCUSDT")  # ordinary perpetual, no month code
        assert not mod.is_cme_month_shape("BTC-01DEC23")  # Bybit's OTHER dated shape, not this one


# ---------------------------------------------------------------------------
# Case A/B: spot-quote-gap (OKX-SPOT + COINBASE-SPOT)
# ---------------------------------------------------------------------------


class TestSpotQuoteGap:
    def test_okx_spot_btc_quote_is_flagged(self, mod: ModuleType) -> None:
        """OKX-SPOT BTC-quoted spot pair (bare wire, no catalogue backing) — a real gap row."""
        manifest = _manifest(
            [
                {
                    "venue": "OKX-SPOT",
                    "instrument_type": "SPOT_PAIR",
                    "instrument_id": "MATIC-BTC",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        catalog_wires: dict[str, set[str]] = {"OKX-SPOT": {"BTC-USDT", "BTC-USD", "BTC-USDC"}}
        result = mod.find_spot_quote_gaps(manifest, catalog_wires)
        assert len(result) == 1
        assert result.iloc[0]["gap_token"] == "BTC"
        assert result.iloc[0]["gap_case"] == "spot-quote-gap"

    def test_coinbase_spot_eth_quote_is_flagged(self, mod: ModuleType) -> None:
        """COINBASE-SPOT ETH-quoted spot pair (bare wire, empty_confirmed) — a real gap row."""
        manifest = _manifest(
            [
                {
                    "venue": "COINBASE-SPOT",
                    "instrument_type": "spot",  # lowercase drift — must still be recognised
                    "instrument_id": "STETH-ETH",
                    "capture_status": "empty_confirmed",
                    "data_type": "trades",
                },
            ]
        )
        catalog_wires: dict[str, set[str]] = {}
        result = mod.find_spot_quote_gaps(manifest, catalog_wires)
        assert len(result) == 1
        assert result.iloc[0]["gap_token"] == "ETH"

    def test_fleet_default_quote_is_not_flagged(self, mod: ModuleType) -> None:
        """Negative control: an ordinary USDT-quoted OKX-SPOT pair is NOT a gap."""
        manifest = _manifest(
            [
                {
                    "venue": "OKX-SPOT",
                    "instrument_type": "SPOT_PAIR",
                    "instrument_id": "OKX-SPOT:SPOT_PAIR:BTC-USDT",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.find_spot_quote_gaps(manifest, {})
        assert result.empty

    def test_gap_quote_already_catalogued_is_not_flagged(self, mod: ModuleType) -> None:
        """A gap-shaped quote that the catalogue ALREADY carries is not a live gap (fix has landed)."""
        manifest = _manifest(
            [
                {
                    "venue": "OKX-SPOT",
                    "instrument_type": "SPOT_PAIR",
                    "instrument_id": "MATIC-BTC",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        catalog_wires = {"OKX-SPOT": {"MATIC-BTC"}}
        result = mod.find_spot_quote_gaps(manifest, catalog_wires)
        assert result.empty

    def test_upbit_krw_extension_is_not_flagged(self, mod: ModuleType) -> None:
        """UPBIT's UAC-SSOT KRW extension must be honoured live (imported, not hardcoded)."""
        manifest = _manifest(
            [
                {
                    "venue": "UPBIT",
                    "instrument_type": "SPOT_PAIR",
                    "instrument_id": "BTC-KRW",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.find_spot_quote_gaps(manifest, {})
        assert result.empty

    def test_derivative_rows_are_not_in_scope(self, mod: ModuleType) -> None:
        """PERPETUAL rows never enter the spot-quote-gap case, regardless of quote shape."""
        manifest = _manifest(
            [
                {
                    "venue": "OKX-SPOT",
                    "instrument_type": "PERPETUAL",
                    "instrument_id": "MATIC-BTC",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.find_spot_quote_gaps(manifest, {})
        assert result.empty


# ---------------------------------------------------------------------------
# Case C: cme-letter-month-gap (BITGET-FUTURES)
# ---------------------------------------------------------------------------


class TestCmeMonthGap:
    def test_bitget_futures_cme_wire_not_in_catalogue_is_flagged(self, mod: ModuleType) -> None:
        manifest = _manifest(
            [
                {
                    "venue": "BITGET-FUTURES",
                    "instrument_type": "FUTURE",
                    "instrument_id": "BTCUSDH26",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.find_cme_month_gaps(manifest, {})
        assert len(result) == 1
        assert result.iloc[0]["gap_token"] == "BTCUSDH26"
        assert result.iloc[0]["gap_case"] == "cme-letter-month-gap"

    def test_bybit_same_shape_already_catalogued_is_not_flagged(self, mod: ModuleType) -> None:
        """BYBIT is the shape's origin venue — its instances already have catalogue rows."""
        manifest = _manifest(
            [
                {
                    "venue": "BYBIT",
                    "instrument_type": "FUTURE",
                    "instrument_id": "BTCUSDH26",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        catalog_wires = {"BYBIT": {"BTCUSDH26"}}
        result = mod.find_cme_month_gaps(manifest, catalog_wires)
        assert result.empty

    def test_ordinary_perpetual_wire_is_not_flagged(self, mod: ModuleType) -> None:
        """Negative control: an ordinary perpetual (no month code) is never a cme-letter-month-gap row."""
        manifest = _manifest(
            [
                {
                    "venue": "BITGET-FUTURES",
                    "instrument_type": "FUTURE",
                    "instrument_id": "BTCUSDT",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.find_cme_month_gaps(manifest, {})
        assert result.empty

    def test_spot_typed_row_with_month_shape_is_out_of_scope(self, mod: ModuleType) -> None:
        """The CME-month case is FUTURE-typed only; a SPOT_PAIR row never enters it, even with the same wire shape."""
        manifest = _manifest(
            [
                {
                    "venue": "BITGET-FUTURES",
                    "instrument_type": "SPOT_PAIR",
                    "instrument_id": "BTCUSDH26",
                    "capture_status": "captured",
                    "data_type": "trades",
                },
            ]
        )
        result = mod.find_cme_month_gaps(manifest, {})
        assert result.empty


# ---------------------------------------------------------------------------
# summarize() — grouping/aggregation sanity
# ---------------------------------------------------------------------------


class TestSummarize:
    def test_summarize_groups_by_venue_and_case_with_captured_split(self, mod: ModuleType) -> None:
        gap_df = pd.DataFrame(
            [
                {"venue": "OKX-SPOT", "gap_case": "spot-quote-gap", "gap_token": "BTC", "capture_status": "captured"},
                {"venue": "OKX-SPOT", "gap_case": "spot-quote-gap", "gap_token": "TRY", "capture_status": "captured"},
                {
                    "venue": "OKX-SPOT",
                    "gap_case": "spot-quote-gap",
                    "gap_token": "ETH",
                    "capture_status": "empty_confirmed",
                },
                {
                    "venue": "BITGET-FUTURES",
                    "gap_case": "cme-letter-month-gap",
                    "gap_token": "BTCUSDH26",
                    "capture_status": "captured",
                },
            ]
        )
        rows = mod.summarize(gap_df)
        by_key = {(r.venue, r.gap_case): r for r in rows}
        assert by_key[("OKX-SPOT", "spot-quote-gap")].row_count == 3
        assert by_key[("OKX-SPOT", "spot-quote-gap")].captured_count == 2
        assert by_key[("BITGET-FUTURES", "cme-letter-month-gap")].row_count == 1

    def test_summarize_empty_input_returns_empty_list(self, mod: ModuleType) -> None:
        assert mod.summarize(pd.DataFrame()) == []
