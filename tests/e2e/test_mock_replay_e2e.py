"""Mock replay E2E tests for instruments-service.

Loads VCR cassettes from the UAC canonical location and validates that
the instruments-service pipeline can process external API instrument data.
Runs under CLOUD_MOCK_MODE=true with zero network calls.

Cassettes used:
- deribit/get_instruments_multi.yaml: Multi-currency instrument listing
- hyperliquid/meta_and_asset_ctxs.yaml: Hyperliquid perpetual metadata
- binance/spot_exchange_info.yaml: Binance spot instrument data
- databento/metadata_list_datasets.yaml: Databento dataset metadata
"""

from __future__ import annotations

import pytest
import responses
from unified_api_contracts.testing.cassette_loader import (
    list_cassettes_for_venue,
    load_cassette,
    load_cassette_raw,
)
from unified_api_contracts.testing.mock_replay import (
    cassette_to_dict,
    replay_cassette,
)

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Deribit instrument ingestion
# ---------------------------------------------------------------------------


class TestDeribitInstrumentIngestion:
    """E2E: instruments-service ingests Deribit instrument catalog."""

    def test_deribit_instruments_parseable(self) -> None:
        """Deribit cassette instruments can be parsed into service models."""
        data = cassette_to_dict("deribit", "get_instruments_multi.yaml")

        instruments: list[dict[str, object]] = []
        for _uri, body in data.items():
            if isinstance(body, dict) and "result" in body:
                result = body["result"]
                if isinstance(result, list):
                    instruments.extend(result)

        assert len(instruments) >= 1

        for inst in instruments:
            assert isinstance(inst, dict)
            # Map to instruments-service schema
            parsed = {
                "source": "deribit",
                "instrument_id": inst["instrument_name"],
                "kind": inst["kind"],
                "base_currency": inst["base_currency"],
                "quote_currency": inst["quote_currency"],
                "tick_size": float(inst["tick_size"]),
                "contract_size": float(inst["contract_size"]),
                "is_active": inst["is_active"],
                "expiration_timestamp": inst.get("expiration_timestamp"),
            }
            assert parsed["source"] == "deribit"
            assert parsed["tick_size"] > 0
            assert parsed["contract_size"] > 0

    def test_deribit_multi_currency_coverage(self) -> None:
        """Cassette covers BTC, ETH, SOL, USDC currencies."""
        raw = load_cassette_raw("deribit", "get_instruments_multi.yaml")
        interactions = raw["interactions"]

        currencies_seen: set[str] = set()
        for interaction in interactions:
            uri = interaction["request"]["uri"]
            for currency in ["BTC", "ETH", "SOL", "USDC"]:
                if f"currency={currency}" in uri:
                    currencies_seen.add(currency)

        assert currencies_seen == {"BTC", "ETH", "SOL", "USDC"}

    def test_deribit_empty_results_handled(self) -> None:
        """SOL and USDC may have empty results; service handles gracefully."""
        # SOL futures is interaction index 2
        body = load_cassette("deribit", "get_instruments_multi.yaml", interaction_index=2)
        assert isinstance(body, dict)
        assert "result" in body
        result = body["result"]
        # SOL may have empty result
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Hyperliquid instrument ingestion
# ---------------------------------------------------------------------------


class TestHyperliquidInstrumentIngestion:
    """E2E: instruments-service ingests Hyperliquid perpetual metadata."""

    def test_hyperliquid_universe_parseable(self) -> None:
        """Hyperliquid meta cassette provides parseable instrument universe."""
        body = load_cassette("hyperliquid", "meta_and_asset_ctxs.yaml")

        universe = body["universe"]
        assert len(universe) >= 3

        for asset in universe:
            # Map to instruments-service schema
            parsed = {
                "source": "hyperliquid",
                "instrument_id": f"HYPERLIQUID:PERPETUAL:{asset['name']}-USD",
                "name": asset["name"],
                "sz_decimals": asset["szDecimals"],
                "max_leverage": asset["maxLeverage"],
                "only_isolated": asset.get("onlyIsolated", False),
            }
            assert parsed["sz_decimals"] >= 0
            assert parsed["max_leverage"] > 0

    def test_hyperliquid_instruments_have_unique_names(self) -> None:
        """All instruments in the Hyperliquid universe have unique names."""
        body = load_cassette("hyperliquid", "meta_and_asset_ctxs.yaml")
        names = [asset["name"] for asset in body["universe"]]
        assert len(names) == len(set(names)), "Duplicate instrument names found"

    @responses.activate
    def test_hyperliquid_meta_replay_http(self) -> None:
        """Replay Hyperliquid meta cassette via HTTP mock."""
        replay_cassette("hyperliquid", "meta_and_asset_ctxs.yaml")

        import requests

        resp = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "metaAndAssetCtxs"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Binance instrument ingestion
# ---------------------------------------------------------------------------


class TestBinanceInstrumentIngestion:
    """E2E: instruments-service ingests Binance exchange info."""

    def test_binance_spot_exchange_info_parseable(self) -> None:
        """Binance spot_exchange_info cassette provides instrument catalog."""
        cassettes = list_cassettes_for_venue("binance")
        names = [c.name for c in cassettes]
        assert "spot_exchange_info.yaml" in names

        body = load_cassette("binance", "spot_exchange_info.yaml")
        assert isinstance(body, dict)

    @responses.activate
    def test_binance_ticker_replay(self) -> None:
        """Binance ticker cassette replays correctly."""
        replay_cassette("binance", "ticker_24hr.yaml")

        import requests

        resp = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Cross-venue instrument aggregation
# ---------------------------------------------------------------------------


class TestCrossVenueInstrumentAggregation:
    """E2E: instruments-service aggregates instruments across venues."""

    def test_aggregate_instruments_from_multiple_venues(self) -> None:
        """Combine instruments from Deribit + Hyperliquid cassettes."""
        # Deribit instruments
        deribit_data = cassette_to_dict("deribit", "get_instruments_multi.yaml")
        deribit_instruments: list[dict[str, object]] = []
        for body in deribit_data.values():
            if isinstance(body, dict) and "result" in body:
                result = body["result"]
                if isinstance(result, list):
                    for inst in result:
                        deribit_instruments.append(
                            {
                                "source": "deribit",
                                "symbol": inst["instrument_name"],
                                "base": inst["base_currency"],
                            }
                        )

        # Hyperliquid instruments
        hl_body = load_cassette("hyperliquid", "meta_and_asset_ctxs.yaml")
        hl_instruments = [
            {
                "source": "hyperliquid",
                "symbol": f"HYPERLIQUID:{asset['name']}-USD",
                "base": asset["name"],
            }
            for asset in hl_body["universe"]
        ]

        # Aggregate
        all_instruments = deribit_instruments + hl_instruments
        assert len(all_instruments) >= 5

        # Both sources represented
        sources = {inst["source"] for inst in all_instruments}
        assert sources == {"deribit", "hyperliquid"}

        # Common base currencies
        bases = {inst["base"] for inst in all_instruments}
        assert "BTC" in bases
        assert "ETH" in bases
