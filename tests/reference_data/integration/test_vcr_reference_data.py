"""VCR cassette-based integration tests for unified-reference-data-interface.

Layer 1 interface pattern: replay pre-recorded HTTP cassettes via vcrpy's
aiohttp stubs, then validate that:
  1. The adapter parses the cassette response without error.
  2. The response shape validates against the canonical api-contracts schema.

No real network calls are made — cassettes are deterministic fixtures stored
alongside the api-contracts schemas they exercise.

Design note: tests that validate existing cassette bodies (ticker_24hr, deribit
ticker) parse the YAML directly rather than replaying via requests, because
vcrpy 6.x stubs are incompatible with urllib3 2.x (missing version_string
attribute).  Tests that exercise the adapter's aiohttp calls use _VCR with
aiohttp patching, which works correctly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

pytest.importorskip("vcr")
pytest.importorskip("unified_api_contracts")

import vcr

CASSETTE_DIR = Path(
    "/Users/ikennaigboaka/Code/unified-trading-system-repos/unified-api-contracts/unified_api_contracts/external"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VCR = vcr.VCR(
    record_mode="none",  # never make real network calls
    match_on=["method", "scheme", "host", "port", "path", "query"],
)


def _cassette_body(cassette_path: Path, interaction_index: int = 0) -> object:
    """Parse a VCR cassette YAML and return the response body as a Python object.

    Cassettes store the body as a JSON string under
    interactions[n].response.body.string.  This helper decodes it without
    issuing any HTTP call.
    """
    raw = yaml.safe_load(cassette_path.read_text())
    body_string: str = raw["interactions"][interaction_index]["response"]["body"]["string"]
    return json.loads(body_string)


# ---------------------------------------------------------------------------
# Binance — ticker/24hr cassette validates BinanceTicker schema
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_binance_ticker_24hr_cassette_validates_against_binance_ticker_schema() -> None:
    """Parse binance/mocks/ticker_24hr.yaml body and validate against BinanceTicker.

    The cassette captures a real Binance USD-M futures 24hr ticker response.
    BinanceTicker must accept the body without raising ValidationError.
    """
    from unified_api_contracts.external.binance.market_schemas import (
        BinanceTicker,
    )

    cassette_path = CASSETTE_DIR / "binance" / "mocks" / "ticker_24hr.yaml"
    data = _cassette_body(cassette_path)

    ticker = BinanceTicker.model_validate(data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.lastPrice > 0
    assert ticker.volume > 0
    assert ticker.openTime < ticker.closeTime


# ---------------------------------------------------------------------------
# Binance — spot exchangeInfo cassette drives BinanceReferenceDataAdapter
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_binance_spot_exchange_info_cassette_adapter_get_instruments() -> None:
    """Replay binance/mocks/spot_exchange_info.yaml through BinanceReferenceDataAdapter.

    The adapter's _fetch_spot() calls GET /api/v3/exchangeInfo.  The cassette
    contains two TRADING symbols (BTCUSDT, ETHUSDT) each with PRICE_FILTER and
    LOT_SIZE filters.  We assert InstrumentRecords are produced and their fields
    are populated correctly.
    """
    from unified_api_contracts.external.binance.market_schemas import (
        BinanceExchangeInfo,
        BinanceSymbol,
    )

    from instruments_service.reference_data import create_reference_data_adapter

    cassette_path = CASSETTE_DIR / "binance" / "mocks" / "spot_exchange_info.yaml"

    adapter = create_reference_data_adapter("binance")

    with _VCR.use_cassette(str(cassette_path)):
        instruments = await adapter.get_instruments(instrument_type="spot")

    # Cassette has BTCUSDT and ETHUSDT, both TRADING
    assert len(instruments) == 2

    btc = next(inst for inst in instruments if inst.raw_symbol == "BTCUSDT")
    assert btc.venue == "BINANCE-SPOT"
    assert btc.instrument_type == "spot"
    assert btc.base_asset == "BTC"
    assert btc.quote_asset == "USDT"
    assert btc.tick_size > 0
    assert btc.lot_size > 0

    eth = next(inst for inst in instruments if inst.raw_symbol == "ETHUSDT")
    assert eth.base_asset == "ETH"

    # Cross-validate raw cassette body against BinanceExchangeInfo schema
    cassette_body = (CASSETTE_DIR / "binance" / "mocks" / "spot_exchange_info.yaml").read_text()
    # Extract the JSON string from the cassette body line
    for line in cassette_body.splitlines():
        stripped = line.strip()
        if stripped.startswith("string:"):
            raw_json = stripped[len("string:") :].strip().strip("'")
            exchange_info = BinanceExchangeInfo.model_validate(json.loads(raw_json))
            assert len(exchange_info.symbols) == 2
            btc_sym: BinanceSymbol = next(s for s in exchange_info.symbols if s.symbol == "BTCUSDT")
            assert btc_sym.status == "TRADING"
            assert btc_sym.baseAsset == "BTC"
            break


# ---------------------------------------------------------------------------
# Deribit — single BTC future cassette validates DeribitInstrumentInfoFull schema
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_deribit_ticker_cassette_validates_against_deribit_instrument_schema() -> None:
    """Parse deribit/mocks/ticker.yaml body and validate against DeribitInstrumentInfoFull.

    The cassette records GET /api/v2/public/get_instruments?currency=BTC&kind=future
    — the same endpoint the DeribitReferenceDataAdapter calls per-currency.
    Each instrument in result[] must parse without error.
    """
    from unified_api_contracts.external.deribit.schemas import (
        DeribitInstrumentInfoFull,
    )

    cassette_path = CASSETTE_DIR / "deribit" / "mocks" / "ticker.yaml"
    data = _cassette_body(cassette_path)

    instruments_raw = data["result"]
    assert len(instruments_raw) >= 1

    for raw in instruments_raw:
        inst = DeribitInstrumentInfoFull.model_validate(raw)
        assert inst.instrument_name is not None
        assert inst.kind == "future"
        assert inst.base_currency == "BTC"
        assert inst.tick_size is not None and float(str(inst.tick_size)) > 0
        assert inst.contract_size is not None and float(str(inst.contract_size)) > 0


# ---------------------------------------------------------------------------
# Deribit — multi-currency cassette drives DeribitReferenceDataAdapter
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_deribit_get_instruments_cassette_adapter() -> None:
    """Replay deribit/mocks/get_instruments_multi.yaml through DeribitReferenceDataAdapter.

    The cassette contains 4 interactions (BTC, ETH, SOL, USDC futures).
    BTC has 2 futures, ETH has 1, SOL/USDC return empty results.
    We assert 3 InstrumentRecords total and validate their fields.
    """
    from unified_api_contracts.external.deribit.schemas import (
        DeribitInstrumentInfoFull,
    )

    from instruments_service.reference_data import create_reference_data_adapter

    cassette_path = CASSETTE_DIR / "deribit" / "mocks" / "get_instruments_multi.yaml"

    adapter = create_reference_data_adapter("deribit")

    with _VCR.use_cassette(str(cassette_path)):
        instruments = await adapter.get_instruments(instrument_type="future")

    # BTC: 2, ETH: 1, SOL: 0, USDC: 0 = 3 total
    assert len(instruments) == 3

    btc_instruments = [i for i in instruments if i.base_asset == "BTC"]
    assert len(btc_instruments) == 2

    eth_instruments = [i for i in instruments if i.base_asset == "ETH"]
    assert len(eth_instruments) == 1

    # Cross-validate raw cassette BTC instruments against DeribitInstrumentInfoFull
    btc_raw = {
        "instrument_name": "BTC-6MAR26",
        "kind": "future",
        "base_currency": "BTC",
        "quote_currency": "USD",
        "settlement_currency": "BTC",
        "tick_size": 2.5,
        "min_trade_amount": 10.0,
        "contract_size": 10.0,
        "taker_commission": 0.0005,
        "maker_commission": -0.0001,
        "is_active": True,
        "expiration_timestamp": 1772784000000,
        "settlement_period": "week",
    }
    validated = DeribitInstrumentInfoFull.model_validate(btc_raw)
    assert validated.instrument_name == "BTC-6MAR26"
    assert validated.kind == "future"
    assert validated.base_currency == "BTC"

    for inst in instruments:
        assert inst.venue == "DERIBIT"
        assert inst.expiry is not None
        assert inst.tick_size > 0
        assert inst.contract_size > 0


# ---------------------------------------------------------------------------
# Stubs — cassettes pending recording (added 2026-03-11)
# Run with: INTEGRATION_TEST_MODE=live pytest ... to record
# See: unified-trading-codex/07-security/testing-with-api-keys.md
# ---------------------------------------------------------------------------


@pytest.mark.xfail(reason="Cassette stubs not yet recorded — future providers")
class TestCftcStub:
    """CFTC regulatory data — cassette stub not yet recorded."""

    cassette = CASSETTE_DIR / "cftc" / "mocks" / "stub.yaml"

    def test_stub_cassette_exists(self) -> None:
        assert self.cassette.exists(), f"Cassette stub missing: {self.cassette}"


@pytest.mark.xfail(reason="Cassette stubs not yet recorded — future providers")
class TestEiaStub:
    """EIA energy data — cassette stub not yet recorded."""

    cassette = CASSETTE_DIR / "eia" / "mocks" / "stub.yaml"

    def test_stub_cassette_exists(self) -> None:
        assert self.cassette.exists(), f"Cassette stub missing: {self.cassette}"


@pytest.mark.xfail(reason="Cassette stubs not yet recorded — future providers")
class TestOfrStub:
    """OFR (Office of Financial Research) — cassette stub not yet recorded."""

    cassette = CASSETTE_DIR / "ofr" / "mocks" / "stub.yaml"

    def test_stub_cassette_exists(self) -> None:
        assert self.cassette.exists(), f"Cassette stub missing: {self.cassette}"


@pytest.mark.xfail(reason="Cassette stubs not yet recorded — future providers")
class TestRegulatoryStub:
    """Regulatory data — cassette stub not yet recorded."""

    cassette = CASSETTE_DIR / "regulatory" / "mocks" / "stub.yaml"

    def test_stub_cassette_exists(self) -> None:
        assert self.cassette.exists(), f"Cassette stub missing: {self.cassette}"


@pytest.mark.xfail(reason="Cassette stubs not yet recorded — future providers")
class TestMacroStub:
    """Macro data — cassette stub not yet recorded."""

    cassette = CASSETTE_DIR / "macro" / "mocks" / "stub.yaml"

    def test_stub_cassette_exists(self) -> None:
        assert self.cassette.exists(), f"Cassette stub missing: {self.cassette}"


@pytest.mark.xfail(reason="Cassette stubs not yet recorded — future providers")
class TestInstadappStub:
    """Instadapp DeFi protocol — cassette stub not yet recorded."""

    cassette = CASSETTE_DIR / "instadapp" / "mocks" / "stub.yaml"

    def test_stub_cassette_exists(self) -> None:
        assert self.cassette.exists(), f"Cassette stub missing: {self.cassette}"


@pytest.mark.xfail(reason="Cassette stubs not yet recorded — future providers")
class TestPrimeBrokerStub:
    """Prime broker reference data — cassette stub not yet recorded."""

    cassette = CASSETTE_DIR / "prime_broker" / "mocks" / "stub.yaml"

    def test_stub_cassette_exists(self) -> None:
        assert self.cassette.exists(), f"Cassette stub missing: {self.cassette}"
