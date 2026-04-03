"""VCR cassette tests for instrument definition schema validation.

Each test loads a pre-recorded cassette YAML and validates that:
  1. The raw reference data API response body parses without error.
  2. Instrument fields (tick_size, lot_size, expiry, base_asset, etc.) are
     populated and within expected ranges.

No live API credentials are required — cassettes contain pre-recorded
real-shaped responses.

Cassette layout: tests/cassettes/<venue>/<endpoint>.yaml
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

CASSETTE_DIR = Path(__file__).parent.parent / "cassettes"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_cassette_response_body(cassette_path: Path, interaction_index: int = 0) -> object:
    """Parse a VCR cassette YAML and return the response body as a Python object."""
    import yaml  # type: ignore[import-untyped]

    data: dict[str, object] = yaml.safe_load(cassette_path.read_text())
    interactions: list[dict[str, object]] = data["interactions"]  # type: ignore[index]
    assert len(interactions) > interaction_index, f"Cassette {cassette_path} has only {len(interactions)} interactions"
    body_str: str = interactions[interaction_index]["response"]["body"]["string"]  # type: ignore[index]
    return json.loads(body_str)


# ---------------------------------------------------------------------------
# Binance — GET /fapi/v1/exchangeInfo (perpetual futures instruments)
# ---------------------------------------------------------------------------


def test_binance_instrument_schema_fields() -> None:
    """Binance USD-M exchangeInfo response validates instrument definition schema fields.

    Cassette captures a real-shaped Binance perpetual futures exchange info response
    with BTCUSDT PERPETUAL instrument, PRICE_FILTER and LOT_SIZE filters.
    """
    from unified_api_contracts.external.binance.market_schemas import (
        BinanceExchangeInfo,
        BinanceSymbol,
    )

    cassette = CASSETTE_DIR / "binance" / "instrument_info.yaml"
    raw = _load_cassette_response_body(cassette)

    assert isinstance(raw, dict)
    exchange_info = BinanceExchangeInfo.model_validate(raw)

    assert len(exchange_info.symbols) >= 1

    btc_sym: BinanceSymbol = next(s for s in exchange_info.symbols if s.symbol == "BTCUSDT")
    assert btc_sym.status == "TRADING"
    assert btc_sym.baseAsset == "BTC"
    assert btc_sym.quoteAsset == "USDT"


def test_binance_instrument_price_filter_fields() -> None:
    """Binance exchangeInfo PRICE_FILTER provides tick_size and valid price bounds."""
    cassette = CASSETTE_DIR / "binance" / "instrument_info.yaml"
    raw = _load_cassette_response_body(cassette)

    assert isinstance(raw, dict)
    symbols: list[dict[str, object]] = raw["symbols"]  # type: ignore[index]
    assert len(symbols) >= 1

    btc = symbols[0]
    assert btc["symbol"] == "BTCUSDT"
    assert btc["baseAsset"] == "BTC"
    assert btc["quoteAsset"] == "USDT"

    filters: list[dict[str, object]] = btc["filters"]  # type: ignore[index]
    price_filter = next(f for f in filters if f["filterType"] == "PRICE_FILTER")
    lot_size = next(f for f in filters if f["filterType"] == "LOT_SIZE")

    tick_size = float(str(price_filter["tickSize"]))
    step_size = float(str(lot_size["stepSize"]))
    assert tick_size > 0, "tickSize must be positive"
    assert step_size > 0, "stepSize must be positive"

    min_qty = float(str(lot_size["minQty"]))
    max_qty = float(str(lot_size["maxQty"]))
    assert 0 < min_qty < max_qty, "minQty must be less than maxQty"


def test_binance_instrument_schema_roundtrip_serialization() -> None:
    """Binance instrument cassette body survives a JSON serialize/deserialize roundtrip."""
    cassette = CASSETTE_DIR / "binance" / "instrument_info.yaml"
    raw = _load_cassette_response_body(cassette)

    serialized = json.dumps(raw)
    deserialized = json.loads(serialized)
    assert deserialized == raw

    symbols = deserialized["symbols"]
    assert symbols[0]["symbol"] == "BTCUSDT"


# ---------------------------------------------------------------------------
# Deribit — GET /api/v2/public/get_instruments (BTC futures)
# ---------------------------------------------------------------------------


def test_deribit_instrument_schema_fields() -> None:
    """Deribit get_instruments response validates DeribitInstrumentInfoFull schema fields.

    Cassette captures 2 BTC weekly future instruments (BTC-6MAR26 and BTC-13MAR26).
    """
    from unified_api_contracts.external.deribit.schemas import (
        DeribitInstrumentInfoFull,
    )

    cassette = CASSETTE_DIR / "deribit" / "instruments.yaml"
    raw = _load_cassette_response_body(cassette)

    assert isinstance(raw, dict)
    assert raw["jsonrpc"] == "2.0"

    instruments_raw: list[dict[str, object]] = raw["result"]  # type: ignore[index]
    assert len(instruments_raw) == 2

    instruments = [DeribitInstrumentInfoFull.model_validate(item) for item in instruments_raw]

    btc_mar6 = instruments[0]
    assert btc_mar6.instrument_name == "BTC-6MAR26"
    assert btc_mar6.kind == "future"
    assert btc_mar6.base_currency == "BTC"
    assert btc_mar6.is_active is True
    assert btc_mar6.tick_size is not None
    assert float(str(btc_mar6.tick_size)) == 2.5
    assert btc_mar6.contract_size is not None
    assert float(str(btc_mar6.contract_size)) == 10.0
    assert btc_mar6.expiration_timestamp is not None
    assert int(str(btc_mar6.expiration_timestamp)) > 1_700_000_000_000


def test_deribit_instrument_all_fields_positive() -> None:
    """All Deribit instruments in cassette have positive tick_size and contract_size."""
    from unified_api_contracts.external.deribit.schemas import (
        DeribitInstrumentInfoFull,
    )

    cassette = CASSETTE_DIR / "deribit" / "instruments.yaml"
    raw = _load_cassette_response_body(cassette)

    assert isinstance(raw, dict)
    instruments_raw: list[dict[str, object]] = raw["result"]  # type: ignore[index]

    for item in instruments_raw:
        inst = DeribitInstrumentInfoFull.model_validate(item)
        assert inst.instrument_name is not None
        assert inst.kind == "future"
        assert inst.base_currency == "BTC"
        assert inst.tick_size is not None and float(str(inst.tick_size)) > 0
        assert inst.contract_size is not None and float(str(inst.contract_size)) > 0
        assert inst.taker_commission is not None and float(str(inst.taker_commission)) >= 0


def test_deribit_instrument_schema_roundtrip_serialization() -> None:
    """Deribit instrument cassette body survives a JSON serialize/deserialize roundtrip."""
    cassette = CASSETTE_DIR / "deribit" / "instruments.yaml"
    raw = _load_cassette_response_body(cassette)

    serialized = json.dumps(raw)
    deserialized = json.loads(serialized)
    assert deserialized == raw

    result = deserialized["result"]
    assert result[0]["instrument_name"] == "BTC-6MAR26"
    assert result[1]["instrument_name"] == "BTC-13MAR26"
