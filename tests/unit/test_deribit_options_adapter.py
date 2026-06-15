"""Unit tests for the Deribit options-chain adapter (mocked — no live network).

Runs under the QG ``--block-network`` suite. The live connectivity proof is in
``scripts/deribit_options_connectivity_check.py`` (network, run by hand / a
``requires_network`` lane — NOT this suite).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from unittest.mock import AsyncMock, patch

import pytest
from unified_api_contracts._instrument_enums import OptionType
from unified_api_contracts.internal import InstrumentType

from instruments_service.reference_data.adapters.cefi.deribit_options_adapter import (
    DeribitOptionsReferenceDataAdapter,
    _parse_option,
)

# Two expiries → exercise nearest-expiry selection. 25APR26 is sooner than 26JUN26.
_EXP_NEAR = int(datetime(2026, 4, 25, 8, 0, tzinfo=UTC).timestamp() * 1000)
_EXP_FAR = int(datetime(2026, 6, 26, 8, 0, tzinfo=UTC).timestamp() * 1000)


def _instr(name: str, strike: float, opt: str, exp_ms: int) -> dict[str, object]:
    return {
        "instrument_name": name,
        "base_currency": "BTC",
        "settlement_currency": "BTC",
        "strike": strike,
        "option_type": opt,
        "expiration_timestamp": exp_ms,
    }


_INSTRUMENTS = [
    _instr("BTC-25APR26-90000-C", 90000, "call", _EXP_NEAR),
    _instr("BTC-25APR26-90000-P", 90000, "put", _EXP_NEAR),
    _instr("BTC-25APR26-100000-C", 100000, "call", _EXP_NEAR),
    _instr("BTC-26JUN26-120000-C", 120000, "call", _EXP_FAR),
]

# mark_iv is a PERCENT in the Deribit ticker payload (62.5 → 0.625 fractional).
_TICKERS = {
    "BTC-25APR26-90000-C": {"mark_iv": 62.5, "index_price": 95000.0},
    "BTC-25APR26-90000-P": {"mark_iv": 61.0, "index_price": 95000.0},
    "BTC-25APR26-100000-C": {"mark_iv": 58.25, "index_price": 95000.0},
}


class _FakeResp:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    async def __aenter__(self) -> _FakeResp:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def json(self) -> object:
        return self._payload


class _FakeSession:
    """Minimal aiohttp.ClientSession stand-in returning canned Deribit JSON."""

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(
        self, _t: type[BaseException] | None, _e: BaseException | None, _tb: TracebackType | None
    ) -> None:
        return None

    def get(self, url: str, params: dict[str, str] | None = None) -> _FakeResp:
        params = params or {}
        if url.endswith("/public/get_instruments"):
            return _FakeResp({"result": _INSTRUMENTS})
        if url.endswith("/public/ticker"):
            name = params["instrument_name"]
            return _FakeResp({"result": _TICKERS.get(name, {})})
        raise AssertionError(f"unexpected url {url}")


class TestParseOption:
    def test_parses_call_record(self) -> None:
        rec = _parse_option(_INSTRUMENTS[0])
        assert rec is not None
        assert rec.instrument_type == InstrumentType.OPTION
        assert rec.strike == Decimal("90000")
        assert rec.option_type == OptionType.CALL
        assert rec.expiry == datetime(2026, 4, 25, 8, 0, tzinfo=UTC)
        assert rec.instrument_key == "DERIBIT:OPTION:BTC-25APR26-90000-C"

    def test_malformed_returns_none(self) -> None:
        assert _parse_option({"instrument_name": ""}) is None
        assert _parse_option({"instrument_name": "X", "option_type": "call"}) is None  # no strike
        assert _parse_option("not-a-dict") is None


class TestOptionsChain:
    def test_venue(self) -> None:
        assert DeribitOptionsReferenceDataAdapter().venue == "DERIBIT"

    @pytest.mark.asyncio
    async def test_chain_nearest_expiry_strikes_and_mark_iv(self) -> None:
        adapter = DeribitOptionsReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=_FakeSession()):
            chain = await adapter.get_options_chain("BTC")
        # Nearest expiry chosen (25APR26, not 26JUN26).
        assert chain.expiry == datetime(2026, 4, 25, 8, 0, tzinfo=UTC)
        # Only the near-expiry legs (3), the far 120000 call excluded.
        assert chain.strikes == [Decimal("90000"), Decimal("100000")]
        assert len(chain.calls) == 2
        assert len(chain.puts) == 1
        # mark IV stored FRACTIONAL (62.5% → 0.625).
        assert chain.mark_iv_by_instrument["DERIBIT:OPTION:BTC-25APR26-90000-C"] == Decimal("0.625")
        assert chain.mark_iv_by_instrument["DERIBIT:OPTION:BTC-25APR26-100000-C"] == Decimal("0.5825")
        assert chain.underlying_mark == Decimal("95000.0")

    @pytest.mark.asyncio
    async def test_chain_explicit_expiry_filters(self) -> None:
        adapter = DeribitOptionsReferenceDataAdapter()
        far = datetime(2026, 6, 26, 8, 0, tzinfo=UTC)
        with patch.object(adapter, "_make_session", return_value=_FakeSession()):
            chain = await adapter.get_options_chain("BTC", expiry=far)
        assert chain.expiry == far
        assert chain.strikes == [Decimal("120000")]
        assert len(chain.calls) == 1 and len(chain.puts) == 0

    @pytest.mark.asyncio
    async def test_get_instruments_all_fail_raises(self) -> None:
        adapter = DeribitOptionsReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_FakeSession()),
            patch.object(
                adapter,
                "_fetch_options_for_currency",
                AsyncMock(side_effect=RuntimeError("boom")),
            ),
            pytest.raises(RuntimeError, match=r"all .* currenc"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_returns_records(self) -> None:
        adapter = DeribitOptionsReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=_FakeSession()):
            recs = await adapter.get_instruments()
        # 4 instruments per currency x 5 currencies (same canned list each).
        assert len(recs) == 4 * 5
        assert all(r.instrument_type == InstrumentType.OPTION for r in recs)
