"""Unit tests for the COINBASE-CDE reference data adapter (mocked — no live network).

Regression for a real 2026-07-13 finding (`pipeline_e2e_check` re-sweep triage): the
adapter fetched real, valid Coinbase Derivatives Exchange FUTURE products but built
every ``InstrumentRecord`` WITHOUT ``underlying`` set — ``validate_instrument_records``
requires ``underlying`` for CeFi FUTURE/OPTION instruments, so ALL 99 real live CDE
contracts were silently rejected at validation, and zero rows ever reached the
catalogue/manifest regardless of fetch success (confirmed live via a real
``instr-backfill-cefi-pchk-*-coinbase-cde`` VM run.log: ``COINBASE-CDE: fetched 99
FUTURE instruments`` followed immediately by 99 ``underlying is required for CEFI
derivatives`` rejections and ``Batch complete: 0 results collected``).
"""

from __future__ import annotations

from types import TracebackType
from unittest.mock import patch

import pytest
from unified_api_contracts.internal import InstrumentType
from unified_api_contracts.internal.reference.instrument_validation import (
    validate_instrument_records,
)

from instruments_service.reference_data.adapters.cefi.coinbase_cde import (
    CoinbaseCdeReferenceDataAdapter,
)

_PRODUCTS = {
    "products": [
        {
            "product_id": "BIT-31JUL26-CDE",
            "quote_currency_id": "USD",
            "price_increment": "0.01",
            "base_increment": "1",
            "is_disabled": False,
            "trading_disabled": False,
            "future_product_details": {
                "venue": "cde",
                "contract_root_unit": "BIT",
                "contract_expiry": "2026-07-31T08:00:00Z",
                "contract_size": "1",
            },
        },
        {
            "product_id": "BIP-20DEC30-CDE",
            "quote_currency_id": "USD",
            "price_increment": "0.01",
            "base_increment": "1",
            "is_disabled": False,
            "trading_disabled": False,
            "future_product_details": {
                "venue": "cde",
                "contract_root_unit": "BIP",
                "contract_expiry": "2030-12-20T08:00:00Z",
                "contract_size": "1",
                "funding_rate": "0.0001",
            },
        },
    ]
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
    """Minimal aiohttp.ClientSession stand-in returning canned CDE products JSON."""

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(
        self, _t: type[BaseException] | None, _e: BaseException | None, _tb: TracebackType | None
    ) -> None:
        return None

    def get(self, url: str, params: dict[str, str] | None = None) -> _FakeResp:
        if url.endswith("/api/v3/brokerage/market/products"):
            return _FakeResp(_PRODUCTS)
        raise AssertionError(f"unexpected url {url}")


class TestCoinbaseCdeGetInstruments:
    @pytest.mark.asyncio
    async def test_underlying_is_populated_from_contract_root_unit(self) -> None:
        """Regression: pre-fix, ``underlying`` was never set (defaulted to None)."""
        adapter = CoinbaseCdeReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=_FakeSession()):
            records = await adapter.get_instruments()

        assert len(records) == 2
        by_symbol = {r.raw_symbol: r for r in records}
        assert by_symbol["BIT-31JUL26-CDE"].underlying == "BIT"
        assert by_symbol["BIP-20DEC30-CDE"].underlying == "BIP"
        # underlying must match base_asset — same convention as ccxt_adapter.py's
        # `underlying = base if is_derivative else None`.
        for rec in records:
            assert rec.underlying == rec.base_asset
            assert rec.instrument_type == InstrumentType.FUTURE

    @pytest.mark.asyncio
    async def test_records_survive_instrument_validation(self) -> None:
        """End-to-end regression: every real CDE FUTURE record must pass
        ``validate_instrument_records`` (the exact gate that silently dropped all
        99 real instruments pre-fix — CF-11-adjacent silent-universe-shrink class).
        """
        adapter = CoinbaseCdeReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=_FakeSession()):
            records = await adapter.get_instruments()

        valid, rejected = validate_instrument_records(records)
        assert rejected == []
        assert len(valid) == 2
