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

from datetime import UTC, datetime
from types import TracebackType
from typing import cast
from unittest.mock import patch

import pytest
from unified_api_contracts import VenueMapping
from unified_api_contracts.internal import InstrumentType
from unified_api_contracts.internal.reference.instrument_validation import (
    validate_instrument_records,
)

from instruments_service.reference_data.adapters.cefi.coinbase_cde import (
    _CDE_REGISTRATION_DATE,
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


class TestCdeRegistrationDateMatchesUacRegistry:
    """Regression for `cefi_coinbase_cde_urdi_zero_records_2026_07_28.md`.

    `_CDE_REGISTRATION_DATE` used to be a hardcoded `datetime(2026, 7, 10)` that
    silently diverged from UAC's later-corrected `venue_start_dates["COINBASE-CDE"]`
    (2025-12-12, measured via real backward-paginated trade history) — a real
    backfill over any date in the 2025-12-12..2026-07-10 gap crashed with
    `RuntimeError: URDI returned zero records` because every fetched instrument's
    `available_from_datetime` was stamped with the stale, too-late floor.
    """

    def test_registration_date_derives_from_uac_venue_mapping(self) -> None:
        """The adapter constant is no longer a standalone hardcoded literal — it
        must equal UAC's own SSOT for this venue's instrument-discovery start,
        so the two can never independently drift again (mirrors the HYPERLIQUID
        fix, 2026-05-05)."""
        expected = datetime.fromisoformat(
            cast(str, VenueMapping().get_instrument_discovery_start("COINBASE-CDE"))
        ).replace(tzinfo=UTC)
        assert expected == _CDE_REGISTRATION_DATE

    def test_registration_date_is_not_the_stale_2026_07_10_floor(self) -> None:
        """Direct regression: the specific stale value must not silently come back."""
        assert datetime(2026, 7, 10, tzinfo=UTC) != _CDE_REGISTRATION_DATE

    @pytest.mark.asyncio
    async def test_fetched_instruments_carry_the_corrected_available_from_datetime(self) -> None:
        adapter = CoinbaseCdeReferenceDataAdapter()
        with patch.object(adapter, "_make_session", return_value=_FakeSession()):
            records = await adapter.get_instruments()

        assert records
        for rec in records:
            assert rec.available_from_datetime == _CDE_REGISTRATION_DATE
