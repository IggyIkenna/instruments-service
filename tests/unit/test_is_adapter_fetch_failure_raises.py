"""Regression tests for CF-11 fetch-error-must-raise fix across 5 IS adapters.

Before the fix each adapter caught aiohttp.ClientError and returned [] — the
caller (_fetch_one in urdi_reference_provider) read that as a clean empty
universe (A8 false-complete / silent universe shrink). After the fix every
genuine fetch error RE-RAISES so the caller routes the venue to failed[]
(→ attempted_failed in the manifest).

Coverage:
  - hyperliquid: GET /info POST ClientError → raises
  - aster: GET /fapi/v1/exchangeInfo ClientError → raises
  - tardis: single exchange all-retries-fail → raises; partial success (one
    exchange OK, one fails) → returns results (isolation preserved)
  - betfair: listMarketCatalogue POST ClientError → raises
  - lighter: /orderBookDetails _get_with_retry ClientError → raises
  - genuine-empty sanity: aster with non-PERPETUAL type → returns [] cleanly
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

# ── session mock helpers reused from existing test style ─────────────────────


def _make_session_post_raises(exc: Exception) -> MagicMock:
    """Session mock whose .post() raises *exc* (used by hyperliquid / betfair)."""
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(side_effect=exc)
    mock_cm.__aexit__ = AsyncMock(return_value=None)
    mock_session_obj = MagicMock()
    mock_session_obj.post = MagicMock(return_value=mock_cm)
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


def _make_session_get_raises(exc: Exception) -> MagicMock:
    """Session mock whose .get() raises *exc* (used by aster)."""
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(side_effect=exc)
    mock_cm.__aexit__ = AsyncMock(return_value=None)
    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=mock_cm)
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


# ── Hyperliquid ───────────────────────────────────────────────────────────────


class TestHyperliquidFetchFailureRaises:
    """CF-11 regression: ClientError on /info POST must raise, not return []."""

    @pytest.mark.asyncio
    async def test_get_instruments_raises_on_client_error(self) -> None:
        """aiohttp.ClientError on the /info POST must surface as RuntimeError."""
        from instruments_service.reference_data.adapters.cefi.hyperliquid import (
            HyperliquidReferenceDataAdapter,
        )

        adapter = HyperliquidReferenceDataAdapter()
        err = aiohttp.ClientError("connection refused")
        mock_session_cm = _make_session_post_raises(err)

        with (
            patch(
                "instruments_service.reference_data.adapters.cefi.hyperliquid.HyperliquidReferenceDataAdapter._make_session",
                return_value=mock_session_cm,
            ),
            patch("instruments_service.reference_data.adapters.cefi.hyperliquid.log_event"),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_does_not_return_empty_on_client_error(self) -> None:
        """Ensure the return path is not [] — the pre-fix swallow behaviour."""
        from instruments_service.reference_data.adapters.cefi.hyperliquid import (
            HyperliquidReferenceDataAdapter,
        )

        adapter = HyperliquidReferenceDataAdapter()
        err = aiohttp.ClientError("timeout")
        mock_session_cm = _make_session_post_raises(err)

        raised = False
        try:
            with (
                patch(
                    "instruments_service.reference_data.adapters.cefi.hyperliquid.HyperliquidReferenceDataAdapter._make_session",
                    return_value=mock_session_cm,
                ),
                patch("instruments_service.reference_data.adapters.cefi.hyperliquid.log_event"),
            ):
                result = await adapter.get_instruments()
            # if we reach here the adapter swallowed the error — pre-fix regression
            assert result != [], "adapter must raise, not return [] on fetch error (CF-11)"
        except (RuntimeError, aiohttp.ClientError):
            raised = True
        assert raised, "adapter must raise on ClientError (CF-11 regression)"


# ── Aster ─────────────────────────────────────────────────────────────────────


class TestAsterFetchFailureRaises:
    """CF-11 regression: ClientError on exchangeInfo GET must raise, not return []."""

    @pytest.mark.asyncio
    async def test_get_instruments_raises_on_client_error(self) -> None:
        """aiohttp.ClientError on exchangeInfo GET → RuntimeError raised."""
        from instruments_service.reference_data.adapters.cefi.aster import (
            AsterReferenceDataAdapter,
        )

        adapter = AsterReferenceDataAdapter()
        err = aiohttp.ClientError("connection reset")
        mock_session_cm = _make_session_get_raises(err)

        with (
            patch(
                "instruments_service.reference_data.adapters.cefi.aster.AsterReferenceDataAdapter._make_session",
                return_value=mock_session_cm,
            ),
            patch("instruments_service.reference_data.adapters.cefi.aster.log_event"),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_non_perpetual_returns_empty_cleanly(self) -> None:
        """Genuine-empty path: non-PERPETUAL type filter returns [] without raising."""
        from instruments_service.reference_data.adapters.cefi.aster import (
            AsterReferenceDataAdapter,
        )

        adapter = AsterReferenceDataAdapter()
        # SPOT_PAIR is not supported → returns [] before any HTTP call
        result = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert result == [], "non-PERPETUAL filter must return [] (no raise)"


# ── Tardis ────────────────────────────────────────────────────────────────────


class TestTardisFetchFailureRaises:
    """CF-11 regressions for Tardis — single-exchange failure and partial-success isolation."""

    @pytest.mark.asyncio
    async def test_single_exchange_all_retries_fail_raises(self) -> None:
        """When the only configured exchange fails _fetch_exchange_instruments, get_instruments raises."""
        from instruments_service.reference_data.adapters.cefi.tardis import (
            TardisReferenceDataAdapter,
        )

        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        with (
            patch.object(
                adapter,
                "_fetch_exchange_instruments",
                side_effect=RuntimeError("Tardis deribit: all 2 attempts failed"),
            ),
            pytest.raises(RuntimeError, match="all requested exchange"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_partial_success_one_ok_one_fail_returns_results(self) -> None:
        """Two exchanges: one succeeds, one fails → returns successful results (no raise).

        Isolation rule: a per-exchange failure must NOT abort sibling exchanges.
        The overall raise only fires when ALL exchanges failed (results empty + failures non-empty).
        """
        from decimal import Decimal

        from unified_api_contracts.internal import InstrumentRecord, InstrumentType, MarginType

        from instruments_service.reference_data.adapters.cefi.tardis import (
            TardisReferenceDataAdapter,
        )

        adapter = TardisReferenceDataAdapter(exchanges=["deribit", "binance-futures"])

        ok_record = InstrumentRecord(
            instrument_key="tardis:PERPETUAL:BTC-PERPETUAL",
            venue="tardis",
            raw_symbol="BTC-PERPETUAL",
            instrument_type=InstrumentType.PERPETUAL,
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.5"),
            min_size=Decimal("1"),
            contract_size=Decimal("10"),
        )

        call_count = 0

        async def _side_effect(session: object, api_key: object, exchange: str) -> list[InstrumentRecord]:
            nonlocal call_count
            call_count += 1
            if exchange == "deribit":
                return [ok_record]
            raise RuntimeError(f"Tardis {exchange!r}: simulated failure")

        with patch.object(adapter, "_fetch_exchange_instruments", side_effect=_side_effect):
            result = await adapter.get_instruments()

        assert len(result) == 1
        assert result[0].raw_symbol == "BTC-PERPETUAL"
        assert call_count == 2  # both exchanges attempted

    @pytest.mark.asyncio
    async def test_both_exchanges_fail_raises(self) -> None:
        """Both exchanges fail → RuntimeError with 'all requested exchange(s) failed'."""
        from instruments_service.reference_data.adapters.cefi.tardis import (
            TardisReferenceDataAdapter,
        )

        adapter = TardisReferenceDataAdapter(exchanges=["deribit", "binance-futures"])

        with (
            patch.object(
                adapter,
                "_fetch_exchange_instruments",
                side_effect=RuntimeError("fetch failed"),
            ),
            pytest.raises(RuntimeError, match="all requested exchange"),
        ):
            await adapter.get_instruments()


# ── Betfair ───────────────────────────────────────────────────────────────────


class TestBetfairFetchFailureRaises:
    """CF-11 regression: ClientError on listMarketCatalogue POST must raise, not return []."""

    @pytest.mark.asyncio
    async def test_get_instruments_raises_on_client_error(self) -> None:
        """aiohttp.ClientError on listMarketCatalogue → RuntimeError propagates out."""
        from instruments_service.reference_data.adapters.sports.adapters.betfair import (
            BetfairReferenceDataAdapter,
        )

        adapter = BetfairReferenceDataAdapter(session_token="tok", app_key="key")
        err = aiohttp.ClientError("network error")
        mock_session_cm = _make_session_post_raises(err)

        with (
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.betfair.BetfairReferenceDataAdapter._make_session",
                return_value=mock_session_cm,
            ),
            patch("instruments_service.reference_data.adapters.sports.adapters.betfair.log_event"),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_fetch_markets_raw_raises_on_client_error(self) -> None:
        """_fetch_markets_raw raises RuntimeError (not returns []) on ClientError."""
        from instruments_service.reference_data.adapters.sports.adapters.betfair import (
            BetfairReferenceDataAdapter,
        )

        adapter = BetfairReferenceDataAdapter(session_token="tok", app_key="key")
        err = aiohttp.ClientError("timeout")
        mock_session_cm = _make_session_post_raises(err)

        with (
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.betfair.BetfairReferenceDataAdapter._make_session",
                return_value=mock_session_cm,
            ),
            patch("instruments_service.reference_data.adapters.sports.adapters.betfair.log_event"),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter._fetch_markets_raw("tok", "key")


# ── Lighter ───────────────────────────────────────────────────────────────────


class TestLighterFetchFailureRaises:
    """CF-11 regression: ClientError on /orderBookDetails must raise, not return []."""

    @pytest.mark.asyncio
    async def test_get_instruments_raises_on_client_error(self) -> None:
        """aiohttp.ClientError from _get_with_retry → RuntimeError raised."""
        from instruments_service.reference_data.adapters.defi.lighter import (
            LighterReferenceDataAdapter,
        )

        adapter = LighterReferenceDataAdapter()
        err = aiohttp.ClientError("connection refused")

        with (
            patch.object(
                adapter,
                "_get_with_retry",
                side_effect=err,
            ),
            patch("instruments_service.reference_data.adapters.defi.lighter.log_event"),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            # _get_with_retry is called inside an open _make_session context —
            # mock _make_session to yield a dummy session object
            mock_session_obj = MagicMock()
            mock_session_cm = MagicMock()
            mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
            mock_session_cm.__aexit__ = AsyncMock(return_value=None)
            with patch.object(adapter, "_make_session", return_value=mock_session_cm):
                await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_raises_on_retry_exhausted_runtime_error(self) -> None:
        """RuntimeError from _get_with_retry (retry-exhausted) also propagates out."""
        from instruments_service.reference_data.adapters.defi.lighter import (
            LighterReferenceDataAdapter,
        )

        adapter = LighterReferenceDataAdapter()
        retry_err = RuntimeError("Lighter: all retries exhausted")

        mock_session_obj = MagicMock()
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(adapter, "_make_session", return_value=mock_session_cm),
            patch.object(adapter, "_get_with_retry", side_effect=retry_err),
            patch("instruments_service.reference_data.adapters.defi.lighter.log_event"),
            pytest.raises(RuntimeError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_non_perpetual_returns_empty_cleanly(self) -> None:
        """Lighter: non-PERPETUAL instrument_type → returns [] without any HTTP call."""
        from instruments_service.reference_data.adapters.defi.lighter import (
            LighterReferenceDataAdapter,
        )

        adapter = LighterReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert result == [], "non-PERPETUAL must return [] without raising"
