"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.cefi.hyperliquid import HyperliquidReferenceDataAdapter


class TestHyperliquidAdapter:
    def test_venue_name(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        assert adapter.venue == "HYPERLIQUID"

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_instruments_non_perp_returns_empty(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        with patch("aiohttp.ClientSession"):
            result = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_instrument_key_is_canonical(self) -> None:
        """instrument_key is VENUE:PERPETUAL:BASE-QUOTE via the shared builder.

        2026-07-09 DRY retrofit (canonical_id_builder_retrofit_checklist_2026_07_08.md
        todo 4) routed the ad hoc f-string through
        ``unified_api_contracts.internal.reference.canonical_id_builder.build_instrument_id``
        — this asserts the real adapter output is byte-identical to the prior
        f-string construction (no behavior change expected). The per-coin funding-date
        probe is patched out directly (bounded-concurrent HTTP fan-out, orthogonal to
        instrument_key construction) so the test stays fast and deterministic.
        """
        adapter = HyperliquidReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"universe": [{"name": "BTC", "szDecimals": 5}]})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_probe_earliest_funding_dates", AsyncMock(return_value={})),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].instrument_key == "HYPERLIQUID:PERPETUAL:BTC-USD"

    def test_parse_hl_candle_uses_close_edge_close_field(self) -> None:
        """_parse_hl_candle must stamp the T (close-time ms) field, not t (open-time ms).

        bar_edge fix 2026-06-08: stamping open-edge caused look-ahead leakage in
        feature computation. T is the Hyperliquid candleSnapshot close-time field.
        """
        adapter = HyperliquidReferenceDataAdapter()
        # 1d bar: open at 00:00 UTC, close at 24:00 (next-day 00:00) UTC.
        open_ms = int(datetime(2026, 6, 7, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        close_ms = int(datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        candle = {
            "t": open_ms,
            "T": close_ms,
            "o": "50000",
            "h": "51000",
            "l": "49000",
            "c": "50500",
            "v": "1000",
        }
        result = adapter._parse_hl_candle(candle, "BTC", "1d")
        assert result is not None
        expected_close_ts = datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC)
        assert result.timestamp == expected_close_ts, (
            f"Expected close-edge {expected_close_ts!r}, got {result.timestamp!r}. "
            "Hyperliquid T=close-time must be used, not t=open-time."
        )

    def test_parse_hl_candle_close_field_preferred_over_open_field(self) -> None:
        """When both T and t are present, T (close) must win over t (open)."""
        adapter = HyperliquidReferenceDataAdapter()
        open_ms = int(datetime(2026, 6, 7, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        close_ms = int(datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        candle = {"t": open_ms, "T": close_ms, "o": "1", "h": "2", "l": "0", "c": "1", "v": "10"}
        result = adapter._parse_hl_candle(candle, "BTC", "1d")
        assert result is not None
        assert result.timestamp == datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC)

    def test_parse_hl_candle_open_fields_preserved(self) -> None:
        """OHLCV values must still be read from o/h/l/c/v, not from T."""
        adapter = HyperliquidReferenceDataAdapter()
        close_ms = int(datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        candle = {"T": close_ms, "o": "100", "h": "200", "l": "50", "c": "150", "v": "42"}
        result = adapter._parse_hl_candle(candle, "ETH", "1h")
        assert result is not None
        assert result.open == Decimal("100")
        assert result.high == Decimal("200")
        assert result.low == Decimal("50")
        assert result.close == Decimal("150")
        assert result.volume == Decimal("42")
