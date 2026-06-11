"""Coverage boost for deribit_combo_adapter.py uncovered branches.

Targets:
- _parse_combo_legs: line 115 (< 3 parts → [])
- _classify_deribit_error: lines 127 (429), 129 (503), 132 (UNKNOWN)
- get_instruments: line 188 (UnsupportedCapabilityError re-raise)
- get_instruments: lines 196-212 (generic Exception handler)
- _parse_combo_instrument: line 344 (empty underlying → None)
- get_instrument: line 382 (symbol found → return inst)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

# ---------------------------------------------------------------------------
# Adapter factory
# ---------------------------------------------------------------------------


def _make_adapter() -> object:
    from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
        DeribitComboReferenceDataAdapter,
    )

    return DeribitComboReferenceDataAdapter()


# ---------------------------------------------------------------------------
# _parse_combo_legs
# ---------------------------------------------------------------------------


class TestParseComboLegs:
    """Line 115: < 3 parts → return []."""

    def test_short_name_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import _parse_combo_legs

        assert _parse_combo_legs("BTC") == []
        assert _parse_combo_legs("BTC-STRD") == []

    def test_valid_name_returns_empty_too(self) -> None:
        """Current implementation always returns [] (legs resolved downstream)."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import _parse_combo_legs

        result = _parse_combo_legs("BTC-STRD-25APR26-90000")
        assert result == []


# ---------------------------------------------------------------------------
# _classify_deribit_error
# ---------------------------------------------------------------------------


class TestClassifyDeribitError:
    """Lines 127, 129, 132: error code classification."""

    def test_429_by_status(self) -> None:
        """Line 127: status == 429 → '429'."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import _classify_deribit_error

        assert _classify_deribit_error(Exception("any"), status=429) == "429"

    def test_429_by_message(self) -> None:
        """Line 127: 'rate' in message → '429'."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import _classify_deribit_error

        assert _classify_deribit_error(Exception("rate limit exceeded")) == "429"

    def test_503_by_status(self) -> None:
        """Line 129: status == 503 → '503'."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import _classify_deribit_error

        assert _classify_deribit_error(Exception("any"), status=503) == "503"

    def test_503_by_message(self) -> None:
        """Line 129: 'unavailable' in message → '503'."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import _classify_deribit_error

        assert _classify_deribit_error(Exception("service unavailable")) == "503"

    def test_500_by_status(self) -> None:
        """Line 130-131: status >= 500 → '500'."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import _classify_deribit_error

        assert _classify_deribit_error(Exception("any"), status=500) == "500"
        assert _classify_deribit_error(Exception("any"), status=502) == "500"

    def test_unknown(self) -> None:
        """Line 132: none of the above → 'UNKNOWN'."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import _classify_deribit_error

        assert _classify_deribit_error(ValueError("something else")) == "UNKNOWN"


# ---------------------------------------------------------------------------
# get_instruments — exception handling
# ---------------------------------------------------------------------------


class TestGetInstrumentsExceptions:
    """Lines 188 + 196-212: exception isolation."""

    @pytest.mark.asyncio
    async def test_unsupported_capability_error_re_raised(self) -> None:
        """Line 188: UnsupportedCapabilityError is re-raised, not swallowed."""
        from unified_api_contracts import UnsupportedCapabilityError

        adapter = _make_adapter()

        with (
            patch.object(
                adapter,
                "_fetch_combos_for_currency",
                new_callable=AsyncMock,
                side_effect=UnsupportedCapabilityError(
                    venue="DERIBIT",
                    capability="combo",
                    message="unsupported",
                ),
            ),
            pytest.raises(UnsupportedCapabilityError),
        ):
            await adapter.get_instruments()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_generic_exception_tracked_per_currency(self) -> None:
        """Lines 196-212: generic Exception in one currency → continues, logged, tracked."""
        adapter = _make_adapter()

        # First currency raises a generic Exception; second returns results
        mock_record = MagicMock()

        call_count = 0

        async def fetch_side_effect(currency: str, now: object) -> list:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("parse error")
            return [mock_record]

        with (
            patch.object(adapter, "_fetch_combos_for_currency", side_effect=fetch_side_effect),
            patch(
                "instruments_service.reference_data.adapters.cefi.deribit_combo_adapter.classify_venue_error",
                return_value=None,
            ),
            patch("instruments_service.reference_data.adapters.cefi.deribit_combo_adapter.log_event"),
        ):
            result = await adapter.get_instruments()  # type: ignore[attr-defined]

        # All currencies ran; one failure, rest succeeded
        assert call_count > 1
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_all_currencies_fail_raises_runtime_error(self) -> None:
        """Lines 232-237: all currencies failed → RuntimeError raised."""
        adapter = _make_adapter()

        with (
            patch.object(
                adapter,
                "_fetch_combos_for_currency",
                new_callable=AsyncMock,
                side_effect=RuntimeError("fetch failed"),
            ),
            pytest.raises(RuntimeError),
        ):
            await adapter.get_instruments()  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# _parse_combo_instrument — empty underlying
# ---------------------------------------------------------------------------


class TestParseComboInstrument:
    """Line 344: instrument_name split gives empty first part → None."""

    def test_instrument_name_with_empty_underlying_returns_none(self) -> None:
        """instrument_name = '-STRD-25APR26' → name_parts[0] = '' → return None."""
        from datetime import UTC, datetime

        adapter = _make_adapter()

        # Instrument name that starts with '-' so name_parts[0] == ""
        item = {
            "instrument_name": "-STRD-25APR26-90000",
            "creation_timestamp": 1700000000000,
            "settlement_currency": "USD",
        }
        result = adapter._parse_combo_instrument(item, datetime.now(UTC))  # type: ignore[attr-defined]
        assert result is None

    def test_non_dict_item_returns_none(self) -> None:
        """Line 332-333: item is not a dict → None."""
        from datetime import UTC, datetime

        adapter = _make_adapter()
        result = adapter._parse_combo_instrument("not_a_dict", datetime.now(UTC))  # type: ignore[attr-defined]
        assert result is None

    def test_empty_instrument_name_returns_none(self) -> None:
        """Line 336-337: instrument_name is empty string → None."""
        from datetime import UTC, datetime

        adapter = _make_adapter()
        result = adapter._parse_combo_instrument({"instrument_name": ""}, datetime.now(UTC))  # type: ignore[attr-defined]
        assert result is None


# ---------------------------------------------------------------------------
# get_instrument — symbol found
# ---------------------------------------------------------------------------


class TestGetInstrumentFound:
    """Line 382: get_instrument returns inst when symbol matches."""

    @pytest.mark.asyncio
    async def test_get_instrument_found_by_raw_symbol(self) -> None:
        """Line 382: inst.raw_symbol == symbol → return inst."""
        from unified_api_contracts.internal import InstrumentRecord

        adapter = _make_adapter()

        record = MagicMock(spec=InstrumentRecord)
        record.raw_symbol = "BTC-STRD-25APR26-90000"
        record.instrument_key = "DERIBIT:COMBO:BTC-STRD-25APR26-90000"

        with patch.object(
            adapter,
            "get_instruments",
            new_callable=AsyncMock,
            return_value=[record],
        ):
            result = await adapter.get_instrument("BTC-STRD-25APR26-90000")  # type: ignore[attr-defined]

        assert result is record

    @pytest.mark.asyncio
    async def test_get_instrument_not_found_returns_none(self) -> None:
        """Loop exhausted → return None (line 384)."""
        adapter = _make_adapter()

        with patch.object(
            adapter,
            "get_instruments",
            new_callable=AsyncMock,
            return_value=[],
        ):
            result = await adapter.get_instrument("NOTEXIST")  # type: ignore[attr-defined]

        assert result is None
