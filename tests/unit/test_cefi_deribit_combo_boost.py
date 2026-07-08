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
# _build_legs (structured legs from Deribit get_combos)
# ---------------------------------------------------------------------------


class TestBuildLegs:
    """DeribitComboReferenceDataAdapter._build_legs maps get_combos legs → InstrumentLeg."""

    def test_non_list_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        assert DeribitComboReferenceDataAdapter._build_legs(None) == []
        assert DeribitComboReferenceDataAdapter._build_legs("not_a_list") == []

    def test_signed_amount_maps_to_side_and_ratio(self) -> None:
        """Leg instrument_key carries the full VENUE:TYPE:SYMBOL grammar (real Deribit
        option leg names, verified against live get_combos, 2026-07-09) — the :TYPE:
        segment (here OPTION) was the bug this todo fixes (previously missing entirely)."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        legs = DeribitComboReferenceDataAdapter._build_legs(
            [
                {"amount": -1, "instrument_name": "BTC-19JUN26-69000-C"},
                {"amount": 2, "instrument_name": "BTC-26JUN26-69000-C"},
            ]
        )
        assert len(legs) == 2
        assert legs[0].instrument_key == "DERIBIT:OPTION:BTC-19JUN26-69000-C"
        assert legs[0].side == "SELL"
        assert legs[0].ratio == 1
        assert legs[1].instrument_key == "DERIBIT:OPTION:BTC-26JUN26-69000-C"
        assert legs[1].side == "BUY"
        assert legs[1].ratio == 2

    def test_zero_amount_and_missing_name_skipped(self) -> None:
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        legs = DeribitComboReferenceDataAdapter._build_legs(
            [
                {"amount": 0, "instrument_name": "BTC-X"},  # zero amount → skip
                {"amount": 1, "instrument_name": ""},  # empty name → skip
                "not_a_dict",  # non-dict → skip
                {"amount": 1, "instrument_name": "BTC-26JUN26-69000-C"},  # kept
            ]
        )
        assert len(legs) == 1
        assert legs[0].instrument_key == "DERIBIT:OPTION:BTC-26JUN26-69000-C"

    def test_perpetual_and_future_legs_classified(self) -> None:
        """Real Deribit futures-spread (FS) combo legs — verified against live
        get_combos (BTC-FS-10JUL26_9JUL26 → 2 dated-future legs; BTC-FS-10JUL26_PERP
        → 1 perpetual + 1 dated-future leg), 2026-07-09."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        legs = DeribitComboReferenceDataAdapter._build_legs(
            [
                {"amount": -1, "instrument_name": "BTC-PERPETUAL"},
                {"amount": 1, "instrument_name": "BTC-10JUL26"},
            ]
        )
        assert len(legs) == 2
        assert legs[0].instrument_key == "DERIBIT:PERPETUAL:BTC-PERPETUAL"
        assert legs[0].side == "SELL"
        assert legs[1].instrument_key == "DERIBIT:FUTURE:BTC-10JUL26"
        assert legs[1].side == "BUY"

    def test_unclassifiable_leg_shape_dropped(self) -> None:
        """A leg name that matches none of the known real Deribit shapes (2-part
        dated/PERPETUAL, 4-part option) is dropped, not raised — same
        degrade-gracefully convention as a missing/empty instrument_name."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        legs = DeribitComboReferenceDataAdapter._build_legs(
            [
                {"amount": 1, "instrument_name": "BTC-WEIRD-SHAPE-EXTRA-PART"},  # 5 parts → drop
                {"amount": 1, "instrument_name": "BTC"},  # 1 part → drop
                {"amount": 1, "instrument_name": "BTC-10JUL26"},  # kept
            ]
        )
        assert len(legs) == 1
        assert legs[0].instrument_key == "DERIBIT:FUTURE:BTC-10JUL26"


class TestClassifyDeribitLegInstrumentType:
    """_classify_deribit_leg_instrument_type — real Deribit instrument_name shapes."""

    def test_perpetual_suffix(self) -> None:
        from unified_api_contracts.internal import InstrumentType

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            _classify_deribit_leg_instrument_type,
        )

        assert _classify_deribit_leg_instrument_type("BTC-PERPETUAL") == InstrumentType.PERPETUAL
        assert _classify_deribit_leg_instrument_type("ETH-PERPETUAL") == InstrumentType.PERPETUAL

    def test_dated_future_two_part(self) -> None:
        from unified_api_contracts.internal import InstrumentType

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            _classify_deribit_leg_instrument_type,
        )

        assert _classify_deribit_leg_instrument_type("BTC-10JUL26") == InstrumentType.FUTURE
        assert _classify_deribit_leg_instrument_type("ETH-25DEC26") == InstrumentType.FUTURE

    def test_option_four_part(self) -> None:
        from unified_api_contracts.internal import InstrumentType

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            _classify_deribit_leg_instrument_type,
        )

        assert _classify_deribit_leg_instrument_type("BTC-17JUL26-65000-C") == InstrumentType.OPTION
        assert _classify_deribit_leg_instrument_type("ETH-11JUL26-1800-P") == InstrumentType.OPTION

    def test_unknown_shape_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            _classify_deribit_leg_instrument_type,
        )

        assert _classify_deribit_leg_instrument_type("BTC") is None
        assert _classify_deribit_leg_instrument_type("BTC-10JUL26-65000-X") is None  # not C/P
        assert _classify_deribit_leg_instrument_type("BTC-A-B-C-D") is None  # 5 parts


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
    """_parse_combo_instrument(item, currency, now) parses a get_combos result item."""

    def test_combo_with_legs_returns_record(self) -> None:
        """A get_combos item with id + structured legs → a COMBO InstrumentRecord."""
        from datetime import UTC, datetime

        adapter = _make_adapter()
        item = {
            "id": "BTC-CCAL-26JUN26_19JUN26-69000",
            "creation_timestamp": 1700000000000,
            "state": "active",
            "legs": [
                {"amount": -1, "instrument_name": "BTC-19JUN26-69000-C"},
                {"amount": 1, "instrument_name": "BTC-26JUN26-69000-C"},
            ],
        }
        result = adapter._parse_combo_instrument(item, "BTC", datetime.now(UTC))  # type: ignore[attr-defined]
        assert result is not None
        assert result.venue == "DERIBIT-COMBO"
        assert result.instrument_key == "DERIBIT:COMBO:BTC-CCAL-26JUN26_19JUN26-69000"
        assert result.base_asset == "BTC"
        assert result.legs is not None
        assert len(result.legs) == 2

    def test_combo_without_legs_returns_none(self) -> None:
        """A combo with no parseable legs is invalid (validation requires legs) → None."""
        from datetime import UTC, datetime

        adapter = _make_adapter()
        item = {"id": "BTC-FS-19JUN26_PERP", "legs": []}
        result = adapter._parse_combo_instrument(item, "BTC", datetime.now(UTC))  # type: ignore[attr-defined]
        assert result is None

    def test_non_dict_item_returns_none(self) -> None:
        """item is not a dict → None."""
        from datetime import UTC, datetime

        adapter = _make_adapter()
        result = adapter._parse_combo_instrument("not_a_dict", "BTC", datetime.now(UTC))  # type: ignore[attr-defined]
        assert result is None

    def test_empty_id_returns_none(self) -> None:
        """combo id is empty string → None."""
        from datetime import UTC, datetime

        adapter = _make_adapter()
        result = adapter._parse_combo_instrument({"id": ""}, "BTC", datetime.now(UTC))  # type: ignore[attr-defined]
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
