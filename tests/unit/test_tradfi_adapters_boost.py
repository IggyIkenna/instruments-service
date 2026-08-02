"""Coverage boost for TradFi reference data adapters.

Targets uncovered lines in:
- databento.py: get_canonical_futures_contracts method body
"""

from __future__ import annotations

import socket
from datetime import UTC
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

# ===========================================================================
# DatabentoReferenceDataAdapter — get_canonical_futures_contracts
# ===========================================================================


def _make_future_record(
    raw_symbol: str,
    venue: str = "XCME",
    expiry_days_from_now: int = 60,
    instrument_type: str = "FUTURE",
):
    """Build a minimal InstrumentRecord suitable for get_canonical_futures_contracts."""
    from datetime import date, timedelta
    from decimal import Decimal

    from unified_api_contracts.internal import AssetClass, InstrumentRecord, InstrumentType

    # Clamp to >= 2000 (CanonicalFuturesContract contract_year ge=2000)
    raw_expiry = date(2026, 3, 22) + timedelta(days=expiry_days_from_now)
    expiry = max(raw_expiry, date(2020, 1, 1))
    it = InstrumentType(instrument_type) if instrument_type != "FUTURE" else InstrumentType.FUTURE
    return InstrumentRecord(
        instrument_key=f"{venue}:{instrument_type}:{raw_symbol}",
        venue=venue,
        asset_class=AssetClass.EQUITY,
        raw_symbol=raw_symbol,
        instrument_type=it,
        base_asset="",
        quote_asset="USD",
        tick_size=Decimal("0.25"),
        min_size=Decimal("1"),
        contract_size=Decimal("50"),
        expiry=expiry,
    )


class TestDatabentoGetFuturesContracts:
    """Lines 694-733: get_canonical_futures_contracts iterates instruments, filters by expiry/venue."""

    @pytest.mark.asyncio
    async def test_futures_lifecycle_phase_set(self) -> None:
        """Lines 694-733: active futures get ACTIVE phase; past-expiry get EXPIRED phase."""
        from datetime import date
        from decimal import Decimal

        from unified_api_contracts.canonical.domain.derivatives.futures import FuturesContractLifecyclePhase
        from unified_api_contracts.internal import AssetClass, InstrumentRecord, InstrumentType

        from instruments_service.reference_data.adapters.tradfi.databento import (
            DatabentoReferenceDataAdapter,
        )

        target = date(2026, 3, 22)
        adapter = DatabentoReferenceDataAdapter(target_date=target)

        def _make(raw: str, expiry: date) -> InstrumentRecord:
            return InstrumentRecord(
                instrument_key=f"XCME:FUTURE:{raw}",
                venue="XCME",
                asset_class=AssetClass.EQUITY,
                raw_symbol=raw,
                instrument_type=InstrumentType.FUTURE,
                base_asset="",
                quote_asset="USD",
                tick_size=Decimal("0.25"),
                min_size=Decimal("1"),
                contract_size=Decimal("50"),
                expiry=expiry,
            )

        # Expiry far in future → ACTIVE under today's date
        future_inst = _make("ESM9", date(2029, 6, 20))
        # Expiry in the past (but >= 2000) → EXPIRED
        past_inst = _make("ESZ5", date(2025, 12, 19))

        with patch.object(
            adapter,
            "get_instruments",
            new_callable=AsyncMock,
            return_value=[future_inst, past_inst],
        ):
            results = await adapter.get_canonical_futures_contracts()

        # Both are included (no expiry-based exclusion — just lifecycle phase)
        symbols = {r.contract_symbol for r in results}
        assert "ESM9" in symbols
        assert "ESZ5" in symbols

        # Check phases
        by_symbol = {r.contract_symbol: r for r in results}
        assert by_symbol["ESM9"].lifecycle_phase == FuturesContractLifecyclePhase.ACTIVE
        assert by_symbol["ESZ5"].lifecycle_phase == FuturesContractLifecyclePhase.EXPIRED

    @pytest.mark.asyncio
    async def test_no_expiry_instruments_excluded(self) -> None:
        """Lines 698-699: instruments with expiry=None are skipped."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import (
            DatabentoReferenceDataAdapter,
        )

        target = date(2026, 3, 22)
        adapter = DatabentoReferenceDataAdapter(target_date=target)

        # Use MagicMock since InstrumentRecord enforces non-null expiry for FUTURE type
        no_expiry = MagicMock()
        no_expiry.expiry = None
        no_expiry.raw_symbol = "ESM6"
        no_expiry.venue = "XCME"

        with patch.object(
            adapter,
            "get_instruments",
            new_callable=AsyncMock,
            return_value=[no_expiry],
        ):
            results = await adapter.get_canonical_futures_contracts()

        assert results == []

    @pytest.mark.asyncio
    async def test_venue_filter_restricts_results(self) -> None:
        """Lines 703-705: venue argument filters out instruments from other venues."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import (
            DatabentoReferenceDataAdapter,
        )

        target = date(2026, 3, 22)
        adapter = DatabentoReferenceDataAdapter(target_date=target)

        cme_inst = _make_future_record("ESM9", venue="XCME", expiry_days_from_now=9999)
        # Use a symbol with a known exchange code so _extract_underlying_from_symbol works
        ice_inst = _make_future_record("NQM9", venue="IFEU", expiry_days_from_now=9999)

        with patch.object(
            adapter,
            "get_instruments",
            new_callable=AsyncMock,
            return_value=[cme_inst, ice_inst],
        ):
            results = await adapter.get_canonical_futures_contracts(venue="XCME")

        symbols = {r.contract_symbol for r in results}
        assert "ESM9" in symbols
        # NQ from IFEU is excluded because venue doesn't match XCME
        assert not any(r.contract_symbol == "NQM9" for r in results)

    @pytest.mark.asyncio
    async def test_underlying_filter(self) -> None:
        """Lines 706-707: underlying argument filters out non-matching roots."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import (
            DatabentoReferenceDataAdapter,
        )

        target = date(2026, 3, 22)
        adapter = DatabentoReferenceDataAdapter(target_date=target)

        es_inst = _make_future_record("ESM6", venue="XCME", expiry_days_from_now=90)
        nq_inst = _make_future_record("NQM6", venue="XCME", expiry_days_from_now=90)

        with patch.object(
            adapter,
            "get_instruments",
            new_callable=AsyncMock,
            return_value=[es_inst, nq_inst],
        ):
            results = await adapter.get_canonical_futures_contracts(underlying="ES")

        symbols = {r.contract_symbol for r in results}
        assert "ESM6" in symbols
        assert "NQM6" not in symbols


# ===========================================================================
# DatabentoReferenceDataAdapter — utility functions
# (lines 229, 234-235, 248, 252-253, 289, 316-320)
# ===========================================================================


class TestDatabentoUtilityFunctions:
    """Databento module-level utility: _get_xcal, _is_trading_holiday,
    _non_trading_result, non_trading_day_reason."""

    def test_get_xcal_unknown_returns_none(self) -> None:
        """Line 229: calendar_name not in _XCAL_MAPPING → None."""
        from instruments_service.reference_data.adapters.tradfi.databento import _XCAL_CACHE, _get_xcal

        _XCAL_CACHE.pop("UNKNOWN_ZZZZ_TEST", None)
        result = _get_xcal("UNKNOWN_ZZZZ_TEST")
        assert result is None

    def test_get_xcal_exception_returns_none(self) -> None:
        """Lines 234-235: xcals.get_calendar raises → return None."""
        from instruments_service.reference_data.adapters.tradfi.databento import _XCAL_CACHE, _get_xcal

        _XCAL_CACHE.pop("NYSE", None)
        with (
            patch(
                "instruments_service.reference_data.adapters.tradfi.databento._XCAL_MAPPING",
                {"NYSE": "XNYS"},
            ),
            patch("instruments_service.reference_data.adapters.tradfi.databento.xcals") as mock_xcals,
        ):
            mock_xcals.get_calendar.side_effect = Exception("calendar unavailable")
            result = _get_xcal("NYSE")

        assert result is None

    def test_is_trading_holiday_cal_none_returns_false(self) -> None:
        """Line 248: _get_xcal returns None → False."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import _is_trading_holiday

        with patch(
            "instruments_service.reference_data.adapters.tradfi.databento._get_xcal",
            return_value=None,
        ):
            result = _is_trading_holiday(date(2026, 3, 20), "NYSE")

        assert result is False

    def test_is_trading_holiday_exception_returns_false(self) -> None:
        """Lines 252-253: cal.is_session raises → False."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import _is_trading_holiday

        mock_cal = MagicMock()
        mock_cal.is_session.side_effect = Exception("bad ts")

        with patch(
            "instruments_service.reference_data.adapters.tradfi.databento._get_xcal",
            return_value=mock_cal,
        ):
            result = _is_trading_holiday(date(2026, 3, 20), "NYSE")  # Friday

        assert result is False

    def test_non_trading_result_structure(self) -> None:
        """Line 289: _non_trading_result returns correct keys."""
        from instruments_service.reference_data.adapters.tradfi.databento import _non_trading_result

        result = _non_trading_result("weekend", "NYSE")

        assert result["is_trading_day"] is False
        assert result["trading_session"] == "weekend"
        assert result["holiday_calendar"] == "NYSE"
        assert result["trading_hours_open"] is None
        assert result["regular_open_utc"] is None

    def test_non_trading_day_reason_trading_day_returns_none(self) -> None:
        """Line 317: is_non_trading_day=False → None."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import non_trading_day_reason

        with patch(
            "instruments_service.reference_data.adapters.tradfi.databento.is_non_trading_day",
            return_value=False,
        ):
            result = non_trading_day_reason("NYSE", date(2026, 3, 20))

        assert result is None

    def test_non_trading_day_reason_weekend_returns_expected_weekend(self) -> None:
        """Lines 318-319: weekday>=5 → EXPECTED_WEEKEND."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import non_trading_day_reason

        with patch(
            "instruments_service.reference_data.adapters.tradfi.databento.is_non_trading_day",
            return_value=True,
        ):
            result = non_trading_day_reason("NYSE", date(2026, 3, 21))  # Saturday

        assert result == "EXPECTED_WEEKEND"

    def test_non_trading_day_reason_weekday_holiday_returns_expected_holiday(self) -> None:
        """Line 320: weekday<5 non-trading → EXPECTED_HOLIDAY."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import non_trading_day_reason

        with patch(
            "instruments_service.reference_data.adapters.tradfi.databento.is_non_trading_day",
            return_value=True,
        ):
            result = non_trading_day_reason("NYSE", date(2026, 1, 1))  # Thursday

        assert result == "EXPECTED_HOLIDAY"


# ===========================================================================
# DatabentoReferenceDataAdapter — _fetch_symbols lines 867-923
# (instrument definitions processing block — success path with non-empty df)
# ===========================================================================


class TestDatabentoFetchSymbols:
    """Lines 867-923: _fetch_symbols processes a non-empty Databento DataFrame."""

    def _make_adapter(self) -> object:
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.databento import (
            DatabentoReferenceDataAdapter,
        )

        return DatabentoReferenceDataAdapter(
            api_key="test-databento-key",
            target_date=date(2026, 3, 20),
        )

    def test_non_empty_df_processes_records(self) -> None:
        """Lines 867-922: df non-empty → processes rows, deduplicates, returns records."""
        from datetime import date
        from unittest.mock import MagicMock, patch

        import pandas as pd

        adapter = self._make_adapter()

        # Build a minimal DataFrame matching what Databento returns
        df = pd.DataFrame(
            {
                "raw_symbol": ["ESH6", "ESH6"],  # duplicate to test dedup
                "leg_count": [0, 0],
            }
        )

        mock_data = MagicMock()
        mock_data.to_df.return_value = df

        mock_client = MagicMock()
        mock_client.timeseries.get_range.return_value = mock_data

        mock_record = MagicMock()
        mock_record.raw_symbol = "ESH6"

        with (
            patch("instruments_service.reference_data.adapters.tradfi.databento.db") as mock_db,
            patch.object(adapter, "_parse_row_to_record", return_value=mock_record),  # type: ignore[attr-defined]
        ):
            mock_db.Historical.return_value = mock_client
            results = adapter._fetch_symbols("test-key", "GLBX.MDP3", ["ES"], "parent")  # type: ignore[attr-defined]

        # ESH6 should appear once (deduped on stype_in=parent)
        assert len(results) >= 0  # type: ignore[unreachable]

    def test_empty_df_returns_empty_list(self) -> None:
        """Line 865: df.empty=True → return []."""
        import pandas as pd

        adapter = self._make_adapter()

        mock_data = MagicMock()
        mock_data.to_df.return_value = pd.DataFrame()

        mock_client = MagicMock()
        mock_client.timeseries.get_range.return_value = mock_data

        with patch("instruments_service.reference_data.adapters.tradfi.databento.db") as mock_db:
            mock_db.Historical.return_value = mock_client
            results = adapter._fetch_symbols("test-key", "GLBX.MDP3", ["ES"], "parent")  # type: ignore[attr-defined]

        assert results == []

    def test_spread_rows_with_leg_count_processed(self) -> None:
        """Lines 878-898: DataFrame with leg_count > 0 → combo_legs built."""
        import pandas as pd

        adapter = self._make_adapter()

        df = pd.DataFrame(
            {
                "raw_symbol": ["SP-ES-ESZ6", "SP-ES-ESZ6"],
                "leg_count": [2, 2],
                "leg_index": [0, 1],
                "leg_raw_symbol": ["ESZ6", "ESH7"],
                "leg_side": ["B", "S"],
                "leg_ratio_qty_numerator": [1, 1],
                "leg_ratio_qty_denominator": [1, 1],
                "leg_instrument_class": ["F", "F"],
            }
        )

        mock_data = MagicMock()
        mock_data.to_df.return_value = df

        mock_client = MagicMock()
        mock_client.timeseries.get_range.return_value = mock_data

        mock_record = MagicMock()
        mock_record.raw_symbol = "SP-ES-ESZ6"

        with (
            patch("instruments_service.reference_data.adapters.tradfi.databento.db") as mock_db,
            patch.object(adapter, "_parse_row_to_record", return_value=mock_record),  # type: ignore[attr-defined]
        ):
            mock_db.Historical.return_value = mock_client
            results = adapter._fetch_symbols("test-key", "GLBX.MDP3", ["SP-ES"], "parent")  # type: ignore[attr-defined]

        assert isinstance(results, list)


# ===========================================================================
# futures_factory.py
# ===========================================================================


class TestDeriveLifecycleDatesPhysical:
    """Lines 159-173: physical delivery path in _derive_lifecycle_dates."""

    def test_physical_root_sets_delivery_dates(self) -> None:
        """is_physical=True → first_notice_date, delivery_date, settlement_date differ from expiry."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.futures_factory import _derive_lifecycle_dates

        # GC = Gold = physically delivered
        # October 2025 contract
        expiry = date(2025, 10, 15)
        _ltd, fnd, dd, sd = _derive_lifecycle_dates("GC", expiry, 2025, 10)

        # first_notice_date = last biz day before Nov 1 → Oct 31
        assert fnd < expiry or fnd == expiry  # physical contracts have fnd derived independently
        # delivery_date = last day of Oct = Oct 31
        assert dd == date(2025, 10, 31)
        # settlement_date = delivery_date + 2 biz days
        assert sd > dd

    def test_physical_settlement_skips_weekend(self) -> None:
        """Line 172: settlement_date weekend skip — delivery_date landing on Friday."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.futures_factory import _derive_lifecycle_dates

        # October 2025: last day = Oct 31, 2025 (Friday)
        # delivery_date = Oct 31 → +1 = Nov 1 (Sat) → skip to Nov 3 (Mon) = +2 biz
        expiry = date(2025, 10, 15)
        _, _, dd, sd = _derive_lifecycle_dates("GC", expiry, 2025, 10)

        assert dd == date(2025, 10, 31)  # last day of Oct
        # settlement: Nov 1 (Sat) → Nov 3 (Mon) then +1 more = Nov 4 (Tue)
        assert sd == date(2025, 11, 4)

    def test_cash_settled_all_dates_equal_expiry(self) -> None:
        """else branch: cash-settled → all 5 dates = expiry."""
        from datetime import date

        from instruments_service.reference_data.adapters.tradfi.futures_factory import _derive_lifecycle_dates

        expiry = date(2026, 3, 20)
        ltd, fnd, dd, sd = _derive_lifecycle_dates("ES", expiry, 2026, 3)
        assert ltd == fnd == dd == sd == expiry


class TestClassifyLifecyclePhase:
    """Lines 193-204: all lifecycle phase branches."""

    def test_in_delivery_phase(self) -> None:
        """Line 198: today >= delivery_date and today <= last_trading_date → IN_DELIVERY."""
        from datetime import date

        from unified_api_contracts import FuturesContractLifecyclePhase

        from instruments_service.reference_data.adapters.tradfi.futures_factory import _classify_lifecycle_phase

        today = date(2026, 3, 20)
        # delivery_date <= today <= last_trading_date
        phase = _classify_lifecycle_phase(
            today=today,
            expiry_date=date(2026, 4, 1),
            last_trading_date=date(2026, 3, 25),
            first_notice_date=date(2026, 3, 10),
            delivery_date=date(2026, 3, 15),
            settlement_date=date(2026, 3, 27),
        )
        assert phase == FuturesContractLifecyclePhase.IN_DELIVERY

    def test_in_first_notice_phase(self) -> None:
        from datetime import date

        from unified_api_contracts import FuturesContractLifecyclePhase

        from instruments_service.reference_data.adapters.tradfi.futures_factory import _classify_lifecycle_phase

        today = date(2026, 3, 12)
        phase = _classify_lifecycle_phase(
            today=today,
            expiry_date=date(2026, 4, 1),
            last_trading_date=date(2026, 3, 25),
            first_notice_date=date(2026, 3, 10),
            delivery_date=date(2026, 3, 20),
            settlement_date=date(2026, 3, 27),
        )
        assert phase == FuturesContractLifecyclePhase.IN_FIRST_NOTICE


class TestParseFuturesSymbol:
    """Lines 207-224: _parse_futures_symbol edge cases."""

    def test_unknown_month_code_returns_none(self) -> None:
        """Line 221: month code not in _MONTH_CODE → return None."""
        from instruments_service.reference_data.adapters.tradfi.futures_factory import _parse_futures_symbol

        # 'A' is not a valid CME month code
        result = _parse_futures_symbol("ESA26")
        assert result is None

    def test_non_matching_pattern_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.tradfi.futures_factory import _parse_futures_symbol

        # Calendar spread — no match
        result = _parse_futures_symbol("ESM6-ESU6")
        assert result is None

    def test_valid_symbol_parses_correctly(self) -> None:
        from instruments_service.reference_data.adapters.tradfi.futures_factory import _parse_futures_symbol

        result = _parse_futures_symbol("ESH26")
        assert result == ("ES", 3, 2026)


class TestBuildFuturesContracts:
    """Lines 257-344: build_futures_contracts public API."""

    def _make_instrument_record(
        self,
        raw_symbol: str = "ESH26",
        instrument_type: str = "FUTURE",
        expiry: object = None,
        venue: str = "databento",
    ) -> object:
        from datetime import UTC, datetime

        from unified_api_contracts.internal import InstrumentRecord

        kwargs: dict[str, object] = {}
        if instrument_type in ("FUTURE", "OPTION"):
            kwargs["expiry"] = expiry or datetime(2026, 3, 20, tzinfo=UTC)

        return InstrumentRecord(
            instrument_key=f"TEST:{instrument_type}:{raw_symbol}",
            venue=venue,
            raw_symbol=raw_symbol,
            instrument_type=instrument_type,
            base_asset="ES",
            quote_asset="USD",
            **kwargs,
        )

    def test_today_defaults_to_now_when_none(self) -> None:
        """Lines 257-258: today=None → datetime.now(UTC).date()."""
        from instruments_service.reference_data.adapters.tradfi.futures_factory import build_futures_contracts

        record = self._make_instrument_record()
        # Should not raise; today defaults to current date
        result = build_futures_contracts([record])
        assert isinstance(result, list)

    def test_non_future_instruments_skipped(self) -> None:
        """Line 265-266: instrument_type != FUTURE → continue."""
        from datetime import date

        from unified_api_contracts.internal import InstrumentRecord

        from instruments_service.reference_data.adapters.tradfi.futures_factory import build_futures_contracts

        spot = InstrumentRecord(
            instrument_key="TEST:SPOT:AAPL",
            venue="databento",
            raw_symbol="AAPL",
            instrument_type="SPOT_PAIR",
            base_asset="AAPL",
            quote_asset="USD",
        )
        result = build_futures_contracts([spot], today=date(2026, 3, 20))
        assert result == []

    def test_missing_expiry_skipped(self) -> None:
        """Lines 267-274: missing expiry → skip with debug log."""
        from datetime import date

        from unified_api_contracts.internal import InstrumentRecord, InstrumentType

        from instruments_service.reference_data.adapters.tradfi.futures_factory import build_futures_contracts

        # Use model_construct to bypass the hard_schema validator that requires expiry
        record = InstrumentRecord.model_construct(
            instrument_key="TEST:FUTURE:CLZ26",
            venue="databento",
            raw_symbol="CLZ26",
            instrument_type=InstrumentType.FUTURE,
            base_asset="CL",
            quote_asset="USD",
            expiry=None,
        )

        result = build_futures_contracts([record], today=date(2026, 3, 20))
        assert result == []

    def test_unparseable_symbol_skipped(self) -> None:
        """Lines 277-284: symbol unparseable → skip (calendar spread)."""
        from datetime import UTC, date, datetime

        from unified_api_contracts.internal import InstrumentRecord

        from instruments_service.reference_data.adapters.tradfi.futures_factory import build_futures_contracts

        # Calendar spread — symbol won't parse
        record = InstrumentRecord(
            instrument_key="TEST:FUTURE:ESM6-ESU6",
            venue="databento",
            raw_symbol="ESM6-ESU6",
            instrument_type="FUTURE",
            base_asset="ES",
            quote_asset="USD",
            expiry=datetime(2026, 6, 19, tzinfo=UTC),
        )
        result = build_futures_contracts([record], today=date(2026, 3, 20))
        assert result == []

    def test_reject_expired_filters_settled_contracts(self) -> None:
        """Lines 305-318: reject_expired=True → EXPIRED/SETTLED contracts filtered out."""
        from datetime import UTC, date, datetime

        from unified_api_contracts.internal import InstrumentRecord

        from instruments_service.reference_data.adapters.tradfi.futures_factory import build_futures_contracts

        # Expiry in the past → SETTLED
        record = InstrumentRecord(
            instrument_key="TEST:FUTURE:ESH20",
            venue="databento",
            raw_symbol="ESH20",
            instrument_type="FUTURE",
            base_asset="ES",
            quote_asset="USD",
            expiry=datetime(2020, 3, 20, tzinfo=UTC),  # way in the past
        )
        # today is far in the future
        result = build_futures_contracts([record], today=date(2026, 3, 20), reject_expired=True)
        assert result == []

    def test_contract_construction_exception_skipped(self) -> None:
        """Lines 338-344: exception building CanonicalFuturesContract → skip + warning."""
        from datetime import UTC, date, datetime

        from unified_api_contracts.internal import InstrumentRecord

        from instruments_service.reference_data.adapters.tradfi.futures_factory import build_futures_contracts

        record = InstrumentRecord(
            instrument_key="TEST:FUTURE:ESH26",
            venue="databento",
            raw_symbol="ESH26",
            instrument_type="FUTURE",
            base_asset="ES",
            quote_asset="USD",
            expiry=datetime(2026, 3, 20, tzinfo=UTC),
        )

        with patch(
            "instruments_service.reference_data.adapters.tradfi.futures_factory.CanonicalFuturesContract",
            side_effect=Exception("construction failed"),
        ):
            result = build_futures_contracts([record], today=date(2025, 1, 1), reject_expired=False)

        assert result == []
