"""
Schema robustness tests for instruments-service.

Layer 1 smoke: verifies corporate actions Pydantic models (DividendRecord,
StockSplitRecord, EarningsRecord, CorporateActionsBundle) instantiate with
valid data and enforce field-level validation.
"""

from __future__ import annotations

from datetime import date


class TestCorporateActionTypeEnum:
    """Verify CorporateActionType and DividendType enums."""

    def test_corporate_action_type_importable(self) -> None:
        from instruments_service.corporate_actions.models import CorporateActionType

        assert CorporateActionType is not None

    def test_dividend_type_importable(self) -> None:
        from instruments_service.corporate_actions.models import DividendType

        assert DividendType is not None

    def test_corporate_action_type_values(self) -> None:
        from instruments_service.corporate_actions.models import CorporateActionType

        assert CorporateActionType.DIVIDEND == "dividend"
        assert CorporateActionType.SPLIT == "split"


class TestDividendRecord:
    """Verify DividendRecord Pydantic model."""

    def test_dividend_record_importable(self) -> None:
        from instruments_service.corporate_actions.models import DividendRecord

        assert DividendRecord is not None

    def test_dividend_record_instantiation(self) -> None:
        from instruments_service.corporate_actions.models import DividendRecord

        rec = DividendRecord(
            ticker="AAPL",
            ex_date=date(2024, 5, 10),
            amount=0.25,
        )
        assert rec.ticker == "AAPL"
        assert rec.amount == 0.25

    def test_dividend_record_ticker_uppercased(self) -> None:
        from instruments_service.corporate_actions.models import DividendRecord

        rec = DividendRecord(ticker="aapl", ex_date=date(2024, 5, 10), amount=0.10)
        assert rec.ticker == "AAPL"


class TestStockSplitRecord:
    """Verify StockSplitRecord Pydantic model."""

    def test_stock_split_record_importable(self) -> None:
        from instruments_service.corporate_actions.models import StockSplitRecord

        assert StockSplitRecord is not None

    def test_stock_split_forward_split(self) -> None:
        from instruments_service.corporate_actions.models import StockSplitRecord

        rec = StockSplitRecord(
            ticker="TSLA",
            effective_date=date(2020, 8, 31),
            ratio=5.0,
            split_from=1,
            split_to=5,
        )
        assert rec.is_reverse_split is False
        assert rec.adjustment_factor == 0.2


class TestCorporateActionsBundle:
    """Verify CorporateActionsBundle Pydantic model."""

    def test_bundle_importable(self) -> None:
        from instruments_service.corporate_actions.models import CorporateActionsBundle

        assert CorporateActionsBundle is not None

    def test_bundle_total_records_empty(self) -> None:
        from instruments_service.corporate_actions.models import CorporateActionsBundle

        bundle = CorporateActionsBundle(
            ticker="MSFT",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )
        assert bundle.total_records == 0
