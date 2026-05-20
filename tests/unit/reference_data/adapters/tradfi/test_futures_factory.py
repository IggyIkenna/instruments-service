"""Unit tests — TradFi futures factory.

Tests for ``build_futures_contracts`` and its helper functions:
- Symbol parsing (root/month/year extraction)
- Physical vs cash-settled lifecycle date derivation
- Lifecycle phase classification
- Full factory output (filtering, skipping, contract construction)

No GCS calls; no Databento API calls. Pure-Python unit tests.

Plan: tradfi_canonical_futures_contract_hard_required_fields_2026_05_13.md
Phase 4 — instruments-service futures factory.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from unified_api_contracts import FuturesContractLifecyclePhase
from unified_api_contracts.internal import AssetClass, InstrumentRecord, InstrumentType

from instruments_service.reference_data.adapters.tradfi.futures_factory import (
    _classify_lifecycle_phase,
    _derive_lifecycle_dates,
    _last_business_day_before,
    _last_day_of_month,
    _parse_futures_symbol,
    build_futures_contracts,
)

# ---------------------------------------------------------------------------
# Helpers — fixture builders
# ---------------------------------------------------------------------------


def _make_future(raw_symbol: str, expiry: datetime | None = None, venue: str = "CME") -> InstrumentRecord:
    """Build a minimal InstrumentRecord for a futures contract."""
    return InstrumentRecord(
        instrument_key=f"{venue}:FUTURE:{raw_symbol}",
        venue=venue,
        asset_class=AssetClass.EQUITY,
        raw_symbol=raw_symbol,
        instrument_type=InstrumentType.FUTURE,
        base_asset="",
        quote_asset="USD",
        tick_size=Decimal("0.25"),
        min_size=Decimal("1"),
        contract_size=Decimal("50"),
        expiry=expiry,
    )


def _make_equity(raw_symbol: str) -> InstrumentRecord:
    """Build a minimal InstrumentRecord for an equity (not a future)."""
    return InstrumentRecord(
        instrument_key=f"NYSE:EQUITY:{raw_symbol}",
        venue="NYSE",
        asset_class=AssetClass.EQUITY,
        raw_symbol=raw_symbol,
        instrument_type=InstrumentType.EQUITY,
        base_asset=raw_symbol,
        quote_asset="USD",
        tick_size=Decimal("0.01"),
        min_size=Decimal("1"),
        contract_size=Decimal("1"),
    )


# ---------------------------------------------------------------------------
# Tests for _parse_futures_symbol
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_futures_symbol_es_quarterly() -> None:
    """ESH26 → root=ES, month=3 (H=March), year=2026."""
    result = _parse_futures_symbol("ESH26")
    assert result is not None
    root, month, year = result
    assert root == "ES"
    assert month == 3
    assert year == 2026


@pytest.mark.unit
def test_parse_futures_symbol_cl_energy() -> None:
    """CLZ6 → root=CL, month=12 (Z=December), year=2006 (2-digit year)."""
    result = _parse_futures_symbol("CLZ6")
    assert result is not None
    root, month, year = result
    assert root == "CL"
    assert month == 12
    assert year == 2006


@pytest.mark.unit
def test_parse_futures_symbol_gc_gold() -> None:
    """GCM26 → root=GC, month=6 (M=June), year=2026."""
    result = _parse_futures_symbol("GCM26")
    assert result is not None
    root, month, year = result
    assert root == "GC"
    assert month == 6
    assert year == 2026


@pytest.mark.unit
def test_parse_futures_symbol_fx_root() -> None:
    """6EZ26 → root=6E (FX euro), month=12 (Z=December), year=2026."""
    result = _parse_futures_symbol("6EZ26")
    assert result is not None
    root, month, year = result
    assert root == "6E"
    assert month == 12
    assert year == 2026


@pytest.mark.unit
def test_parse_futures_symbol_calendar_spread_returns_none() -> None:
    """ESM6-ESU6 (calendar spread) → None (not a simple root+month+year)."""
    assert _parse_futures_symbol("ESM6-ESU6") is None


@pytest.mark.unit
def test_parse_futures_symbol_empty_returns_none() -> None:
    """Empty string → None."""
    assert _parse_futures_symbol("") is None


@pytest.mark.unit
def test_parse_futures_symbol_equity_returns_none() -> None:
    """AAPL (equity) → None."""
    assert _parse_futures_symbol("AAPL") is None


# ---------------------------------------------------------------------------
# Tests for _last_business_day_before
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_last_business_day_before_monday() -> None:
    """Monday (2026-06-01) → Friday (2026-05-29)."""
    d = date(2026, 6, 1)  # Monday
    result = _last_business_day_before(d)
    assert result == date(2026, 5, 29)  # Friday


@pytest.mark.unit
def test_last_business_day_before_wednesday() -> None:
    """Wednesday (2026-06-03) → Tuesday (2026-06-02)."""
    d = date(2026, 6, 3)
    result = _last_business_day_before(d)
    assert result == date(2026, 6, 2)


# ---------------------------------------------------------------------------
# Tests for _last_day_of_month
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_last_day_of_month_june() -> None:
    """June 2026 → 30."""
    assert _last_day_of_month(2026, 6) == date(2026, 6, 30)


@pytest.mark.unit
def test_last_day_of_month_december() -> None:
    """December 2026 → 31 (year boundary)."""
    assert _last_day_of_month(2026, 12) == date(2026, 12, 31)


@pytest.mark.unit
def test_last_day_of_month_february_non_leap() -> None:
    """February 2026 (non-leap) → 28."""
    assert _last_day_of_month(2026, 2) == date(2026, 2, 28)


# ---------------------------------------------------------------------------
# Tests for _derive_lifecycle_dates — cash-settled (ES, NQ, 6E)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_derive_lifecycle_dates_cash_settled_all_same() -> None:
    """Cash-settled futures (ES): all 5 dates collapse to expiry_date."""
    expiry = date(2026, 6, 19)  # 3rd Friday June 2026
    ltd, fnd, dd, sd = _derive_lifecycle_dates("ES", expiry, 2026, 6)
    # All should equal expiry for cash-settled
    assert ltd == expiry
    assert fnd == expiry
    assert dd == expiry
    assert sd == expiry


@pytest.mark.unit
def test_derive_lifecycle_dates_cash_settled_nq() -> None:
    """NQ (cash-settled) also collapses to expiry."""
    expiry = date(2026, 9, 18)
    ltd, fnd, dd, sd = _derive_lifecycle_dates("NQ", expiry, 2026, 9)
    assert ltd == fnd == dd == sd == expiry


# ---------------------------------------------------------------------------
# Tests for _derive_lifecycle_dates — physically-settled (CL, GC)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_derive_lifecycle_dates_physical_cl() -> None:
    """CL (WTI, physical): first_notice before delivery month; delivery = last day of month."""
    expiry = date(2026, 5, 20)  # notional expiry mid-May
    ltd, fnd, dd, sd = _derive_lifecycle_dates("CL", expiry, 2026, 5)

    # first_notice_date = last business day before 2026-05-01
    # 2026-05-01 is a Friday → last BD before = 2026-04-30 (Thursday)
    assert fnd == date(2026, 4, 30)
    # delivery_date = last day of May 2026
    assert dd == date(2026, 5, 31)
    # settlement_date = delivery + 2 business days
    assert sd > dd
    # last_trading_date = expiry
    assert ltd == expiry


@pytest.mark.unit
def test_derive_lifecycle_dates_physical_gc() -> None:
    """GC (gold, physical): delivery_date = last day of contract month."""
    expiry = date(2026, 6, 26)
    _ltd, _fnd, dd, sd = _derive_lifecycle_dates("GC", expiry, 2026, 6)

    # delivery = last day of June 2026
    assert dd == date(2026, 6, 30)
    # settlement > delivery
    assert sd > dd


# ---------------------------------------------------------------------------
# Tests for _classify_lifecycle_phase
# ---------------------------------------------------------------------------


def _all_same_date(d: date) -> tuple[date, date, date, date, date, date]:
    """Helper: (today, expiry, ltd, fnd, dd, sd) all set to d."""
    return d, d, d, d, d, d


@pytest.mark.unit
def test_classify_lifecycle_phase_settled() -> None:
    """today > settlement_date → SETTLED."""
    today = date(2026, 9, 1)
    sd = date(2026, 6, 21)
    phase = _classify_lifecycle_phase(today, sd, sd, sd, sd, sd)
    assert phase == FuturesContractLifecyclePhase.SETTLED


@pytest.mark.unit
def test_classify_lifecycle_phase_expired() -> None:
    """today > last_trading_date but <= settlement_date → EXPIRED."""
    today = date(2026, 6, 20)
    expiry = date(2026, 6, 19)  # last trading = expiry (yesterday)
    settlement = date(2026, 6, 22)  # settlement is in 2 more days
    phase = _classify_lifecycle_phase(today, expiry, expiry, expiry, expiry, settlement)
    assert phase == FuturesContractLifecyclePhase.EXPIRED


@pytest.mark.unit
def test_classify_lifecycle_phase_active() -> None:
    """today <= expiry_date and within 90 days → ACTIVE."""
    today = date(2026, 4, 1)
    expiry = date(2026, 6, 19)  # 79 days away
    phase = _classify_lifecycle_phase(today, expiry, expiry, expiry, expiry, expiry)
    assert phase == FuturesContractLifecyclePhase.ACTIVE


@pytest.mark.unit
def test_classify_lifecycle_phase_listed() -> None:
    """today > 90 days before expiry → LISTED."""
    today = date(2026, 1, 1)
    expiry = date(2026, 6, 19)  # 169 days away
    phase = _classify_lifecycle_phase(today, expiry, expiry, expiry, expiry, expiry)
    assert phase == FuturesContractLifecyclePhase.LISTED


@pytest.mark.unit
def test_classify_lifecycle_phase_in_first_notice() -> None:
    """today >= first_notice_date but < delivery_date → IN_FIRST_NOTICE."""
    today = date(2026, 4, 30)
    expiry = date(2026, 5, 20)
    fnd = date(2026, 4, 30)  # first notice = today
    dd = date(2026, 5, 31)  # delivery later
    sd = date(2026, 6, 3)
    phase = _classify_lifecycle_phase(today, expiry, expiry, fnd, dd, sd)
    assert phase == FuturesContractLifecyclePhase.IN_FIRST_NOTICE


# ---------------------------------------------------------------------------
# Tests for build_futures_contracts
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_futures_contracts_basic() -> None:
    """ES quarterly future builds a CanonicalFuturesContract correctly."""
    expiry_dt = datetime(2026, 6, 19, 8, 30, 0, tzinfo=UTC)  # ES cash open
    records = [_make_future("ESH26", expiry=expiry_dt)]
    contracts = build_futures_contracts(records, today=date(2026, 5, 1))
    assert len(contracts) == 1
    c = contracts[0]
    assert c.root == "ES"
    assert c.contract_month == 3  # H = March
    assert c.contract_year == 2026
    assert c.venue == "CME"
    assert c.contract_symbol == "ESH26"
    # Cash-settled: all 5 dates = expiry
    assert c.expiry_date == date(2026, 6, 19)
    assert c.last_trading_date == c.expiry_date
    assert c.first_notice_date == c.expiry_date
    assert c.delivery_date == c.expiry_date
    assert c.settlement_date == c.expiry_date


@pytest.mark.unit
def test_build_futures_contracts_filters_non_futures() -> None:
    """Equity records are filtered out; only FUTURE records kept."""
    expiry_dt = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
    records = [
        _make_future("ESH26", expiry=expiry_dt),
        _make_equity("AAPL"),
    ]
    contracts = build_futures_contracts(records, today=date(2026, 5, 1))
    assert len(contracts) == 1
    assert contracts[0].root == "ES"


@pytest.mark.unit
def test_build_futures_contracts_skips_missing_expiry() -> None:
    """Future with no expiry is skipped — hard_schema validator rejects at adapter level."""
    records: list[InstrumentRecord] = []
    contracts = build_futures_contracts(records, today=date(2026, 5, 1))
    assert len(contracts) == 0


@pytest.mark.unit
def test_build_futures_contracts_skips_calendar_spread() -> None:
    """Calendar spread (ESM6-ESU6) is skipped — symbol unparseable."""
    expiry_dt = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    records = [_make_future("ESM6-ESU6", expiry=expiry_dt)]
    contracts = build_futures_contracts(records, today=date(2026, 5, 1))
    assert len(contracts) == 0


@pytest.mark.unit
def test_build_futures_contracts_physical_delivery_roots() -> None:
    """CL (physical) builds contract with divergent first_notice and delivery dates."""
    expiry_dt = datetime(2026, 5, 20, 0, 0, 0, tzinfo=UTC)
    records = [_make_future("CLM26", expiry=expiry_dt)]  # June 2026 crude oil
    contracts = build_futures_contracts(records, today=date(2026, 4, 1))
    assert len(contracts) == 1
    c = contracts[0]
    assert c.root == "CL"
    # Physical: first_notice ≠ expiry
    assert c.first_notice_date != c.expiry_date
    # delivery = last day of June 2026
    assert c.delivery_date == date(2026, 6, 30)


@pytest.mark.unit
def test_build_futures_contracts_multiple_records() -> None:
    """Multiple futures records from same fetch → all converted."""
    expiry1 = datetime(2026, 3, 20, 0, 0, 0, tzinfo=UTC)
    expiry2 = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
    expiry3 = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    records = [
        _make_future("ESH26", expiry=expiry1),
        _make_future("ESM26", expiry=expiry2),
        _make_future("ESU26", expiry=expiry3),
    ]
    contracts = build_futures_contracts(records, today=date(2026, 2, 1))
    assert len(contracts) == 3
    roots = {c.root for c in contracts}
    assert roots == {"ES"}
    months = {c.contract_month for c in contracts}
    assert months == {3, 6, 9}


@pytest.mark.unit
def test_build_futures_contracts_lifecycle_phase_active() -> None:
    """Contract expiring in 60 days is ACTIVE."""
    today = date(2026, 4, 20)
    expiry_dt = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)  # 60 days out
    records = [_make_future("ESM26", expiry=expiry_dt)]
    contracts = build_futures_contracts(records, today=today)
    assert contracts[0].lifecycle_phase == FuturesContractLifecyclePhase.ACTIVE


@pytest.mark.unit
def test_build_futures_contracts_lifecycle_phase_settled_with_reject_false() -> None:
    """Contract where today > settlement_date → SETTLED (included when reject_expired=False)."""
    today = date(2026, 9, 1)
    expiry_dt = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
    records = [_make_future("ESM26", expiry=expiry_dt)]
    contracts = build_futures_contracts(records, today=today, reject_expired=False)
    assert contracts[0].lifecycle_phase == FuturesContractLifecyclePhase.SETTLED


@pytest.mark.unit
def test_build_futures_contracts_expiry_guard_rejects_expired(caplog: pytest.LogCaptureFixture) -> None:
    """Expiry guard: EXPIRED contracts are rejected with WARNING when reject_expired=True (default)."""
    today = date(2026, 6, 20)
    # Expiry was yesterday → EXPIRED phase
    expiry_dt = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
    records = [_make_future("ESM26", expiry=expiry_dt)]
    with caplog.at_level("WARNING", logger="instruments_service.reference_data.adapters.tradfi.futures_factory"):
        contracts = build_futures_contracts(records, today=today)
    assert len(contracts) == 0
    assert "Expiry guard" in caplog.text
    assert "ESM26" in caplog.text


@pytest.mark.unit
def test_build_futures_contracts_expiry_guard_rejects_settled(caplog: pytest.LogCaptureFixture) -> None:
    """Expiry guard: SETTLED contracts are also rejected with WARNING when reject_expired=True (default)."""
    today = date(2026, 9, 1)
    expiry_dt = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
    records = [_make_future("ESM26", expiry=expiry_dt)]
    with caplog.at_level("WARNING", logger="instruments_service.reference_data.adapters.tradfi.futures_factory"):
        contracts = build_futures_contracts(records, today=today)
    assert len(contracts) == 0
    assert "Expiry guard" in caplog.text


@pytest.mark.unit
def test_build_futures_contracts_expiry_guard_passes_active() -> None:
    """Expiry guard: ACTIVE contracts pass through even with reject_expired=True."""
    today = date(2026, 4, 20)
    expiry_dt = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)
    records = [_make_future("ESM26", expiry=expiry_dt)]
    contracts = build_futures_contracts(records, today=today)  # default reject_expired=True
    assert len(contracts) == 1
    assert contracts[0].lifecycle_phase == FuturesContractLifecyclePhase.ACTIVE


@pytest.mark.unit
def test_build_futures_contracts_expiry_guard_bypass_historical() -> None:
    """reject_expired=False allows EXPIRED/SETTLED through for historical backfill queries."""
    today = date(2026, 9, 1)
    expiry_expired = datetime(2026, 6, 19, 0, 0, 0, tzinfo=UTC)  # SETTLED relative to today
    expiry_active = datetime(2026, 12, 18, 0, 0, 0, tzinfo=UTC)  # ACTIVE
    records = [
        _make_future("ESM26", expiry=expiry_expired),
        _make_future("ESZ26", expiry=expiry_active),
    ]
    contracts = build_futures_contracts(records, today=today, reject_expired=False)
    assert len(contracts) == 2
