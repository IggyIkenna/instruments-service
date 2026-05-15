"""TradFi futures contract factory — builds CanonicalFuturesContract records.

Takes the list of InstrumentRecord objects already fetched by the Databento
adapter (which populates `expiry` from Databento's `definition` schema) and
derives the 5 hard-required lifecycle date fields using venue-specific
conventions.

## Date derivation conventions

Databento's ``definition`` schema only surfaces ``expiration`` (= the date
the contract is no longer tradeable).  The other 4 dates are derived from
``expiry_date`` using the following rules:

| Field               | Derivation rule                                         |
|---------------------|---------------------------------------------------------|
| ``last_trading_date`` | Same as ``expiry_date`` for CME/ICE financial futures  |
|                       | (cash-settled; final settlement price fixed at expiry) |
| ``first_notice_date`` | CME physically-delivered (CL, GC, SI, …): 1 business  |
|                       | day before the 1st of the delivery month.              |
|                       | Cash-settled (ES, NQ, 6E, …): same as expiry_date      |
|                       | (no physical delivery notice).                         |
| ``delivery_date``     | Physically-settled: last day of delivery month.        |
|                       | Cash-settled: same as expiry_date.                     |
| ``settlement_date``   | delivery_date + 1 calendar day (T+1 convention).       |
|                       | Capped at delivery_date for same-day settled.          |

## Lifecycle phase derivation

``FuturesContractLifecyclePhase`` is assigned based on today's date relative
to the 5 lifecycle dates:

- ``SETTLED``  : today > settlement_date
- ``EXPIRED``  : today > last_trading_date (and <= settlement_date)
- ``IN_DELIVERY``: today >= delivery_date (and <= last_trading_date)
- ``IN_FIRST_NOTICE``: today >= first_notice_date (and < delivery_date)
- ``ACTIVE``   : today <= expiry_date (front-month or near-front)
- ``LISTED``   : long-dated contracts not yet near active status

## Physically-settled roots

CME physically-delivered contracts (agricultural, energy, metals) use the
first-notice / delivery-period convention.  Cash-settled financial futures
(indices, FX, some interest-rate products) collapse all 5 dates to expiry.

Plan: tradfi_canonical_futures_contract_hard_required_fields_2026_05_13.md
Phase 4 — instruments-service consumer.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, date, datetime, timedelta

from unified_api_contracts import CanonicalFuturesContract, FuturesContractLifecyclePhase
from unified_api_contracts.internal import InstrumentRecord, InstrumentType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CME month-code → month number
# ---------------------------------------------------------------------------
_MONTH_CODE: dict[str, int] = {
    "F": 1,  # January
    "G": 2,  # February
    "H": 3,  # March
    "J": 4,  # April
    "K": 5,  # May
    "M": 6,  # June
    "N": 7,  # July
    "Q": 8,  # August
    "U": 9,  # September
    "V": 10,  # October
    "X": 11,  # November
    "Z": 12,  # December
}

# Regex: root (1-4 alpha) + month code (1 alpha) + year (1-2 digit)
# Handles: ESH26, CLZ6, GCM26, 6EZ26, BRN.H26, CL.F27
_FUTURES_SYM_RE = re.compile(
    r"^(?P<root>[A-Z0-9]{1,5})"  # root: 1-5 alphanumeric chars
    r"\.?"  # optional dot separator (BRN.H26)
    r"(?P<month>[FGHJKMNQUVXZ])"  # CME month code
    r"(?P<year>\d{1,2})$",  # 1-2 digit year
)

# CME futures roots that are physically delivered.
# All others are treated as cash-settled (financial futures).
# Source: CME Group rulebooks (energy, metals, agricultural).
_PHYSICAL_DELIVERY_ROOTS: frozenset[str] = frozenset(
    {
        # Energy
        "CL",  # WTI Crude Oil
        "NG",  # Natural Gas
        "RB",  # RBOB Gasoline
        "HO",  # Heating Oil
        "BRN",  # Brent Crude (ICE)
        "G",  # Gas Oil (ICE)
        # Metals
        "GC",  # Gold
        "SI",  # Silver
        "HG",  # Copper
        "PL",  # Platinum
        "PA",  # Palladium
        # Agricultural (select CME)
        "ZC",  # Corn
        "ZS",  # Soybeans
        "ZW",  # Wheat
        "ZL",  # Soybean Oil
        "ZM",  # Soybean Meal
        "CT",  # Cotton
        "KC",  # Coffee
        "SB",  # Sugar
        "CC",  # Cocoa
        "OJ",  # Orange Juice
        # Live/Feeder Cattle
        "LE",  # Live Cattle
        "GF",  # Feeder Cattle
        "HE",  # Lean Hogs
    }
)


def _last_business_day_before(d: date) -> date:
    """Return the last business day strictly before date d.

    Simple Mon-Fri logic; ignores exchange holidays (conservative).
    """
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:  # Saturday=5, Sunday=6
        prev -= timedelta(days=1)
    return prev


def _last_day_of_month(year: int, month: int) -> date:
    """Return the last calendar day of a given year-month."""
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def _derive_lifecycle_dates(
    root: str,
    expiry: date,
    contract_year: int,
    contract_month: int,
) -> tuple[date, date, date, date]:
    """Derive (last_trading_date, first_notice_date, delivery_date, settlement_date).

    Returns a 4-tuple for use in CanonicalFuturesContract construction.
    """
    is_physical = root.upper() in _PHYSICAL_DELIVERY_ROOTS

    last_trading_date: date = expiry  # default: cash-settled
    first_notice_date: date = expiry
    delivery_date: date = expiry
    settlement_date: date = expiry

    if is_physical:
        # Physically-delivered: standard CME/ICE delivery-period convention.
        # first_notice_date = last business day before 1st of delivery month.
        first_of_delivery = date(contract_year, contract_month, 1)
        first_notice_date = _last_business_day_before(first_of_delivery)
        # delivery_date = last day of the delivery month.
        delivery_date = _last_day_of_month(contract_year, contract_month)
        # settlement_date = delivery_date + 2 business days (T+2).
        # Use a simple T+2 calendar-day approximation; weekends add 1 extra.
        candidate = delivery_date
        for _ in range(2):
            candidate += timedelta(days=1)
            while candidate.weekday() >= 5:
                candidate += timedelta(days=1)
        settlement_date = candidate
        # last_trading_date = expiry (set by Databento; for physical futures
        # this is the last day positions can trade before delivery window).
    else:
        # Cash-settled financial futures: all 5 dates collapse to expiry.
        # (Settlement fixes at open on expiry for ES/NQ; same-day cash transfer.)
        pass

    return last_trading_date, first_notice_date, delivery_date, settlement_date


def _classify_lifecycle_phase(
    today: date,
    expiry_date: date,
    last_trading_date: date,
    first_notice_date: date,
    delivery_date: date,
    settlement_date: date,
) -> FuturesContractLifecyclePhase:
    """Derive the current lifecycle phase from today vs the 5 date fields."""
    if today > settlement_date:
        return FuturesContractLifecyclePhase.SETTLED
    if today > last_trading_date:
        return FuturesContractLifecyclePhase.EXPIRED
    if today >= delivery_date:
        return FuturesContractLifecyclePhase.IN_DELIVERY
    if today >= first_notice_date:
        return FuturesContractLifecyclePhase.IN_FIRST_NOTICE
    # Heuristic: "ACTIVE" = within 90 days of expiry; "LISTED" = longer-dated.
    if (expiry_date - today).days <= 90:
        return FuturesContractLifecyclePhase.ACTIVE
    return FuturesContractLifecyclePhase.LISTED


def _parse_futures_symbol(raw_symbol: str) -> tuple[str, int, int] | None:
    """Parse root, contract_month, contract_year from a CME/ICE raw symbol.

    Returns (root, month, year) or None if the symbol doesn't match the
    expected futures format (e.g. calendar spreads like "ESM6-ESU6" → None).
    """
    m = _FUTURES_SYM_RE.match(raw_symbol.strip())
    if not m:
        return None
    root = m.group("root")
    month_code = m.group("month")
    year_suffix = int(m.group("year"))
    month = _MONTH_CODE.get(month_code)
    if month is None:
        return None
    # Expand 1- or 2-digit year to 4-digit using 2000+.
    year = 2000 + year_suffix if year_suffix < 100 else year_suffix
    return root, month, year


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_futures_contracts(
    records: list[InstrumentRecord],
    *,
    today: date | None = None,
    reject_expired: bool = True,
) -> list[CanonicalFuturesContract]:
    """Convert a list of InstrumentRecord objects to CanonicalFuturesContract.

    Filters to ``InstrumentType.FUTURE`` records for tradfi venues, parses
    root/month/year from raw_symbol, and derives the 5 lifecycle dates.

    Args:
        records: InstrumentRecord list (typically the full Databento output).
        today: Reference date for lifecycle phase classification. Defaults to
            ``datetime.now(UTC).date()``.
        reject_expired: When True (default), skip EXPIRED/SETTLED contracts and
            emit a WARNING — they should have been filtered by the adapter.
            Pass False for historical backfill queries where expired contracts
            are intentionally included.

    Returns:
        List of CanonicalFuturesContract objects. Rows where the symbol cannot
        be parsed (calendar spreads, user-defined combos) are silently skipped
        with a DEBUG log.
    """
    if today is None:
        today = datetime.now(UTC).date()

    contracts: list[CanonicalFuturesContract] = []
    skipped = 0
    rejected = 0

    for record in records:
        if record.instrument_type != InstrumentType.FUTURE:
            continue
        if not record.expiry:
            # Cannot build CanonicalFuturesContract without expiry date.
            logger.debug(
                "Skipping futures record %s: missing expiry",
                record.raw_symbol,
            )
            skipped += 1
            continue

        parsed = _parse_futures_symbol(record.raw_symbol)
        if parsed is None:
            # Calendar spread or user-defined combo — skip silently.
            logger.debug(
                "Skipping futures record %s: symbol unparseable as root+month+year",
                record.raw_symbol,
            )
            skipped += 1
            continue

        root, contract_month, contract_year = parsed
        expiry_date = record.expiry.date() if isinstance(record.expiry, datetime) else record.expiry

        (
            last_trading_date,
            first_notice_date,
            delivery_date,
            settlement_date,
        ) = _derive_lifecycle_dates(root, expiry_date, contract_year, contract_month)

        lifecycle_phase = _classify_lifecycle_phase(
            today,
            expiry_date,
            last_trading_date,
            first_notice_date,
            delivery_date,
            settlement_date,
        )

        if reject_expired and lifecycle_phase in (
            FuturesContractLifecyclePhase.EXPIRED,
            FuturesContractLifecyclePhase.SETTLED,
        ):
            logger.warning(
                "Expiry guard: rejecting %s — phase=%s expiry=%s today=%s "
                "(should have been filtered by adapter; check _is_filtered_out)",
                record.raw_symbol,
                lifecycle_phase.value,
                expiry_date,
                today,
            )
            rejected += 1
            continue

        try:
            contract = CanonicalFuturesContract(
                venue=record.venue,
                root=root,
                contract_symbol=record.raw_symbol,
                contract_month=contract_month,
                contract_year=contract_year,
                expiry_date=expiry_date,
                last_trading_date=last_trading_date,
                first_notice_date=first_notice_date,
                delivery_date=delivery_date,
                settlement_date=settlement_date,
                lifecycle_phase=lifecycle_phase,
                tick_size=record.tick_size,
                contract_size=record.contract_size,
                listed_at=record.available_from_datetime,
            )
            contracts.append(contract)
        except Exception as exc:  # broad-except-ok: per-contract shard isolation
            logger.warning(
                "Failed to build CanonicalFuturesContract for %s: %s",
                record.raw_symbol,
                exc,
            )
            skipped += 1

    logger.info(
        "build_futures_contracts: built %d contracts, skipped %d, rejected %d expired/settled",
        len(contracts),
        skipped,
        rejected,
    )
    return contracts
