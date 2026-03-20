#!/usr/bin/env python3
"""Validate mock-generated instruments against real venue patterns.

Self-test that generates instruments via InstrumentGenerator, then validates
every instrument against the canonical format rules for each venue and
instrument type.

Checks:
    1. instrument_key matches {VENUE}:{INSTRUMENT_TYPE}:{SYMBOL}
    2. Options: $500 BTC strike intervals, BTC-{DDMMMYY}-{STRIKE}-{C|P} naming
    3. Futures: real expiry dates (last Friday of month for Deribit, quarterly for CME)
    4. DeFi: wrapped tokens (wETH not ETH for Aave aTokens), 42-char hex pool_address
    5. All: available_from_datetime is set and reasonable (before today)

Usage:
    instruments-service/.venv/bin/python scripts/validate_mock_instruments.py
    instruments-service/.venv/bin/python scripts/validate_mock_instruments.py --seed 42 --date 2025-01-15
"""

from __future__ import annotations

import argparse
import calendar
import re
import sys
from datetime import UTC, date, datetime, timedelta
from typing import Final

from unified_api_contracts import CanonicalInstrument, InstrumentType, OptionType
from unified_internal_contracts.testing.instrument_generator import InstrumentGenerator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BTC_STRIKE_INTERVAL: Final[int] = 500
_DERIBIT_EXPIRY_RE: Final[re.Pattern[str]] = re.compile(r"^BTC-(\d{1,2})([A-Z]{3})(\d{2})-(\d+)-(C|P)$")
_DERIBIT_FUTURE_RE: Final[re.Pattern[str]] = re.compile(r"^BTC-(\d{1,2})([A-Z]{3})(\d{2})$")
_CME_FUTURE_RE: Final[re.Pattern[str]] = re.compile(r"^ES([HMUZ])(\d{2})$")
_POOL_ADDRESS_RE: Final[re.Pattern[str]] = re.compile(r"^0x[0-9a-f]{40}$")
_INSTRUMENT_KEY_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Z0-9_-]+:[A-Z0-9_]+:.+$")
_MONTH_MAP: Final[dict[str, int]] = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_CME_MONTH_CODES: Final[dict[str, int]] = {
    "H": 3,
    "M": 6,
    "U": 9,
    "Z": 12,
}
_QUARTERLY_MONTHS: Final[set[int]] = {3, 6, 9, 12}

# Aave aToken underlyings must be wrapped tokens
_AAVE_WRAPPED_TOKENS: Final[set[str]] = {"WETH", "USDC", "USDT", "WBTC", "DAI"}


# ---------------------------------------------------------------------------
# Validation result tracking
# ---------------------------------------------------------------------------


class ValidationResult:
    """Tracks pass/fail for validation checks."""

    def __init__(self) -> None:
        self.total: int = 0
        self.passed: int = 0
        self.failed: int = 0
        self.errors: list[str] = []
        self.by_venue: dict[str, dict[str, int]] = {}
        self.by_type: dict[str, dict[str, int]] = {}

    def record(
        self,
        inst: CanonicalInstrument,
        check_name: str,
        passed: bool,
        detail: str = "",
    ) -> None:
        """Record a single check result."""
        self.total += 1
        venue = inst.venue
        itype = inst.instrument_type.value if inst.instrument_type else "UNKNOWN"

        if venue not in self.by_venue:
            self.by_venue[venue] = {"pass": 0, "fail": 0}
        if itype not in self.by_type:
            self.by_type[itype] = {"pass": 0, "fail": 0}

        if passed:
            self.passed += 1
            self.by_venue[venue]["pass"] += 1
            self.by_type[itype]["pass"] += 1
        else:
            self.failed += 1
            self.by_venue[venue]["fail"] += 1
            self.by_type[itype]["fail"] += 1
            msg = f"FAIL [{check_name}] {inst.instrument_key}: {detail}"
            self.errors.append(msg)

    @property
    def accuracy_pct(self) -> float:
        """Return pass percentage."""
        if self.total == 0:
            return 0.0
        return (self.passed / self.total) * 100.0


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def _last_friday_of_month(year: int, month: int) -> date:
    """Return the last Friday of the given month/year."""
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    offset = (d.weekday() - 4) % 7
    return d - timedelta(days=offset)


# ---------------------------------------------------------------------------
# Per-type validators
# ---------------------------------------------------------------------------


def validate_instrument_key(inst: CanonicalInstrument, result: ValidationResult) -> None:
    """Validate instrument_key matches VENUE:INSTRUMENT_TYPE:SYMBOL."""
    key = inst.instrument_key
    matched = bool(_INSTRUMENT_KEY_RE.match(key))
    result.record(inst, "key_format", matched, f"'{key}' does not match VENUE:TYPE:SYMBOL")

    # Cross-check components
    if inst.instrument_type is not None:
        expected_prefix = f"{inst.venue}:{inst.instrument_type.value}:"
        prefix_ok = key.startswith(expected_prefix)
        result.record(
            inst,
            "key_components",
            prefix_ok,
            f"key '{key}' does not start with '{expected_prefix}'",
        )


def validate_available_from(inst: CanonicalInstrument, result: ValidationResult) -> None:
    """Validate available_from_datetime is set and reasonable."""
    avail = inst.available_from_datetime
    has_avail = avail is not None
    result.record(inst, "available_from_set", has_avail, "available_from_datetime is None")

    if avail is not None:
        # Should be before "now" (generation time)
        is_past = avail <= datetime.now(UTC)
        result.record(
            inst,
            "available_from_past",
            is_past,
            f"available_from_datetime {avail.isoformat()} is in the future",
        )


def validate_options(inst: CanonicalInstrument, result: ValidationResult) -> None:
    """Validate options chain conventions."""
    if inst.instrument_type != InstrumentType.OPTION:
        return

    symbol = inst.symbol

    # Naming convention: BTC-{DDMMMYY}-{STRIKE}-{C|P}
    m = _DERIBIT_EXPIRY_RE.match(symbol)
    name_ok = m is not None
    result.record(
        inst,
        "option_naming",
        name_ok,
        f"symbol '{symbol}' does not match BTC-DDMMMYY-STRIKE-C|P",
    )

    if m is not None:
        # Strike must be at $500 intervals
        strike_str = m.group(4)
        strike_val = int(strike_str)
        interval_ok = strike_val % _BTC_STRIKE_INTERVAL == 0
        result.record(
            inst,
            "option_strike_interval",
            interval_ok,
            f"strike {strike_val} is not a ${_BTC_STRIKE_INTERVAL} interval",
        )

        # Expiry date must be a valid date
        day_str = m.group(1)
        month_str = m.group(2)
        year_str = m.group(3)
        month_num = _MONTH_MAP.get(month_str, 0)
        date_valid = month_num > 0
        result.record(
            inst,
            "option_expiry_month",
            date_valid,
            f"month '{month_str}' is not a valid month code",
        )

        if date_valid:
            exp_year = 2000 + int(year_str)
            exp_day = int(day_str)
            try:
                exp_date = date(exp_year, month_num, exp_day)
                # Should be a Friday
                is_friday = exp_date.weekday() == 4
                result.record(
                    inst,
                    "option_expiry_friday",
                    is_friday,
                    f"expiry {exp_date.isoformat()} is weekday {exp_date.weekday()} (not Friday=4)",
                )
            except ValueError:
                result.record(
                    inst,
                    "option_expiry_date",
                    False,
                    f"invalid date: {exp_year}-{month_num}-{exp_day}",
                )

        # C/P must match option_type
        cp_str = m.group(5)
        if inst.option_type is not None:
            expected_cp = "C" if inst.option_type == OptionType.CALL else "P"
            cp_match = cp_str == expected_cp
            result.record(
                inst,
                "option_type_match",
                cp_match,
                f"symbol suffix '{cp_str}' does not match option_type '{inst.option_type.value}'",
            )

    # Strike field should be set
    strike_set = inst.strike is not None
    result.record(inst, "option_strike_set", strike_set, "strike field is None for option")

    # option_type field should be set
    opt_type_set = inst.option_type is not None
    result.record(inst, "option_type_set", opt_type_set, "option_type field is None for option")

    # expiry field should be set
    expiry_set = inst.expiry is not None
    result.record(inst, "option_expiry_set", expiry_set, "expiry field is None for option")


def validate_futures(inst: CanonicalInstrument, result: ValidationResult) -> None:
    """Validate futures expiry dates use real venue conventions."""
    if inst.instrument_type != InstrumentType.FUTURE:
        return

    venue = inst.venue
    symbol = inst.symbol

    # Expiry must be set
    expiry_set = inst.expiry is not None
    result.record(inst, "future_expiry_set", expiry_set, "expiry field is None for future")

    if venue == "DERIBIT":
        # Deribit BTC futures: BTC-{DDMMMYY}
        m = _DERIBIT_FUTURE_RE.match(symbol)
        name_ok = m is not None
        result.record(
            inst,
            "deribit_future_naming",
            name_ok,
            f"symbol '{symbol}' does not match BTC-DDMMMYY",
        )

        if m is not None and inst.expiry is not None:
            day_str = m.group(1)
            month_str = m.group(2)
            year_str = m.group(3)
            month_num = _MONTH_MAP.get(month_str, 0)
            if month_num > 0:
                exp_year = 2000 + int(year_str)
                exp_day = int(day_str)
                try:
                    exp_date = date(exp_year, month_num, exp_day)
                    expected_lf = _last_friday_of_month(exp_year, month_num)
                    is_last_friday = exp_date == expected_lf
                    result.record(
                        inst,
                        "deribit_future_last_friday",
                        is_last_friday,
                        f"expiry {exp_date.isoformat()} is not the last Friday of {exp_year}-{month_num:02d} ({expected_lf.isoformat()})",
                    )
                    # Must be a quarterly month
                    is_quarterly = month_num in _QUARTERLY_MONTHS
                    result.record(
                        inst,
                        "deribit_future_quarterly",
                        is_quarterly,
                        f"month {month_num} is not a quarterly month (3,6,9,12)",
                    )
                except ValueError:
                    result.record(inst, "deribit_future_date", False, "invalid date")

    elif venue == "CME":
        # CME ES futures: ES{MONTH_CODE}{YY}
        m = _CME_FUTURE_RE.match(symbol)
        name_ok = m is not None
        result.record(
            inst,
            "cme_future_naming",
            name_ok,
            f"symbol '{symbol}' does not match ES{{H|M|U|Z}}{{YY}}",
        )

        if m is not None:
            month_code = m.group(1)
            year_str = m.group(2)
            month_num = _CME_MONTH_CODES.get(month_code, 0)
            code_ok = month_num > 0
            result.record(
                inst,
                "cme_future_month_code",
                code_ok,
                f"month code '{month_code}' is not valid (H,M,U,Z)",
            )

            if code_ok:
                # Verify it's a quarterly month
                is_quarterly = month_num in _QUARTERLY_MONTHS
                result.record(
                    inst,
                    "cme_future_quarterly",
                    is_quarterly,
                    f"month {month_num} is not a quarterly month",
                )


def validate_defi(inst: CanonicalInstrument, result: ValidationResult) -> None:
    """Validate DeFi-specific patterns."""
    venue = inst.venue
    itype = inst.instrument_type

    # Check Aave aToken underlyings are wrapped
    if venue in ("AAVE_V3", "AAVE_V3_ETH") and itype == InstrumentType.A_TOKEN:
        base = inst.base_asset or ""
        is_wrapped = base in _AAVE_WRAPPED_TOKENS
        result.record(
            inst,
            "aave_wrapped_token",
            is_wrapped,
            f"base_asset '{base}' should be a wrapped token (e.g. WETH not ETH)",
        )

    # DeFi pools/LSTs should have pool_address
    defi_venues = {
        "AAVE_V3",
        "AAVE_V3_ETH",
        "COMPOUND_V3_ETH",
        "UNISWAPV3-ETH",
        "UNISWAPV2-ETH",
        "UNISWAPV4-ETH",
        "LIDO",
        "ETHERFI",
        "MORPHO-ETHEREUM",
    }
    if venue in defi_venues:
        addr = inst.pool_address
        has_addr = addr is not None and len(addr) > 0
        result.record(
            inst,
            "defi_pool_address_set",
            has_addr,
            "pool_address is missing for DeFi instrument",
        )

        if has_addr and addr is not None:
            # Must be 42-char hex (0x + 40 hex)
            is_valid_hex = bool(_POOL_ADDRESS_RE.match(addr))
            result.record(
                inst,
                "defi_pool_address_format",
                is_valid_hex,
                f"pool_address '{addr}' is not a 42-char hex address (0x + 40 hex)",
            )


# ---------------------------------------------------------------------------
# Main validation
# ---------------------------------------------------------------------------


def validate_all(instruments: list[CanonicalInstrument]) -> ValidationResult:
    """Run all validation checks on a list of instruments."""
    result = ValidationResult()

    for inst in instruments:
        validate_instrument_key(inst, result)
        validate_available_from(inst, result)
        validate_options(inst, result)
        validate_futures(inst, result)
        validate_defi(inst, result)

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Validate mock-generated instruments against real venue patterns.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for InstrumentGenerator (default: 42)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default="2025-06-15",
        help="Reference date for generation (default: 2025-06-15)",
    )
    parser.add_argument(
        "--no-options-chain",
        action="store_true",
        help="Skip generating the options chain",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print all failure details",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for validation."""
    args = parse_args(argv)
    ref_date = date.fromisoformat(args.date)
    include_options = not args.no_options_chain

    print("=== Mock Instrument Validation ===")
    print(f"Seed: {args.seed}, Date: {ref_date.isoformat()}, Options: {include_options}")

    gen = InstrumentGenerator(seed=args.seed)
    instruments = gen.generate_all(
        ref_date=ref_date,
        include_options_chain=include_options,
        options_underlying="BTC",
    )
    print(f"Generated {len(instruments)} instruments")

    result = validate_all(instruments)

    # Report by venue
    print("\n--- Results by venue ---")
    for venue in sorted(result.by_venue):
        stats = result.by_venue[venue]
        total_v = stats["pass"] + stats["fail"]
        pct = (stats["pass"] / total_v * 100) if total_v > 0 else 0.0
        status = "PASS" if stats["fail"] == 0 else "FAIL"
        print(f"  {venue:<25s} {status:4s}  {stats['pass']}/{total_v} checks ({pct:.0f}%)")

    # Report by instrument_type
    print("\n--- Results by instrument_type ---")
    for itype in sorted(result.by_type):
        stats = result.by_type[itype]
        total_t = stats["pass"] + stats["fail"]
        pct = (stats["pass"] / total_t * 100) if total_t > 0 else 0.0
        status = "PASS" if stats["fail"] == 0 else "FAIL"
        print(f"  {itype:<25s} {status:4s}  {stats['pass']}/{total_t} checks ({pct:.0f}%)")

    # Print failures
    if result.errors:
        print(f"\n--- Failures ({len(result.errors)}) ---")
        limit = len(result.errors) if args.verbose else min(20, len(result.errors))
        for err in result.errors[:limit]:
            print(f"  {err}")
        if not args.verbose and len(result.errors) > 20:
            print(f"  ... and {len(result.errors) - 20} more (use --verbose to see all)")

    # Summary
    print("\n=== SUMMARY ===")
    print(f"Total checks: {result.total}")
    print(f"Passed: {result.passed}")
    print(f"Failed: {result.failed}")
    print(f"Accuracy: {result.accuracy_pct:.1f}%")

    if result.failed > 0:
        print("\nResult: FAIL")
        return 1

    print("\nResult: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
