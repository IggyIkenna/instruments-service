"""Resolve StrategyInstrumentIntent to specific instrument IDs from GCS catalog.

Includes ``validate_resolved_instruments`` to enforce that resolved instrument
IDs satisfy strategy expectations. Raises ``ValueError`` on hard failures
(missing critical currencies) instead of silently continuing, aligning with
the rule: "no silent skip when instruments are missing".
"""

import logging

from unified_api_contracts.internal.domain.strategy_service import (
    ResolvedInstruments,
    StrategyInstrumentIntent,
)

logger = logging.getLogger(__name__)


def resolve_instruments(
    intent: StrategyInstrumentIntent,
    available_instruments: list[dict[str, str]],
    date: str,
) -> ResolvedInstruments:
    """Resolve an intent against available instruments for a given date.

    Args:
        intent: What the strategy needs.
        available_instruments: List of instrument dicts with at least 'instrument_id',
            'venue', 'instrument_type', 'base_currency' keys.
        date: Date string (YYYY-MM-DD) for resolution.

    Returns:
        ResolvedInstruments with matching IDs.
    """
    matched: list[str] = []
    missing: list[str] = []

    for currency in intent.base_currencies:
        currency_matched = False
        for inst in available_instruments:
            inst_id = inst.get("instrument_id", "")
            venue = inst.get("venue", "")
            inst_type = inst.get("instrument_type", "")
            base = inst.get("base_currency", "")

            # Check protocol match (venue contains protocol)
            if intent.protocol and intent.protocol.upper() not in venue.upper():
                continue

            # Check chain match (venue contains chain)
            if intent.chain and intent.chain.upper() not in venue.upper():
                continue

            # Check currency match
            if currency.upper() != base.upper():
                continue

            # Check instrument type filter
            if intent.instrument_types and inst_type.upper() not in [t.upper() for t in intent.instrument_types]:
                continue

            # Check venue filter
            if intent.venue_filter and venue not in intent.venue_filter:
                continue

            matched.append(inst_id)
            currency_matched = True

        if not currency_matched:
            missing.append(currency)

    if missing:
        logger.warning(
            "Intent resolution for %s/%s: missing currencies %s on date %s",
            intent.protocol,
            intent.chain,
            missing,
            date,
        )

    logger.info(
        "Resolved %d instruments for %s/%s (%d currencies, %d missing)",
        len(matched),
        intent.protocol,
        intent.chain,
        len(intent.base_currencies),
        len(missing),
    )

    return ResolvedInstruments(
        intent=intent,
        instrument_ids=matched,
        date=date,
        missing_currencies=missing,
    )


def validate_resolved_instruments(
    resolved: ResolvedInstruments,
    *,
    required_currencies: list[str] | None = None,
    min_instruments: int = 1,
    raise_on_missing: bool = True,
) -> list[str]:
    """Validate that resolved instruments satisfy strategy expectations.

    Logs a warning for every missing currency and raises ``ValueError`` when
    required instruments are absent (unless ``raise_on_missing=False``).

    This enforces the rule: *if instruments-service can't resolve what the
    strategy expects, that's an error — not a silent skip*.

    Args:
        resolved: Result of a ``resolve_instruments()`` call.
        required_currencies: Currencies that MUST have at least one matching
            instrument. When ``None``, defaults to all ``resolved.intent.base_currencies``.
        min_instruments: Minimum total number of resolved instruments required.
        raise_on_missing: When ``True`` (default), raises ``ValueError`` on
            any validation failure. When ``False``, only warns.

    Returns:
        List of validation warning messages (empty when all checks pass).

    Raises:
        ValueError: When ``raise_on_missing=True`` and required currencies
            are missing or ``min_instruments`` is not met.
    """
    required = set(required_currencies or resolved.intent.base_currencies)
    missing_required = required & set(resolved.missing_currencies)
    warnings: list[str] = []

    for currency in sorted(missing_required):
        msg = (
            f"Required currency {currency!r} has no matching instruments for "
            f"{resolved.intent.protocol}/{resolved.intent.chain} on {resolved.date}"
        )
        logger.warning("%s", msg)
        warnings.append(msg)

    if len(resolved.instrument_ids) < min_instruments:
        msg = (
            f"Only {len(resolved.instrument_ids)} instruments resolved for "
            f"{resolved.intent.protocol}/{resolved.intent.chain} on {resolved.date}; "
            f"minimum required: {min_instruments}"
        )
        logger.warning("%s", msg)
        warnings.append(msg)

    if warnings and raise_on_missing:
        raise ValueError(
            f"Instrument resolution failed for intent "
            f"{resolved.intent.protocol}/{resolved.intent.chain}: " + "; ".join(warnings)
        )

    return warnings
