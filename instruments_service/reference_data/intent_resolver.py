"""Resolve StrategyInstrumentIntent to specific instrument IDs from GCS catalog."""

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
