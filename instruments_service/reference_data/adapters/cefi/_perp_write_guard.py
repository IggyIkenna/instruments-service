"""Write-time guardrail for cefi `*-PERP` venue reference-data adapters.

Root cause this closes: KALSHI-PERP's parser used to treat an empty/null
``category`` field as a pass, which let Kalshi's binary event-contract universe
(``KXMVESPORTSMULTIGAMEEXTENDED``, ``KXMVECROSSCATEGORY``, ...) through as fake
``PERPETUAL`` rows — 25,473 contaminated cefi catalogue rows (see
``kalshi_perp.py`` module docstring,
``plans/active/prediction_phase_ab_residuals_2026_07_24.md``). The per-adapter
category check fixes the specific field that failed; this module is the
second, independent layer — a `*-PERP` venue's own parser must call
``validate_perp_instrument_record`` on every ``InstrumentRecord`` it builds, so
a record is rejected at write time even if a future field-mapping regression
(wrong host, renamed field, upstream schema change) reintroduces the same class
of bug through a different door.
"""

from unified_api_contracts.internal import InstrumentRecord, InstrumentType

# Kalshi's prediction-market series stamp these prefixes on binary event-contract
# tickers. A genuine crypto-perp ticker (KXBTCUSD-PERP, BTC-PERPETUAL, BTC-USD, ...)
# never starts with them — any ticker that does is an event contract that leaked
# into a `*-PERP` feed, not a perpetual future.
EVENT_CONTRACT_TICKER_PREFIXES: tuple[str, ...] = ("KXMVE",)


class PerpRecordRejectedError(ValueError):
    """A `*-PERP` venue's InstrumentRecord failed the write-time sanity check."""


def validate_perp_instrument_record(record: InstrumentRecord) -> None:
    """Guardrail for any `*-PERP` venue's InstrumentRecord — call this on every
    record a `*-PERP` adapter's parser builds, before returning it, so a bad row
    is rejected at the writer rather than silently accepted into the catalogue.

    Raises PerpRecordRejectedError if:
      - ``instrument_type`` is not PERPETUAL (a `*-PERP` venue must only ever
        emit perpetual futures), or
      - ``instrument_key`` matches a known event-contract ticker prefix (the
        exact pattern that produced the KALSHI-PERP contamination).
    """
    if record.instrument_type != InstrumentType.PERPETUAL:
        raise PerpRecordRejectedError(
            f"{record.venue}: non-PERPETUAL instrument_type={record.instrument_type!r} "
            f"on a *-PERP venue feed (ticker={record.instrument_key!r})"
        )
    ticker = record.instrument_key.upper()
    for prefix in EVENT_CONTRACT_TICKER_PREFIXES:
        if ticker.startswith(prefix):
            raise PerpRecordRejectedError(
                f"{record.venue}: ticker {record.instrument_key!r} matches event-contract "
                f"prefix {prefix!r} — rejecting fake PERPETUAL (see the KALSHI-PERP "
                "contamination incident, kalshi_perp.py module docstring)"
            )
