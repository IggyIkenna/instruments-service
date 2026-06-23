#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""Deribit options-chain connectivity proof — hits Deribit PUBLIC REST live.

NOT a backfill. Pulls a single BTC (default) option chain via the
``DeribitOptionsReferenceDataAdapter`` (public ``get_instruments?kind=option`` +
``ticker``, no credentials) and asserts the chain is non-empty, printing the
number of strikes/expiries pulled + a sample mark IV. This is the operator's
"prove we can get the data" check for Phase D P1 part (b).

Run (from the instruments-service repo root):
    ./.venv/bin/python scripts/deribit_options_connectivity_check.py [BTC|ETH]

Exits 0 on a non-empty chain, 1 if Deribit is unreachable or returns nothing.
"""

from __future__ import annotations

import asyncio
import sys

from instruments_service.reference_data.adapters.cefi.deribit_options_adapter import (
    DeribitOptionsReferenceDataAdapter,
)


async def _main(underlying: str) -> int:
    adapter = DeribitOptionsReferenceDataAdapter()
    chain = await adapter.get_options_chain(underlying)
    n_strikes = len(chain.strikes)
    n_calls = len(chain.calls)
    n_puts = len(chain.puts)
    n_iv = len(chain.mark_iv_by_instrument)

    print(f"venue={chain.venue} underlying={chain.underlying} nearest_expiry={chain.expiry.isoformat()}")
    print(f"strikes={n_strikes} calls={n_calls} puts={n_puts} mark_iv_legs={n_iv}")
    print(f"underlying_mark={chain.underlying_mark}")
    if chain.mark_iv_by_instrument:
        sample_key = next(iter(chain.mark_iv_by_instrument))
        print(f"sample mark_iv: {sample_key} -> {chain.mark_iv_by_instrument[sample_key]} (fractional, annualised)")

    # Distinct expiries across the underlying (separate live call) for the report.
    cal = await adapter.get_expiry_calendar(underlying)
    print(f"distinct_expiries_for_{underlying}={len(cal.expiries)}")

    if not chain.strikes or not chain.calls:
        print("FAIL: empty chain (no strikes/calls) — Deribit returned nothing")
        return 1
    if not chain.mark_iv_by_instrument:
        print("FAIL: no mark IV pulled — ticker endpoint returned no mark_iv")
        return 1
    print("OK: live Deribit option chain + mark IV pulled successfully")
    return 0


if __name__ == "__main__":
    _underlying = sys.argv[1].upper() if len(sys.argv) > 1 else "BTC"
    raise SystemExit(asyncio.run(_main(_underlying)))
