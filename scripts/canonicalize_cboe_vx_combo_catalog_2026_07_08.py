#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: prod/catalog.parquet has 0 CBOE SPOT_PAIR rows carrying a " - "-joined
#              multi-leg raw_symbol (verified via --dry-run reporting 0 candidates).
"""canonicalize_cboe_vx_combo_catalog_2026_07_08.py — one-off in-place catalog rewrite.

Plan: ``unified-trading-pm/plans/active/canonical_id_p1_tradfi_combo_leg_canonicalization_2026_07_08.md``
Issue: ``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md`` (finding 7).

Real bug (confirmed against ``prod/catalog.parquet``, 2026-07-08): CBOE/XCBF.PITCH VX calendar
spreads/butterflies were captured, before the 2026-06-25 G1.c fix, as mis-typed ``SPOT_PAIR`` rows
with a whitespace-padded-dash leg separator baked into the ``instrument_id`` payload — e.g.
``CBOE:SPOT_PAIR:VX/F1:1:S - VX/G1:1:B``. The 2026-07-08 code fix
(``instruments_service/reference_data/adapters/tradfi/databento/{symbology,adapter}.py``) routes
these through the same ``InstrumentLeg``/``InstrumentType.COMBO`` pathway already proven for CME,
so any FUTURE capture will land correctly. This script is the operator-decided (2026-07-08,
``instrument_id_format_canonicalization_2026_07_08.md`` § migration mechanics) in-place rewrite of
the HISTORICAL rows already sitting in the catalogue — re-derived from the row's own already-stored
``raw_symbol``, never re-downloaded from Databento.

Real, measured scope — **re-verified 2026-07-09, and volatile day-to-day; always re-run --dry-run
immediately before --apply, never trust a stale count**: the originating finding (2026-07-08) quoted
``34,017``/``4,211``/``5``; a same-day catalog read found that wrong and measured 4,211 (2-leg) + 5
(3-leg) = 4,216 instead (matching the ``databento/adapter.py`` G1.c fix comment's own "4,216 mis-typed
SPOT_PAIR rows"). A FRESH read one calendar day later (2026-07-09, this fix's actual completion pass)
found the real population had shrunk to **91 rows, all 2-leg, 0 three-leg** — confirmed stable across 2
back-to-back dry-runs. This is real, expected volatility, not a bug: ``prod/catalog.parquet`` is a
**lifecycle** catalogue (see the docstring below) and VX calendar spreads are short-dated instruments
(weekly/monthly expiries) — yesterday's spread population rolls off as those specific contracts expire,
while today's freshly-listed spreads are still landing under the pre-fix "dropped, not decomposed"
behavior (this script's own code fix not yet deployed anywhere) and so don't yet appear as CBOE
SPOT_PAIR rows to reclassify at all. Treat the count this script logs at run time as the only
authoritative number — do not hardcode expectations from either historical figure above.

Scope of this fix, and its honest limit:

* ``prod/catalog.parquet`` is the ONE rolled-up lifecycle-catalogue file this script rewrites —
  it is a real, load-bearing artifact (expected-universe enumeration, every audit in this
  investigation cites it) but its schema (``InstrumentCatalogEntry``) does NOT carry an
  ``instrument_key``/``legs`` column at all (only ``instrument_id``/``instrument_type``/
  ``raw_symbol``/etc.) — so this script can only correct ``instrument_type`` (``SPOT_PAIR`` →
  ``COMBO``) and ``instrument_id`` (the ``SPOT_PAIR`` token inside the payload → ``COMBO``); the
  raw_symbol payload itself is preserved verbatim (it is NOT rewritten into per-leg human-name
  form — that transformation only exists in the structured ``legs`` field the FIXED adapter code
  now produces for future captures, which lives in the separate ``instrument_availability/by_date/
  day=*/venue=CBOE/instruments.parquet`` per-day definitional snapshots, not this rolled-up file).
* The full per-day snapshot corpus (thousands of ``by_date`` parquet files, 2015-present) is NOT
  rewritten by this script — that is a separate, larger full-corpus walk gated by this workspace's
  single-walk discipline (any new whole-corpus GCS walk is review-blocking) and is out of scope
  for this pass. Flagged as a real, deliberate scoping decision, not a silent gap: see the plan's
  Progress Log.

GATE (refuse to --apply if violated): total row count UNCHANGED, and every OTHER
(venue, instrument_type) pair's row count UNCHANGED — this migration ONLY ever reclassifies
CBOE SPOT_PAIR rows whose raw_symbol is a real ``" - "``-joined multi-leg spread (verified via the
same :func:`_parse_cboe_spread_legs` the fixed adapter code uses, so "this row would decompose
under the new code" is the actual reclassification criterion, not a heuristic re-implementation).

Usage::

    python scripts/canonicalize_cboe_vx_combo_catalog_2026_07_08.py            # dry-run (default)
    python scripts/canonicalize_cboe_vx_combo_catalog_2026_07_08.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from collections import Counter

import pandas as pd

from instruments_service.reference_data.adapters.tradfi.databento import _parse_cboe_spread_legs

logger = logging.getLogger(__name__)

_CATALOGUE_BLOB = "prod/catalog.parquet"
_SNAPSHOT_BLOB = "prod/snapshots/pre_cboe_vx_combo_canon_2026_07_08.parquet"


def _bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="tradfi")


def _counts(df: pd.DataFrame) -> dict[tuple[str, str], int]:
    key = list(zip(df["venue"].fillna(""), df["instrument_type"].fillna(""), strict=True))
    return dict(Counter(key))


def classify(df: pd.DataFrame) -> pd.Series:
    """Real-decomposable CBOE SPOT_PAIR spread rows — same predicate the fixed adapter uses."""
    venue = df["venue"].fillna("").astype(str)
    itype = df["instrument_type"].fillna("").astype(str)
    raw = df["raw_symbol"].fillna("").astype(str)
    is_cboe_spot_pair = (venue == "CBOE") & (itype == "SPOT_PAIR")
    decomposable = raw.apply(lambda s: _parse_cboe_spread_legs(s) is not None if s else False)
    return is_cboe_spot_pair & decomposable


def run(*, apply: bool) -> int:
    from unified_trading_library import get_storage_client

    bucket = _bucket()
    st = get_storage_client()
    logger.info("CBOE VX combo catalog canonicalization: bucket=%s blob=%s apply=%s", bucket, _CATALOGUE_BLOB, apply)
    raw_bytes = st.download_bytes(bucket, _CATALOGUE_BLOB)
    before = pd.read_parquet(io.BytesIO(raw_bytes))
    n0 = len(before)
    counts0 = _counts(before)
    logger.info("BEFORE: rows=%d", n0)

    target = classify(before)
    logger.info(
        "CBOE SPOT_PAIR spread rows to reclassify: %d (of %d total CBOE SPOT_PAIR rows)",
        int(target.sum()),
        int(((before["venue"].fillna("") == "CBOE") & (before["instrument_type"].fillna("") == "SPOT_PAIR")).sum()),
    )
    leg_count = before.loc[target, "raw_symbol"].fillna("").apply(lambda s: s.count(" - ") + 1)
    logger.info("leg-count distribution among reclassified rows: %s", dict(Counter(leg_count)))

    after = before.copy()
    after.loc[target, "instrument_type"] = "COMBO"
    after.loc[target, "instrument_id"] = after.loc[target, "instrument_id"].str.replace(
        ":SPOT_PAIR:", ":COMBO:", n=1, regex=False
    )

    counts1 = _counts(after)
    # GATE: total rows unchanged, and every (venue, instrument_type) pair EXCEPT
    # (CBOE, SPOT_PAIR) / (CBOE, COMBO) is unchanged.
    rows_ok = len(after) == n0
    drift = {
        k: (counts0.get(k, 0), counts1.get(k, 0))
        for k in set(counts0) | set(counts1)
        if counts0.get(k, 0) != counts1.get(k, 0) and k not in {("CBOE", "SPOT_PAIR"), ("CBOE", "COMBO")}
    }
    gate_ok = rows_ok and not drift
    logger.info(
        "GATE: rows %d->%d (ok=%s) | unexpected (venue,type) drift=%s => %s",
        n0,
        len(after),
        rows_ok,
        drift,
        "OK" if gate_ok else "VIOLATION",
    )
    if not gate_ok:
        logger.error("GATE FAILED — aborting before write.")
        return 2

    if not apply:
        logger.info("DRY RUN — live catalogue NOT modified. Re-run with --apply to write.")
        return 0

    if not st.blob_exists(bucket, _SNAPSHOT_BLOB):
        st.upload_bytes(bucket, _SNAPSHOT_BLOB, raw_bytes)
        logger.info("snapshot written: gs://%s/%s", bucket, _SNAPSHOT_BLOB)
    else:
        logger.info("snapshot already exists: gs://%s/%s (kept)", bucket, _SNAPSHOT_BLOB)

    buf = io.BytesIO()
    after.to_parquet(buf, index=False)
    st.upload_bytes(bucket, _CATALOGUE_BLOB, buf.getvalue())
    logger.info("APPLIED — live catalogue written: gs://%s/%s (%d rows)", bucket, _CATALOGUE_BLOB, len(after))
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    p.add_argument("--apply", action="store_true", default=False, help="Write back (default: dry-run)")
    args = p.parse_args(argv)
    return run(apply=args.apply)


if __name__ == "__main__":
    sys.exit(main())
