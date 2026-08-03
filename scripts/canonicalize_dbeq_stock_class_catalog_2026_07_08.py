#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: prod/catalog.parquet has 0 NASDAQ/NYSE SPOT_PAIR rows (verified via
#              --dry-run reporting 0 candidates).
"""canonicalize_dbeq_stock_class_catalog_2026_07_08.py — one-off in-place catalog rewrite.

Issue: ``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
("224 securities double-keyed as both EQUITY and SPOT_PAIR").

Real root cause (confirmed 2026-07-08 via the real ``databento`` SDK's own ``InstrumentClass``
enum PLUS a live Databento definition-schema call for AAPL/SPY/IBIT on ``DBEQ.BASIC``): Databento's
real wire instrument_class for a plain stock is ``"K"`` (``InstrumentClass.STOCK``). The adapter's
``_CLASS_TO_TYPE`` map (``instruments_service/reference_data/adapters/tradfi/databento/symbology.py``)
mapped ``"K"`` to ``SPOT_PAIR`` (mislabeled "Forex spot" in the comment — FX spot pairs actually
come from a wholly separate static builder, never from this class map at all). This was a real,
LIVE, ongoing bug: the 2026-07-08 ``instrument_availability/by_date/day=2026-07-08/venue=NASDAQ/
instruments.parquet`` snapshot showed 100/100 rows typed SPOT_PAIR, zero EQUITY. Fixed in code
(``_CLASS_TO_TYPE["K"] = InstrumentType.EQUITY``); this script is the in-place historical rewrite
of the affected catalog rows, per this workspace's migration-mechanics decision (rewrite already-
captured rows in place, re-derived from the row's own already-stored fields — never re-download).

Real, measured scope (2026-07-08): 318 NASDAQ/NYSE ``SPOT_PAIR`` rows in ``prod/catalog.parquet``
(216 NYSE + 102 NASDAQ) are real single-stock/ETF tickers mistyped by the "K" bug — NOT the 11
genuine FX ``SPOT_PAIR`` rows (``venue=FX``, from the static Yahoo-sourced FX builder, unaffected
by this bug and NOT touched by this script). Of the 318, exactly 1 (``IBIT``) is a registered
``KNOWN_ETFS`` ticker and is reclassified ``ETF`` (matching the adapter's own downstream
``KNOWN_ETFS`` override); the remaining 317 are reclassified ``EQUITY``.

**Re-verified 2026-07-09** (this fix's actual completion pass, one calendar day later): the real,
live population is now **312** (ordinary day-to-day catalogue churn — confirmed stable across 2
back-to-back dry-runs), and 0 of the 312 match ``KNOWN_ETFS`` this time (``IBIT`` itself is not in
today's mistyped population — it may already have rolled through a fresher capture). As with the
CBOE/VX combo migration, treat the count this script logs at run time as authoritative, not either
historical figure above — always re-run ``--dry-run`` immediately before ``--apply``.

Honest scope note: ``ETHA`` (the other curated BTC/ETH spot-ETF ticker) is NOT currently in the
``KNOWN_ETFS`` registry, so this migration reclassifies it ``EQUITY`` rather than ``ETF`` — this
mirrors exactly what the FIXED adapter code itself would produce today (a separate,
smaller, non-blocking ``KNOWN_ETFS`` registry-completeness gap, not something this script papers
over or silently "fixes" beyond what the real code does).

GATE (refuse to --apply if violated): total row count UNCHANGED, and every OTHER (venue,
instrument_type) pair's row count UNCHANGED except (NASDAQ|NYSE, SPOT_PAIR) shrinking and
(NASDAQ|NYSE, EQUITY)/(NASDAQ, ETF) growing by the exact reclassified amount.

Usage::

    python scripts/canonicalize_dbeq_stock_class_catalog_2026_07_08.py            # dry-run (default)
    python scripts/canonicalize_dbeq_stock_class_catalog_2026_07_08.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from collections import Counter

import pandas as pd

logger = logging.getLogger(__name__)

_CATALOGUE_BLOB = "prod/catalog.parquet"
_SNAPSHOT_BLOB = "prod/snapshots/pre_dbeq_stock_class_canon_2026_07_08.parquet"


def _bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="tradfi")


def _counts(df: pd.DataFrame) -> dict[tuple[str, str], int]:
    key = list(zip(df["venue"].fillna(""), df["instrument_type"].fillna(""), strict=True))
    return dict(Counter(key))


def classify(df: pd.DataFrame) -> pd.Series:
    """Real NASDAQ/NYSE SPOT_PAIR rows — the "K" (STOCK) mismap population."""
    venue = df["venue"].fillna("").astype(str)
    itype = df["instrument_type"].fillna("").astype(str)
    return (venue.isin(["NASDAQ", "NYSE"])) & (itype == "SPOT_PAIR")


def run(*, apply: bool) -> int:
    from unified_api_contracts import KNOWN_ETFS
    from unified_trading_library import get_storage_client

    bucket = _bucket()
    st = get_storage_client()
    logger.info("DBEQ stock-class catalog canonicalization: bucket=%s blob=%s apply=%s", bucket, _CATALOGUE_BLOB, apply)
    raw_bytes = st.download_bytes(bucket, _CATALOGUE_BLOB)
    before = pd.read_parquet(io.BytesIO(raw_bytes))
    n0 = len(before)
    counts0 = _counts(before)
    logger.info("BEFORE: rows=%d", n0)

    target = classify(before)
    is_etf = before["raw_symbol"].fillna("").isin(KNOWN_ETFS)
    to_etf = target & is_etf
    to_equity = target & ~is_etf
    logger.info(
        "reclassifying %d NASDAQ/NYSE SPOT_PAIR rows: %d -> EQUITY, %d -> ETF (KNOWN_ETFS: %s)",
        int(target.sum()),
        int(to_equity.sum()),
        int(to_etf.sum()),
        sorted(before.loc[to_etf, "raw_symbol"].tolist()),
    )

    after = before.copy()
    after.loc[to_equity, "instrument_type"] = "EQUITY"
    after.loc[to_equity, "instrument_id"] = after.loc[to_equity, "instrument_id"].str.replace(
        ":SPOT_PAIR:", ":EQUITY:", n=1, regex=False
    )
    after.loc[to_etf, "instrument_type"] = "ETF"
    after.loc[to_etf, "instrument_id"] = after.loc[to_etf, "instrument_id"].str.replace(
        ":SPOT_PAIR:", ":ETF:", n=1, regex=False
    )

    counts1 = _counts(after)
    rows_ok = len(after) == n0
    expected_changed = {
        ("NASDAQ", "SPOT_PAIR"),
        ("NYSE", "SPOT_PAIR"),
        ("NASDAQ", "EQUITY"),
        ("NYSE", "EQUITY"),
        ("NASDAQ", "ETF"),
    }
    drift = {
        k: (counts0.get(k, 0), counts1.get(k, 0))
        for k in set(counts0) | set(counts1)
        if counts0.get(k, 0) != counts1.get(k, 0) and k not in expected_changed
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
