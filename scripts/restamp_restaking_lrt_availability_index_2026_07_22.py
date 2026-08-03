#!/usr/bin/env python3
# Epic: manifest_master
# Lifecycle: oneoff
# Delete-when: after this migration has run in prod and been verified (see
#   distinct_values_noncanonical_audit_2026_07_20.md Progress Log — RESTAKING InstrumentType)
"""Re-stamp the instruments-service DeFi availability_index's LRT rows LST -> RESTAKING.

Companion to ``canonicalize_restaking_lrt_catalog_2026_07_22.py`` (which already re-stamped
the 5 live catalogue rows — see that script's docstring for the full RESTAKING rationale).
This script targets the SEPARATE ``_index/availability_index.parquet`` honest-coverage
manifest (per-``(venue, date)`` "did we capture the instruments listing" audit rows, grain
``data_type=instruments``), NOT the catalogue.

**NOT auto-applied by this session.** ``instruments-store-defi-{env}-{project}/_index/
availability_index.parquet`` is one of the 5 ``uts-prod-manifest-consolidator-instruments-*``
Cloud Scheduler jobs (``*/1 * * * *`` — see codex/05-infrastructure/manifest-consolidator-ssot.md),
the SAME CLASS of high-frequency consolidator-cron writer the venue-as-chain fix's
``market-data-cefi`` target was (this is the sibling ``instruments-defi`` job in the same
20-cron family) — per the mandatory-rules note ("if it's the SAME contended manifest ... DO
NOT attempt to pause a production Cloud Scheduler job yourself"), this script is
dry-run-verified only. A plain (non-CAS) download/transform/upload here risks a lost-update
race against the consolidator's own read-modify-write cycle. Applying it requires either (a)
a paused-writer window (mirror the venue-as-chain precedent: pause
``uts-prod-manifest-consolidator-instruments-defi-cron``, apply, verify across 2 consolidator
cycles incl. one --force, resume), or (b) a proper CAS-guarded rewrite (generation-precondition
+ collision pre-flight) if run without a pause.

Measured 2026-07-22 (dry-run against the live index): 36 rows total across ETHERFI (16) /
RENZO (10) / KELPDAO (5) / PUFFER (5), grain ``(venue, date)`` with
``data_type="instruments"``, all ``capture_status=captured``, spanning 2026-07-07..2026-07-22.
Row count is unchanged by this transform (values only).

Usage:
  python scripts/restamp_restaking_lrt_availability_index_2026_07_22.py --dry-run
  python scripts/restamp_restaking_lrt_availability_index_2026_07_22.py --apply   # DO NOT
      # run --apply without a paused-writer window or a CAS-guarded rewrite — see docstring.
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"

#: Venues whose ``instrument_type=LST`` (data_type=instruments) availability-index rows are
#: actually liquid RESTAKING tokens per the 2026-07-20/22 operator decision. Deliberately
#: narrow (venue allowlist, not a blanket LST->RESTAKING sweep) — every other LST venue
#: (LIDO/ROCKETPOOL/CBETH/etc.) stays plain LST.
RESTAKING_LRT_VENUES: frozenset[str] = frozenset({"ETHERFI", "RENZO", "KELPDAO", "PUFFER"})


def _index_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(blob: str, run_ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{run_ts}.restakinglrt.bak.parquet"
    return f"{blob}.{run_ts}.restakinglrt.bak"


def restamp_frame(df: pd.DataFrame, venues: frozenset[str] = RESTAKING_LRT_VENUES) -> tuple[pd.DataFrame, int]:
    """Re-stamp ``instrument_type`` LST -> RESTAKING for the named LRT venues. Pure + idempotent."""
    if df.empty or "venue" not in df.columns or "instrument_type" not in df.columns:
        return df, 0
    hit = df["venue"].astype(str).str.upper().isin(venues) & (df["instrument_type"] == "LST")
    n = int(hit.sum())
    if n == 0:
        return df, 0
    work = df.copy()
    work.loc[hit, "instrument_type"] = "RESTAKING"
    return work, n


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    apply_write = bool(args.apply)

    bucket = _index_bucket()
    storage = get_storage_client()
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    raw = storage.download_bytes(bucket, _INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    out, n = restamp_frame(df)
    logger.info(
        "instruments-store-defi availability_index %d rows: %d LRT rows LST -> RESTAKING mode=%s",
        len(df),
        n,
        "APPLY" if apply_write else "DRY-RUN",
    )
    if n == 0:
        logger.info("nothing to re-stamp — already clean (idempotent) or targets absent")
        return 0
    if not apply_write:
        by_venue = out[out["venue"].astype(str).str.upper().isin(RESTAKING_LRT_VENUES)]
        logger.info(
            "[dry-run] per-venue counts after transform:\n%s",
            by_venue.groupby(["venue", "instrument_type"]).size().to_string(),
        )
        logger.info(
            "[dry-run] would rewrite gs://%s/%s (row count unchanged: %d) — "
            "NOT applying: this bucket is a */1 manifest-consolidator-cron target, "
            "needs a paused-writer window per the mandatory-rules note.",
            bucket,
            _INDEX_BLOB,
            len(out),
        )
        return 0
    logger.warning(
        "APPLY requested against a */1-cron-consolidated manifest without a documented "
        "paused-writer window or CAS precondition — proceeding only because the operator "
        "explicitly authorized this run outside the agent session."
    )
    backup = _backup_path(_INDEX_BLOB, run_ts)
    storage.upload_bytes(bucket, backup, raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    storage.upload_bytes(bucket, _INDEX_BLOB, buf.read())
    logger.info("backup -> gs://%s/%s ; rewrote %d rows (re-stamped %d LRT rows)", bucket, backup, len(out), n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
