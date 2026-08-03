#!/usr/bin/env python3
# Epic: defi_consolidated_closeout
# Lifecycle: oneoff
# Delete-when: after the defi catalogue frozen tail carries 0 cluster-date false available_to + honest_cov re-measured
"""Purge the PROVEN-FALSE DeFi ``available_to`` stamps the roll-up cannot re-derive.

The ``available_to`` false-delisting fix (``instruments-service@c37d4f96``) makes a DeFi
drop-out stay live, and a ``--mode full`` regen re-derived every instrument PRESENT in the
``by_date`` corpus (verified: PANCAKESWAP_V3 POOL 135→1, MORPHO LENDING 895→0). But the full
rebuild's FROZEN-TAIL merge (``close_absent=False``) copies through, UNCHANGED, every prev-
catalogue row that no longer appears in any ``by_date`` snapshot — 2,345 rows — so those keep
their PRE-FIX (false) ``available_to``. Measured post-regen: 2,349 defi rows still carry a
non-blank ``available_to``, and **2,244 (95.5%) sit on the three PROVEN-FALSE cluster dates**.

Those three dates are proven pipeline/capture events, not delistings: each is a single calendar
day on which a whole cohort of LONG-LIVED instruments (first-seen back to 2020-2021) across
FOUR UNRELATED protocols (TRADER_JOE_V2 / PANCAKESWAP_V3 / AAVE_V3 / MORPHO) simultaneously
stopped appearing. Independent protocols do not delist on the same day. Full evidence:
``plans/active/issues/defi_catalogue_available_to_false_delisting_2026_07_20.md``.

This clears ``available_to`` → null for defi rows stamped on EXACTLY those dates, restoring the
Option A semantics the fix establishes: a DeFi instrument is LIVE unless a genuine truth signal
(``delisted_at`` / ``expiry``) or the on-chain removal probe (Option B) says otherwise.
DELIBERATELY NARROW — the 105 non-cluster residual rows (2026-05-08, 2024-01-09, scattered) are
LEFT ALONE (they may be genuine); only the proven-false cohort is purged.

Row count is unchanged (values only), so the catalogue monotonic guard is unaffected.
Backup-then-write, idempotent, ``--dry-run``/``--apply``.

Usage:
  python scripts/purge_defi_false_available_to_2026_07_20.py --dry-run
  python scripts/purge_defi_false_available_to_2026_07_20.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_CATALOG_BLOB = "prod/catalog.parquet"

#: The three PROVEN-FALSE cluster dates (cross-protocol, long-lived cohorts — see module docstring).
FALSE_CLUSTER_DATES: frozenset[str] = frozenset({"2026-06-26", "2026-07-06", "2026-07-08"})


def _catalogue_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(blob: str, run_ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{run_ts}.falsedelist.bak.parquet"
    return f"{blob}.{run_ts}.falsedelist.bak"


def _nonblank_count(df: pd.DataFrame) -> int:
    """Rows with a real (non-null, non-empty) ``available_to``. ``astype(str)`` alone is wrong —
    it renders null as the literal ``"None"``/``"nan"``, which counts as non-blank."""
    if "available_to" not in df.columns:
        return 0
    col = df["available_to"]
    s = col.astype(str).str.strip()
    return int((col.notna() & (s != "") & (s != "None") & (s != "nan") & (s != "NaT")).sum())


def purge_frame(df: pd.DataFrame, dates: frozenset[str] = FALSE_CLUSTER_DATES) -> tuple[pd.DataFrame, int]:
    """Clear ``available_to`` for rows stamped on a proven-false cluster date. Pure + idempotent."""
    if df.empty or "available_to" not in df.columns:
        return df, 0
    at10 = df["available_to"].astype(str).str.strip().str[:10]
    hit = at10.isin(dates)
    n = int(hit.sum())
    if n == 0:
        return df, 0
    work = df.copy()
    work.loc[hit, "available_to"] = None
    return work, n


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    apply_write = bool(args.apply)

    bucket = _catalogue_bucket()
    storage = get_storage_client()
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    raw = storage.download_bytes(bucket, _CATALOG_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    before = _nonblank_count(df)
    out, n = purge_frame(df)
    after = _nonblank_count(out)
    logger.info(
        "defi catalogue %d rows: non-blank available_to %d → %d (purged %d on %s) mode=%s",
        len(df),
        before,
        after,
        n,
        sorted(FALSE_CLUSTER_DATES),
        "APPLY" if apply_write else "DRY-RUN",
    )
    if n == 0:
        logger.info("nothing to purge — already clean (idempotent)")
        return 0
    if not apply_write:
        logger.info("[dry-run] would rewrite gs://%s/%s (row count unchanged: %d)", bucket, _CATALOG_BLOB, len(out))
        return 0
    backup = _backup_path(_CATALOG_BLOB, run_ts)
    storage.upload_bytes(bucket, backup, raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    storage.upload_bytes(bucket, _CATALOG_BLOB, buf.read())
    logger.info("backup → gs://%s/%s ; rewrote %d rows (purged %d false available_to)", bucket, backup, len(out), n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
