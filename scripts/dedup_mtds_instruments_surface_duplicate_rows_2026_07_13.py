#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 service_name=market-tick-data-service rows with a canonical instruments-service
#   identity twin remain in instruments-store-sports-{env} (verified post-apply)
"""dedup_mtds_instruments_surface_duplicate_rows_2026_07_13.py — drop the
683,592-row ``service_name=market-tick-data-service`` duplicate twins that
``rebuild_sports_manifest_v9.py --surface instruments`` accidentally wrote
into instruments-service's OWN reference-data manifest during the
2026-07-13 sports_manifest_canonicalisation E4 apply-pass (16-VM migration
fleet), since a canonical ``service_name=instruments-service`` sibling
already covers the same cell.

ROOT CAUSE (fixed going forward at ``market-tick-data-service@55f9e961``):
the rebuild script always stamped ``service_name="market-tick-data-service"``
regardless of ``--surface``, so rebuilding the ``instruments`` surface
re-emitted every existing row under the wrong service_name with a blank
asset_group.

A FIRST ATTEMPT at this cleanup (2026-07-13, this same session) tried the
shard-merge convention used elsewhere in this codebase — write a per-VM
shard re-stamping the CANONICAL rows with a fresh timestamp, expecting the
consolidator's last-write-wins window-dedup to somehow also collapse the
market-tick-data-service twin. It does not: ``service_name`` is a
``_BASE_DEDUP_COLS`` member, so a market-tick-data-service-keyed row and an
instruments-service-keyed row are, and remain, DIFFERENT dedup-key groups no
matter how either side's timestamp changes. A verified ``--force`` full
rebuild after that shard write confirmed it: ``dedup_dropped=0``, the
market-tick-data-service row count was UNCHANGED (683,680 before and after).
There is no delete-via-shard mechanism in the consolidator (append-only
merge, dedup only within a matching key) — this is the SAME lesson already
documented by ``drop_stale_xg_shots_shot_rows_2026_07_09.py`` for the
instrument_type='shot' case. The only way to remove a mis-keyed row is a
direct canonical rewrite, which is this script's approach (same accepted
convention: plain gcsfs read/write, no generation-match, per that script's
own precedent + ``manifest_writer._maintenance.purge_venue_before_date``).

OPERATOR DIRECTIVE (2026-07-13): "keep the ones which are canonical and if
entirely duplicated remove the less good ones." SAFETY: a
market-tick-data-service row is only dropped when a
service_name=instruments-service row exists for the EXACT SAME identity
(BASE + OPTIONAL dedup columns minus service_name) — i.e. only when the
fact is already correctly represented elsewhere, regardless of whether the
two rows' capture_status agrees (14,770 of 683,592 matched pairs disagree,
e.g. MTDS says attempted_failed/captured where canonical says
empty_confirmed — the MTDS value reflects a stale v8 snapshot, not new
information; instruments-service's own row is authoritative and is left
completely untouched by this script). Rows with NO canonical identity match
(88 as of the 2026-07-13 audit, 0.01% of 683,680) are left untouched and
logged for manual review — never silently dropped.

DRY-RUN by default — logs what WOULD be dropped without writing. ``--apply``
performs the canonical rewrite.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/dedup_mtds_instruments_surface_duplicate_rows_2026_07_13.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os

import gcsfs
import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dedup_mtds_instruments_surface_duplicate_rows")

_MANIFEST_BLOB = "_index/availability_index.parquet"

_BAD_SERVICE = "market-tick-data-service"
_GOOD_SERVICE = "instruments-service"

_IDENTITY_COLS: tuple[str, ...] = (
    "date",
    "venue",
    "data_type",
    "timeframe",
    "league_id",
    "chain",
    "instrument_type",
    "underlying",
    "feature_group",
    "model_family",
    "training_period",
    "strategy_id",
    "client_id",
    "instruction_type",
    "instrument_id",
)


def _normalised_identity(df: pd.DataFrame) -> pd.DataFrame:
    out = df[list(_IDENTITY_COLS)].copy()
    for c in _IDENTITY_COLS:
        out[c] = out[c].astype("string").fillna("")
    return out


def _drop_mask(df: pd.DataFrame) -> tuple[pd.Series, int]:
    """Return (mask of market-tick-data-service rows safe to drop, orphan count)."""
    bad = df[df["service_name"] == _BAD_SERVICE]
    good = df[df["service_name"] == _GOOD_SERVICE]

    bad_key = _normalised_identity(bad)
    good_key = _normalised_identity(good)
    bad_key["_idx_bad"] = bad.index
    good_key["_idx_good"] = good.index

    merged = bad_key.merge(good_key, on=list(_IDENTITY_COLS), how="left", indicator=True)
    matched_idx = merged.loc[merged["_merge"] == "both", "_idx_bad"]
    orphans = int(merged.loc[merged["_merge"] == "left_only", "_idx_bad"].nunique())

    drop_idx = {int(i) for i in matched_idx.unique()}
    mask = pd.Series(False, index=df.index)
    mask.loc[list(drop_idx)] = True
    return mask, orphans


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the filtered index back to the canonical.")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if not bucket.startswith(f"instruments-store-sports-{env_short}"):
        logger.error("Resolved bucket %s is not the expected env-short shape. Refusing.", bucket)
        return 1

    fs = gcsfs.GCSFileSystem()
    logger.info("Reading live _index gs://%s/%s", bucket, _MANIFEST_BLOB)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)

    n_bad_total = int((df["service_name"] == _BAD_SERVICE).sum())
    logger.info("Total %s rows in this manifest: %d", _BAD_SERVICE, n_bad_total)
    if n_bad_total == 0:
        logger.info("Nothing to drop — already clean.")
        return 0

    mask, orphans = _drop_mask(df)
    n_drop = int(mask.sum())
    logger.info(
        "Eligible for drop (confirmed canonical instruments-service twin exists): %d. "
        "Orphans (no canonical twin, NOT touched, needs manual review): %d",
        n_drop,
        orphans,
    )

    if not args.apply:
        logger.info("DRY RUN — canonical untouched. Re-run with --apply to drop the %d confirmed rows.", n_drop)
        return 0

    if n_drop == 0:
        logger.info("No rows confirmed safe to drop. Nothing to apply.")
        return 0

    cleaned = df.loc[~mask].reset_index(drop=True)
    buf = io.BytesIO()
    cleaned.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    buf.seek(0)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "wb") as fh:
        fh.write(buf.getvalue())
    logger.info(
        "APPLIED. Canonical rewritten: rows_in=%d rows_out=%d (dropped=%d).",
        len(df),
        len(cleaned),
        n_drop,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
