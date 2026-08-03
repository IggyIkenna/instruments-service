#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: XG_SHOTS instrument_type='shot' rows==0 in the sports canonical (verified post-apply)
"""drop_stale_xg_shots_shot_rows_2026_07_09.py — drop the 5 stale understat
XG_SHOTS ``instrument_type='shot'`` manifest rows directly from the canonical,
since a correctly-blank-``instrument_type`` sibling row already covers the
same (date, league_id, data_type) cell.

CONTEXT: ``fix_xg_shots_instrument_type_dedup_2026_07_09.py`` (this same
commit) relabeled the 5 stale rows to ``instrument_type=''`` via a per-VM
shard, expecting the consolidator's last-write-wins window-dedup to collapse
them onto the existing blank-tagged row on the next ``--force`` merge. That
assumption was WRONG: ``instrument_type`` is part of the resolved dedup KEY
(``_OPTIONAL_DEDUP_COLS`` in ``manifest_consolidator.py``), so window-dedup
only merges rows that already share an IDENTICAL key — changing a row's
``instrument_type`` value does not "supersede" its old-keyed self, it
creates an entirely separate key-partition. A ``--force`` full rebuild after
the relabel confirmed this: the 5 ``instrument_type='shot'`` rows survived
UNCHANGED (their key never matched anything in the corrective shard), while
the corrective shard's blank-tagged rows separately deduped against the
PRE-EXISTING blank-tagged sibling (from the 2026-07-06 producer fix) — net
result: still 2 rows per cell, just with the stale 'shot' row now paired
against a freshly-timestamped blank row instead of the original one.

There is no delete-via-shard mechanism in the consolidator (append-only
merge, dedup only within a matching key). The only way to remove the
mis-keyed rows is a direct canonical rewrite — this script follows the SAME
established convention as ``manifest_writer._maintenance.purge_venue_before_date``
(read the live index, drop the confirmed-bad rows, write the index back) and
``reclassify_xg_shots_false_failed_2026_06_29.py`` (plain gcsfs read/write,
no generation-match — the accepted pattern for these one-off manifest
corrections elsewhere in this codebase).

SAFETY: only drops a ``data_type='XG_SHOTS'`` + ``instrument_type='shot'``
row when a SIBLING row exists for the exact same (date, league_id) with
``instrument_type`` blank AND ``capture_status='captured'`` — i.e. only when
the fact is already correctly represented elsewhere. A 'shot' row without a
confirmed sibling is left untouched and logged for manual review (never
silently dropped).

DRY-RUN by default — logs what WOULD be dropped without writing. ``--apply``
performs the canonical rewrite.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/drop_stale_xg_shots_shot_rows_2026_07_09.py [--apply]
"""

from __future__ import annotations

import argparse
import logging
import os

import gcsfs
import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("drop_stale_xg_shots_shot_rows")

_MANIFEST_BLOB = "_index/availability_index.parquet"


def _shot_mask(df: pd.DataFrame) -> pd.Series:
    dt = df["data_type"].astype("string").fillna("").str.upper()
    itype = df["instrument_type"].astype("string").fillna("")
    return (dt == "XG_SHOTS") & (itype == "shot")


def _sibling_keys(df: pd.DataFrame) -> set[tuple[str, str]]:
    """(date, league_id) pairs with a captured, blank-instrument_type XG_SHOTS row."""
    dt = df["data_type"].astype("string").fillna("").str.upper()
    itype = df["instrument_type"].astype("string").fillna("")
    cs = df["capture_status"].astype("string").fillna("")
    sib = df[(dt == "XG_SHOTS") & (itype == "") & (cs == "captured")]
    return set(zip(sib["date"].astype("string"), sib["league_id"].astype("string"), strict=False))


def _drop_mask(df: pd.DataFrame) -> pd.Series:
    """Rows eligible for removal: instrument_type='shot' with a confirmed blank sibling."""
    mask = _shot_mask(df)
    if not mask.any():
        return mask
    siblings = _sibling_keys(df)
    key_series = list(zip(df["date"].astype("string"), df["league_id"].astype("string"), strict=False))
    has_sibling = pd.Series([k in siblings for k in key_series], index=df.index)
    return mask & has_sibling


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

    mask = _shot_mask(df)
    n_shot = int(mask.sum())
    logger.info("XG_SHOTS instrument_type='shot' rows present: %d", n_shot)
    if n_shot == 0:
        logger.info("Nothing to drop — already clean.")
        return 0

    drop_mask = _drop_mask(df)
    n_drop = int(drop_mask.sum())
    n_orphan = n_shot - n_drop
    pending = df[drop_mask]
    logger.info("  Eligible for drop (confirmed blank sibling exists): %d", n_drop)
    if n_drop:
        logger.info("  Leagues: %s", sorted(pending["league_id"].unique()))
        logger.info("  Dates: %s", sorted(pending["date"].unique()))
    if n_orphan:
        orphans = df[mask & ~drop_mask]
        logger.warning(
            "  %d 'shot' row(s) have NO confirmed blank sibling — left untouched, review manually: %s",
            n_orphan,
            orphans[["date", "league_id", "capture_status"]].to_dict("records"),
        )

    if not args.apply:
        logger.info("DRY RUN — canonical untouched. Re-run with --apply to drop the %d confirmed rows.", n_drop)
        return 0

    if n_drop == 0:
        logger.info("No rows confirmed safe to drop. Nothing to apply.")
        return 0

    cleaned = df.drop(index=pending.index).reset_index(drop=True)
    import io

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
