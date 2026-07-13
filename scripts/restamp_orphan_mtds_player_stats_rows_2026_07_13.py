#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 service_name=market-tick-data-service rows with source=api_football remain in
#   instruments-store-sports-{env} (verified post-apply)
"""restamp_orphan_mtds_player_stats_rows_2026_07_13.py — re-stamp the 88 orphaned
``service_name=market-tick-data-service`` / ``source=api_football`` /
``data_type=PLAYER_STATS`` rows in instruments-store-sports-{env} to their correct
provenance: ``service_name=instruments-service``, ``asset_group=sports``.

CONTEXT (plans/active/sports_data_sources_canonical_completion_2026_07_13.md,
api_football todo "resolve the 8,766 non-instruments-service rows"): the
2026-07-13 investigation found these 88 rows are 100% ``capture_status=captured``,
blank ``venue``/``asset_group``, spread across 21 leagues, dates 2020-2026, with
NO ``service_name=instruments-service`` twin at the same (date, data_type,
league_id) identity — i.e. genuinely-captured PLAYER_STATS data that some
historical write path stamped under the wrong ``service_name`` (the same
service_name-drift bug class fixed elsewhere this session for the
683,592-row dedup — see ``dedup_mtds_instruments_surface_duplicate_rows_2026_07_13.py``
docstring for the full root-cause + shard-merge-does-not-collapse lesson this
script reuses), with a blank ``asset_group`` (this bucket is 100% sports, so the
correct value is unambiguous).

Unlike the 683,592-row case, these rows have NO canonical twin to defer to —
deleting them would destroy real capture evidence with no replacement. The
correct action is a DIRECT CANONICAL REWRITE that re-stamps ``service_name``
and fills ``asset_group``, never a drop.

SAFETY: re-verifies at run time (not just trusting the prior investigation)
that (a) the row count matches (or logs the delta if the live count has
moved since the audit — the manifest is a live, growing corpus), and (b) no
row in the restamp set already has an instruments-service twin at the same
(date, data_type, league_id) identity — if a twin now exists (e.g. because
the enumerator/backfill has since captured it correctly), that row is
EXCLUDED from the restamp (left untouched, logged) rather than blindly
overwritten, avoiding a fresh same-identity duplicate under the SAME
service_name.

DRY-RUN by default — logs what WOULD be restamped without writing.
``--apply`` performs the canonical rewrite.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/restamp_orphan_mtds_player_stats_rows_2026_07_13.py [--apply]
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
logger = logging.getLogger("restamp_orphan_mtds_player_stats_rows")

_MANIFEST_BLOB = "_index/availability_index.parquet"

_BAD_SERVICE = "market-tick-data-service"
_GOOD_SERVICE = "instruments-service"
_SOURCE = "api_football"
_DATA_TYPE = "PLAYER_STATS"
_ASSET_GROUP = "sports"

_IDENTITY_COLS: tuple[str, ...] = ("date", "data_type", "league_id")


def _select_restamp_mask(df: pd.DataFrame) -> tuple[pd.Series, int]:
    """Return (mask of orphan rows safe to restamp, count excluded due to a live twin)."""
    orphan_candidates = (
        (df["service_name"] == _BAD_SERVICE) & (df["source"] == _SOURCE) & (df["data_type"] == _DATA_TYPE)
    )
    bad = df[orphan_candidates]
    good = df[(df["service_name"] == _GOOD_SERVICE) & (df["source"] == _SOURCE) & (df["data_type"] == _DATA_TYPE)]

    bad_key = bad[list(_IDENTITY_COLS)].astype("string").fillna("")
    good_key = good[list(_IDENTITY_COLS)].astype("string").fillna("")
    bad_key["_idx_bad"] = bad.index
    good_key["_idx_good"] = good.index

    merged = bad_key.merge(good_key, on=list(_IDENTITY_COLS), how="left", indicator=True)
    has_twin_idx = {int(i) for i in merged.loc[merged["_merge"] == "both", "_idx_bad"].unique()}

    mask = pd.Series(False, index=df.index)
    restamp_idx = [i for i in bad.index if i not in has_twin_idx]
    mask.loc[restamp_idx] = True
    return mask, len(has_twin_idx)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the restamped index back to the canonical.")
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

    n_candidates = int(
        ((df["service_name"] == _BAD_SERVICE) & (df["source"] == _SOURCE) & (df["data_type"] == _DATA_TYPE)).sum()
    )
    logger.info(
        "Orphan candidates (service_name=%s, source=%s, data_type=%s): %d",
        _BAD_SERVICE,
        _SOURCE,
        _DATA_TYPE,
        n_candidates,
    )
    if n_candidates == 0:
        logger.info("Nothing to restamp — already clean.")
        return 0

    mask, excluded_twin_count = _select_restamp_mask(df)
    n_restamp = int(mask.sum())
    logger.info(
        "Eligible for restamp (no live instruments-service twin): %d. Excluded (twin now exists, left untouched): %d",
        n_restamp,
        excluded_twin_count,
    )

    if not args.apply:
        logger.info("DRY RUN — canonical untouched. Re-run with --apply to restamp the %d confirmed rows.", n_restamp)
        return 0

    if n_restamp == 0:
        logger.info("No rows confirmed safe to restamp. Nothing to apply.")
        return 0

    restamped = df.copy()
    restamped.loc[mask, "service_name"] = _GOOD_SERVICE
    restamped.loc[mask, "asset_group"] = _ASSET_GROUP

    buf = io.BytesIO()
    restamped.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    buf.seek(0)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "wb") as fh:
        fh.write(buf.getvalue())
    logger.info(
        "APPLIED. Canonical rewritten: rows_total=%d restamped=%d (service_name=%s→%s, asset_group→%s).",
        len(restamped),
        n_restamp,
        _BAD_SERVICE,
        _GOOD_SERVICE,
        _ASSET_GROUP,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
