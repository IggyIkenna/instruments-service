#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 source=mdps_odds_horizon_bucket rows remain in
#   market-data-tick-sports-{env} whose (date, venue, data_type, timeframe,
#   league_id, service_name) identity is absent from instruments-store-sports-{env}
#   (verified post-apply, this script's own dry-run count == 0)
"""migrate_orphaned_mdps_odds_horizon_bucket_rows_2026_07_13.py -- copy the
124,294 real ``source=mdps_odds_horizon_bucket`` rows (123,642 captured / 652
empty_confirmed) out of the ORPHANED ``market-data-tick-sports-{env}`` manifest
and into the CANONICAL ``instruments-store-sports-{env}`` manifest.

ROOT CAUSE (fixed going forward at
``market-data-processing-service@6907257``): ``reprocess_sports_odds.py``
constructed its ``ManifestWriter`` with ``catalogue_bucket=_resolve_bucket()``
(``market-data-tick-sports-{env}``) -- the SAME bucket as its raw-odds input +
bucketed-output DATA. ``instruments-service/scripts/enumerate_expected_universe.py``'s
``_default_bucket_for("sports")`` seeds the expected-universe denominator for
ALL of sports (including MDPS-owned data_types) into a DIFFERENT bucket
(``instruments-store-sports-{env}``, per the deliberate 2026-06-07
sports-manifest-canonicalisation routing exception). Numerator and denominator
landed in two manifests nothing ever merges -- the manifest consolidator only
collapses shards WITHIN one bucket -- so every real MDPS capture was invisible
to the canonical coverage view. The code fix points the writer's
``catalogue_bucket`` at the same bucket the enumerator uses; this script
backfills the ALREADY-CAPTURED historical rows so they show up too (a pure
metadata migration -- no re-derivation, no GCS data-byte writes, the underlying
``bucketed.parquet`` output this describes is untouched and already correct).

WHY A METADATA MIGRATION, NOT A ``--force`` RE-RUN: ``reprocess_sports_odds.py
--force`` re-reads raw odds via ``_read_raw_odds()``, whose
``_CANONICAL_PREFIX_TEMPLATES`` expect a ``data_source=ODDS_API`` path segment
that does not match ANY on-disk shape found in a live GCS probe (2026-05
per-bookmaker layout uses ``venue={BOOKMAKER}``; 2026-06+ per-sport layout uses
``venue=ODDS_API`` with non-``ticks.parquet`` filenames) -- confirmed via direct
``list_blobs`` probes for 2026-05-10 / 2026-06-24 turning up zero matches
against the current templates. Re-running blind would silently reclassify all
123,642 real captures as ``empty_confirmed`` (a regression, not a backfill).
That prefix-template drift is a SEPARATE, deeper bug tracked as its own
follow-on todo (NOT fixed by this script) -- see the Progress Log entry dated
2026-07-13 in ``sports_data_sources_canonical_completion_2026_07_13.md``.

SAFETY -- confirmed via a live-manifest dry probe before writing this script:
  - Zero identity collision: the 123,968 distinct (date, venue, data_type,
    timeframe, league_id) tuples in the source-captured rows share NO overlap
    with the 215,481 existing target rows' identities (the enumerator's seed
    uses ``venue=""`` + UPPERCASE ``data_type="ODDS_HORIZON_BUCKET"`` +
    ``timeframe=None`` -- a coarser, differently-shaped grain than MDPS's own
    ``venue=ODDS_API`` / lowercase ``data_type`` / per-``T-*``-timeframe rows).
    So this migration cannot create a new duplicate-dedup-key group (the
    2026-07-13 ``dedup_mtds_instruments_surface_duplicate_rows`` bug class) --
    it purely ADDS previously-absent identities. This grain mismatch ALSO
    means the migrated captured rows will NOT reconcile the enumerator's
    209,526 ``expected_unattempted`` seed rows down to zero -- that reseed-grain
    alignment is itself a separate, deeper follow-on (see the same Progress
    Log entry / a new plan todo).
  - Schema: target carries 2 columns absent from source (``enumerator_run_id``,
    ``available_at``) -- confirmed null/None for every existing non-enumerator
    captured row in the target (e.g. ``source=api_football``), so backfilling
    them as ``None`` on the migrated rows matches the established convention.

Same accepted one-off convention as
``dedup_mtds_instruments_surface_duplicate_rows_2026_07_13.py`` (plain gcsfs
read/write, no generation-match -- this workspace's established pattern for
this class of manifest surgery). DRY-RUN by default; ``--apply`` writes.

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
      .venv/bin/python scripts/migrate_orphaned_mdps_odds_horizon_bucket_rows_2026_07_13.py [--apply]
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
logger = logging.getLogger("migrate_orphaned_mdps_odds_horizon_bucket_rows")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_SOURCE_NAME = "mdps_odds_horizon_bucket"

_IDENTITY_COLS: tuple[str, ...] = (
    "date",
    "venue",
    "data_type",
    "timeframe",
    "league_id",
    "service_name",
)

# Columns present in the target (instruments-store-sports) schema but absent
# from the source (market-data-tick-sports) schema -- backfilled as null,
# matching every other non-enumerator captured row already in the target.
_TARGET_ONLY_NULL_COLS: tuple[str, ...] = ("enumerator_run_id", "available_at")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the merged index back to the canonical target.")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    src_bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="sports")
    tgt_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if not src_bucket.startswith(f"market-data-tick-sports-{env_short}"):
        logger.error("Resolved source bucket %s is not the expected env-short shape. Refusing.", src_bucket)
        return 1
    if not tgt_bucket.startswith(f"instruments-store-sports-{env_short}"):
        logger.error("Resolved target bucket %s is not the expected env-short shape. Refusing.", tgt_bucket)
        return 1

    fs = gcsfs.GCSFileSystem()
    logger.info("Reading orphaned source _index gs://%s/%s", src_bucket, _MANIFEST_BLOB)
    with fs.open(f"{src_bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        src = pd.read_parquet(fh)
    src = src[src["source"] == _SOURCE_NAME].copy()
    logger.info(
        "Source rows for source=%s: %d (%s)",
        _SOURCE_NAME,
        len(src),
        src["capture_status"].value_counts().to_dict(),
    )
    if src.empty:
        logger.info("Nothing to migrate -- source side already empty.")
        return 0

    logger.info("Reading canonical target _index gs://%s/%s", tgt_bucket, _MANIFEST_BLOB)
    with fs.open(f"{tgt_bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        tgt = pd.read_parquet(fh)
    tgt_source_rows = int((tgt["source"] == _SOURCE_NAME).sum())
    logger.info("Target rows already carrying source=%s: %d", _SOURCE_NAME, tgt_source_rows)

    # Collision check: never migrate an identity already present in the target
    # under the SAME service_name (that would be a true duplicate). Cross-
    # service_name identity matches to the enumerator's own seed rows are
    # EXPECTED (different dedup-key group by design -- service_name is a
    # _BASE_DEDUP_COLS member) and are not a collision.
    id_cols = list(_IDENTITY_COLS)
    for c in id_cols:
        src[c] = src[c].astype("string").fillna("")
    merge_cols = [c for c in id_cols if c in tgt.columns]
    tgt_key = tgt[merge_cols].copy()
    for c in tgt_key.columns:
        tgt_key[c] = tgt_key[c].astype("string").fillna("")
    tgt_key = tgt_key.drop_duplicates()

    merged = src[merge_cols].merge(tgt_key, on=merge_cols, how="left", indicator=True)
    n_collide = int((merged["_merge"] == "both").sum())
    if n_collide:
        logger.error(
            "Refusing: %d source rows already have an identical (date,venue,data_type,"
            "timeframe,league_id,service_name) identity in the target -- investigate before retrying.",
            n_collide,
        )
        return 1
    logger.info("Collision check passed: 0 of %d source rows collide with an existing target identity.", len(src))

    for col in _TARGET_ONLY_NULL_COLS:
        if col in tgt.columns and col not in src.columns:
            src[col] = None

    # Align column order + drop any source-only columns absent from target (none expected --
    # confirmed src is a subset of target columns at authoring time, this guards silent future
    # schema drift).
    extra_src_cols = set(src.columns) - set(tgt.columns)
    if extra_src_cols:
        logger.error("Refusing: source has columns absent from target schema: %s", sorted(extra_src_cols))
        return 1
    src = src[tgt.columns]

    logger.info(
        "Eligible to migrate: %d rows (%s). Target rows before: %d.",
        len(src),
        src["capture_status"].value_counts().to_dict(),
        len(tgt),
    )

    if not args.apply:
        logger.info("DRY RUN -- target untouched. Re-run with --apply to migrate the %d rows.", len(src))
        return 0

    merged_df = pd.concat([tgt, src], ignore_index=True)
    buf = io.BytesIO()
    merged_df.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    buf.seek(0)
    with fs.open(f"{tgt_bucket}/{_MANIFEST_BLOB}", "wb") as fh:
        fh.write(buf.getvalue())
    logger.info(
        "APPLIED. Target rewritten: rows_in=%d rows_added=%d rows_out=%d.",
        len(tgt),
        len(src),
        len(merged_df),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
