#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after both phantom rows are confirmed removed from the live
#   instruments-store-sports-{env} manifest (this script's own dry-run
#   count == 0).
"""delete_cross_ag_phantom_rows_sports_manifest_2026_07_27.py -- one-off
CAS-safe removal of the 2 confirmed cross-asset_group phantom rows sitting
in the ``instruments-store-sports-{env}`` manifest, tracked as Finding C /
the follow-up todo in
``plans/active/issues/api_football_reverify_attempted_failed_and_asset_group_2026_07_14.md``
and dispatched via
``plans/active/sports_satellite_ao_dispatch_batch3_2026_07_25.md``.

THE 2 ROWS (both ``date=2026-06-26``, both ``service_name=instruments-service``,
both ``pipeline_mode=batch_instruments_service`` -- confirmed via a live
gcsfs read 2026-07-27):

  1. ``venue=UNISWAP_V3-BASE source=api_football asset_group=defi
     capture_status=attempted_failed error_reason=UNCLASSIFIED_ADAPTER_ERROR``
     (written_at 18:13:50Z) -- a DeFi adapter-failure diagnostic row.
  2. ``venue=BITGET-FUTURES source=instruments_service asset_group=cefi
     capture_status=captured instrument_type=PERPETUAL row_count=39``
     (written_at 18:56:40Z) -- a REAL CeFi capture with real data.

ROOT CAUSE (confirmed via code read, not guessed): ``_write_all_venues``
(``instruments_service/engine/orchestrator/process_write.py``) resolves ONE
primary bucket per run and only switches to per-venue bucket routing when
its ``_is_all_run`` discriminator is True. Before this script's companion
fix (same commit), ``_is_all_run`` checked ONLY whether
``asset_groups[0] == "ALL"`` -- it never checked ``len(asset_groups) > 1``.
The service's own CLI (``unified_trading_library.service_cli``) defines
``--asset-group`` with ``nargs="+"``, so a genuine, currently-supported
invocation like ``--asset-group SPORTS CEFI`` (no "ALL" sentinel at all)
silently left ``_is_all_run`` False, forcing EVERY venue in that run --
including a real CEFI capture (row 2) -- into the single SPORTS-primary
bucket. Row 1 (a DIAGNOSTIC honest-coverage write from
``process_completeness.py``'s missing-shards fallback) is a SEPARATE,
still-open structural gap: that module's ``ManifestWriter`` instances are
never given per-venue bucket routing at all (unlike the main write loop),
so ANY combined multi-AG run -- including the correctly-detected "ALL"
sentinel case -- can still misroute a missing/adapter-failed non-primary-AG
venue's honest-coverage row into the primary bucket. Filed as a new
follow-up todo (not fixed in this pass -- see the dated section this script's
companion issue-doc update adds) because it requires threading a bucket
resolver through ``_completeness_and_retry``/``_finalize_completeness``, a
larger change to the sports daily producer's shared completeness-check than
this cleanup's scope.

Neither row is reproducible via a live re-run today under NORMAL invocation
shapes (no automated cron passes a genuine multi-value, non-"ALL"
``--asset-group`` list) -- these are believed to be 2 one-off writes from a
historical manual/ad-hoc multi-AG invocation on 2026-06-26, now closed off
for row 2's class by the ``_is_all_run`` fix. Row 1's class remains
theoretically live until the follow-up lands; low volume (0 new rows
observed since 2026-06-26 across 2 full days of measurement) means no active
ongoing leak.

SAFETY (mirrors ``reconcile_mdps_odds_horizon_bucket_venue_grain_2026_07_14.py``'s
"CAS-SAFE DIRECT REWRITE" pattern exactly):
  - Snapshots the manifest object (server-side GCS copy, no data egress) to
    ``_index/snapshots/pre_cross_ag_phantom_delete_2026_07_27.parquet``
    BEFORE any write.
  - Row count strictly DECREASES by exactly 2 (never re-derives/mutates any
    other column) -- cannot trip ``ManifestIndexShrinkRefusedError`` (only
    fires past a >2% shrink; removing 2 rows out of 6.8M is far under that).
  - CAS retry loop (``download_bytes_with_generation`` /
    ``conditional_upload_bytes``, ``if_generation_match``) bounded past
    several manifest-consolidator cycles, same as every prior one-off in
    this directory.
  - Predicate is over-specified (date + venue + source + asset_group +
    capture_status, per row) so it can ONLY ever match these exact 2 rows,
    never a broader class.

DRY-RUN by default; ``--apply`` writes (via the CAS retry loop).

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
      .venv/bin/python scripts/delete_cross_ag_phantom_rows_sports_manifest_2026_07_27.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import time

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("delete_cross_ag_phantom_rows_sports_manifest")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_SNAPSHOT_BLOB = "_index/snapshots/pre_cross_ag_phantom_delete_2026_07_27.parquet"
_ASSET_GROUP = "sports"

# Mirrors the bound used by every prior one-off touching this bucket's
# manifest (the consolidator cron fires every ~60s).
_MAX_CAS_ATTEMPTS = 30
_CAS_RETRY_SLEEP_SECONDS = 5.0


def _phantom_mask(df: pd.DataFrame) -> pd.Series:
    row1 = (
        (df["date"].astype(str) == "2026-06-26")
        & (df["venue"].fillna("").astype(str) == "UNISWAP_V3-BASE")
        & (df["source"].fillna("").astype(str) == "api_football")
        & (df["asset_group"].fillna("").astype(str) == "defi")
        & (df["capture_status"].fillna("").astype(str) == "attempted_failed")
    )
    row2 = (
        (df["date"].astype(str) == "2026-06-26")
        & (df["venue"].fillna("").astype(str) == "BITGET-FUTURES")
        & (df["source"].fillna("").astype(str) == "instruments_service")
        & (df["asset_group"].fillna("").astype(str) == "cefi")
        & (df["capture_status"].fillna("").astype(str) == "captured")
    )
    return row1 | row2


def _plan_delete(tgt: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Compute the reconciled frame against a fresh ``tgt`` snapshot.

    Pure function of ``tgt`` so it can be re-run against a freshly-read
    snapshot on every CAS retry.
    """
    mask = _phantom_mask(tgt)
    new_df = tgt.drop(index=tgt.loc[mask].index).copy()
    return new_df, int(mask.sum())


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the reconciled index back to the canonical target.")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    tgt_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=_ASSET_GROUP)
    if not tgt_bucket.startswith(f"instruments-store-{_ASSET_GROUP}-{env_short}"):
        logger.error("Resolved target bucket %s is not the expected env-short shape. Refusing.", tgt_bucket)
        return 1

    client = get_storage_client(provider="gcp")

    if not args.apply:
        data, generation = client.download_bytes_with_generation(tgt_bucket, _MANIFEST_BLOB)
        if data is None:
            logger.error("Refusing: target object gs://%s/%s does not exist.", tgt_bucket, _MANIFEST_BLOB)
            return 1
        tgt = pd.read_parquet(io.BytesIO(data))
        logger.info(
            "Read gs://%s/%s at generation=%d: %d total rows.", tgt_bucket, _MANIFEST_BLOB, generation, len(tgt)
        )
        new_df, n_deleted = _plan_delete(tgt)
        logger.info(
            "DRY RUN -- %d phantom row(s) would be DELETED. rows_in=%d rows_out=%d. Re-run with --apply to commit.",
            n_deleted,
            len(tgt),
            len(new_df),
        )
        return 0

    # --apply: snapshot first, then the atomic CAS retry loop (see module
    # docstring "SAFETY" for why a plain gcsfs read/write is unsafe against
    # this bucket's live per-minute manifest consolidator).
    logger.info(
        "Snapshotting gs://%s/%s -> gs://%s/%s before any write.",
        tgt_bucket,
        _MANIFEST_BLOB,
        tgt_bucket,
        _SNAPSHOT_BLOB,
    )
    client.copy_blob(tgt_bucket, _MANIFEST_BLOB, tgt_bucket, _SNAPSHOT_BLOB)
    logger.info("Snapshot copy complete.")

    for attempt in range(1, _MAX_CAS_ATTEMPTS + 1):
        logger.info(
            "Reading canonical target gs://%s/%s (CAS attempt %d/%d)",
            tgt_bucket,
            _MANIFEST_BLOB,
            attempt,
            _MAX_CAS_ATTEMPTS,
        )
        data, generation = client.download_bytes_with_generation(tgt_bucket, _MANIFEST_BLOB)
        if data is None:
            logger.error("Refusing: target object gs://%s/%s does not exist.", tgt_bucket, _MANIFEST_BLOB)
            return 1
        tgt = pd.read_parquet(io.BytesIO(data))
        new_df, n_deleted = _plan_delete(tgt)
        logger.info(
            "Target read at generation=%d: %d total rows, %d phantom row(s) to delete.",
            generation,
            len(tgt),
            n_deleted,
        )

        if n_deleted == 0:
            logger.info("Nothing left to delete -- target already clean. No write performed.")
            return 0

        buf = io.BytesIO()
        new_df.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        new_generation = client.conditional_upload_bytes(
            tgt_bucket,
            _MANIFEST_BLOB,
            buf.getvalue(),
            if_generation_match=generation,
            content_type="application/octet-stream",
        )
        if new_generation is not None:
            logger.info(
                "APPLIED on attempt %d/%d (generation %d -> %d). Deleted=%d rows_in=%d rows_out=%d.",
                attempt,
                _MAX_CAS_ATTEMPTS,
                generation,
                new_generation,
                n_deleted,
                len(tgt),
                len(new_df),
            )
            return 0

        logger.warning(
            "CAS precondition failed on attempt %d/%d (generation %d changed underneath us -- a "
            "concurrent writer, likely the manifest consolidator, won the race). Retrying in %.0fs.",
            attempt,
            _MAX_CAS_ATTEMPTS,
            generation,
            _CAS_RETRY_SLEEP_SECONDS,
        )
        time.sleep(_CAS_RETRY_SLEEP_SECONDS)

    logger.error("Exhausted %d CAS attempts without a successful write. Refusing.", _MAX_CAS_ATTEMPTS)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
