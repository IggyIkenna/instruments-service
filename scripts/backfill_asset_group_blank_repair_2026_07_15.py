#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 blank-``asset_group`` rows remain in
#   instruments-store-sports-{env} (verified post-apply, this script's own
#   dry-run count == 0).
"""backfill_asset_group_blank_repair_2026_07_15.py -- one-off repair of the
~966,925 blank-``asset_group`` rows already sitting in the canonical
``instruments-store-sports-{env}`` manifest, root-caused + fixed going
forward in ``unified-trading-library`` (commit 86f3da96, "fix(manifest):
extend consolidator asset_group heal to instruments-store buckets") and
tracked as todo B in
``plans/active/issues/api_football_reverify_attempted_failed_and_asset_group_2026_07_14.md``.

ROOT CAUSE: the manifest consolidator's v9 ``asset_group`` self-heal
(a REPLACE-coalesce that stamps ``asset_group=<ag>`` onto blank/pre-v9 rows
at consolidation time, ``unified_trading_library.manifest_consolidator``)
only recognised ``market-data-tick-{ag}`` bucket names -- it returned
``None``/no-op for ``instruments-store-{ag}`` buckets, so a blank
``asset_group`` row written by a pre-v9 producer into ANY instruments-store-*
bucket was NEVER healed by any consolidation cycle. Live-measured impact
(2026-07-15, ``instruments-store-sports-{env}``, read directly via gcsfs --
see WHY GCSFS below): 966,925 blank-``asset_group`` rows out of 5,432,299
total, breaking down as:

  source=api_football            865,646 (2026-07-14 issue-doc measurement was 844,209 --
                                           risen with continued backfill activity in the
                                           interim, before the code fix landed)
  source=footystats                99,048
  source=soccer_football_info         360
  source=transfermarkt                 45
  source=open_meteo                 1,804
  source=understat                     16  (not in the issue doc's named list --
  source=instruments_service            6   found live; included below, see SCOPE)

This undercounted sports (and would have undercounted any OTHER
asset_group's reference data hit by the same gap) on every
``GROUP BY asset_group`` coverage rollup. The code fix (Part 1 of this
repair) prevents the count from growing on FUTURE stale-producer writes --
it does NOT retroactively fix rows already blank as of the fix landing. THIS
script is that one-off retroactive repair.

SCOPE -- why ALL blank rows in this bucket, not just the 5 named sources:
``instruments-store-sports-{env}`` is a per-AG bucket by construction (see
``cf_manifest_audit.py``'s ``AG_BUCKET_TOKENS``/``_bucket()`` and the v9
self-heal comment block in ``manifest_consolidator.py`` this fix extends) --
every row physically stored in it belongs to asset_group=sports, regardless
of its ``source`` label. A live pre-repair scan found 966,903 blank rows
across the 5 named sports sources PLUS 22 additional blank rows under
``source=understat`` (16) and ``source=instruments_service`` (6) -- the same
bug, same bucket, same fix. Scoping the repair to the bucket-name invariant
(mirroring the Part 1 code fix's own logic) rather than a hardcoded source
allowlist closes the gap completely instead of leaving a known residual, and
carries zero additional risk (same one-column mutation, same identity
preserved, same row count).

WHY GCSFS DIRECT READ, NOT ``read_availability_index()``: the sports
instruments-store consolidator has a known, currently-active sustained
slowdown (root-caused in this session, see the Progress Log entries on
"MDPS venue-grain mismatch" / the 328K-row purge in
``sports_data_sources_canonical_completion_2026_07_13.md``) -- a reader-side
merge-with-live-shards fallback through that code path is needlessly slow
for a pure read-only diagnostic/dry-run scan. A direct gcsfs read of
``_index/availability_index.parquet`` avoids it entirely.

WHY A DIRECT CAS-SAFE CANONICAL REWRITE, NOT 966,925 INDIVIDUAL
``ManifestWriter`` CALLS: the canonical index is a single ~111 MiB parquet
(5.43M rows) -- trivially safe to read into memory once. Driving 966,925
individual ``record_captured``-superseding calls through ``ManifestWriter``
would (a) require re-deriving each row's own ``pipeline_mode``/coherence
context (risking the same ``PipelineModeSourceMismatchError`` class hit
elsewhere this session) for zero benefit, since every row's ``asset_group``
is the ONLY field changing and its ``pipeline_mode``/``source``/identity
columns are already fully populated and internally coherent (confirmed via
live probe: 100% of the blank-asset_group rows across the 5 named sources
carry a non-null, source-coherent ``pipeline_mode`` -- e.g.
``batch_api_football`` for ``source=api_football``), and (b) is untested at
this row count for memory/time safety. A direct in-memory column mutation +
CAS-safe write-back (mirrors
``scripts/reconcile_mdps_odds_horizon_bucket_venue_grain_2026_07_14.py``'s
"CAS-SAFE DIRECT REWRITE" section exactly -- same
``download_bytes_with_generation``/``conditional_upload_bytes`` bounded
retry loop) is the simplest-correct approach: it changes ONLY the
``asset_group`` column for matching row indices, preserves every other
column (identity, ``pipeline_mode``, ``source``, ``capture_status``, etc.)
byte-for-byte, and NEVER changes row count -- so it cannot trip
``unified_trading_library``'s ``ManifestIndexShrinkRefusedError`` guard
(which only fires on a >2% row-count SHRINK; this write's row count is
always identical in vs out) even though this script bypasses that guard's
codepath entirely (it lives inside ``ManifestWriter``'s own merge-write
logic, not the raw ``StorageClient`` CAS primitives used here).

SAFETY -- confirmed via a live dry-run probe before writing this script:
  - 966,925 blank-``asset_group`` rows total in the bucket (966,903 across
    the 5 named sports sources + 22 under understat/instruments_service),
    0 fully-duplicated rows among them.
  - 100% ``schema_version=9`` (post-v9 schema; the blank is a genuinely
    pre-v9-producer-written NULL/empty value, not a schema-version artifact).
  - The consolidator schedulers for this bucket
    (``uts-prod-manifest-consolidator-instruments-sports-cron``,
    ``uts-prod-manifest-consolidator-market-data-sports-cron``) are NOT
    paused for this script -- a per-VM-shard write is always safe against
    them, and a direct rewrite here uses the SAME CAS-retry pattern the
    venue_grain/eu_grain reconcile scripts already established specifically
    so it does not need to pause anything (a losing CAS attempt means the
    consolidator won the race; re-read the fresh snapshot and retry).
  - Never touches the 2 separately-tracked MISLABELED (non-blank, WRONG
    value) rows found in the same live probe (1 ``source=instruments_service``
    row stamped ``asset_group=cefi``, 1 ``source=api_football`` row stamped
    ``asset_group=defi``) -- that is a different bug class (a wrong value,
    not a blank one) and is out of scope for this repair; flagged separately
    in the issue doc as a new follow-up finding, not fixed here.

DRY-RUN by default; ``--apply`` writes (via the CAS retry loop).

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
      .venv/bin/python scripts/backfill_asset_group_blank_repair_2026_07_15.py [--apply] [--dry-run]
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
logger = logging.getLogger("backfill_asset_group_blank_repair")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_ASSET_GROUP = "sports"

# The live sports manifest consolidator cron fires every ~60s (30-90s runtime
# per cycle, slower on this bucket's known sustained slowdown) -- bound the
# CAS retry loop generously past several full cycles rather than failing on
# the first collision with an in-flight run (mirrors
# reconcile_mdps_odds_horizon_bucket_venue_grain_2026_07_14.py's bound).
_MAX_CAS_ATTEMPTS = 30
_CAS_RETRY_SLEEP_SECONDS = 5.0


def _blank_asset_group_mask(df: pd.DataFrame) -> pd.Series[bool]:
    return df["asset_group"].isna() | (df["asset_group"].astype("string").fillna("") == "")


def _plan_repair(tgt: pd.DataFrame) -> tuple[pd.DataFrame, int, dict[str, int]]:
    """Compute the repaired frame against a fresh ``tgt`` snapshot.

    Returns ``(new_df, n_repaired, by_source)``. Pure function of ``tgt`` so
    it can be re-run against a freshly-read snapshot on every CAS retry --
    never mutates the row count, only the ``asset_group`` value of matching
    rows.
    """
    mask = _blank_asset_group_mask(tgt)
    by_source = tgt.loc[mask, "source"].value_counts(dropna=False).to_dict()
    new_df = tgt.copy()
    new_df.loc[mask, "asset_group"] = _ASSET_GROUP
    return new_df, int(mask.sum()), {str(k): int(v) for k, v in by_source.items()}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the repaired index back to the canonical target.")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Explicit no-op flag (default behavior when --apply is absent) -- report counts only.",
    )
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
        new_df, n_repaired, by_source = _plan_repair(tgt)
        logger.info(
            "DRY RUN -- %d blank-asset_group rows would be repaired to asset_group=%s. rows_in=%d rows_out=%d "
            "(row count unchanged: %s). Breakdown by source: %s. Re-run with --apply to commit.",
            n_repaired,
            _ASSET_GROUP,
            len(tgt),
            len(new_df),
            len(tgt) == len(new_df),
            by_source,
        )
        return 0

    # --apply: atomic CAS retry loop (see module docstring "WHY A DIRECT
    # CAS-SAFE CANONICAL REWRITE" for why a plain gcsfs read/write is unsafe
    # against this bucket's live per-minute manifest consolidator).
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
        new_df, n_repaired, by_source = _plan_repair(tgt)
        logger.info(
            "Target read at generation=%d: %d total rows, %d blank-asset_group (breakdown: %s).",
            generation,
            len(tgt),
            n_repaired,
            by_source,
        )

        if n_repaired == 0:
            logger.info("Nothing left to repair -- target already fully healed. No write performed.")
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
                "APPLIED on attempt %d/%d (generation %d -> %d). Repaired=%d rows_in=%d rows_out=%d "
                "(row count unchanged: %s). Breakdown: %s.",
                attempt,
                _MAX_CAS_ATTEMPTS,
                generation,
                new_generation,
                n_repaired,
                len(tgt),
                len(new_df),
                len(tgt) == len(new_df),
                by_source,
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
