#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 source=odds_api rows remain in market-data-tick-sports-{env}
#   whose canonical dedup-key identity (date, venue, data_type, service_name +
#   the populated _OPTIONAL_DEDUP_COLS) is absent from
#   instruments-store-sports-{env} (verified post-apply, this script's own
#   dry-run count == 0).
"""migrate_orphaned_mtds_odds_api_bucket_rows_2026_07_13.py -- copy the
362,665 real ``source=odds_api`` rows (362,631 ``captured`` / 34
``empty_confirmed``) out of the ORPHANED ``market-data-tick-sports-{env}``
manifest and into the CANONICAL ``instruments-store-sports-{env}`` manifest.

ROOT CAUSE (fixed going forward at ``market-tick-data-service@ad76547c``):
the shared cross-asset-group MTDS orchestrator
(``engine/orchestrator/__init__.py::get_tick_data_bucket()`` /
``_DateRunState.bucket`` / ``manifest_finalize.py``'s
``catalogue_bucket=state.bucket``) resolved the MANIFEST bucket for every
asset_group -- including sports -- to the SAME bucket as its raw tick-BYTE
write (``market-data-tick-sports-{env}``).
``instruments-service/scripts/enumerate_expected_universe.py``'s
``_default_bucket_for("sports")`` seeds the expected-universe denominator for
ALL of sports into a DIFFERENT bucket (``instruments-store-sports-{env}``,
per the deliberate 2026-06-07 sports-manifest-canonicalisation routing
exception). Numerator and denominator landed in two manifests nothing ever
merges (the manifest consolidator only collapses shards WITHIN one bucket),
so every real odds_api capture was invisible to the canonical coverage view
-- the exact same split-brain bug class already fixed for
``source=mdps_odds_horizon_bucket`` by
``market-data-processing-service@6907257e4`` +
``instruments-service@0ae48c3b0`` (see
``migrate_orphaned_mdps_odds_horizon_bucket_rows_2026_07_13.py``, this
script's direct template). The MTDS-side code fix
(``market-tick-data-service@ad76547c``) points the writer's/preflight's
manifest-bucket resolution at ``instruments-store-sports-{env}`` for sports
going forward WITHOUT moving the raw tick-byte write path (still
``market-data-tick-sports-{env}`` for every asset_group, sports included).
This script backfills the ALREADY-CAPTURED historical rows so they show up
in the canonical coverage view too (a pure metadata migration -- no
re-derivation, no GCS data-byte writes; the underlying ``bucketed``/tick
parquet output this describes is untouched and already correct).

WHY A METADATA MIGRATION, NOT A RE-RUN: the 362,665 rows are real, already-
captured tick data written by the live ``sports_scheduler_cron`` (5-min
cadence) over 2020-06-06 through 2026-06-24. Re-deriving them would mean
re-fetching from ``odds_api`` (a paid, rate-limited vendor) for years of
history that already exists correctly on GCS -- pure manifest surgery is the
correct, safe fix (same rationale as the MDPS-sibling script).

SAFETY -- confirmed via a live-manifest dry probe before writing this script:
  - **Zero identity collision** using the CANONICAL manifest-consolidator
    dedup key (``unified_trading_library.manifest_consolidator._BASE_DEDUP_COLS``
    + the ``_OPTIONAL_DEDUP_COLS`` members present in both frames --
    ``date``, ``venue``, ``data_type``, ``service_name``, ``timeframe``,
    ``league_id``, ``chain``, ``instrument_type``, ``underlying``,
    ``feature_group``, ``model_family``, ``training_period``,
    ``strategy_id``, ``client_id``, ``instruction_type``,
    ``instrument_id``): live merge of all 362,665 source rows against the
    FULL target manifest (4,988,148 rows, not just its 2,667 pre-existing
    odds_api rows) on this key returns **0** matches, and the 362,665 source
    rows are already internally unique on this same key (0 self-duplicates).
    So this migration cannot create a new duplicate-dedup-key group (the
    2026-07-13 ``dedup_mtds_instruments_surface_duplicate_rows`` bug class)
    -- it purely ADDS previously-absent identities. The existing 2,667
    ``odds_api`` rows already in the target (2,661 ``empty_confirmed`` + 6
    ``attempted_failed``, dated 2018-01-01..2020-06-05, a disjoint
    pre-backfill window) are left untouched.
  - **Schema**: target carries 2 columns absent from source
    (``enumerator_run_id``, ``available_at``) -- confirmed null/None for
    every existing non-enumerator captured row in the target, so backfilling
    them as ``None`` on the migrated rows matches the established
    convention (same as the MDPS-sibling script).

DEVIATION FROM THE MDPS-SIBLING SCRIPT'S "plain gcsfs read/write, no
generation-match" CONVENTION -- DELIBERATE, LIVE-PROVEN NECESSARY: a first
``--apply`` attempt using that exact plain-write convention was silently
clobbered within ~60-90s by ``instruments-store-sports``'s live manifest
consolidator (GCP Cloud Run Job, Cloud Scheduler cron ``*/1 * * * *`` UTC --
confirmed via the object's own ``consolidator_run_at``/
``consolidator_content_write_at`` custom metadata advancing to a fresh
generation moments after this script's write landed, with the row count back
at the pre-migration baseline). The consolidator is a live read-merge-write
cron with NO coordination with this out-of-band script, and its own
in-flight cycle can start reading the canonical BEFORE this script's write
lands and finish AFTER, silently overwriting it with a merge computed from
the stale pre-migration snapshot. This script therefore uses the UTL
generation-precondition CAS primitive
(``StorageClient.download_bytes_with_generation`` /
``StorageClient.conditional_upload_bytes(if_generation_match=...)``,
``unified_trading_library/cloud_interface/abstractions.py`` -- "Sanctioned
home for a distributed-lock/lease primitive") in a bounded retry loop: read
target + its generation together (one GET, no metadata/download race),
recompute the merge, attempt an atomic compare-and-swap write; a
``PreconditionFailed`` (412, i.e. ``conditional_upload_bytes`` returns
``None``) means the consolidator (or another writer) won the race in the
interim -- re-read the now-current generation and retry rather than
blind-overwriting. This guarantees the write this script performs is never
lost to a concurrent writer (it only ever commits against the generation it
actually read); it does NOT by itself guarantee the consolidator's own
*next* cycle preserves the migrated rows going forward -- that is verified
empirically post-apply by polling across multiple consolidator cycles (the
same persistence the MDPS-sibling migration's rows already demonstrate,
still present at that plan's later "final re-verify" checkpoint).

Same accepted one-off convention as
``migrate_orphaned_mdps_odds_horizon_bucket_rows_2026_07_13.py`` /
``dedup_mtds_instruments_surface_duplicate_rows_2026_07_13.py`` for
everything else (identity/collision-check shape, dry-run-by-default,
metadata-only no-re-derivation). DRY-RUN by default; ``--apply`` writes
(via the CAS retry loop above).

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
      .venv/bin/python scripts/migrate_orphaned_mtds_odds_api_bucket_rows_2026_07_13.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import time

import gcsfs
import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("migrate_orphaned_mtds_odds_api_bucket_rows")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_SOURCE_NAME = "odds_api"

# Canonical manifest-consolidator dedup key
# (unified_trading_library.manifest_consolidator._BASE_DEDUP_COLS +
# _OPTIONAL_DEDUP_COLS) -- the identity a genuine duplicate row would share.
# Kept as a literal tuple here (not imported) to avoid a private cross-module
# import; verified to match the live constant at authoring time.
_BASE_DEDUP_COLS: tuple[str, ...] = ("date", "venue", "data_type", "service_name")
_OPTIONAL_DEDUP_COLS: tuple[str, ...] = (
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

# Columns present in the target (instruments-store-sports) schema but absent
# from the source (market-data-tick-sports) schema -- backfilled as null,
# matching every other non-enumerator captured row already in the target.
_TARGET_ONLY_NULL_COLS: tuple[str, ...] = ("enumerator_run_id", "available_at")

# The live sports manifest consolidator cron fires every ~60s (30-90s runtime
# per cycle) -- bound the CAS retry loop generously past a few full cycles
# rather than failing on the first collision with an in-flight run.
_MAX_CAS_ATTEMPTS = 30
_CAS_RETRY_SLEEP_SECONDS = 5.0


def _build_migration_frame(src: pd.DataFrame, tgt: pd.DataFrame) -> tuple[pd.DataFrame | None, str | None]:
    """Collision-check ``src`` (already filtered to ``source=odds_api``)
    against the CURRENT ``tgt`` snapshot and return the column-aligned frame
    ready to concat -- or ``(None, <error>)`` if a genuine collision (not a
    race) is found. Re-run against a freshly-read ``tgt`` on every CAS retry
    so a fact the consolidator itself added in the interim is also honored.
    """
    id_cols = [c for c in (*_BASE_DEDUP_COLS, *_OPTIONAL_DEDUP_COLS) if c in src.columns and c in tgt.columns]
    src = src.copy()
    for c in id_cols:
        src[c] = src[c].astype("string").fillna("")
    tgt_key = tgt[id_cols].copy()
    for c in tgt_key.columns:
        tgt_key[c] = tgt_key[c].astype("string").fillna("")
    tgt_key = tgt_key.drop_duplicates()

    n_self_dupe = int(src.duplicated(subset=id_cols, keep=False).sum())
    if n_self_dupe:
        return None, (
            f"Refusing: {n_self_dupe} of {len(src)} source rows share a duplicate identity WITHIN the "
            "source side itself -- investigate before retrying (expected 0 per this script's "
            "authoring-time dry probe)."
        )

    merged = src[id_cols].merge(tgt_key, on=id_cols, how="left", indicator=True)
    n_collide = int((merged["_merge"] == "both").sum())
    if n_collide:
        return None, (
            f"Refusing: {n_collide} source rows already have an identical canonical-dedup-key identity "
            "in the target -- investigate before retrying."
        )

    for col in _TARGET_ONLY_NULL_COLS:
        if col in tgt.columns and col not in src.columns:
            src[col] = None

    extra_src_cols = set(src.columns) - set(tgt.columns)
    if extra_src_cols:
        return None, f"Refusing: source has columns absent from target schema: {sorted(extra_src_cols)}"

    return src[tgt.columns], None


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

    if not args.apply:
        logger.info("Reading canonical target _index gs://%s/%s (dry-run, plain read)", tgt_bucket, _MANIFEST_BLOB)
        with fs.open(f"{tgt_bucket}/{_MANIFEST_BLOB}", "rb") as fh:
            tgt = pd.read_parquet(fh)
        tgt_source_rows = int((tgt["source"] == _SOURCE_NAME).sum())
        logger.info("Target rows already carrying source=%s: %d", _SOURCE_NAME, tgt_source_rows)
        id_cols_preview = [
            c for c in (*_BASE_DEDUP_COLS, *_OPTIONAL_DEDUP_COLS) if c in src.columns and c in tgt.columns
        ]
        logger.info("Collision-check identity columns: %s", id_cols_preview)
        src_ready, err = _build_migration_frame(src, tgt)
        if err is not None:
            logger.error(err)
            return 1
        assert src_ready is not None
        logger.info(
            "Collision check passed: 0 of %d source rows collide with an existing target identity "
            "(checked against the FULL %d-row target manifest, not just its %d pre-existing source=%s rows).",
            len(src_ready),
            len(tgt),
            tgt_source_rows,
            _SOURCE_NAME,
        )
        logger.info(
            "Eligible to migrate: %d rows (%s). Target rows before: %d.",
            len(src_ready),
            src_ready["capture_status"].value_counts().to_dict(),
            len(tgt),
        )
        logger.info("DRY RUN -- target untouched. Re-run with --apply to migrate the %d rows.", len(src_ready))
        return 0

    # --apply: atomic CAS retry loop (see module docstring "DEVIATION..." for
    # why a plain gcsfs read/write is unsafe against this bucket's live
    # per-minute manifest consolidator).
    client = get_storage_client(provider="gcp")
    for attempt in range(1, _MAX_CAS_ATTEMPTS + 1):
        logger.info(
            "Reading canonical target _index gs://%s/%s (CAS attempt %d/%d)",
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
        tgt_source_rows = int((tgt["source"] == _SOURCE_NAME).sum())
        logger.info(
            "Target read at generation=%d: %d total rows, %d already source=%s",
            generation,
            len(tgt),
            tgt_source_rows,
            _SOURCE_NAME,
        )

        src_ready, err = _build_migration_frame(src, tgt)
        if err is not None:
            logger.error(err)
            return 1
        assert src_ready is not None
        logger.info(
            "Collision check passed: 0 of %d source rows collide with an existing target identity "
            "(checked against the FULL %d-row target manifest, not just its %d pre-existing source=%s rows).",
            len(src_ready),
            len(tgt),
            tgt_source_rows,
            _SOURCE_NAME,
        )

        merged_df = pd.concat([tgt, src_ready], ignore_index=True)
        buf = io.BytesIO()
        merged_df.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        new_generation = client.conditional_upload_bytes(
            tgt_bucket,
            _MANIFEST_BLOB,
            buf.getvalue(),
            if_generation_match=generation,
            content_type="application/octet-stream",
        )
        if new_generation is not None:
            logger.info(
                "APPLIED on attempt %d/%d (generation %d -> %d). Target rewritten: "
                "rows_in=%d rows_added=%d rows_out=%d.",
                attempt,
                _MAX_CAS_ATTEMPTS,
                generation,
                new_generation,
                len(tgt),
                len(src_ready),
                len(merged_df),
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
