#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: 0 blank-``source``/null-``available_at`` rows remain under
#   pipeline_mode=batch_instruments_service in every non-sports
#   instruments-store-{cefi,defi,tradfi,pred}-{env} bucket (verified post-apply,
#   this script's own dry-run count == 0).
"""backfill_is_source_blank_and_available_at_null_2026_07_26.py -- one-off
repair of the CF-4 (blank ``source``) / CF-8 (null ``available_at``) rows
found live (2026-07-26) in the 4 non-sports instruments-store manifests while
executing ``cross_cutting_satellite_ao_dispatch_batch1-012`` (see
``plans/active/instruments_store_cf_canonicalization_single_walk_2026_07_24.md``).

ROOT CAUSE: ``record_expected_empty``/``record_expected_unattempted`` (UTL
``manifest_writer/_writer_record.py``) never threaded an ``available_at``
parameter at all (CF-8), and a handful of ``empty_confirmed``/
``attempted_failed``/``expected_unattempted`` rows predate the 2026-07-08
``_stamp_producer_source`` self-heal (``ca5f1dbd``) for blank ``source`` (CF-4).
Both are fixed going forward (UTL@<see plan> + instruments-service
``process_write.py`` threading, same session as this script). THIS script is
the one-off retroactive repair of rows already written before those fixes
landed.

LIVE-MEASURED SCOPE (2026-07-26, read-only `cf_manifest_audit_2026_06_01.py`
against every non-sports instruments-store prod bucket):

  bucket   blank-source rows   null-available_at rows
  cefi     36                  484
  defi     62                  848
  tradfi   30                  164
  pred     6                   388

100% of blank-source rows across all 4 buckets carry
``pipeline_mode=batch_instruments_service`` (confirmed via live probe) -- the
fix is therefore ``source="instruments_service"`` (``source_string_for``
applied to that one pipeline_mode; the same value already stamped on
99.9%+ of every bucket's other rows). 100% of null-available_at rows carry a
non-null ``written_at`` (confirmed via live probe); the established convention
elsewhere in this manifest (91%+ exact-match probed live) is
``available_at == written_at`` for producer/batch rows with no independent
data-arrival timestamp, so the fix is ``available_at = written_at``.

WHY A DIRECT CAS-SAFE CANONICAL REWRITE, NOT PER-ROW ``ManifestWriter`` CALLS:
mirrors ``backfill_asset_group_blank_repair_2026_07_15.py``'s own reasoning --
each canonical ``_index/availability_index.parquet`` here is small (27k-136k
rows, single-digit-MB), trivially safe to read into memory once; a direct
in-memory column mutation + CAS-safe write-back (``download_bytes_with_
generation``/``conditional_upload_bytes``, same bounded retry loop) changes
ONLY the ``source``/``available_at`` columns for matching row indices,
preserves every other column byte-for-byte, and NEVER changes row count (so
it cannot trip ``ManifestIndexShrinkRefusedError``).

DRY-RUN by default; ``--apply`` writes (via the CAS retry loop). Runs against
ALL 4 non-sports asset_groups by default; ``--asset-group`` scopes to one.

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
      .venv/bin/python scripts/backfill_is_source_blank_and_available_at_null_2026_07_26.py \
      [--asset-group {cefi,defi,tradfi,pred}] [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import time

import pandas as pd
from unified_api_contracts import PipelineMode, source_string_for
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_is_source_blank_and_available_at_null")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_ASSET_GROUPS = ("cefi", "defi", "tradfi", "pred")
_TARGET_PIPELINE_MODE = source_string_for(PipelineMode.BATCH_INSTRUMENTS_SERVICE) or ""

# Mirrors backfill_asset_group_blank_repair_2026_07_15.py's bound -- generous
# past several manifest-consolidator cycles rather than failing on the first
# collision with an in-flight run.
_MAX_CAS_ATTEMPTS = 30
_CAS_RETRY_SLEEP_SECONDS = 5.0


def _blank_source_mask(df: pd.DataFrame) -> pd.Series[bool]:
    src = df["source"].astype("string").fillna("")
    pm = df["pipeline_mode"].astype("string").fillna("")
    # Scoped to the ONE confirmed pipeline_mode this repair verified live --
    # never blind-stamp a blank source for any other pipeline_mode (would
    # need its own source_string_for derivation + a fresh live probe first).
    return (src.str.len() == 0) & (pm == "batch_instruments_service")


def _null_available_at_mask(df: pd.DataFrame) -> pd.Series[bool]:
    return df["available_at"].isna()


def _plan_repair(tgt: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Compute the repaired frame against a fresh ``tgt`` snapshot.

    Pure function of ``tgt`` so it can be re-run against a freshly-read
    snapshot on every CAS retry -- never mutates row count, only the
    ``source``/``available_at`` VALUES of matching rows.
    """
    new_df = tgt.copy()

    src_mask = _blank_source_mask(new_df)
    n_source = int(src_mask.sum())
    if n_source:
        new_df.loc[src_mask, "source"] = _TARGET_PIPELINE_MODE

    aa_mask = _null_available_at_mask(new_df)
    n_available_at = int(aa_mask.sum())
    if n_available_at:
        new_df.loc[aa_mask, "available_at"] = new_df.loc[aa_mask, "written_at"]

    return new_df, n_source, n_available_at


def _run_one(asset_group: str, env_short: str, apply: bool) -> int:
    client = get_storage_client(provider="gcp")
    tgt_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=asset_group)
    if not tgt_bucket.startswith(f"instruments-store-{asset_group}-{env_short}"):
        logger.error(
            "[%s] Resolved target bucket %s is not the expected env-short shape. Refusing.", asset_group, tgt_bucket
        )
        return 1

    if not apply:
        data, generation = client.download_bytes_with_generation(tgt_bucket, _MANIFEST_BLOB)
        if data is None:
            logger.error(
                "[%s] Refusing: target object gs://%s/%s does not exist.", asset_group, tgt_bucket, _MANIFEST_BLOB
            )
            return 1
        tgt = pd.read_parquet(io.BytesIO(data))
        new_df, n_source, n_available_at = _plan_repair(tgt)
        logger.info(
            "[%s] DRY RUN at generation=%d -- %d blank-source rows -> source=%r, %d null-available_at rows -> "
            "available_at=written_at. rows_in=%d rows_out=%d (row count unchanged: %s). Re-run with --apply to commit.",
            asset_group,
            generation,
            n_source,
            _TARGET_PIPELINE_MODE,
            n_available_at,
            len(tgt),
            len(new_df),
            len(tgt) == len(new_df),
        )
        return 0

    for attempt in range(1, _MAX_CAS_ATTEMPTS + 1):
        logger.info(
            "[%s] Reading canonical target gs://%s/%s (CAS attempt %d/%d)",
            asset_group,
            tgt_bucket,
            _MANIFEST_BLOB,
            attempt,
            _MAX_CAS_ATTEMPTS,
        )
        data, generation = client.download_bytes_with_generation(tgt_bucket, _MANIFEST_BLOB)
        if data is None:
            logger.error(
                "[%s] Refusing: target object gs://%s/%s does not exist.", asset_group, tgt_bucket, _MANIFEST_BLOB
            )
            return 1
        tgt = pd.read_parquet(io.BytesIO(data))
        new_df, n_source, n_available_at = _plan_repair(tgt)
        logger.info(
            "[%s] Target read at generation=%d: %d rows, %d blank-source, %d null-available_at.",
            asset_group,
            generation,
            len(tgt),
            n_source,
            n_available_at,
        )

        if n_source == 0 and n_available_at == 0:
            logger.info("[%s] Nothing left to repair -- target already fully healed. No write performed.", asset_group)
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
                "[%s] APPLIED on attempt %d/%d (generation %d -> %d). Repaired source=%d available_at=%d "
                "rows_in=%d rows_out=%d (row count unchanged: %s).",
                asset_group,
                attempt,
                _MAX_CAS_ATTEMPTS,
                generation,
                new_generation,
                n_source,
                n_available_at,
                len(tgt),
                len(new_df),
                len(tgt) == len(new_df),
            )
            return 0

        logger.warning(
            "[%s] CAS precondition failed on attempt %d/%d (generation %d changed underneath us -- a concurrent "
            "writer, likely the manifest consolidator, won the race). Retrying in %.0fs.",
            asset_group,
            attempt,
            _MAX_CAS_ATTEMPTS,
            generation,
            _CAS_RETRY_SLEEP_SECONDS,
        )
        time.sleep(_CAS_RETRY_SLEEP_SECONDS)

    logger.error("[%s] Exhausted %d CAS attempts without a successful write. Refusing.", asset_group, _MAX_CAS_ATTEMPTS)
    return 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--asset-group", choices=_ASSET_GROUPS, help="Scope to one asset_group. Default: all 4.")
    p.add_argument("--apply", action="store_true", help="Write the repaired index back to the canonical target.")
    args = p.parse_args(argv)

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    ags = [args.asset_group] if args.asset_group else list(_ASSET_GROUPS)
    exit_code = 0
    for ag in ags:
        rc = _run_one(ag, env_short, args.apply)
        exit_code = exit_code or rc
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
