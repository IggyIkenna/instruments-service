#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after the 24-league de-registration run (2026-07-13) is confirmed applied in prod
"""De-registration step 1 — RE-KEY the single cleanly-copyable LA_LIGA_2 atom.

Per the 2026-07-13 operator ruling (24-league de-registration): of the 708
re-key-candidate captured atoms (692 LA_LIGA_2 -> SEGUNDA_DIVISION, 16
SCOTTISH_LEAGUE_CUP_185 -> SCOTTISH_LEAGUE_CUP), exactly ONE atom is cleanly
re-keyable — LA_LIGA_2 / 2019-06-04 / MATCHES / footystats / batch_footystats —
whose sole source GCS object has NO object at the canonical target path
(everything else is a different-content collision or has no per-league source
object, and routes to the PARK path per the operator hard rule).

Flow (hard-rule compliant):
  1. describe source object (size+crc32c);
  2. describe target: ABSENT -> copy; IDENTICAL -> skip-copy; DIFFERENT -> abort
     (atom falls back to PARK; never overwrite);
  3. gcs_copy_object + re-describe both sides, verify size+crc32c equality;
  4. download the CANONICAL object, verify it parses + row count;
  5. record_captured under league_id=SEGUNDA_DIVISION via a manifest-writer
     per-VM shard (VM_NAME=league-rekey-20260713, MANIFEST_PER_VM_SHARDS=true,
     explicit close()) — NEVER a direct canonical-index write;
  6. verify the per-VM shard object exists and carries the row.

Source object is NOT deleted (final verify after the purge gates that).

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    VM_NAME=league-rekey-20260713 MANIFEST_PER_VM_SHARDS=true \
    .venv/bin/python scripts/dereg_rekey_la_liga_2_2026_07_13.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

import pandas as pd
from unified_api_contracts import PipelineMode
from unified_trading_library import (
    GcsEventSink,
    ManifestWriter,
    get_storage_client,
    resolve_bucket_name,
    setup_events,
)
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    gcs_copy_object,
    gcs_describe_object,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dereg_rekey_la_liga_2")

SRC_OBJ = (
    "sports_reference/by_date/day=2019-06-04/pipeline_mode=batch_footystats/"
    "entity=footystats_matches/league=LA_LIGA_2/footystats_matches.parquet"
)
TGT_OBJ = (
    "sports_reference/by_date/day=2019-06-04/pipeline_mode=batch_footystats/"
    "entity=footystats_matches/league=SEGUNDA_DIVISION/footystats_matches.parquet"
)
SHARD_INSTANCE = "league-rekey-20260713"
PER_VM_SHARD_PATH = f"_index/per_vm/{SHARD_INSTANCE}.parquet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Default dry-run. Pass to copy + write the shard.")
    args = parser.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    src_uri = f"gs://{bucket}/{SRC_OBJ}"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name
    tgt_uri = f"gs://{bucket}/{TGT_OBJ}"  # noqa: gs-uri — one-off migration script, bucket via resolve_bucket_name

    src_meta = gcs_describe_object(src_uri)
    if src_meta is None:
        logger.error("HARD-ABORT: source object missing: %s", src_uri)
        return 3
    logger.info("SOURCE %s size=%s crc32c=%s", src_uri, src_meta.size, src_meta.crc32c)

    tgt_meta = gcs_describe_object(tgt_uri)
    if tgt_meta is not None:
        if (tgt_meta.size, tgt_meta.crc32c) == (src_meta.size, src_meta.crc32c):
            logger.info("TARGET already exists with IDENTICAL content — skip-copy, proceed to manifest row.")
        else:
            logger.error(
                "HARD-ABORT: target exists with DIFFERENT content (size=%s crc32c=%s) — "
                "collision rule: leave both, atom must be PARKED instead.",
                tgt_meta.size,
                tgt_meta.crc32c,
            )
            return 3
    elif args.apply:
        gcs_copy_object(src_uri, tgt_uri)
        logger.info("COPIED %s -> %s", src_uri, tgt_uri)
    else:
        logger.info("DRY-RUN: would copy %s -> %s", src_uri, tgt_uri)

    if not args.apply:
        logger.info("DRY-RUN complete — no writes performed.")
        return 0

    # Post-copy content verification (size + crc32c on both sides).
    src_meta2 = gcs_describe_object(src_uri)
    tgt_meta2 = gcs_describe_object(tgt_uri)
    if src_meta2 is None or tgt_meta2 is None:
        logger.error("HARD-ABORT: post-copy describe failed (src=%s tgt=%s)", src_meta2, tgt_meta2)
        return 3
    if (src_meta2.size, src_meta2.crc32c) != (tgt_meta2.size, tgt_meta2.crc32c):
        logger.error(
            "HARD-ABORT: post-copy mismatch src(size=%s crc=%s) tgt(size=%s crc=%s)",
            src_meta2.size,
            src_meta2.crc32c,
            tgt_meta2.size,
            tgt_meta2.crc32c,
        )
        return 3
    logger.info("VERIFIED copy: size=%s crc32c=%s identical on both sides.", tgt_meta2.size, tgt_meta2.crc32c)

    # Events bootstrap — record_captured's schema-contract warn path emits an
    # event; batch mode requires an explicit sink (mirrors build_instrument_catalogue).
    setup_events(
        service_name="instruments-service",
        mode="batch",
        sink=GcsEventSink(
            project_id="central-element-323112",
            bucket="central-element-323112-events",
            service_name="instruments-service",
        ),
    )

    storage = get_storage_client()
    payload = storage.download_bytes(bucket, TGT_OBJ)
    df = pd.read_parquet(io.BytesIO(payload))
    logger.info("Canonical object parses: %d rows, %d cols.", len(df), len(df.columns))
    if df.empty:
        logger.error("HARD-ABORT: canonical object parsed to 0 rows — refusing to record a captured row.")
        return 3

    # Manifest row under the canonical league via a per-VM shard (never the
    # canonical index directly). Mirrors the production footystats MATCHES
    # record_captured call shape (engine/orchestrator/footystats.py).
    writer = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
        per_vm_shards=True,
    )
    writer.record_captured(  # QG-allow: emission-policy-not-applicable
        row_key={"date": "2019-06-04", "data_type": "MATCHES", "league_id": "SEGUNDA_DIVISION"},
        df=df,
        asset_group="sports",
        instrument_type="",
        data_type="MATCHES",
        league_id="SEGUNDA_DIVISION",
        pipeline_mode=PipelineMode.BATCH_FOOTYSTATS,
        source="footystats",
        service_emission_state=None,
    )
    writer.close()
    logger.info("Manifest row staged + writer closed (per-VM shard mode).")

    # Verify the shard landed with the row.
    if not storage.blob_exists(bucket, PER_VM_SHARD_PATH):
        logger.error("HARD-ABORT: per-VM shard %s not found after close().", PER_VM_SHARD_PATH)
        return 3
    shard_df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, PER_VM_SHARD_PATH)))
    match = shard_df[
        (shard_df["league_id"] == "SEGUNDA_DIVISION")
        & (shard_df["date"].astype(str) == "2019-06-04")
        & (shard_df["data_type"] == "MATCHES")
        & (shard_df["capture_status"] == "captured")
    ]
    logger.info(
        "Per-VM shard %s: %d total rows, %d matching re-key row(s).", PER_VM_SHARD_PATH, len(shard_df), len(match)
    )
    if match.empty:
        logger.error("HARD-ABORT: re-key row not present in the per-VM shard.")
        return 3
    logger.info("RE-KEY COMPLETE. Source object intentionally NOT deleted (final verify gates deletion).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
