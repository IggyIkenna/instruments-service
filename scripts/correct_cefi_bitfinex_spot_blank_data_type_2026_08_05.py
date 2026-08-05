#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off manifest correction for the single BITFINEX-SPOT captured+blank
#   data_type row found in cefi_instruments_store_blank_data_type_residual_2026_07_29.md.
# Delete-when: after the --apply run's post-write spot-check confirms 0 captured+blank
#   rows remain (re-run cf_manifest_audit.audit()), and the plan todo is flipped.
"""Correct the single cefi instruments-store row with blank data_type.

BACKGROUND (``cefi_instruments_store_blank_data_type_residual_2026_07_29.md``).
The cefi instruments-store manifest (``instruments-store-cefi-prd-*``, 84,542 rows)
has exactly ONE row with ``capture_status=captured`` + blank ``data_type``:

    date=2023-12-16, venue=BITFINEX-SPOT, capture_status=captured,
    row_count=284, data_type="", pipeline_mode=batch_instruments_service,
    written_at=2026-07-06T15:00:10.079766Z

All 2,398 OTHER BITFINEX-SPOT ``captured`` rows carry ``data_type=instruments`` — this
is the sole exception. The write batch was isolated: a small July-2026 backfill re-captured
this one historical date across several venues; every sibling in the same batch behaved
correctly. Root cause: the writer call for this specific cell didn't pass ``data_type=``
in its ``row_key``.

``data_type`` is in ``_ROW_KEY_COLUMNS`` — the manifest's ROW IDENTITY key, not a
payload field. A row keyed ``(date=2023-12-16, venue=BITFINEX-SPOT, data_type="")``
and one keyed ``(..., data_type="instruments")`` are different logical shards.
"Backfilling" requires writing a NEW correctly-keyed row via the standard writer
path (generation-matched, concurrent-write-safe), then the manifest consolidator
reconciles the now-orphaned blank-key row.

APPROACH — two-step (both parts required for a complete fix):
1. Write the correctly-keyed row (``data_type="instruments"``) via
   ``record_captured_from_counts`` per the IS catalogue's own cefi write path
   (``catalogue.py`` L182-200), as a per-VM shard.
2. Remove the now-orphaned blank-key row (``data_type=""``, same date/venue)
   from the canonical index via a scoped in-place read→filter→CAS-safe write.
   ``data_type`` IS in ``_BASE_DEDUP_COLS``, so the correctly-keyed row and the
   blank-key row are different dedup keys — the consolidator does NOT auto-
   reconcile them. A direct in-place remove is safe: the IS cefi index is ~84K
   rows (< 10 MB uncompressed), trivially within memory bounds, and the
   CAS-safe write guards against concurrent consolidator races.

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python -u \\
        scripts/correct_cefi_bitfinex_spot_blank_data_type_2026_08_05.py --dry-run
    GCP_PROJECT_ID=central-element-323112 MANIFEST_PER_VM_SHARDS=true \\
        VM_NAME=cefi-bitfinex-spot-dt-fix .venv/bin/python -u \\
        scripts/correct_cefi_bitfinex_spot_blank_data_type_2026_08_05.py --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# The single affected cell, per the issue doc's live diagnosis (2026-07-30).
AFFECTED_DATE = "2023-12-16"
AFFECTED_VENUE = "BITFINEX-SPOT"
AFFECTED_ROW_COUNT = 284
ASSET_GROUP = "cefi"
DATA_TYPE = "instruments"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    from unified_api_contracts import PipelineMode
    from unified_trading_library import (
        GcsEventSink,
        ManifestWriter,
        UnifiedCloudConfig,
        get_storage_client,
        setup_events,
    )

    project_id = UnifiedCloudConfig().gcp_project_id
    setup_events(
        service_name="instruments-service",
        mode="batch",
        sink=GcsEventSink(project_id=project_id, bucket=f"{project_id}-events", service_name="instruments-service"),
    )

    # Resolve the instruments-store bucket for cefi — the IS manifest SSOT,
    # NOT the raw-tick-data bucket.
    from instruments_service.cli.instruments_handler import (
        _get_instruments_bucket_for_asset_group,
    )

    bucket = _get_instruments_bucket_for_asset_group(ASSET_GROUP)
    logger.info(
        "bucket=%s date=%s venue=%s row_count=%d data_type=%s",
        bucket,
        AFFECTED_DATE,
        AFFECTED_VENUE,
        AFFECTED_ROW_COUNT,
        DATA_TYPE,
    )

    if args.dry_run:
        # Dry-run: read the canonical index and report how many blank-key
        # rows would be affected.
        client = get_storage_client()
        index_path = "_index/availability_index.parquet"
        from unified_trading_library import (
            read_availability_index,
        )

        idx = read_availability_index(bucket)
        blank_mask = (
            (idx["date"] == AFFECTED_DATE)
            & (idx["venue"] == AFFECTED_VENUE)
            & (idx["data_type"] == "")
            & (idx["capture_status"] == "captured")
            & (idx["service_name"] == "instruments-service")
        )
        blank_count = int(blank_mask.sum())
        typed_mask = (
            (idx["date"] == AFFECTED_DATE)
            & (idx["venue"] == AFFECTED_VENUE)
            & (idx["data_type"] == DATA_TYPE)
            & (idx["service_name"] == "instruments-service")
        )
        typed_count = int(typed_mask.sum())
        logger.info(
            "DRY-RUN: blank-key captured rows to remove=%d, typed-key rows existing=%d (total index=%d rows)",
            blank_count,
            typed_count,
            len(idx),
        )
        if blank_count == 0:
            logger.info("No blank-key row found — correction may already be applied.")
        return 0

    # ── Step 1: write the correctly-keyed row as a per-VM shard ──────────
    writer = ManifestWriter(
        service_name="instruments-service",
        catalogue_bucket=bucket,
        per_vm_shards=True,
        batch_size=1,
        strict_validation=False,
    )

    now = datetime.now(UTC)
    writer.record_captured_from_counts(
        row_key={
            "date": AFFECTED_DATE,
            "venue": AFFECTED_VENUE,
            "data_type": DATA_TYPE,
        },
        total_rows=AFFECTED_ROW_COUNT,
        expected_root_clusters={},
        observed_clusters={"": AFFECTED_ROW_COUNT},
        available_at_envelope=pd.Timestamp(now),
        pipeline_mode=PipelineMode.BATCH_INSTRUMENTS_SERVICE,
        asset_group=ASSET_GROUP,
        service_emission_state=None,
    )
    writer.close()
    logger.info("Step 1/2 done: wrote correctly-keyed row (data_type=%s) as per-VM shard", DATA_TYPE)

    # ── Step 2: remove the orphaned blank-key row from canonical index ────
    # data_type IS in _BASE_DEDUP_COLS, so the correctly-keyed row and the
    # blank-key row are different dedup keys — the consolidator does NOT
    # auto-reconcile them. Read the canonical index (84K rows, <10 MB),
    # filter out the single bad row, upload back with CAS precondition.
    client = get_storage_client()
    index_path = "_index/availability_index.parquet"
    from unified_trading_library import (
        read_availability_index,
    )

    idx = read_availability_index(bucket)
    n_before = len(idx)

    # Identify the single blank-key orphan row.
    blank_mask = (
        (idx["date"] == AFFECTED_DATE)
        & (idx["venue"] == AFFECTED_VENUE)
        & (idx["data_type"] == "")
        & (idx["capture_status"] == "captured")
        & (idx["service_name"] == "instruments-service")
    )
    blank_count = int(blank_mask.sum())
    if blank_count == 0:
        logger.info("Step 2/2: no blank-key row found in canonical index — already reconciled.")
    elif blank_count > 1:
        logger.warning("Step 2/2: found %d blank-key rows (expected 1) — removing all", blank_count)
    else:
        logger.info("Step 2/2: identified 1 blank-key row to remove")

    if blank_count > 0:
        cleaned = idx[~blank_mask].copy()
        n_after = len(cleaned)
        logger.info("Step 2/2: filtered %d → %d rows, uploading CAS-safe", n_before, n_after)

        import io as _io

        buf = _io.BytesIO()
        cleaned.to_parquet(buf, index=False, compression="snappy")
        payload = buf.getvalue()

        # Read the current blob generation for CAS precondition via the
        # UTL storage-client abstraction (not raw google-cloud-storage).
        __, gen = client.download_bytes_with_generation(bucket, index_path)
        logger.info("Step 2/2: current generation=%s, payload=%d bytes", gen, len(payload))

        # CAS-safe upload: only write if generation hasn't changed.
        new_gen = client.conditional_upload_bytes(
            bucket, index_path, payload, if_generation_match=gen, content_type="application/octet-stream"
        )
        if new_gen is None:
            logger.error(
                "Step 2/2: CAS precondition failed — a concurrent writer landed between "
                "our read and write. Re-run the script to retry with the updated generation."
            )
            return 2
        logger.info(
            "Step 2/2 done: canonical index rewritten with %d rows (%d removed), new gen=%s",
            n_after,
            blank_count,
            new_gen,
        )

    logger.info(
        "APPLY COMPLETE: corrected cell (%s, %s, %s) row_count=%d. "
        "Next: re-run cf_manifest_audit.audit() to confirm 0 captured+blank rows remain.",
        AFFECTED_DATE,
        AFFECTED_VENUE,
        DATA_TYPE,
        AFFECTED_ROW_COUNT,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
