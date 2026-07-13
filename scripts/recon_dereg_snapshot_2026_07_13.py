#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after the 24-league de-registration run (2026-07-13) is confirmed applied in prod
"""Recon step 1: fresh availability-index snapshot + local download (READ-ONLY apart from the snapshot copy).

Copies gs://<sports-instruments-bucket>/_index/availability_index.parquet to
_index/snapshots/availability_index_<UTCts>.parquet via UTL gcs_copy_object,
verifies size+crc32c source vs snapshot, then downloads the SNAPSHOT object
(immutable) to the local path given as argv[1] so local analysis is guaranteed
to match the snapshot byte-for-byte.

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    python scripts/recon_dereg_snapshot_2026_07_13.py /path/to/local/index.parquet
"""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from unified_trading_library import get_storage_client, resolve_bucket_name
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    gcs_copy_object,
    gcs_describe_object,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("recon_dereg_snapshot")

INDEX_BLOB = "_index/availability_index.parquet"


def main() -> int:
    local_path = Path(sys.argv[1])
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    snap_blob = f"_index/snapshots/availability_index_{ts}.parquet"
    src_uri = f"gs://{bucket}/{INDEX_BLOB}"  # noqa: gs-uri — one-off recon script
    dst_uri = f"gs://{bucket}/{snap_blob}"  # noqa: gs-uri — one-off recon script

    src_meta_before = gcs_describe_object(src_uri)
    if src_meta_before is None:
        logger.error("Source index does not exist: %s", src_uri)
        return 2
    logger.info(
        "Source before copy: size=%s crc32c=%s generation=%s updated=%s",
        src_meta_before.size,
        src_meta_before.crc32c,
        getattr(src_meta_before, "generation", "?"),
        getattr(src_meta_before, "updated", "?"),
    )

    gcs_copy_object(src_uri, dst_uri)
    snap_meta = gcs_describe_object(dst_uri)
    src_meta_after = gcs_describe_object(src_uri)
    if snap_meta is None or src_meta_after is None:
        logger.error("Post-copy describe failed (snap=%s src=%s)", snap_meta, src_meta_after)
        return 2

    logger.info("Snapshot: %s size=%s crc32c=%s", dst_uri, snap_meta.size, snap_meta.crc32c)
    logger.info("Source after copy: size=%s crc32c=%s", src_meta_after.size, src_meta_after.crc32c)

    if (snap_meta.size, snap_meta.crc32c) == (src_meta_before.size, src_meta_before.crc32c):
        logger.info("VERIFY OK: snapshot matches source-as-described-before-copy (size+crc32c).")
    elif (snap_meta.size, snap_meta.crc32c) == (src_meta_after.size, src_meta_after.crc32c):
        logger.info(
            "VERIFY OK: snapshot matches source-after-copy (index rewritten mid-window; snapshot is a valid version)."
        )
    else:
        logger.error("VERIFY FAILED: snapshot matches neither before- nor after-copy source metadata.")
        return 3

    storage = get_storage_client()
    raw = storage.download_bytes(bucket, snap_blob)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(raw)
    logger.info("Downloaded snapshot to %s (%d bytes; expected %s)", local_path, len(raw), snap_meta.size)
    if len(raw) != int(snap_meta.size):
        logger.error("Local size mismatch vs snapshot metadata.")
        return 4

    print(f"SNAPSHOT_URI={dst_uri}")
    print(f"SNAPSHOT_SIZE={snap_meta.size}")
    print(f"SNAPSHOT_CRC32C={snap_meta.crc32c}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
