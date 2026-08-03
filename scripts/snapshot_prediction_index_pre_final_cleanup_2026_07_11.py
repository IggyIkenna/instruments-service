#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after prediction__index final residual cleanup 2026-07-11 lands + is verified
"""Snapshot the prediction pred-prd _index BEFORE the final residual cleanup (classes A/B/C).

Server-side copy (no data egress) via the UTL StorageClient's ``copy_blob`` — never inline
gs:// / subprocess gsutil per codex/05-infrastructure/gcs-object-operations.md. (Uses
``get_storage_client().copy_blob(...)`` directly rather than the ``gcs_copy_object`` URI
helper — the latter is not re-exported from the top-level ``unified_trading_library``
package, only from ``unified_trading_library.cloud_interface``, which trips the repo's
import-pattern gate for a deep import.)
"""

from __future__ import annotations

import sys

from unified_trading_library import get_storage_client, resolve_bucket_name

_BUCKET = resolve_bucket_name(cloud="gcp", kind="market-data-tick-prediction")
_INDEX_BLOB = "_index/availability_index.parquet"
_SNAPSHOT_BLOB = "_index/snapshots/pre_final_cleanup_2026_07_11.parquet"


def main() -> int:
    storage = get_storage_client()
    print(f"Copying gs://{_BUCKET}/{_INDEX_BLOB} -> gs://{_BUCKET}/{_SNAPSHOT_BLOB}")
    storage.copy_blob(_BUCKET, _INDEX_BLOB, _BUCKET, _SNAPSHOT_BLOB)
    print("Snapshot copy complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
