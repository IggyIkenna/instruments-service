"""One-shot purge of `PANCAKESWAPV3-ZKSYNC` rows from the DeFi manifest.

Why:
    These 446 rows were the only legacy DeFi rows the canonical
    ``migrate_defi_legacy_venue_chain.py`` migration left untouched (because
    ZKSYNC is not in UAC ``KNOWN_CHAINS``). Operator decision 2026-05-06:
    the data is low-quality + low-volume, and we stopped capturing this venue.
    Cleanest fix is delete + scrub UAC entries, NOT add ZKSYNC to KNOWN_CHAINS.

Companion UAC removals:
    * ``unified_api_contracts/registry/defi_venue_capabilities.py`` — entry deleted
    * ``unified_api_contracts/registry/defi_venues.py`` — entry deleted

Usage:
    python instruments-service/scripts/purge_pancakeswapv3_zksync.py \\
        --project central-element-323112 [--dry-run]
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from typing import cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

_MANIFEST_PATH = "_index/availability_index.parquet"
_TARGET_VENUE = "PANCAKESWAPV3-ZKSYNC"


def _bucket_name(project_id: str) -> str:
    return f"instruments-store-defi-{project_id}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Purge PANCAKESWAPV3-ZKSYNC rows from DeFi manifest.")
    parser.add_argument("--project", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = get_storage_client(project_id=args.project)
    bucket = _bucket_name(args.project)
    blob_bytes = client.download_bytes(bucket=bucket, blob_path=_MANIFEST_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    df = cast(pd.DataFrame, pq.read_table(io.BytesIO(blob_bytes)).to_pandas())  # pyright: ignore[reportUnknownMemberType]

    if "venue" not in df.columns:
        print("manifest has no `venue` column — nothing to do.")
        return 0

    mask = df["venue"].astype(str) == _TARGET_VENUE
    n_drop = int(mask.sum())
    print(f"rows_total: {len(df)}")
    print(f"rows_to_drop ({_TARGET_VENUE}): {n_drop}")
    if n_drop == 0:
        print("nothing to purge — exiting clean.")
        return 0

    if args.dry_run:
        print("DRY RUN: not writing.")
        return 0

    pruned = df[~mask].copy()
    table = pa.Table.from_pandas(pruned)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    client.upload_bytes(  # pyright: ignore[reportAttributeAccessIssue]
        bucket=bucket,
        blob_path=_MANIFEST_PATH,
        data=buf.getvalue(),
        content_type="application/octet-stream",
    )
    print(f"DONE: wrote {len(pruned)} rows (dropped {n_drop}) to gs://{bucket}/{_MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
