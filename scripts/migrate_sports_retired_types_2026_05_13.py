#!/usr/bin/env python3
"""migrate_sports_retired_types_2026_05_13.py — flip retired sports data_types.

Generalization of ``migrate_leagues_kill_2026_05_07.py`` covering the full set of
retired sports refdata data_types discovered during slot 4 phantom audit
2026-05-13 (99,620 sports phantoms; 88,737 from retired data types).

Retired sports data_types covered:

* ``TRANSFERMARKT_LEAGUES`` — retired 2026-05-05; provider catalog now in UAC
  ``TRANSFERMARKT_IDS`` (provider-id config), not captured daily.
* ``SFI_LEAGUES`` — retired 2026-05-05; provider catalog now in UAC
  ``SOCCER_FOOTBALL_INFO_IDS``.
* ``SFI_STANDINGS`` — retired 2026-04-24; SFI has no standings endpoint.

Each row matching ``data_type in {RETIRED_TYPES}`` is flipped from any current
``capture_status`` to ``empty_confirmed`` + ``error_reason=EXPECTED_DEPRECATED_DATA_TYPE``
(per UAC ``EmptyConfirmedReason`` shipped in unified-api-contracts@97dccc3).

The api_football ``LEAGUES`` daily-dump is handled by
``migrate_leagues_kill_2026_05_07.py``. This script handles the THREE other retired
sports types.

Downstream effect (per the C.1 LEAGUES kill SSOT):

* Orchestrator pre-flight uses these rows to know "already handled, don't
  re-attempt." Marking them ``empty_confirmed`` means VMs will not retry the
  (now-deleted) write path.
* deployment-api data-status panel reads the manifest as honest absence —
  retired types clip the denominator instead of inflating phantom counts.
* features-sports / ML / strategy consumers do not consume these data_types
  (UAC TRANSFERMARKT_IDS / SOCCER_FOOTBALL_INFO_IDS canonicalise them as code).

GCS parquet deletion is a separate operator step (preserves rollback path):

    gcloud storage rm -r 'gs://instruments-store-sports-{pid}/sports_reference/by_date/day=*/entity=tm_leagues/'
    gcloud storage rm -r 'gs://instruments-store-sports-{pid}/sports_reference/by_date/day=*/entity=sfi_leagues/'
    gcloud storage rm -r 'gs://instruments-store-sports-{pid}/sports_reference/by_date/day=*/entity=sfi_standings/'

Usage::

    # Scan-only (default) — produces /tmp/migrate-sports-retired-{ts}.csv
    cd instruments-service
    .venv/bin/python scripts/migrate_sports_retired_types_2026_05_13.py

    # Apply (after CSV review)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=migrate-sports-retired-$(date +%s) \
    .venv/bin/python scripts/migrate_sports_retired_types_2026_05_13.py --apply

    # Restrict to a subset (debug)
    .venv/bin/python scripts/migrate_sports_retired_types_2026_05_13.py \
        --data-types TRANSFERMARKT_LEAGUES,SFI_LEAGUES
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
SPORTS_BUCKET = f"instruments-store-sports-{PROJECT_ID}"
MANIFEST_BLOB = "_index/availability_index.parquet"
NEW_REASON = "EXPECTED_DEPRECATED_DATA_TYPE"

DEFAULT_RETIRED_TYPES = frozenset(
    {
        "TRANSFERMARKT_LEAGUES",
        "SFI_LEAGUES",
        "SFI_STANDINGS",
    }
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually flip rows. Default scan-only (dry-run).",
    )
    p.add_argument(
        "--data-types",
        type=str,
        default=",".join(sorted(DEFAULT_RETIRED_TYPES)),
        help=f"Comma-separated retired data_types to flip (default: {','.join(sorted(DEFAULT_RETIRED_TYPES))}).",
    )
    p.add_argument(
        "--max-flips",
        type=int,
        default=200_000,
        help="Halt safety cap (default 200k). Sports retired-types universe is ~88,737 rows.",
    )
    args = p.parse_args()

    retired_types = frozenset(s.strip() for s in args.data_types.split(",") if s.strip())
    if not retired_types:
        logger.error("--data-types resolved to empty set. Refusing.")
        return 1

    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not os.environ.get("VM_NAME")):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique> per the manifest "
            "concurrency principle. Without per-VM isolation a multi-worker run clobbers the canonical "
            "CAS. Refusing to mutate."
        )
        return 1

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(SPORTS_BUCKET)
    blob = bucket.blob(MANIFEST_BLOB)

    logger.info("Loading sports manifest from gs://%s/%s", SPORTS_BUCKET, MANIFEST_BLOB)
    logger.info("Retired data_types to flip: %s", sorted(retired_types))
    with tempfile.NamedTemporaryFile(prefix="migrate-sports-retired-", suffix=".parquet", delete=False) as _tf:
        manifest_path = _tf.name
    try:
        blob.download_to_filename(manifest_path)
        df = pd.read_parquet(manifest_path)
    finally:
        import contextlib

        with contextlib.suppress(OSError):
            os.unlink(manifest_path)
    logger.info("Manifest rows: %d", len(df))

    retired_mask = df["data_type"].fillna("").isin(retired_types)
    already_flipped_mask = retired_mask & (
        (df["capture_status"].fillna("") == "empty_confirmed") & (df["error_reason"].fillna("") == NEW_REASON)
    )
    to_flip_mask = retired_mask & ~already_flipped_mask

    n_total = int(retired_mask.sum())
    n_already = int(already_flipped_mask.sum())
    n_to_flip = int(to_flip_mask.sum())
    logger.info("=" * 60)
    logger.info("Retired-type manifest rows total:    %d", n_total)
    logger.info("Already flipped (idempotent skip):   %d", n_already)
    logger.info("Will flip to empty_confirmed:        %d", n_to_flip)
    logger.info("=" * 60)

    if n_to_flip == 0:
        logger.info("Nothing to flip — manifest is already clean for these retired types.")
        return 0

    if n_to_flip > args.max_flips:
        logger.error(
            "n_to_flip=%d exceeds --max-flips=%d halt safety. Investigate before lifting the cap.",
            n_to_flip,
            args.max_flips,
        )
        return 2

    flip_df = df.loc[to_flip_mask]
    by_data_type = flip_df.groupby("data_type", dropna=False).size()
    by_status = flip_df.groupby("capture_status", dropna=False).size()
    logger.info("Flip distribution by data_type:\n%s", by_data_type.to_string())
    logger.info("Flip distribution by current capture_status:\n%s", by_status.to_string())
    logger.info("=" * 60)

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    csv_path = Path(tempfile.gettempdir()) / f"migrate-sports-retired-{ts}.csv"
    audit_cols = [
        "date",
        "venue",
        "data_type",
        "league_id",
        "instrument_id",
        "capture_status",
        "error_reason",
        "attempted_at",
    ]
    audit_existing = [c for c in audit_cols if c in flip_df.columns]
    flip_df[audit_existing].to_csv(csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logger.info("CSV audit written to %s (%d rows)", csv_path, n_to_flip)

    if not args.apply:
        logger.info("DRY RUN — manifest not modified. Re-run with --apply to flip.")
        return 0

    now_iso = datetime.now(UTC).isoformat()
    df.loc[to_flip_mask, "capture_status"] = "empty_confirmed"
    df.loc[to_flip_mask, "error_reason"] = NEW_REASON
    df.loc[to_flip_mask, "attempted_at"] = now_iso

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    logger.info("Uploading flipped manifest (%d rows total, %d flipped)", len(df), n_to_flip)
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("Done. CSV audit at %s", csv_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
