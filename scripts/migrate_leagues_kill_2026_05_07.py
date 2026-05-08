#!/usr/bin/env python3
"""migrate_leagues_kill_2026_05_07.py — flip retired LEAGUES manifest rows.

C.1 audit (manifest_migration_master_2026_05_07.md § Audit findings
2026-05-07): retire the api_football LEAGUES daily-dump (3046 daily shards
of identical static league refdata) because UAC ``LeagueDefinition`` +
``provider_league_ids`` (FOOTYSTATS_SEASON_IDS, FOOTYSTATS_HISTORICAL_SEASON_IDS,
TRANSFERMARKT_IDS, etc.) canonicalise the league metadata via code commits
at season start.

The orchestrator write path was killed in instruments-service@93efebf —
no new LEAGUES rows are written from now on. This script handles the
existing manifest rows: flips every ``data_type=LEAGUES`` row from
``captured`` (or any other status) to
``capture_status=empty_confirmed`` + ``error_reason=EXPECTED_DEPRECATED_DATA_TYPE``
(per UAC ``EmptyConfirmedReason`` shipped in unified-api-contracts@97dccc3).

Downstream effect:

* Orchestrator pre-flight skip uses these rows to know "already handled,
  don't re-attempt." Marking them ``empty_confirmed`` means VMs will not
  retry the (now-deleted) write path.
* deployment-api data-status panel reads the manifest as honest absence —
  LEAGUES disappears from the per-day denominator (per the codex SSOT,
  ``empty_confirmed`` rows clip the denominator) so the panel stops
  rendering 3046 phantom-missing days.
* ML / features consumers do not consume LEAGUES (verified via
  workspace-wide ``rg`` 2026-05-07 — features-sports declares
  ``LEAGUES_COLUMNS`` in its schema but no actual feature compute reads
  ``logo_url`` or other LEAGUES-specific fields beyond what UAC provides).

The on-disk parquets at ``gs://instruments-store-sports-{pid}/sports_reference/
by_date/day=*/entity=leagues/leagues.parquet`` are NOT deleted by this script.
That is a separate operator-confirmed step (preserves a rollback path until
we're sure no consumer regresses). Operator runs:

    gcloud storage rm -r 'gs://instruments-store-sports-{pid}/sports_reference/by_date/day=*/entity=leagues/'

after this manifest flip lands and a panel re-walk confirms LEAGUES is
gone from every render.

Usage::

    # Scan-only (default) — produces /tmp/migrate-leagues-kill-{ts}.csv
    cd instruments-service
    .venv/bin/python scripts/migrate_leagues_kill_2026_05_07.py

    # Apply (after CSV review)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=migrate-leagues-kill-$(date +%s) \\
    .venv/bin/python scripts/migrate_leagues_kill_2026_05_07.py --apply

Workspace rules honoured:

* Per-VM shard write isolation (``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=...``)
  required when ``--apply`` per the manifest concurrency principle. Without
  per-VM isolation a multi-worker run would clobber the canonical CAS.
* CSV audit listing every flipped row (shard_key, original capture_status).
* Idempotent: re-running on already-flipped rows is a no-op (rows already
  ``empty_confirmed`` with reason ``EXPECTED_DEPRECATED_DATA_TYPE`` are
  excluded from the flip set).
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
RETIRED_DATA_TYPE = "LEAGUES"
NEW_REASON = "EXPECTED_DEPRECATED_DATA_TYPE"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually flip rows. Default scan-only (dry-run).",
    )
    p.add_argument(
        "--max-flips",
        type=int,
        default=100_000,
        help="Halt safety cap (default 100k). LEAGUES universe is ~3046 rows; default cap leaves plenty of headroom.",
    )
    args = p.parse_args()

    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not os.environ.get("VM_NAME")):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique> per the manifest concurrency "
            "principle. Without per-VM isolation a multi-worker run clobbers the canonical CAS. "
            "Refusing to mutate."
        )
        return 1

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(SPORTS_BUCKET)
    blob = bucket.blob(MANIFEST_BLOB)

    logger.info("Loading sports manifest from gs://%s/%s", SPORTS_BUCKET, MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(prefix="migrate-leagues-", suffix=".parquet", delete=False) as _tf:
        manifest_path = _tf.name
    try:
        blob.download_to_filename(manifest_path)
        df = pd.read_parquet(manifest_path)
    finally:
        import contextlib

        with contextlib.suppress(OSError):
            os.unlink(manifest_path)
    logger.info("Manifest rows: %d", len(df))

    # Find rows with data_type=LEAGUES that need flipping.
    leagues_mask = df["data_type"].fillna("") == RETIRED_DATA_TYPE
    already_flipped_mask = leagues_mask & (
        (df["capture_status"].fillna("") == "empty_confirmed") & (df["error_reason"].fillna("") == NEW_REASON)
    )
    to_flip_mask = leagues_mask & ~already_flipped_mask

    n_total = int(leagues_mask.sum())
    n_already = int(already_flipped_mask.sum())
    n_to_flip = int(to_flip_mask.sum())
    logger.info("=" * 60)
    logger.info("LEAGUES manifest rows total:        %d", n_total)
    logger.info("Already flipped (idempotent skip):  %d", n_already)
    logger.info("Will flip to empty_confirmed:       %d", n_to_flip)
    logger.info("=" * 60)

    if n_to_flip == 0:
        logger.info("Nothing to flip — manifest is already clean for LEAGUES kill.")
        return 0

    if n_to_flip > args.max_flips:
        logger.error(
            "n_to_flip=%d exceeds --max-flips=%d halt safety. LEAGUES universe is ~3046 rows; "
            "if you are seeing >100k rows something is wrong. Investigate before lifting the cap.",
            n_to_flip,
            args.max_flips,
        )
        return 2

    # Distribution by current capture_status (for audit).
    flip_df = df.loc[to_flip_mask]
    by_status = flip_df.groupby("capture_status", dropna=False).size()
    logger.info("Flip distribution by current capture_status:\n%s", by_status.to_string())
    logger.info("=" * 60)

    # CSV audit.
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    csv_path = Path(tempfile.gettempdir()) / f"migrate-leagues-kill-{ts}.csv"
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

    # Apply the flip.
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
