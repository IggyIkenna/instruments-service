#!/usr/bin/env python3
"""migrate_teams_cadence_2026_05_07.py — flip TEAMS daily-cadence manifest rows.

C.11 audit (manifest_migration_master_2026_05_07.md § Refdata cadence
SSOT — groups C.1 + C.11): TEAMS rosters change per-season at most (transfer
windows are bounded events, not daily drift), but the orchestrator currently
writes one TEAMS shard per league per day — ~830x denominator inflation
relative to the actual per-season refresh cadence.

UAC SchemaContract added a ``cadence`` field 2026-05-07 (unified-api-contracts@e12af89);
``SPORTS_TEAMS.cadence = "per_season"`` declares the canonical shape going
forward. This script handles the LEGACY daily shards: flips every existing
``data_type=TEAMS`` daily-cadence row to
``capture_status=empty_confirmed`` + ``error_reason=EXPECTED_REFDATA_CADENCE_CHANGE``
(per UAC ``EmptyConfirmedReason`` shipped in unified-api-contracts@97dccc3).

The expectation under the new cadence: one shard per (league, season,
fetch_day_when_changed). Until the orchestrator ships the per-season writer
(C.11 Unit 2 — DEFERRED to a follow-up commit per the manifest_migration_master
plan), this script only flips the legacy daily rows; new per-(league, season)
rows will land later.

Downstream effect post-apply:

* Orchestrator pre-flight skip uses these rows to know "already handled" —
  VMs do not retry the daily write (which the per-season writer will replace).
* deployment-api data-status panel reads the legacy daily rows as honest
  absence under the new cadence — TEAMS denominator falls from ~80k daily
  shards to ~(95 leagues x N active seasons) per-season shards once the
  panel reads ``SchemaContract.cadence`` (C.11 Unit 4, also DEFERRED).
* features-sports does NOT consume TEAMS for any actual feature compute
  (verified via workspace-wide rg 2026-05-07 — TEAMS_COLUMNS is declared
  in schemas/output_schemas.py but no feature reads team_logo / venue_*
  fields beyond what UAC LeagueDefinition + per-source mapping provide).

The on-disk parquets at ``gs://instruments-store-sports-{pid}/sports_reference/
by_date/day=*/entity=teams/league={L}/teams.parquet`` are NOT deleted by this
script. The per-season writer (C.11 Unit 2) will populate the new shape;
operator can run a separate cleanup step after a panel re-walk confirms the
new shape is rendering correctly:

    gcloud storage rm -r 'gs://instruments-store-sports-{pid}/sports_reference/by_date/day=*/entity=teams/'

(only after the per-season writer is shipping AND the panel shows the new
cadence-aware denominator with TEAMS at expected per-season counts).

Usage::

    # Scan-only (default) — produces /tmp/migrate-teams-cadence-{ts}.csv
    cd instruments-service
    .venv/bin/python scripts/migrate_teams_cadence_2026_05_07.py

    # Apply (after CSV review + after the per-season writer is shipped)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=migrate-teams-cadence-$(date +%s) \\
    .venv/bin/python scripts/migrate_teams_cadence_2026_05_07.py --apply

Workspace rules honoured:

* Per-VM shard write isolation (``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=...``)
  required when ``--apply`` per the manifest concurrency principle. Without
  per-VM isolation a multi-worker run would clobber the canonical CAS.
* CSV audit listing every flipped row (shard_key, original capture_status).
* Idempotent: re-running on already-flipped rows is a no-op.
* ``--max-flips=200k`` halt safety cap (TEAMS universe is ~80k daily shards
  if the LEAGUES experience generalises; default cap leaves headroom for
  larger truth).

**ORDER-OF-OPERATIONS CAUTION**: this flip should ship AFTER the per-season
writer is live (C.11 Unit 2). If you flip the legacy daily rows BEFORE the
per-season writer ships, downstream consumers reading `data_type=TEAMS` get
empty results (no daily rows AND no per-season rows yet). Coordinate with
sports-master operator gates.
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
RETIRED_CADENCE_DATA_TYPE = "TEAMS"
NEW_REASON = "EXPECTED_REFDATA_CADENCE_CHANGE"


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
        default=200_000,
        help="Halt safety cap (default 200k). LEAGUES experience suggests ~80k for TEAMS daily shards.",
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
    with tempfile.NamedTemporaryFile(prefix="migrate-teams-cadence-", suffix=".parquet", delete=False) as _tf:
        manifest_path = _tf.name
    try:
        blob.download_to_filename(manifest_path)
        df = pd.read_parquet(manifest_path)
    finally:
        import contextlib

        with contextlib.suppress(OSError):
            os.unlink(manifest_path)
    logger.info("Manifest rows: %d", len(df))

    # Find rows with data_type=TEAMS that need flipping (idempotent skip on
    # already-flipped rows so re-runs are no-ops).
    teams_mask = df["data_type"].fillna("") == RETIRED_CADENCE_DATA_TYPE
    already_flipped_mask = teams_mask & (
        (df["capture_status"].fillna("") == "empty_confirmed") & (df["error_reason"].fillna("") == NEW_REASON)
    )
    to_flip_mask = teams_mask & ~already_flipped_mask

    n_total = int(teams_mask.sum())
    n_already = int(already_flipped_mask.sum())
    n_to_flip = int(to_flip_mask.sum())
    logger.info("=" * 60)
    logger.info("TEAMS manifest rows total:          %d", n_total)
    logger.info("Already flipped (idempotent skip):  %d", n_already)
    logger.info("Will flip to empty_confirmed:       %d", n_to_flip)
    logger.info("=" * 60)

    if n_to_flip == 0:
        logger.info("Nothing to flip — manifest is already clean for TEAMS cadence migration.")
        return 0

    if n_to_flip > args.max_flips:
        logger.error(
            "n_to_flip=%d exceeds --max-flips=%d halt safety. Investigate before lifting the cap.",
            n_to_flip,
            args.max_flips,
        )
        return 2

    # Distribution by current capture_status (for audit).
    flip_df = df.loc[to_flip_mask]
    by_status = flip_df.groupby("capture_status", dropna=False).size()
    logger.info("Flip distribution by current capture_status:\n%s", by_status.to_string())
    # Per-league breakdown if league_id is populated.
    if "league_id" in flip_df.columns:
        by_league = flip_df["league_id"].fillna("<empty>").value_counts().head(20)
        logger.info("Top 20 league_ids in flip set:\n%s", by_league.to_string())
    logger.info("=" * 60)

    # CSV audit.
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    csv_path = Path(tempfile.gettempdir()) / f"migrate-teams-cadence-{ts}.csv"
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
        logger.info(
            "ORDER-OF-OPERATIONS CAUTION: ship the per-season writer (C.11 Unit 2) FIRST. "
            "Flipping legacy daily rows before the per-season writer is live leaves downstream "
            "consumers with empty results."
        )
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
