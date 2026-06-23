#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + GCS orphan-sweep = 0 for migration targets
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

CONSOLIDATOR-SAFE WRITE (2026-06-23 — supersedes the full-``_index`` overwrite).
A live-odds MTDS VM + ~25 backfill VMs write per-VM shards while the manifest
consolidator (Cloud Run cron) merges them into the canonical
``_index/availability_index.parquet`` every minute. A full-index ``upload`` of
the whole frame RACES that merge and can DROP live rows written since this
script's read (the pre-migration-drain HARD RULE). Instead ``--apply`` writes
ONLY the flipped rows as a **per-VM shard** at ``_index/per_vm/{VM_NAME}.parquet``
(the canonical fleet write path, ``manifest_writer._PER_VM_PATH_TEMPLATE``). The
consolidator's DuckDB last-write-wins merge (``manifest_consolidator``:
``PARTITION BY (date, venue, data_type, service_name, <dims…>) ORDER BY
attempted_at DESC NULLS LAST, written_at DESC NULLS LAST``) then collapses each
(canonical retired row, shard flipped row) pair into ONE group and picks the
shard row because its ``attempted_at`` is fresher → the reclassify WINS for its
keys without ever touching the canonical blob. The shard carries the SAME
dedup-key column values as the canonical rows it replaces (only
``capture_status`` / ``error_reason`` / ``attempted_at`` / ``written_at`` change)
so the key-match is exact. Live-odds rows have different keys → never in the
shard → never contested → never lost. SSOT:
``codex/05-infrastructure/manifest-consolidator-ssot.md`` § "Merge engine" +
``codex/02-data/availability-manifest-and-data-status.md``.

Downstream effect (per the C.1 LEAGUES kill SSOT):

* Orchestrator pre-flight uses these rows to know "already handled, don't
  re-attempt." Marking them ``empty_confirmed`` means VMs will not retry the
  (now-deleted) write path.
* deployment-api data-status panel reads the manifest as honest absence —
  retired types clip the denominator instead of inflating phantom counts.
* features-sports / ML / strategy consumers do not consume these data_types
  (UAC TRANSFERMARKT_IDS / SOCCER_FOOTBALL_INFO_IDS canonicalise them as code).

GCS parquet deletion is a separate operator step (preserves rollback path):

    gcloud storage rm -r 'gs://instruments-store-sports-{env}-{pid}/sports_reference/by_date/day=*/entity=tm_leagues/'
    gcloud storage rm -r 'gs://instruments-store-sports-{env}-{pid}/sports_reference/by_date/day=*/entity=sfi_leagues/'
    gcloud storage rm -r 'gs://instruments-store-sports-{env}-{pid}/sports_reference/by_date/day=*/entity=sfi_standings/'

Usage::

    # Scan-only (default) — produces /tmp/migrate-sports-retired-{ts}.csv
    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
      .venv/bin/python scripts/migrate_sports_retired_types_2026_05_13.py

    # Apply (after CSV review) — writes a per-VM shard the consolidator merges
    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
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

import gcsfs
import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MANIFEST_BLOB = "_index/availability_index.parquet"
# Per-VM shard path template — matches the canonical fleet write path
# ``unified_trading_library.manifest_writer._state._PER_VM_PATH_TEMPLATE`` so the
# consolidator lists + merges this shard exactly like a writer-VM's shard.
PER_VM_PATH_TEMPLATE = "_index/per_vm/{instance}.parquet"
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
        help="Actually flip rows (writes a consolidator-merged per-VM shard). Default scan-only (dry-run).",
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
        help="Halt safety cap (default 200k). Sports retired-types universe is ~88,740 rows.",
    )
    args = p.parse_args()

    # Fail loud if the env-short isn't set — otherwise resolve_bucket_name falls
    # back to the env-LESS name and we'd silently mutate the STALE bucket
    # (frozen 2026-06-08). The LIVE canonical is the env-short ``-prd-`` one.
    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing — would resolve the stale env-less bucket.")
        return 1

    retired_types = frozenset(s.strip() for s in args.data_types.split(",") if s.strip())
    if not retired_types:
        logger.error("--data-types resolved to empty set. Refusing.")
        return 1

    instance = os.environ.get("VM_NAME", "")
    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not instance):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique> per the manifest "
            "concurrency principle. The flip is written as a per-VM shard the consolidator merges "
            "(never a full-index overwrite), so a unique VM_NAME is mandatory. Refusing."
        )
        return 1

    sports_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if not sports_bucket.startswith(f"instruments-store-sports-{env_short}"):
        logger.error("Resolved bucket %s is not the expected env-short shape. Refusing.", sports_bucket)
        return 1
    fs = gcsfs.GCSFileSystem()

    logger.info("Loading sports manifest from gs://%s/%s", sports_bucket, MANIFEST_BLOB)
    logger.info("Retired data_types to flip: %s", sorted(retired_types))
    with fs.open(f"{sports_bucket}/{MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)
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

    flip_df = df.loc[to_flip_mask].copy()
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
        logger.info("DRY RUN — manifest not modified. Re-run with --apply to write the per-VM shard.")
        return 0

    # CONSOLIDATOR-SAFE APPLY: build the flipped rows from the canonical matched
    # rows (so every dedup-key dim is preserved verbatim) and write ONLY those as
    # this VM's per-VM shard. The consolidator's last-write-wins merge picks them
    # over the canonical rows because attempted_at/written_at are fresher. The
    # canonical blob is NEVER overwritten here, so no race with the live writers.
    now_iso = datetime.now(UTC).isoformat()
    flip_df["capture_status"] = "empty_confirmed"
    flip_df["error_reason"] = NEW_REASON
    flip_df["attempted_at"] = now_iso
    if "written_at" in flip_df.columns:
        flip_df["written_at"] = now_iso

    shard_path = PER_VM_PATH_TEMPLATE.format(instance=instance)
    out = io.BytesIO()
    flip_df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    out.seek(0)
    logger.info(
        "Writing %d flipped rows as per-VM shard gs://%s/%s (consolidator will merge, last-write-wins)",
        n_to_flip,
        sports_bucket,
        shard_path,
    )
    with fs.open(f"{sports_bucket}/{shard_path}", "wb") as fh:
        fh.write(out.read())
    logger.info(
        "Done. Per-VM shard written. The next consolidator cycle (~1 min) merges it into the canonical _index. "
        "Verify with a re-run dry-run AFTER consolidation: already_flipped should == %d, will_flip == 0. CSV audit at %s",
        n_total,
        csv_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
