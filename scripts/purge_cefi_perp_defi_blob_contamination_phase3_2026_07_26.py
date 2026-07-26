#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the EXTENDED/PACIFICA/LIGHTER defi by_date blob purge is verified in prod
"""Phase 3 — purge the defi ``by_date`` blob contamination for the 3 cefi on-chain perps.

EXTENDED-STARKNET/PACIFICA-SOLANA/LIGHTER-ZKSYNC were reclassified defi->cefi (Phase 1,
IS@2f7d454, 2026-06-25). Phase 2 (``purge_cefi_perp_defi_contamination_2026_06_25.py``)
already purged the manifest ``_index`` rows for these venues under ``asset_group=defi``, but
never touched the underlying ``instrument_availability/by_date/`` blob files themselves —
those are the actual GCS objects this script targets.

These venues are excluded from ``_build_defi_venues()`` (``engine/orchestrator/defi.py``)
since the reclassification, so nothing writes new objects to this defi-bucket path for these
venues, and nothing under deployment-api/strategy-service reads the DEFI-bucket-scoped path
for them (their live data is read from the cefi bucket/manifest post-reclassification) — the
objects here are pure historical contamination, not a live secondary copy of anything.

This script:
  1. Lists every object under ``instrument_availability/by_date/**/venue={V}/`` for each of
     the 3 venues (prefix-scoped listing under the by_date subtree, not a whole-corpus walk).
  2. SNAPSHOTS each object (server-side copy) to
     ``_purge_snapshots/cefi_perp_defi_blob_contamination_phase3_2026_07_26/<original_path>``
     before deleting it — real content backup, not just a manifest-row snapshot, since this
     script deletes raw blobs rather than a rewritable index.
  3. Deletes the original object.
  4. Verifies 0 objects remain under the 3 venue prefixes.

HARD STOP (codex/02-data/gcs-and-manifest-delete-safety-protocol.md #3.1): this bucket
(``instruments-store-defi-prd-{PID}``) is a `-prd-` production bucket — ANY delete from it is
a human-only hard stop, at any confidence level. This script's --apply path must be run by a
human operator, never by an autonomous agent. Default is --dry-run (list + count only, no
snapshot, no delete, no write of any kind).

Usage:
    python scripts/purge_cefi_perp_defi_blob_contamination_phase3_2026_07_26.py           # dry-run
    python scripts/purge_cefi_perp_defi_blob_contamination_phase3_2026_07_26.py --apply    # OPERATOR ONLY
"""

from __future__ import annotations

import sys

from unified_trading_library import gcs_copy_object, gcs_delete_object, get_storage_client

PID = "central-element-323112"
BUCKET = f"instruments-store-defi-prd-{PID}"
VENUES = ("EXTENDED-STARKNET", "PACIFICA-SOLANA", "LIGHTER-ZKSYNC")
BY_DATE_PREFIX = "instrument_availability/by_date/"
SNAPSHOT_PREFIX = "_purge_snapshots/cefi_perp_defi_blob_contamination_phase3_2026_07_26/"
APPLY = "--apply" in sys.argv


def _list_venue_objects(st, venue: str) -> list[str]:
    return [blob.name for blob in st.list_blobs(BUCKET, prefix=BY_DATE_PREFIX) if f"venue={venue}/" in blob.name]


def main() -> int:
    st = get_storage_client(project_id=PID)
    print(f"bucket={BUCKET}")
    per_venue: dict[str, list[str]] = {}
    total = 0
    for venue in VENUES:
        objs = _list_venue_objects(st, venue)
        per_venue[venue] = objs
        total += len(objs)
        print(f"venue={venue}: object_count={len(objs)}")
    print(f"TOTAL objects to purge: {total}")

    if not APPLY:
        print("DRY-RUN (pass --apply to write). Snapshot + delete skipped.")
        return 0

    for venue, objs in per_venue.items():
        for path in objs:
            src_uri = f"gs://{BUCKET}/{path}"
            dst_uri = f"gs://{BUCKET}/{SNAPSHOT_PREFIX}{path}"
            gcs_copy_object(src_uri, dst_uri)
            gcs_delete_object(src_uri)
        print(f"venue={venue}: snapshotted + deleted {len(objs)} objects")

    # verify: 0 objects remain under any of the 3 venue prefixes
    remaining_total = 0
    for venue in VENUES:
        remaining = len(_list_venue_objects(st, venue))
        remaining_total += remaining
        print(f"VERIFY venue={venue}: remaining={remaining}")
    if remaining_total != 0:
        print(f"FAILED: {remaining_total} objects still remain after purge")
        return 1
    print(f"APPLIED + VERIFIED: {total} objects snapshotted to {SNAPSHOT_PREFIX} and deleted; 0 remain.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
