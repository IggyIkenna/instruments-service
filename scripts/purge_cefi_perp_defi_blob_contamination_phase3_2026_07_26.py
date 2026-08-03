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

REVERSIBILITY-QUALIFIED (codex/02-data/gcs-and-manifest-delete-safety-protocol.md §3a, 2026-07-27): this bucket
(``instruments-store-defi-prd-{PID}``) is a `-prd-` production bucket, but this delete is object/prefix-scoped
(3 named venue prefixes, never the bucket) with a content-correctness proof already established (no live writer,
no live reader — see module docstring above) — so it qualifies for the agent-autonomous path IF, and only if, a
FRESH same-run check confirms the bucket's GCS Soft Delete retention is >= 604800s (7 days). ``--apply`` performs
that check itself as its first action and aborts loudly if it does not clear the threshold — never trust a prior
session's claim or this docstring for that number. Default is --dry-run (list + count only, no snapshot, no
delete, no write of any kind).

Usage:
    python scripts/purge_cefi_perp_defi_blob_contamination_phase3_2026_07_26.py           # dry-run
    python scripts/purge_cefi_perp_defi_blob_contamination_phase3_2026_07_26.py --apply    # fresh-checks retention, then applies
"""

from __future__ import annotations

import sys

from unified_trading_library import get_storage_client

_MIN_SOFT_DELETE_RETENTION_SECONDS = 604800  # 7 days — codex/02-data/gcs-and-manifest-delete-safety-protocol.md §3a

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

    # Fresh, same-run reversibility check (§3a) — never trust a prior session's claim.
    # Uses the already-configured `st` client (project_id=PID) directly, not the
    # gcs_bucket_soft_delete_retention_seconds free-function wrapper, which resolves its
    # own uncached client via get_storage_client() with no project_id and falls back to
    # GCP_PROJECT_ID/AWS_ACCOUNT_ID env vars — unset in this script's explicit-PID convention.
    retention = st.get_bucket_soft_delete_retention_seconds(BUCKET)
    print(f"soft_delete_retention_seconds={retention} (need >= {_MIN_SOFT_DELETE_RETENTION_SECONDS})")
    if retention < _MIN_SOFT_DELETE_RETENTION_SECONDS:
        print(
            f"ABORT: {BUCKET} soft-delete retention ({retention}s) is below the {_MIN_SOFT_DELETE_RETENTION_SECONDS}s "
            "reversibility threshold -- this delete no longer qualifies for the autonomous path. Escalate to the "
            "operator per gcs-and-manifest-delete-safety-protocol.md hard-stop #1."
        )
        return 1

    for venue, objs in per_venue.items():
        for path in objs:
            # st.copy_blob/delete_blob directly, not the gcs_copy_object/gcs_delete_object
            # free-function wrappers -- same env-var-dependent client-resolution gap as the
            # retention check above.
            st.copy_blob(BUCKET, path, BUCKET, f"{SNAPSHOT_PREFIX}{path}")
            st.delete_blob(BUCKET, path)
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
