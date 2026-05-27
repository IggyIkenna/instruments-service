#!/usr/bin/env python3
"""Re-key glued DeFi venue partitions to canonical (underscore) form in GCS.

Bug 5 (defi_coverage_capability_alignment_2026_05_22.md § B5.3/B5.5): DeFi
reference/market parquets are stored under glued venue partitions (e.g.
``venue=AAVEV3-ARBITRUM/``) while the consolidated manifest venue is canonical
(``AAVE_V3``). The deployment-ui pool-breakdown resolves the canonical manifest
venue -> derives a canonical GCS path -> misses the glued-path parquet ->
"No pool data". This migration re-keys every glued venue partition to its
canonical form and deletes the old glued keys.

Canonicalisation authority: ``canonicalize_defi_venue_combined`` in UAC
(``unified_api_contracts.registry.capability_declarations._defi``). A venue dir
needs re-keying iff ``canonicalize_defi_venue_combined(venue) != venue``. The
function is a NO-OP for venues already canonical, for ``VELODROMEV2`` /
``TRADER_JOEV2`` (these ARE canonical-glued), and for unknown/non-DeFi venues.

Safety protocol (this is destructive prod data):
  - Uses ``gcs_copy_object`` / ``gcs_delete_object`` / ``gcs_describe_object``
    (NEVER subprocess gsutil), workers=32.
  - For each object under a glued venue dir: COPY old->canonical, then
    ``gcs_describe_object`` BOTH paths to verify the canonical copy exists AND
    size matches (parity), and ONLY THEN delete the old object. Never delete
    before verified copy. Idempotent: if canonical already exists with matching
    size, skip the copy and just delete the old object.
  - ``--dry-run`` (default) reports per-venue counts WITHOUT mutating.
    ``--execute`` actually performs copy+verify+delete.
  - Never raises inside the per-object loop (shard-isolation); collects errors
    and reports per-venue + total tallies.

Usage::

    cd instruments-service
    CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false GCP_PROJECT_ID=central-element-323112 \
      .venv/bin/python scripts/migration_rekey_defi_glued_venues.py            # dry-run all in-scope buckets

    CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false GCP_PROJECT_ID=central-element-323112 \
      .venv/bin/python scripts/migration_rekey_defi_glued_venues.py --execute  # perform migration

    # Single bucket:
    ... scripts/migration_rekey_defi_glued_venues.py --bucket market-data-tick-defi-central-element-323112 --execute
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import cast

from unified_api_contracts.registry.capability_declarations._defi import canonicalize_defi_venue_combined
from unified_trading_library.cloud_interface import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    gcs_copy_object,
    gcs_delete_object,
    gcs_describe_object,
)
from unified_trading_library.cloud_interface.factory import (  # noqa: qg-deep-import — get_storage_client factory for list_blobs
    get_storage_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# Buckets in scope (prefix under which DeFi venue= partitions live).
# features-onchain-defi has 0 objects → not listed.
DEFAULT_BUCKET_PREFIXES: dict[str, str] = {
    f"instruments-store-defi-{PROJECT_ID}": "instrument_availability/by_date/",
    f"market-data-tick-defi-{PROJECT_ID}": "raw_tick_data/by_date/",
}

WORKERS = 32


def _extract_venue(object_name: str) -> str | None:
    """Return the ``venue=<X>`` segment value from an object path, or None."""
    for token in object_name.split("/"):
        if token.startswith("venue="):
            return token[len("venue=") :]
    return None


def _rekey_object_name(object_name: str, glued_venue: str, canonical_venue: str) -> str:
    """Replace the ``venue=<glued>`` path segment with ``venue=<canonical>``.

    Only the exact ``venue=`` segment is replaced (not a substring elsewhere in
    the path), so this is safe even if the venue string appears in a filename.
    """
    parts = object_name.split("/")
    out: list[str] = []
    for token in parts:
        if token == f"venue={glued_venue}":
            out.append(f"venue={canonical_venue}")
        else:
            out.append(token)
    return "/".join(out)


@dataclass
class VenueTally:
    glued_venue: str
    canonical_venue: str
    objects: list[str] = field(default_factory=list)  # object names under the glued venue dir
    copied: int = 0
    verified: int = 0
    deleted: int = 0
    skipped_copy: int = 0  # canonical already existed with matching size
    errors: list[str] = field(default_factory=list)


def _discover(bucket: str, prefix: str) -> dict[str, VenueTally]:
    """Walk ``gs://bucket/prefix`` and group parquet objects by glued venue needing re-key.

    A venue needs re-keying iff ``canonicalize_defi_venue_combined(venue) != venue``.
    """
    client = get_storage_client()
    tallies: dict[str, VenueTally] = {}
    total_objs = 0
    for meta in client.list_blobs(bucket, prefix=prefix):
        total_objs += 1
        name = meta.name
        venue = _extract_venue(name)
        if venue is None:
            continue
        canonical = canonicalize_defi_venue_combined(venue)
        if canonical == venue:
            continue  # already canonical / no-op venue → skip
        tally = tallies.get(venue)
        if tally is None:
            tally = VenueTally(glued_venue=venue, canonical_venue=canonical)
            tallies[venue] = tally
        tally.objects.append(name)
    logger.info(
        "  [%s] walked %d objects under %s — %d glued venues need re-key",
        bucket,
        total_objs,
        prefix,
        len(tallies),
    )
    return tallies


def _migrate_one_object(bucket: str, object_name: str, glued_venue: str, canonical_venue: str) -> str:
    """Copy old->canonical, parity-verify, delete old. Returns a status token.

    Status tokens: "copied" | "skipped_copy" | "error:<msg>". Verification and
    deletion are folded into "copied"/"skipped_copy" outcomes (a non-error
    return means the object is now safely at its canonical path and the old key
    deleted). Never raises — returns ``error:<msg>`` on any failure.
    """
    canonical_name = _rekey_object_name(object_name, glued_venue, canonical_venue)
    src_uri = f"gs://{bucket}/{object_name}"  # noqa: gs-uri — bucket already resolved, URI composer
    dst_uri = f"gs://{bucket}/{canonical_name}"  # noqa: gs-uri — bucket already resolved, URI composer
    try:
        src_meta = gcs_describe_object(src_uri)
        if src_meta is None:
            # Source already gone (idempotent re-run, prior partial). Nothing to do.
            return "skipped_copy"
        src_size = src_meta.size

        dst_meta = gcs_describe_object(dst_uri)
        did_copy = False
        if dst_meta is not None and dst_meta.size == src_size:
            # Canonical already exists with matching size → idempotent skip of copy.
            status = "skipped_copy"
        else:
            gcs_copy_object(src_uri, dst_uri)
            did_copy = True
            status = "copied"

        # Verify canonical copy exists AND size matches (parity) BEFORE deleting old.
        verify_meta = gcs_describe_object(dst_uri)
        if verify_meta is None:
            return f"error:copy-verify-missing {dst_uri}"
        if verify_meta.size != src_size:
            return f"error:size-mismatch src={src_size} dst={verify_meta.size} {dst_uri}"

        # Only now delete the old glued object.
        gcs_delete_object(src_uri)
        _ = did_copy
        return status
    except (OSError, ValueError, RuntimeError) as exc:
        return f"error:{type(exc).__name__}:{exc} ({src_uri})"


def _execute_bucket(bucket: str, tallies: dict[str, VenueTally]) -> None:
    """Run copy+verify+delete for every object across all glued venues in this bucket."""
    # Flatten all (venue, object) work units; run with a single pool for max parallelism.
    work: list[tuple[str, VenueTally]] = []
    for tally in tallies.values():
        for obj in tally.objects:
            work.append((obj, tally))

    logger.info(
        "  [%s] executing %d object migrations across %d venues (workers=%d)", bucket, len(work), len(tallies), WORKERS
    )
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(_migrate_one_object, bucket, obj, tally.glued_venue, tally.canonical_venue): tally
            for obj, tally in work
        }
        for done, fut in enumerate(as_completed(futures), start=1):
            tally = futures[fut]
            status = fut.result()
            if status == "copied":
                tally.copied += 1
                tally.verified += 1
                tally.deleted += 1
            elif status == "skipped_copy":
                tally.skipped_copy += 1
                tally.verified += 1
                tally.deleted += 1
            else:  # error:...
                tally.errors.append(status)
            if done % 500 == 0:
                logger.info("    ... %d/%d objects processed", done, len(work))


def _print_report(bucket: str, tallies: dict[str, VenueTally], *, executed: bool) -> tuple[int, int]:
    """Print per-venue + total tallies. Returns (total_objects, total_errors)."""
    print(f"\n=== {'EXECUTE' if executed else 'DRY-RUN'} report: gs://{bucket} ===")
    if not tallies:
        print("  No glued venues need re-keying (all already canonical).")
        return (0, 0)
    total_objs = 0
    total_copied = 0
    total_skipped = 0
    total_deleted = 0
    total_errors = 0
    print(
        f"  {'GLUED VENUE':<28} -> {'CANONICAL':<28} {'OBJS':>7} {'COPIED':>7} {'SKIP':>6} {'DELETED':>8} {'ERRORS':>7}"
    )
    for glued in sorted(tallies):
        t = tallies[glued]
        n = len(t.objects)
        total_objs += n
        total_copied += t.copied
        total_skipped += t.skipped_copy
        total_deleted += t.deleted
        total_errors += len(t.errors)
        print(
            f"  {t.glued_venue:<28} -> {t.canonical_venue:<28} {n:>7} {t.copied:>7} {t.skipped_copy:>6} {t.deleted:>8} {len(t.errors):>7}"
        )
        for err in t.errors[:5]:
            print(f"      ERROR: {err}")
        if len(t.errors) > 5:
            print(f"      ... +{len(t.errors) - 5} more errors")
    print(
        f"  {'TOTAL':<28}    {'':<28} {total_objs:>7} {total_copied:>7} {total_skipped:>6} {total_deleted:>8} {total_errors:>7}"
    )
    return (total_objs, total_errors)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform copy+verify+delete. Default: dry-run (report only, no mutation).",
    )
    parser.add_argument(
        "--bucket",
        action="append",
        default=None,
        help="Bucket to operate on (repeatable). Default: all in-scope buckets.",
    )
    args = parser.parse_args()
    execute: bool = bool(cast(object, args.execute))
    bucket_args: list[str] | None = cast("list[str] | None", args.bucket)

    if bucket_args:
        buckets = {b: DEFAULT_BUCKET_PREFIXES.get(b, "") for b in bucket_args}
        for b, p in buckets.items():
            if not p:
                logger.error("Unknown bucket %r — no prefix mapping. Known: %s", b, list(DEFAULT_BUCKET_PREFIXES))
                sys.exit(2)
    else:
        buckets = dict(DEFAULT_BUCKET_PREFIXES)

    mode = "EXECUTE" if execute else "DRY-RUN"
    logger.info("DeFi glued-venue re-key migration — mode=%s, buckets=%s", mode, list(buckets))

    grand_total_objs = 0
    grand_total_errors = 0
    for bucket, prefix in buckets.items():
        logger.info("Discovering glued venues in gs://%s/%s ...", bucket, prefix)
        tallies = _discover(bucket, prefix)
        if execute and tallies:
            _execute_bucket(bucket, tallies)
        objs, errors = _print_report(bucket, tallies, executed=execute)
        grand_total_objs += objs
        grand_total_errors += errors

    print(f"\n=== GRAND TOTAL: {grand_total_objs} objects in scope, {grand_total_errors} errors ===")
    if not execute:
        print("→ Re-run with --execute to perform the migration.")
    if grand_total_errors > 0:
        logger.error("Completed with %d errors — see per-venue ERROR lines above.", grand_total_errors)
        sys.exit(1)


if __name__ == "__main__":
    main()
