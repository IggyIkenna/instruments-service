"""instrument_availability / futures_contracts / market_lifecycle FLAT source PURGE
(todo 7d — the gated purge half; never deletes without a fresh §3a reversibility check).

# Epic: infrastructure_master
# Lifecycle: one-off remediation executor — todo 7d of
#   plans/active/issues/instrument_availability_hive_canonicalisation_2026_07_21.md ("purge half").
#   Runs only after todo 7c's copy (migrate_instrument_availability_hive_2026_08_03.py) is verified
#   complete for an asset_group.
# Delete-when: 7d fully verified complete (a fresh dry-run reports 0 purge-eligible flat candidates)
#   across all 5 asset_group buckets.

CONTEXT
------------------------------------------------------------------------------------------------------
Todo 7c copied every recognized FLAT ``day={D}/venue={V}/...`` object under
``instrument_availability/by_date`` (+ nested ``futures_contracts.parquet``) and, for prediction,
``market_lifecycle/by_canonical_group`` up to its full-hive twin
(``day={D}/pipeline_mode={pm}/asset_group={ag}/venue={V}/...``), verified via a (crc32c, size)
metadata compare (a pure server-side copy preserves content exactly, so crc32c+size equality IS
content-verification here — not merely existence or size, see Part 2 of
``/codex/02-data/gcs-and-manifest-delete-safety-protocol.md``). It correctly refused to overwrite
32,846 objects whose existing hive twin already had DIFFERENT content
(cefi 1,494 / defi 31,315 / tradfi 37) — those are tracked separately (todo 4 of
``instrument_availability_hive_migration_unrecognized_shapes_and_content_mismatch_2026_08_03.md``)
and this script must NEVER purge their flat source.

This script is the separate, later, gated Part of that pair: delete the now-redundant flat
originals, but ONLY the ones with a content-verified twin. **Never trusts 7c's prior run report —
re-verifies fresh, immediately before every delete** (matches
``market-tick-data-service/scripts/sports/k1k2_casing_revert_2026_07_27/delete_stale_uppercase_2026_07_27.py``'s
structural template, cited by this todo). For each flat candidate: re-describe the source AND the
hive twin (Part 1 + Part 2), delete the source ONLY when the twin exists and its (crc32c, size)
match the source exactly, via a generation-matched conditional delete
(``gcs_conditional_delete``) — this closes the verify-then-delete race a plain
``gcs_delete_object`` would leave open (something could rewrite the source between the read and
the delete).

Writer/reader facts this purge depends on (re-confirmed 2026-08-03, not re-derived — see the parent
issue doc todos 3 and 6): the live writer has stopped emitting the flat shape for cefi/defi/tradfi/
prediction (``_instrument_availability_sink_for`` / ``_market_lifecycle_sink_for``,
``instruments-service@a9be6ce9``) and every consuming reader is layout-tolerant across both shapes
(``unified_trading_library/core/cloud_data_provider.py:_resolve_instrument_availability_path`` etc,
``unified-trading-library@43fa6f3f``) — so deleting a flat source once its hive twin is
content-verified present causes no read regression (the reader simply resolves the hive object).

SAFETY SHAPE (mirrors delete_stale_uppercase_2026_07_27.py)
  (default)                    DRY-RUN: bounded prefix listing only (the SAME prefixes 7b/7c
                                already sized), classifies candidate/already-hive/unrecognized by
                                path shape alone. No per-object GCS describes, no writes.
  --apply-prod (no --confirm-prod-write)
                                PLAN: fresh §3a retention check + fresh per-object Part1+Part2
                                verify (re-describes source AND twin for every candidate). Reports
                                eligible / skipped-no-twin / skipped-content-mismatch counts.
                                Still performs NO deletes.
  --apply-prod --confirm-prod-write --asset-group <ag>
                                EXECUTES for ONE asset_group's bucket (bounded blast radius per
                                invocation, mirrors the fleet's per-category VM convention): fresh
                                §3a retention check (ABORTS before any mutation if < 7 days), then
                                fresh per-object verify + generation-matched conditional delete for
                                every content-verified-twin candidate. Never deletes when the twin
                                is absent (not-yet-migrated — no-migrate-first), the twin content
                                diverges (content_mismatch — needs the operator's authoritative-
                                source decision, tracked separately), or the source vanished/raced
                                mid-run.

Usage::

    # DRY-RUN one asset_group (safe, no GCS writes, real bounded listing)
    python scripts/purge_flat_instrument_availability_hive_2026_08_03.py --asset-group cefi

    # PLAN (fresh verify, no deletes)
    python scripts/purge_flat_instrument_availability_hive_2026_08_03.py \\
        --asset-group cefi --apply-prod

    # EXECUTE (deletes PROD flat originals — VM-only per the heavy-I/O rule)
    python scripts/purge_flat_instrument_availability_hive_2026_08_03.py \\
        --asset-group cefi --apply-prod --confirm-prod-write --workers 32
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from unified_trading_library import (
    gcs_bucket_soft_delete_retention_seconds,
    gcs_conditional_delete,
    gcs_describe_object,
    get_storage_client,
    resolve_bucket_name,
)

ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")

INSTRUMENT_AVAILABILITY_ROOT = "instrument_availability/by_date"
MARKET_LIFECYCLE_ROOT = "market_lifecycle/by_canonical_group"

# delete-safety-protocol §3a — a fresh per-bucket check clearing this window is what lets an agent
# execute the delete itself, no separate operator sign-off (finding T, cited by this todo's text).
MIN_SOFT_DELETE_RETENTION_SECONDS = 604800  # 7 days

# Matches the SAME FLAT legacy shape migrate_instrument_availability_hive_2026_08_03.py targets —
# kept in lockstep deliberately (both are one-off scripts for the same migration, see
# codex/06-coding-standards/script-homes.md; a shared corpus-facing regex belongs in a library only
# once a THIRD one-off script needs it).
_FLAT_RE = re.compile(
    rf"^({re.escape(INSTRUMENT_AVAILABILITY_ROOT)}|{re.escape(MARKET_LIFECYCLE_ROOT)})/day=([^/]+)/venue=([^/]+)/(.+)$"
)


def _bucket_for(asset_group: str) -> str:
    if asset_group == "prediction":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store-prediction")
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=asset_group)


def _pipeline_mode_for(root: str, asset_group: str, venue: str) -> str:
    """Classify (root, asset_group, venue) -> the pipeline_mode the live writer stamps today."""
    venue_upper = venue.upper()
    if root == MARKET_LIFECYCLE_ROOT:
        return "batch_polymarket_gamma_api" if venue_upper == "POLYMARKET" else "batch_instruments_service"
    if asset_group == "prediction":
        if venue_upper == "POLYMARKET":
            return "batch_polymarket_clob"
        if venue_upper == "KALSHI":
            return "batch_kalshi"
    return "batch_instruments_service"


def hive_target_for(flat_path: str, asset_group: str) -> str | None:
    """Map a FLAT object path to its full-hive twin path, or None if not a recognized flat shape."""
    m = _FLAT_RE.match(flat_path)
    if not m:
        return None
    root, day, venue, tail = m.groups()
    pipeline_mode = _pipeline_mode_for(root, asset_group, venue)
    return f"{root}/day={day}/pipeline_mode={pipeline_mode}/asset_group={asset_group}/venue={venue}/{tail}"


def _scan_prefixes_for(asset_group: str) -> list[str]:
    prefixes = [f"{INSTRUMENT_AVAILABILITY_ROOT}/"]
    if asset_group == "prediction":
        prefixes.append(f"{MARKET_LIFECYCLE_ROOT}/")
    return prefixes


@dataclass
class ScanResult:
    candidates: list[tuple[str, str]] = field(default_factory=list)  # (flat_src, hive_dst)
    already_hive: int = 0
    unrecognized: int = 0

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)


def scan(client: object, bucket: str, asset_group: str) -> ScanResult:
    """Bounded prefix listing over the SAME prefixes 7b/7c already sized — never a whole-bucket walk."""
    result = ScanResult()
    for prefix in _scan_prefixes_for(asset_group):
        for blob in client.list_blobs(bucket, prefix=prefix):  # pyright: ignore[reportAttributeAccessIssue]  # client typed as object (no concrete StorageClient import); real methods exercised by tests
            name = blob.name
            if "/pipeline_mode=" in name:
                result.already_hive += 1
                continue
            target = hive_target_for(name, asset_group)
            if target is None:
                result.unrecognized += 1
                continue
            result.candidates.append((name, target))
    return result


@dataclass
class PurgeResult:
    asset_group: str
    deleted: int = 0  # or "eligible" count when apply=False
    skipped_no_twin: list[str] = field(default_factory=list)
    skipped_content_mismatch: list[str] = field(default_factory=list)
    skipped_source_vanished: list[str] = field(default_factory=list)
    skipped_race_lost: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed


_ObjectOutcome = tuple[str, str, str | None]  # (src, outcome, error_detail)


def _metadata_equal(a: object, b: object) -> bool:
    return bool(a.crc32c == b.crc32c and a.size == b.size)  # pyright: ignore[reportAttributeAccessIssue]  # BlobMetadata duck-typed to avoid importing the concrete cloud_interface type here


def _process_one_object(bucket: str, src: str, dst: str, *, apply: bool) -> _ObjectOutcome:
    """Fresh Part1+Part2 verify for ONE flat/hive pair, then (if apply) a generation-matched delete.

    Never trusts a prior migration report — re-describes both objects fresh on every call. Only
    deletes the flat SOURCE when the full-hive TARGET exists and its (crc32c, size) match the
    source exactly — the byte-identical server-side-copy invariant this migration depends on.
    """
    src_uri = f"gs://{bucket}/{src}"
    dst_uri = f"gs://{bucket}/{dst}"
    try:
        src_meta = gcs_describe_object(src_uri)
        if src_meta is None:
            return src, "source_vanished", "flat source already gone — nothing to purge"
        dst_meta = gcs_describe_object(dst_uri)
        if dst_meta is None:
            return src, "no_twin", "full-hive target absent — NOT migrated yet, refusing (no-migrate-first)"
        if not _metadata_equal(src_meta, dst_meta):
            return (
                src,
                "content_mismatch",
                "target exists with a DIFFERENT (crc32c, size) — tracked separately, never purge",
            )
        if not apply:
            return src, "eligible", None
        if src_meta.generation is None:  # pyright: ignore[reportAttributeAccessIssue]
            return src, "failed", "source BlobMetadata has no generation — cannot CAS-delete safely"
        deleted = gcs_conditional_delete(src_uri, if_generation_match=src_meta.generation)  # pyright: ignore[reportAttributeAccessIssue]
        if not deleted:
            return src, "race_lost", "generation changed between verify and delete — source was rewritten, skipped"
        return src, "deleted", None
    except Exception as exc:
        return src, "failed", f"{type(exc).__name__}: {exc}"


def _record_outcome(result: PurgeResult, src: str, outcome: str, detail: str | None) -> None:
    if outcome in ("deleted", "eligible"):
        result.deleted += 1
    elif outcome == "no_twin":
        result.skipped_no_twin.append(src)
    elif outcome == "content_mismatch":
        result.skipped_content_mismatch.append(src)
    elif outcome == "source_vanished":
        result.skipped_source_vanished.append(src)
    elif outcome == "race_lost":
        result.skipped_race_lost.append(src)
    elif outcome == "failed":
        result.failed.append((src, detail or "unknown"))
    else:
        raise ValueError(f"unrecognized outcome {outcome!r} for {src!r}")


def dry_run(*, asset_group: str) -> int:
    """Cheap listing-only census (no per-object describes) — mirrors the copy tool's own dry_run()."""
    bucket = _bucket_for(asset_group)
    client = get_storage_client()
    t0 = time.monotonic()
    result = scan(client, bucket, asset_group)
    elapsed = time.monotonic() - t0
    print(
        f"=== DRY-RUN asset_group={asset_group} bucket={bucket} ({elapsed:.1f}s) ===\n"
        f"  flat candidates (purge-eligible pending fresh per-object verify): {result.candidate_count:,}\n"
        f"  already full-hive:              {result.already_hive:,}\n"
        f"  unrecognized shapes (ignored):  {result.unrecognized:,}",
        flush=True,
    )
    return 0


def purge_asset_group(*, asset_group: str, apply: bool, workers: int = 32) -> PurgeResult:
    """Fresh per-object verify (+ delete when apply=True) for every flat candidate, concurrently."""
    bucket = _bucket_for(asset_group)
    client = get_storage_client()
    result = PurgeResult(asset_group=asset_group)
    scanned = scan(client, bucket, asset_group)
    verb = "verifying + deleting" if apply else "verifying (no deletes)"
    print(f"  scanned {scanned.candidate_count:,} flat candidates in {bucket}, {verb}...", flush=True)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_process_one_object, bucket, src, dst, apply=apply) for src, dst in scanned.candidates]
        for done, fut in enumerate(as_completed(futures), start=1):
            src, outcome, detail = fut.result()
            _record_outcome(result, src, outcome, detail)
            if done % 5000 == 0:
                print(f"  progress: {done:,}/{scanned.candidate_count:,} objects processed", flush=True)
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--asset-group", required=True, choices=ASSET_GROUPS)
    ap.add_argument("--apply-prod", action="store_true")
    ap.add_argument("--confirm-prod-write", action="store_true")
    ap.add_argument("--workers", type=int, default=32, help="concurrent per-object workers (default 32)")
    ap.add_argument("--snapshot", default=None, help="optional json path for the pre-delete outcome snapshot")
    args = ap.parse_args()

    if not args.apply_prod:
        return dry_run(asset_group=args.asset_group)

    bucket = _bucket_for(args.asset_group)
    retention = gcs_bucket_soft_delete_retention_seconds(bucket)
    print(f"=== fresh soft-delete retention check :: {bucket} -> {retention}s ===", flush=True)
    if retention < MIN_SOFT_DELETE_RETENTION_SECONDS:
        print(
            f"!!! retention {retention}s < {MIN_SOFT_DELETE_RETENTION_SECONDS}s (7d) — does NOT qualify for the "
            "§3a carve-out. ABORT — escalate to the operator with this measured value, do not purge.",
            flush=True,
        )
        return 1

    apply = args.confirm_prod_write
    t0 = time.monotonic()
    result = purge_asset_group(asset_group=args.asset_group, apply=apply, workers=args.workers)
    elapsed = time.monotonic() - t0

    if args.snapshot:
        with open(args.snapshot, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "asset_group": args.asset_group,
                    "bucket": bucket,
                    "retention_seconds": retention,
                    "applied": apply,
                    "deleted_or_eligible": result.deleted,
                    "skipped_no_twin": result.skipped_no_twin,
                    "skipped_content_mismatch": result.skipped_content_mismatch,
                    "skipped_source_vanished": result.skipped_source_vanished,
                    "skipped_race_lost": result.skipped_race_lost,
                    "failed": result.failed,
                },
                f,
            )
        print(f"  snapshot written: {args.snapshot}", flush=True)

    label = "deleted" if apply else "eligible_for_delete"
    print(
        f"\n=== {'APPLY' if apply else 'PLAN'} asset_group={args.asset_group} ({elapsed:.1f}s) ===\n"
        f"  {label}:{' ' * (25 - len(label))}{result.deleted:,}\n"
        f"  skipped_no_twin:          {len(result.skipped_no_twin):,}\n"
        f"  skipped_content_mismatch: {len(result.skipped_content_mismatch):,}\n"
        f"  skipped_source_vanished:  {len(result.skipped_source_vanished):,}\n"
        f"  skipped_race_lost:        {len(result.skipped_race_lost):,}\n"
        f"  failed:                   {len(result.failed):,}",
        flush=True,
    )
    if result.skipped_no_twin:
        print(f"  !!! NO TWIN (unexpected post-7c, needs investigation): {result.skipped_no_twin[:5]}")
    if result.failed:
        print(f"  !!! FAILED: {result.failed[:5]}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
