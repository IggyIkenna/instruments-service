"""instrument_availability / futures_contracts / market_lifecycle FLAT -> FULL-HIVE COPY-UP
migration (COPY + VERIFY only — never deletes the flat source).

# Epic: infrastructure_master
# Lifecycle: one-off remediation executor — todo 7c of
#   plans/active/issues/instrument_availability_hive_canonicalisation_2026_07_21.md ("copy-and-verify
#   half"). The separate, later, gated purge of the flat originals is todo 7d — NOT run by this tool.
# Delete-when: 7c fully verified complete (0 remaining flat-shape candidates across all 5 asset_group
#   buckets, confirmed via a fresh dry-run) AND 7d's gated purge has run.

CONTEXT (operator HARD RULE 2026-07-21, see the issue doc above)
------------------------------------------------------------------------------------------------------
``instrument_availability`` (+ the nested ``futures_contracts.parquet`` under the same prefix) and, for
the ``prediction`` asset_group only, ``market_lifecycle`` were historically written FLAT —
``day={D}/venue={V}/...`` — missing the mandatory ``pipeline_mode=``/``asset_group=`` hive keys. The
writer was fixed 2026-07-21 (instruments-service@a9be6ce9 / unified-trading-library@43fa6f3f) to emit the
full canonical hive shape via the sink PREFIX mechanism (never the partition dict — see that commit's
``_instrument_availability_sink_for``/``_market_lifecycle_sink_for`` docstrings for the alphabetical-sort
trap). Todo 7b sized the historical backlog (2026-07-27, bounded prefix-listing, not a corpus walk):

  | asset_group | instrument_availability (+futures_contracts) | market_lifecycle |
  |---|---|---|
  | cefi       |  53,419 | 0 |
  | defi       | 177,346 | 0 |
  | tradfi     |  50,700 | 0 |
  | sports     | 148,691 | 0 |
  | prediction |  22,637 | 12,582 |

This tool is the COPY half (7c): for every FLAT object under those two prefixes, copy it (server-side,
no egress) to its full-hive target path, then verify parity via a (crc32c, size) metadata compare —
NOT a content download, which would be prohibitively expensive at this population (~465K objects). The
flat original is NEVER deleted or modified — that is 7d's separate, later, gated purge, run only after
this copy is verified 100% complete.

pipeline_mode derivation (mirrors the live writer's own classification, see process_write.py
``_write_prediction_venue``/``_write_venue`` — NOT re-derived from scratch):
  - instrument_availability / futures_contracts tree:
      asset_group == "prediction" and venue == POLYMARKET -> batch_polymarket_clob
      asset_group == "prediction" and venue == KALSHI      -> batch_kalshi
      every other (asset_group, venue)                     -> batch_instruments_service
  - market_lifecycle tree (prediction-only):
      venue == POLYMARKET -> batch_polymarket_gamma_api
      otherwise            -> batch_instruments_service

SAFETY SHAPE (mirrors migrate_sports_casing_revert_2026_07_27.py)
  (default)                    DRY-RUN: bounded prefix listing (the SAME prefixes 7b already sized —
                                not a new whole-corpus walk), classifies every object found, reports
                                counts. No GCS writes.
  --apply-prod (no --confirm-prod-write)
                                Re-verifies live reachability. Still performs NO writes.
  --apply-prod --confirm-prod-write --asset-group <ag>
                                EXECUTES for ONE asset_group's bucket (bounded blast radius per
                                invocation, mirrors the fleet's per-category VM convention):
                                copy-if-missing (target absent) or metadata-verify (target already
                                present from a prior partial run), NEVER deletes the flat source.

Usage::

    # DRY-RUN one asset_group (safe, no GCS writes, real bounded listing)
    python scripts/migrate_instrument_availability_hive_2026_08_03.py --asset-group cefi

    # EXECUTE one asset_group (writes PROD — human-reviewed step, VM-only per the heavy-I/O rule)
    python scripts/migrate_instrument_availability_hive_2026_08_03.py \\
        --asset-group cefi --apply-prod --confirm-prod-write --workers 32

    # RESOLVE content_mismatch (todo 6 of instrument_availability_hive_migration_unrecognized_
    # shapes_and_content_mismatch_2026_08_03.md, per todo 4's ruled "superset wins" policy —
    # downloads + parses BOTH sides per pair, writes PROD when the flat side wins/ties)
    python scripts/migrate_instrument_availability_hive_2026_08_03.py \\
        --asset-group defi --apply-prod --confirm-prod-write --resolve-content-mismatch --workers 16
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import pandas as pd
from unified_trading_library import (
    gcs_copy_object,
    gcs_describe_object,
    gcs_read_object_with_generation,
    get_storage_client,
    resolve_bucket_name,
)

ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")

INSTRUMENT_AVAILABILITY_ROOT = "instrument_availability/by_date"
MARKET_LIFECYCLE_ROOT = "market_lifecycle/by_canonical_group"

# Matches the FLAT legacy shape ``<root>/day={D}/venue={V}/<tail>`` for either tree. A path that is
# already full-hive has ``pipeline_mode=`` immediately after ``day={D}/`` instead of ``venue=`` and
# simply will not match — no separate "already migrated" filter needed.
_FLAT_RE = re.compile(
    rf"^({re.escape(INSTRUMENT_AVAILABILITY_ROOT)}|{re.escape(MARKET_LIFECYCLE_ROOT)})/day=([^/]+)/venue=([^/]+)/(.+)$"
)

# Sports league-sharded FLAT shape (API_FOOTBALL only, instrument_availability tree only — sports has
# no market_lifecycle analogue): ``<root>/day={D}/league={L}/venue={V}/<tail>``. ``league=`` sits
# BEFORE ``venue=`` in this legacy flat shape — inverse-ordered from the ruled hive's TRAILING
# ``league=`` position (operator ruling on todo 1 of
# instrument_availability_hive_migration_unrecognized_shapes_and_content_mismatch_2026_08_03.md,
# recorded in cross-asset-canonical-target-ssot.md §8 sports-exception banner). ``_FLAT_RE`` above
# never matches this shape (it expects ``venue=`` immediately after ``day=``), so every such object
# was previously bucketed into "unrecognized shapes (ignored)".
_SPORTS_LEAGUE_FLAT_RE = re.compile(
    rf"^{re.escape(INSTRUMENT_AVAILABILITY_ROOT)}/day=([^/]+)/league=([^/]+)/venue=([^/]+)/(.+)$"
)

# Prediction canonical_question_group-BEFORE-day FLAT shape (instrument_availability tree only):
# ``<root>/canonical_question_group={G}/day={D}/venue={V}/<tail>``. This is the pre-2026-07-21
# writer's partition-dict ALPHABETICAL sort of ``{day, venue, canonical_question_group}``
# (``canonical_question_group`` < ``day`` < ``venue`` — see the ``a9be6ce9`` diff's removed
# ``_write_prediction_venue`` block) — inverse-ordered from the ruled hive's day-first /
# trailing ``canonical_question_group=`` position. Confirmed via a live GCS sample (2026-08-03)
# that every sampled canonical group's LAST object in this shape was written 2026-07-22T00:37:29Z
# — the batch run immediately before the ``a9be6ce9`` writer-fix deploy (03:20:56Z same day) — so
# this shape is NOT still being written (todo 3 of
# instrument_availability_hive_migration_unrecognized_shapes_and_content_mismatch_2026_08_03.md).
_PREDICTION_GROUP_FIRST_FLAT_RE = re.compile(
    rf"^{re.escape(INSTRUMENT_AVAILABILITY_ROOT)}/canonical_question_group=([^/]+)/day=([^/]+)/venue=([^/]+)/(.+)$"
)

# Prediction market_lifecycle legacy FLAT shape: ``<root>/day={D}/group={G}/venue={V}/<tail>``
# (day first, but ``group=`` sits BEFORE ``venue=`` — venue is not immediately after ``day=``, so
# it never matched ``_FLAT_RE`` above). Pre-2026-07-21 partition dict was
# ``{"group": G, "day": D, "venue": V}``, alphabetically sorted (``day`` < ``group`` < ``venue``,
# see the ``a9be6ce9`` diff's ``_write_market_lifecycle`` change). Same cutover timing as the
# shape above — confirmed live, last write 2026-07-22T00:37:29Z, not written since.
_PREDICTION_LIFECYCLE_DAY_GROUP_VENUE_FLAT_RE = re.compile(
    rf"^{re.escape(MARKET_LIFECYCLE_ROOT)}/day=([^/]+)/group=([^/]+)/venue=([^/]+)/(.+)$"
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
    # instrument_availability / futures_contracts tree.
    if asset_group == "prediction":
        if venue_upper == "POLYMARKET":
            return "batch_polymarket_clob"
        if venue_upper == "KALSHI":
            return "batch_kalshi"
    if asset_group == "sports" and venue_upper == "API_FOOTBALL":
        return "batch_api_football"
    return "batch_instruments_service"


def hive_target_for(flat_path: str, asset_group: str) -> str | None:
    """Map a FLAT object path to its full-hive target path, or None if not a recognized flat shape."""
    m = _FLAT_RE.match(flat_path)
    if m:
        root, day, venue, tail = m.groups()
        pipeline_mode = _pipeline_mode_for(root, asset_group, venue)
        return f"{root}/day={day}/pipeline_mode={pipeline_mode}/asset_group={asset_group}/venue={venue}/{tail}"
    if asset_group == "sports":
        m_league = _SPORTS_LEAGUE_FLAT_RE.match(flat_path)
        if m_league:
            day, league, venue, tail = m_league.groups()
            pipeline_mode = _pipeline_mode_for(INSTRUMENT_AVAILABILITY_ROOT, asset_group, venue)
            return (
                f"{INSTRUMENT_AVAILABILITY_ROOT}/day={day}/pipeline_mode={pipeline_mode}/"
                f"asset_group=sports/venue={venue}/league={league}/{tail}"
            )
    if asset_group == "prediction":
        m_group_first = _PREDICTION_GROUP_FIRST_FLAT_RE.match(flat_path)
        if m_group_first:
            group, day, venue, tail = m_group_first.groups()
            pipeline_mode = _pipeline_mode_for(INSTRUMENT_AVAILABILITY_ROOT, asset_group, venue)
            return (
                f"{INSTRUMENT_AVAILABILITY_ROOT}/day={day}/pipeline_mode={pipeline_mode}/"
                f"asset_group=prediction/venue={venue}/canonical_question_group={group}/{tail}"
            )
        m_lifecycle = _PREDICTION_LIFECYCLE_DAY_GROUP_VENUE_FLAT_RE.match(flat_path)
        if m_lifecycle:
            day, group, venue, tail = m_lifecycle.groups()
            pipeline_mode = _pipeline_mode_for(MARKET_LIFECYCLE_ROOT, asset_group, venue)
            return (
                f"{MARKET_LIFECYCLE_ROOT}/day={day}/pipeline_mode={pipeline_mode}/"
                f"asset_group=prediction/venue={venue}/group={group}/{tail}"
            )
    return None


@dataclass
class ScanResult:
    candidates: list[tuple[str, str]] = field(default_factory=list)  # (src_path, dst_path)
    already_hive: int = 0
    unrecognized: int = 0

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)


def _scan_prefixes_for(asset_group: str) -> list[str]:
    prefixes = [f"{INSTRUMENT_AVAILABILITY_ROOT}/"]
    if asset_group == "prediction":
        prefixes.append(f"{MARKET_LIFECYCLE_ROOT}/")
    return prefixes


def scan(client: object, bucket: str, asset_group: str) -> ScanResult:
    """Bounded prefix listing over the SAME prefixes todo 7b already sized — never a whole-bucket walk."""
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


def dry_run(*, asset_group: str) -> int:
    bucket = _bucket_for(asset_group)
    client = get_storage_client()
    t0 = time.monotonic()
    result = scan(client, bucket, asset_group)
    elapsed = time.monotonic() - t0
    print(
        f"=== DRY-RUN asset_group={asset_group} bucket={bucket} ({elapsed:.1f}s) ===\n"
        f"  flat candidates (need copy-up): {result.candidate_count:,}\n"
        f"  already full-hive:              {result.already_hive:,}\n"
        f"  unrecognized shapes (ignored):  {result.unrecognized:,}",
        flush=True,
    )
    return 0


@dataclass
class ApplyResult:
    asset_group: str
    copied: int = 0
    already_present_verified: int = 0
    source_vanished: list[str] = field(default_factory=list)
    content_mismatch: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.content_mismatch and not self.failed


_ObjectOutcome = tuple[str, str, str | None]  # (src, outcome, error_detail)


def _metadata_equal(a: object, b: object) -> bool:
    return bool(a.crc32c == b.crc32c and a.size == b.size)  # pyright: ignore[reportAttributeAccessIssue]  # BlobMetadata duck-typed to avoid importing the concrete cloud_interface type here


def _process_one_object(bucket: str, src: str, dst: str) -> _ObjectOutcome:
    """Copy-if-missing (server-side, no egress) + metadata-verify for ONE flat object.

    Never deletes or mutates the flat source. Verification is a (crc32c, size) metadata compare, not a
    content download — a full download at this population (~465K objects across 5 buckets) would be a
    prohibitively expensive re-scan of data that never changes shape (a pure server-side copy preserves
    content exactly; crc32c+size is the standard cheap parity proof, same as the 7a proof's own
    "object signature (etag+crc32c+size) unchanged" check).
    """
    src_uri = f"gs://{bucket}/{src}"
    dst_uri = f"gs://{bucket}/{dst}"
    try:
        dst_meta = gcs_describe_object(dst_uri)
        src_meta = gcs_describe_object(src_uri)
        if src_meta is None:
            return src, "source_vanished", "source object 404'd before copy (safe to skip — copy-only, never deletes)"
        if dst_meta is not None:
            if _metadata_equal(src_meta, dst_meta):
                return src, "already_present", None
            return src, "content_mismatch", "target exists with a DIFFERENT (crc32c, size) — needs manual review"
        gcs_copy_object(src_uri, dst_uri)
        post_meta = gcs_describe_object(dst_uri)
        if post_meta is None or not _metadata_equal(src_meta, post_meta):
            return src, "content_mismatch", "post-copy verify: (crc32c, size) mismatch or target still absent"
        return src, "copied", None
    except Exception as exc:
        return src, "failed", f"{type(exc).__name__}: {exc}"


def _record_outcome(result: ApplyResult, src: str, outcome: str, detail: str | None) -> None:
    if outcome == "copied":
        result.copied += 1
    elif outcome == "already_present":
        result.already_present_verified += 1
    elif outcome == "source_vanished":
        result.source_vanished.append(src)
    elif outcome == "content_mismatch":
        result.content_mismatch.append(src)
    elif outcome == "failed":
        result.failed.append((src, detail or "unknown"))
    else:
        raise ValueError(f"unrecognized outcome {outcome!r} for {src!r}")


def apply_asset_group(*, asset_group: str, workers: int = 32) -> ApplyResult:
    """EXECUTE for one asset_group's bucket: copy-if-missing + verify every flat candidate, concurrently."""
    bucket = _bucket_for(asset_group)
    client = get_storage_client()
    result = ApplyResult(asset_group=asset_group)
    scanned = scan(client, bucket, asset_group)
    print(f"  scanned {scanned.candidate_count:,} candidates in {bucket}, starting apply...", flush=True)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_process_one_object, bucket, src, dst) for src, dst in scanned.candidates]
        for done, fut in enumerate(as_completed(futures), start=1):
            src, outcome, detail = fut.result()
            _record_outcome(result, src, outcome, detail)
            if done % 5000 == 0:
                print(f"  progress: {done:,}/{scanned.candidate_count:,} objects processed", flush=True)
    return result


_FUTURES_CONTRACTS_IDENTITY_COLUMN = "contract_symbol"
_DEFAULT_IDENTITY_COLUMN = "raw_symbol"


def _identity_column_for(path: str) -> str:
    """Stable cross-generation identity column for the parquet at *path*.

    Ruled todo-4 methodology (instrument_availability_hive_migration_unrecognized_shapes_and_
    content_mismatch_2026_08_03.md): ``instrument_key`` is NOT a stable identity across writer
    generations (e.g. flat ``DERIBIT:FUTURE:BTC-27SEP19`` vs hive ``DERIBIT:FUTURE:BTC@INV-20190927``
    are the SAME instrument) -- ``raw_symbol`` (``contract_symbol`` for ``futures_contracts.parquet``,
    a distinct 16-column schema with no ``raw_symbol``) is the stable cross-generation identity key.
    """
    return (
        _FUTURES_CONTRACTS_IDENTITY_COLUMN
        if path.rsplit("/", 1)[-1] == "futures_contracts.parquet"
        else _DEFAULT_IDENTITY_COLUMN
    )


def _identity_set(data: bytes, column: str) -> frozenset[str]:
    """Non-null identity-column values in parquet *data* -- column-pruned read, never the full frame."""
    df = pd.read_parquet(io.BytesIO(data), columns=[column])
    return frozenset(v for v in df[column].tolist() if v is not None)


@dataclass
class MismatchResolution:
    src: str
    dst: str
    outcome: str  # "flat_wins" | "hive_wins" | "tie_flat_bytes" | "disjoint_needs_review" | "failed"
    detail: str | None = None


def _resolve_one_mismatch(bucket: str, src: str, dst: str) -> MismatchResolution:
    """Resolve ONE content_mismatch pair per the ruled todo-4 "superset wins" policy.

    Per-object completeness compare (not a blanket side-label/timestamp rule): the identity-set
    SUPERSET side's bytes become authoritative for the hive target -- whichever original path held
    them. Equal-membership-but-different-bytes ties (schema_version/column-order/float-precision
    drift) default to the flat side's bytes (keeps single-writer provenance, matches the tool's
    existing copy direction). Genuinely disjoint sets (neither is a superset of the other) are NOT
    auto-resolved -- flagged for manual per-object review/union, per the ruling's explicit backstop.
    """
    src_uri = f"gs://{bucket}/{src}"
    dst_uri = f"gs://{bucket}/{dst}"
    column = _identity_column_for(src)
    try:
        flat_bytes, _ = gcs_read_object_with_generation(src_uri)
        hive_bytes, _ = gcs_read_object_with_generation(dst_uri)
        if flat_bytes is None or hive_bytes is None:
            return MismatchResolution(src, dst, "failed", "source or target vanished mid-resolve")
        flat_ids = _identity_set(flat_bytes, column)
        hive_ids = _identity_set(hive_bytes, column)
        if flat_ids == hive_ids:
            gcs_copy_object(src_uri, dst_uri)
            return MismatchResolution(src, dst, "tie_flat_bytes")
        if flat_ids > hive_ids:
            gcs_copy_object(src_uri, dst_uri)
            return MismatchResolution(src, dst, "flat_wins")
        if hive_ids > flat_ids:
            return MismatchResolution(src, dst, "hive_wins")
        return MismatchResolution(
            src, dst, "disjoint_needs_review", f"flat={len(flat_ids)} hive={len(hive_ids)} ids, neither is a superset"
        )
    except Exception as exc:
        return MismatchResolution(src, dst, "failed", f"{type(exc).__name__}: {exc}")


@dataclass
class ResolveResult:
    asset_group: str
    flat_wins: int = 0
    hive_wins: int = 0
    tie_flat_bytes: int = 0
    disjoint: list[tuple[str, str, str | None]] = field(default_factory=list)
    failed: list[tuple[str, str, str | None]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failed


def resolve_content_mismatches(*, asset_group: str, workers: int = 16) -> ResolveResult:
    """Re-derive the current content_mismatch population (the same safe, idempotent re-scan+verify
    path already run twice today -- ``apply_asset_group`` never deletes, copy-if-missing is a no-op
    on already-migrated objects) and resolve each pair per the ruled todo-4 policy.
    """
    bucket = _bucket_for(asset_group)
    apply_result = apply_asset_group(asset_group=asset_group, workers=workers)
    pairs: list[tuple[str, str]] = []
    for src in apply_result.content_mismatch:
        dst = hive_target_for(src, asset_group)
        if dst is not None:
            pairs.append((src, dst))
    result = ResolveResult(asset_group=asset_group)
    print(f"  resolving {len(pairs):,} content_mismatch pairs in {bucket}...", flush=True)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_resolve_one_mismatch, bucket, src, dst) for src, dst in pairs]
        for done, fut in enumerate(as_completed(futures), start=1):
            res = fut.result()
            if res.outcome == "flat_wins":
                result.flat_wins += 1
            elif res.outcome == "hive_wins":
                result.hive_wins += 1
            elif res.outcome == "tie_flat_bytes":
                result.tie_flat_bytes += 1
            elif res.outcome == "disjoint_needs_review":
                result.disjoint.append((res.src, res.dst, res.detail))
            elif res.outcome == "failed":
                result.failed.append((res.src, res.dst, res.detail))
            else:
                raise ValueError(f"unrecognized resolution outcome {res.outcome!r} for {res.src!r}")
            if done % 2000 == 0:
                print(f"  progress: {done:,}/{len(pairs):,} pairs resolved", flush=True)
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--asset-group", required=True, choices=ASSET_GROUPS)
    ap.add_argument("--apply-prod", action="store_true")
    ap.add_argument("--confirm-prod-write", action="store_true")
    ap.add_argument("--workers", type=int, default=32, help="concurrent per-object workers (default 32)")
    ap.add_argument(
        "--resolve-content-mismatch",
        action="store_true",
        help="Resolve todo-4's ruled 'superset wins' content_mismatch population instead of the copy-up "
        "(requires --apply-prod --confirm-prod-write; downloads + parses both sides per pair).",
    )
    args = ap.parse_args()

    if args.apply_prod and args.confirm_prod_write and args.resolve_content_mismatch:
        t0 = time.monotonic()
        rr = resolve_content_mismatches(asset_group=args.asset_group, workers=args.workers)
        elapsed = time.monotonic() - t0
        print(
            f"\n=== RESOLVE CONTENT_MISMATCH asset_group={rr.asset_group} ({elapsed:.1f}s) ===\n"
            f"  flat_wins:         {rr.flat_wins:,}\n"
            f"  hive_wins:         {rr.hive_wins:,}\n"
            f"  tie_flat_bytes:    {rr.tie_flat_bytes:,}\n"
            f"  disjoint (review): {len(rr.disjoint):,}\n"
            f"  failed:            {len(rr.failed):,}",
            flush=True,
        )
        if rr.disjoint:
            print(f"  !!! DISJOINT (needs manual review): {rr.disjoint[:5]}")
        if rr.failed:
            print(f"  !!! FAILED: {rr.failed[:5]}")
        return 0 if rr.ok else 1

    if args.apply_prod and args.confirm_prod_write:
        t0 = time.monotonic()
        result = apply_asset_group(asset_group=args.asset_group, workers=args.workers)
        elapsed = time.monotonic() - t0
        print(
            f"\n=== APPLY asset_group={args.asset_group} ({elapsed:.1f}s) ===\n"
            f"  copied:                   {result.copied:,}\n"
            f"  already_present_verified: {result.already_present_verified:,}\n"
            f"  source_vanished:          {len(result.source_vanished):,}\n"
            f"  content_mismatch:         {len(result.content_mismatch):,}\n"
            f"  failed:                   {len(result.failed):,}",
            flush=True,
        )
        if result.source_vanished:
            print(f"  !!! SOURCE VANISHED (non-fatal, copy-only never deletes): {result.source_vanished[:5]}")
        if result.content_mismatch:
            print(f"  !!! CONTENT MISMATCH (needs manual review): {result.content_mismatch[:5]}")
        if result.failed:
            print(f"  !!! FAILED: {result.failed[:5]}")
        return 0 if result.ok else 1

    return dry_run(asset_group=args.asset_group)


if __name__ == "__main__":
    sys.exit(main())
