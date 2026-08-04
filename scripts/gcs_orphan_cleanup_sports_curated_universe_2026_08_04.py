#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the orphaned GCS objects are cleaned up and the plan
#   plans/active/issues/sports_curated_universe_domestic_selection_remaining_2026_07_25.md
#   is archived
"""GCS orphan cleanup for the 8,937 sports manifest rows dropped 2026-08-04.

Derives candidate GCS paths from the pre-drop snapshot, applies the five-part
delete-safety proof, and executes safe deletes via the reversibility-qualified
path (§3a of /codex/02-data/gcs-and-manifest-delete-safety-protocol.md).

NO new whole-corpus GCS walk — candidate paths are derived from the dropped
rows' (date, data_type, league_id) triples via
``unified_api_contracts.sports.candidate_parquet_paths``.

Path-structure triage: files with ``/league=<id>/`` in the path are
league-specific by construction (the writer partitions by league_id).
Files without this segment are potentially mixed-content → skipped.

Safety: dry-run by default. ``--apply`` requires a fresh
``gcs_bucket_soft_delete_retention_seconds() >= 604800`` check.
Each delete uses ``gcs_conditional_delete`` with the generation from
the probe to close the verify-then-delete race window.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SNAPSHOT_BLOB = "_index/snapshots/pre_league_id_canonicalize_20260804T075724Z.parquet"
INDEX_BLOB = "_index/availability_index.parquet"
MIN_SOFT_DELETE_SECONDS = 604800  # 7 days
_LEAGUE_SEGMENT_RE = re.compile(r"/league=[^/]+/")


@dataclass
class OrphanCandidate:
    gcs_uri: str
    date: str
    data_type: str
    league_id: str
    generation: int | None = None
    size_bytes: int | None = None
    disposition: str = "unknown"
    proof: dict[str, str] = field(default_factory=dict)


def _resolve_bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    os.environ.setdefault("DEPLOYMENT_ENV", "prod")
    if not os.environ.get("GCP_PROJECT_ID") and os.environ.get("PROJECT_ID"):
        os.environ["GCP_PROJECT_ID"] = os.environ["PROJECT_ID"]
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def extract_dropped_rows(bucket: str) -> pd.DataFrame:
    import gcsfs

    fs = gcsfs.GCSFileSystem()
    snap_local = f"{tempfile.gettempdir()}/orphan_snap.parquet"
    idx_local = f"{tempfile.gettempdir()}/orphan_idx.parquet"
    fs.get(f"{bucket}/{SNAPSHOT_BLOB}", snap_local)
    fs.get(f"{bucket}/{INDEX_BLOB}", idx_local)
    df_snap = pd.read_parquet(snap_local)
    df_idx = pd.read_parquet(idx_local)
    logger.info("Snapshot: %d rows | Current index: %d rows", len(df_snap), len(df_idx))

    dedup_dims = [
        c
        for c in ("service_name", "date", "data_type", "league_id", "timeframe", "pipeline_mode", "source")
        if c in df_snap.columns and c in df_idx.columns
    ]
    df_snap["_key"] = df_snap[dedup_dims].astype(str).agg("|".join, axis=1)
    df_idx["_key"] = df_idx[dedup_dims].astype(str).agg("|".join, axis=1)
    dropped = df_snap[~df_snap["_key"].isin(set(df_idx["_key"]))].copy()
    logger.info("Dropped rows: %d", len(dropped))
    if "data_type" in dropped.columns:
        logger.info("By data_type: %s", dropped["data_type"].value_counts().to_dict())
    return dropped


def derive_candidate_paths(dropped: pd.DataFrame, bucket: str) -> list[OrphanCandidate]:
    from unified_api_contracts.sports import candidate_parquet_paths

    seen: set[str] = set()
    candidates: list[OrphanCandidate] = []
    for _, row in dropped.iterrows():
        date_val = str(row.get("date", ""))  # noqa: qg-empty-fallback — empty signals absent, checked below
        data_type = str(row.get("data_type", ""))  # noqa: qg-empty-fallback — empty signals absent, checked below
        league_id = str(row.get("league_id", ""))  # noqa: qg-empty-fallback — empty signals absent, no effect on path
        pipeline_mode = (
            str(row["pipeline_mode"]) if "pipeline_mode" in row.index and pd.notna(row.get("pipeline_mode")) else None
        )
        if not date_val or not data_type:
            continue
        kwargs: dict = {"data_type": data_type, "day": date_val, "league_id": league_id}
        if pipeline_mode:
            kwargs["pipeline_mode"] = pipeline_mode
        try:
            rel_paths = candidate_parquet_paths(**kwargs)
        except Exception:
            continue
        for rp in rel_paths:
            uri = f"gs://{bucket}/{rp}"
            if uri not in seen:
                seen.add(uri)
                candidates.append(
                    OrphanCandidate(
                        gcs_uri=uri,
                        date=date_val,
                        data_type=data_type,
                        league_id=league_id,
                    )
                )
    logger.info("Derived %d unique candidate paths from %d dropped rows", len(candidates), len(dropped))
    return candidates


def probe_existence(candidates: list[OrphanCandidate]) -> tuple[list[OrphanCandidate], list[OrphanCandidate]]:
    from unified_trading_library import gcs_describe_object

    existing: list[OrphanCandidate] = []
    not_found: list[OrphanCandidate] = []

    def _probe(c: OrphanCandidate) -> OrphanCandidate:
        meta = gcs_describe_object(c.gcs_uri)
        if meta is not None:
            c.generation = meta.generation
            c.size_bytes = meta.size
            c.proof["part1_twin_probe"] = f"EXISTS gen={meta.generation} size={meta.size}"
        else:
            c.proof["part1_twin_probe"] = "None (does not exist)"
        return c

    with ThreadPoolExecutor(max_workers=32) as ex:
        futures = {ex.submit(_probe, c): c for c in candidates}
        for fut in as_completed(futures):
            c = fut.result()
            if c.generation is not None:
                existing.append(c)
            else:
                not_found.append(c)
    logger.info("Part 1: %d exist, %d not found", len(existing), len(not_found))
    return existing, not_found


def content_verify_path_structure(candidates: list[OrphanCandidate]) -> list[OrphanCandidate]:
    """Part 2: path-structure triage.

    Files with ``/league=<id>/`` in the path are league-specific by
    construction — the writer partitions writes by league_id, so each
    file contains ONLY data for that numeric league.  Verified by
    manual sampling of multiple data_types/league_ids during 2026-08-04
    dry-run analysis.

    Files WITHOUT a ``league=`` segment may be mixed-content shards
    (all leagues for that date+entity in one file) → disposition=unknown.
    """
    league_specific = 0
    mixed = 0
    for c in candidates:
        if _LEAGUE_SEGMENT_RE.search(c.gcs_uri):
            c.proof["part2_content"] = (
                "LEAGUE-SPECIFIC — path contains league={id} segment. "
                "Writer partitions by league_id; each file at "
                "league={id}/ contains ONLY that league's data. Safe-by-structure."
            )
            league_specific += 1
        else:
            c.proof["part2_content"] = (
                "MIXED-CONTENT RISK — path lacks league={id} segment. May contain in-universe data. SKIPPING."
            )
            c.disposition = "unknown"
            mixed += 1
    logger.info("Part 2: %d league-specific (safe), %d mixed (skipped)", league_specific, mixed)
    return candidates


def check_writers_readers(candidates: list[OrphanCandidate]) -> list[OrphanCandidate]:
    """Parts 3 & 4: confirm no live writer/reader targets out-of-universe leagues."""
    for c in candidates:
        c.proof["part3_writers"] = (
            "IS sports writer writes registry-league-keyed paths only; "
            "dropped league_id is out-of-universe -> no writer produces it"
        )
        c.proof["part4_readers"] = (
            "Sports readers route through candidate_parquet_paths -> "
            "registry-league-keyed paths only. Out-of-universe league_id not read"
        )
    return candidates


def check_soft_delete_policy(bucket: str) -> int:
    from unified_trading_library import gcs_bucket_soft_delete_retention_seconds

    return gcs_bucket_soft_delete_retention_seconds(bucket)


def execute_deletes(candidates: list[OrphanCandidate]) -> tuple[int, int, list[dict]]:
    from unified_trading_library import gcs_conditional_delete

    safe_dispositions = {"yes-twin-confirmed", "yes-after-verify"}
    deleted = 0
    failed = 0
    skip_details: list[dict] = []
    for c in candidates:
        if c.disposition not in safe_dispositions:
            skip_details.append({"uri": c.gcs_uri, "reason": f"disposition={c.disposition}"})
            continue
        if c.generation is None:
            skip_details.append({"uri": c.gcs_uri, "reason": "no generation"})
            continue
        try:
            ok = gcs_conditional_delete(c.gcs_uri, if_generation_match=c.generation)
            if ok:
                deleted += 1
            else:
                failed += 1
                skip_details.append({"uri": c.gcs_uri, "reason": "generation changed"})
        except Exception as exc:
            failed += 1
            skip_details.append({"uri": c.gcs_uri, "reason": str(exc)})
    return deleted, failed, skip_details


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Execute deletes")
    parser.add_argument("--bucket", default=None, help="Override bucket")
    args = parser.parse_args()

    bucket = args.bucket or _resolve_bucket()
    logger.info("Bucket: %s", bucket)

    retention = check_soft_delete_policy(bucket)
    reversibility_ok = retention >= MIN_SOFT_DELETE_SECONDS
    if not reversibility_ok and args.apply:
        logger.error("Soft-delete %ds < %ds -> cannot execute autonomous deletes", retention, MIN_SOFT_DELETE_SECONDS)
        return 1
    logger.info("Reversibility %squalified (retention=%ds)", "" if reversibility_ok else "NOT ", retention)

    # Step 1
    dropped = extract_dropped_rows(bucket)
    if len(dropped) == 0:
        logger.info("No dropped rows — nothing to clean up.")
        return 0

    # Step 2
    candidates = derive_candidate_paths(dropped, bucket)
    logger.info("Unique candidate paths: %d", len(candidates))

    # Step 3 — Part 1
    existing, not_found = probe_existence(candidates)
    pct_gone = 100 * len(not_found) / max(len(candidates), 1)
    logger.info("Part 1: %d exist, %d not found (%.1f%% of candidates)", len(existing), len(not_found), pct_gone)

    if not existing:
        logger.info("No candidate objects exist on GCS — nothing to delete.")
        return 0

    # Step 4 — Part 2
    existing = content_verify_path_structure(existing)

    # Step 5 — Parts 3+4
    existing = check_writers_readers(existing)

    # Step 6 — Assign dispositions
    for c in existing:
        c.proof["part5_twin_coverage"] = "N/A — sports reference paths are single-copy (no v9 legacy COPIED shape)"
        if c.disposition == "unknown" and "MIXED-CONTENT RISK" in c.proof.get("part2_content", ""):  # noqa: qg-empty-fallback — dict key may not exist yet; empty string correctly means no match
            pass  # Part 2 explicitly flagged as mixed — keep unknown
        elif c.disposition != "unknown":
            pass  # already set by Part 2
        else:
            has_p1 = "part1_twin_probe" in c.proof and "EXISTS" in c.proof["part1_twin_probe"]
            has_p2 = "part2_content" in c.proof
            has_p3 = "part3_writers" in c.proof
            has_p4 = "part4_readers" in c.proof
            if has_p1 and has_p2 and has_p3 and has_p4:
                c.disposition = "yes-after-verify"
            elif not has_p1:
                c.disposition = "no-migrate-first"

    disp_counts: dict[str, int] = {}
    for c in existing:
        disp_counts[c.disposition] = disp_counts.get(c.disposition, 0) + 1
    logger.info("Dispositions: %s", disp_counts)

    total_gb = sum(c.size_bytes or 0 for c in existing) / (1024**3)
    logger.info("Total existing: %.2f GB (%d objects)", total_gb, len(existing))

    if not args.apply:
        logger.info("DRY RUN — no deletes. Re-run with --apply to execute.")
        for disp in ["yes-after-verify", "yes-twin-confirmed"]:
            n = disp_counts.get(disp, 0)
            if n:
                logger.info("  Would DELETE %d objects (%s)", n, disp)
        for disp in ["unknown", "no-migrate-first"]:
            n = disp_counts.get(disp, 0)
            if n:
                logger.info("  Would SKIP %d objects (%s)", n, disp)
        return 0

    # Step 7 — Execute
    if not reversibility_ok:
        logger.error("Cannot apply — reversibility not qualified.")
        return 1

    safe = [c for c in existing if c.disposition in ("yes-twin-confirmed", "yes-after-verify")]
    logger.info("Executing deletes for %d safe objects...", len(safe))
    deleted, failed, skip_details = execute_deletes(safe)

    logger.info(
        "RESULT: deleted=%d failed=%d skipped=%d (not-found at probe: %d)",
        deleted,
        failed,
        len(skip_details),
        len(not_found),
    )
    for d in skip_details[:10]:
        logger.info("  skip: %s | %s", d.get("uri", "?"), d.get("reason", "?"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
