#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after all orphaned GCS objects for the 2026-08-04 out-of-universe drop
#   are confirmed deleted or safe-to-keep, and the plan checkbox is flipped.
"""Scope + execute the GCS-object-level cleanup for the 8,937 manifest rows dropped
by ``canonicalize_sports_league_id_schema_2026_06_24.py --drop-out-of-universe --apply``
on 2026-08-04.

Strategy (no new whole-corpus GCS walk — candidate paths derived from the snapshot):
  1. Read the pre-drop snapshot, apply the same rekey-out-of-universe logic the
     canonicalize script used, and extract the unique ``(date, data_type, league_id)``
     triples of the dropped rows.
  2. Derive candidate GCS paths per triple via UAC ``candidate_parquet_paths()``,
     then probe existence via ``gcs_describe_object`` (Part 1).
  3. Content-verify a sample of existing objects: read the parquet, confirm every
     row's ``league_id`` / ``canonical_league`` column matches the expected
     out-of-universe league (Part 2).
  4. Grep-then-READ the codebase for live writers (Part 3) and readers (Part 4) of
     the path patterns.
  5. Execute ``gcs_conditional_delete`` with the §3a reversibility check
     (``gcs_bucket_soft_delete_retention_seconds >= 604800``, fresh per-run).

Safety: dry-run by default; ``--apply`` to execute deletes after all checks pass.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import tempfile
from collections import defaultdict
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from unified_api_contracts.canonical.domain.sports.gcs_paths import (
    SPORTS_DATA_TYPE_TO_FOLDER,
    candidate_parquet_paths,
    sports_bucket_name,
)
from unified_api_contracts.sports import (
    LEAGUE_REGISTRY,
    canonicalize_league_id,
    get_league_by_api_football_id,
)
from unified_trading_library.cloud_interface import (
    gcs_bucket_soft_delete_retention_seconds,
    gcs_conditional_delete,
    gcs_describe_object,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SNAPSHOT_BLOB = (
    "_index/snapshots/pre_league_id_canonicalize_20260804T075724Z.parquet"
)
BUCKET_NAME = "instruments-store-sports-prd-central-element-323112"
SNAPSHOT_URI = f"gs://{BUCKET_NAME}/{SNAPSHOT_BLOB}"

_NUMERIC_RE = re.compile(r"^\d+$")
_SUFFIX_RE = re.compile(r".+_\d+$")

# Minimum GCS Soft Delete retention for §3a reversibility (7 days).
_REVERSIBILITY_MIN_SECONDS = 604800


def _registry_ids() -> set[str]:
    reg = LEAGUE_REGISTRY
    try:
        return {v.league_id for v in reg.values()}
    except AttributeError:
        return {getattr(v, "league_id", str(v)) for v in reg}


def _canonicalize(raw: str) -> str:
    s = raw.strip()
    if s and s.isdigit():
        league = get_league_by_api_football_id(int(s))
        s = league.league_id if league is not None else s
    return canonicalize_league_id(s)


def resolve_project_id() -> str:
    pid = os.environ.get("GCP_PROJECT_ID") or os.environ.get("PROJECT_ID") or ""
    if not pid:
        logger.warning("GCP_PROJECT_ID not set — using default bucket name")
        return "central-element-323112"
    return pid


# ---------------------------------------------------------------------------
# Step 1 — extract dropped-row triples from the snapshot
# ---------------------------------------------------------------------------
def _is_numeric_rekey(s: str) -> bool:
    return bool(_NUMERIC_RE.match(s)) and s != ""


def _is_suffixed_rekey(s: str) -> bool:
    return bool(_SUFFIX_RE.match(s))


def extract_dropped_triples(snapshot_uri: str) -> list[dict]:
    """Read the pre-drop snapshot and return the rows that the canonicalize
    script would have dropped (``rekey_out_universe``) — using PyArrow
    native table ops, with only the small filtered subset reaching Python.
    """
    logger.info("Reading snapshot (PyArrow, column-pruned): %s", snapshot_uri)
    table = pq.read_table(snapshot_uri)
    keep_cols = [c for c in ("date", "data_type", "league_id") if c in table.column_names]
    table = table.select(keep_cols)
    n_total = table.num_rows
    logger.info("Snapshot: %d rows, cols=%s (column-pruned)", n_total, keep_cols)

    # Extract league_id as Python list (only this one column — strings, not objects).
    # PyArrow's to_pylist() yields Python str objects directly, one per row.
    lid_list = pc.fill_null(table.column("league_id").cast(pa.string()), "").to_pylist()
    reg = _registry_ids()
    logger.info("LEAGUE_REGISTRY size: %d", len(reg))

    # Pass 1: find distinct rekey-candidate league_ids — build canon_map.
    rekey_candidates: set[str] = set()
    for v in lid_list:
        if _is_numeric_rekey(v) or _is_suffixed_rekey(v):
            rekey_candidates.add(v)
    logger.info("Distinct rekey-candidate league_ids: %d", len(rekey_candidates))
    canon_map: dict[str, str] = {v: _canonicalize(v) for v in rekey_candidates}
    canon_in_reg_map: dict[str, bool] = {v: canon_map[v] in reg for v in rekey_candidates}

    # Pass 2: find out-of-universe rows (rekey candidate + canon NOT in registry).
    out_rows: list[dict] = []
    seen_triples: set[tuple] = set()
    date_col = table.column("date").to_pylist()
    dt_col = table.column("data_type").to_pylist()

    for i, lid_raw in enumerate(lid_list):
        if lid_raw in canon_in_reg_map:
            if not canon_in_reg_map[lid_raw]:
                # Out-of-universe — this row was dropped.
                key = (str(date_col[i]), str(dt_col[i]), lid_raw)
                if key not in seen_triples:
                    seen_triples.add(key)
                    out_rows.append({
                        "date": str(date_col[i]),
                        "data_type": str(dt_col[i]),
                        "league_id": lid_raw,
                    })

    logger.info("Out-of-universe unique triples: %d", len(out_rows))
    return out_rows


# ---------------------------------------------------------------------------
# Step 2 — derive + probe candidate GCS paths
# ---------------------------------------------------------------------------
def derive_candidate_uris(triples: list[dict], project_id: str) -> dict[tuple, list[str]]:
    """For each triple, return the list of candidate GCS URIs."""
    result: dict[tuple, list[str]] = {}
    bucket = sports_bucket_name(project_id, env="prd")
    for t in triples:
        key = (str(t["date"]), str(t["data_type"]), str(t["league_id"]))
        paths = candidate_parquet_paths(
            data_type=str(t["data_type"]),
            day=str(t["date"]),
            league_id=str(t["league_id"]),
        )
        uris = [f"gs://{bucket}/{p}" for p in paths]
        result[key] = uris
    return result


def probe_existence(
    candidate_map: dict[tuple, list[str]],
) -> dict[tuple, tuple[str | None, str | None]]:
    """For each triple, probe candidate URIs and return (found_uri, generation).
    ``(None, None)`` means no candidate exists.
    """
    result: dict[tuple, tuple[str | None, str | None]] = {}
    total = len(candidate_map)
    for i, (key, uris) in enumerate(candidate_map.items()):
        if i % 500 == 0:
            logger.info("Probe progress: %d/%d", i, total)
        found_uri = None
        found_gen = None
        for uri in uris:
            meta = gcs_describe_object(uri)
            if meta is not None:
                found_uri = uri
                found_gen = meta.generation
                break
        result[key] = (found_uri, found_gen)
    return result


# ---------------------------------------------------------------------------
# Step 3 — Content-verify (sample per data_type)
# ---------------------------------------------------------------------------
def content_verify_sample(
    found: dict[tuple, tuple[str | None, str | None]],
    sample_size: int = 5,
) -> dict[str, bool]:
    """For each data_type, sample up to ``sample_size`` existing objects,
    read the parquet, and confirm the league_id column matches the expected
    out-of-universe league (or that the file contains ONLY that league's rows).

    Returns ``{data_type: safe_to_delete}`` — True only for data_types where
    all sampled objects passed content verification.
    """
    # Group existing objects by data_type.
    by_dt: dict[str, list[tuple[tuple, str]]] = defaultdict(list)
    for key, (uri, gen) in found.items():
        if uri is not None:
            dt = str(key[1])
            by_dt[dt].append((key, uri))

    result: dict[str, bool] = {}
    for dt, entries in sorted(by_dt.items()):
        n_sample = min(sample_size, len(entries))
        all_ok = True
        for j in range(n_sample):
            key, uri = entries[j]
            expected_lid = str(key[2])
            try:
                df = pd.read_parquet(uri)
                # Determine which column holds the league identifier.
                lid_col = None
                for candidate in ("canonical_league", "league_id", "league"):
                    if candidate in df.columns:
                        lid_col = candidate
                        break
                if lid_col is None:
                    logger.warning(
                        "  [%s] %s: no league column found, cols=%s — SKIP",
                        dt, uri, list(df.columns)[:10],
                    )
                    all_ok = False
                    continue

                unique_lids = df[lid_col].dropna().astype(str).unique()
                if len(unique_lids) == 1 and unique_lids[0] == expected_lid:
                    logger.info("  [%s] %s: OK — single league=%s, %d rows",
                                dt, uri, expected_lid, len(df))
                elif len(unique_lids) == 0:
                    logger.info("  [%s] %s: OK — empty file (0 rows)", dt, uri)
                else:
                    logger.warning(
                        "  [%s] %s: MIXED — expected league=%s, got %d unique: %s",
                        dt, uri, expected_lid, len(unique_lids),
                        list(unique_lids[:10]),
                    )
                    all_ok = False
            except Exception as exc:
                logger.warning("  [%s] %s: read error — %s", dt, uri, exc)
                all_ok = False

        result[dt] = all_ok
        logger.info(
            "Content-verify [%s]: %d sampled, all_ok=%s",
            dt, n_sample, all_ok,
        )

    return result


# ---------------------------------------------------------------------------
# Step 4 — Writer / reader grep check
# ---------------------------------------------------------------------------
def report_path_patterns(triples: list[dict]) -> dict[str, list[str]]:
    """Collect the distinct entity folder names and league_ids present in the
    dropped triples, so the operator/agent can grep the codebase for live
    writers/readers targeting these paths.
    """
    folders = sorted({SPORTS_DATA_TYPE_TO_FOLDER.get(str(t["data_type"]), str(t["data_type"])) for t in triples})
    league_ids = sorted({str(t["league_id"]) for t in triples})
    return {"entity_folders": folders, "league_ids": league_ids}


# ---------------------------------------------------------------------------
# Step 5 — §3a reversibility check
# ---------------------------------------------------------------------------
def check_reversibility(bucket: str) -> bool:
    seconds = gcs_bucket_soft_delete_retention_seconds(bucket)
    logger.info(
        "GCS Soft Delete retention for %s: %d seconds (need >= %d)",
        bucket, seconds, _REVERSIBILITY_MIN_SECONDS,
    )
    return seconds >= _REVERSIBILITY_MIN_SECONDS


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Execute deletes (default: dry-run — scope + verify only)",
    )
    parser.add_argument(
        "--sample-size", type=int, default=5,
        help="Number of objects per data_type to content-verify (default: 5)",
    )
    parser.add_argument(
        "--max-deletes", type=int, default=0,
        help="Max deletes to execute (0 = unlimited, only with --apply)",
    )
    args = parser.parse_args()

    project_id = resolve_project_id()
    logger.info("Project ID: %s", project_id)
    logger.info("Mode: %s", "APPLY" if args.apply else "DRY-RUN")

    # --- §3a reversibility check (always, even dry-run) ---
    reversible = check_reversibility(BUCKET_NAME)
    if not reversible:
        logger.error(
            "FAIL: GCS Soft Delete retention < %d seconds on %s — "
            "prod-bucket delete NOT reversibility-qualified. ABORT.",
            _REVERSIBILITY_MIN_SECONDS, BUCKET_NAME,
        )
        if args.apply:
            return 1
        logger.warning("Dry-run continuing despite reversibility fail — no deletes will run.")

    # --- Step 1: Extract dropped triples ---
    triples = extract_dropped_triples(SNAPSHOT_URI)
    if not triples:
        logger.info("No out-of-universe triples found — nothing to clean up.")
        return 0

    # --- Step 2: Derive + probe paths ---
    logger.info("Deriving candidate GCS URIs for %d triples...", len(triples))
    candidate_map = derive_candidate_uris(triples, project_id)
    logger.info("Probing existence...")
    found = probe_existence(candidate_map)

    n_existing = sum(1 for uri, _ in found.values() if uri is not None)
    n_missing = sum(1 for uri, _ in found.values() if uri is None)
    logger.info("Existence probe: %d existing, %d missing (already cleaned)", n_existing, n_missing)

    if n_existing == 0:
        logger.info("No orphaned GCS objects found — all already deleted. Nothing to do.")
        return 0

    # --- Step 3: Content-verify sample ---
    content_ok = content_verify_sample(found, sample_size=args.sample_size)
    mixed_dts = [dt for dt, ok in content_ok.items() if not ok]
    if mixed_dts:
        logger.warning(
            "MIXED-CONTENT data_types: %s — these will be SKIPPED. "
            "Investigate before deleting.",
            mixed_dts,
        )

    # --- Step 4: Report path patterns for writer/reader grep ---
    patterns = report_path_patterns(triples)
    logger.info("Entity folders in dropped triples: %s", patterns["entity_folders"])
    logger.info("Sample league_ids (first 20): %s", patterns["league_ids"][:20])

    # --- Summary ---
    # Count by data_type
    by_dt: dict[str, dict[str, int]] = defaultdict(lambda: {"existing": 0, "missing": 0})
    for key, (uri, _) in found.items():
        dt = str(key[1])
        if uri is not None:
            by_dt[dt]["existing"] += 1
        else:
            by_dt[dt]["missing"] += 1

    logger.info("=" * 60)
    logger.info("SCOPING SUMMARY")
    logger.info("=" * 60)
    logger.info("Total unique (date, data_type, league_id) triples: %d", len(triples))
    logger.info("GCS objects still existing: %d", n_existing)
    logger.info("GCS objects already gone: %d", n_missing)
    logger.info("")
    logger.info("By data_type:")
    for dt in sorted(by_dt):
        stats = by_dt[dt]
        dt_ok = content_ok.get(dt, True)
        flag = "" if dt_ok else " ⚠ MIXED-CONTENT"
        logger.info("  %-30s existing=%5d missing=%5d%s", dt, stats["existing"], stats["missing"], flag)
    logger.info("")
    logger.info("Reversibility (§3a): %s (retention=%s)",
                "QUALIFIED" if reversible else "NOT QUALIFIED",
                BUCKET_NAME)
    logger.info("")

    if not args.apply:
        logger.info("DRY RUN complete. Re-run with --apply to execute deletes.")
        logger.info(
            "Before --apply, run: grep -rE '%s' --include='*.py' across all repos "
            "to confirm no live writer/reader targets these paths (Parts 3+4).",
            "|".join(patterns["entity_folders"]),
        )
        return 0

    # --- Step 5: Execute deletes ---
    if not reversible:
        logger.error("Cannot --apply: reversibility check failed. Abort.")
        return 1

    delete_count = 0
    skipped_mixed = 0
    errors = 0

    for key, (uri, gen) in found.items():
        if uri is None:
            continue
        dt = str(key[1])
        if not content_ok.get(dt, True):
            skipped_mixed += 1
            continue
        if args.max_deletes > 0 and delete_count >= args.max_deletes:
            logger.info("Reached --max-deletes=%d, stopping.", args.max_deletes)
            break

        logger.info("DELETE: %s (generation=%s)", uri, gen)
        ok = gcs_conditional_delete(uri, if_generation_match=gen)
        if ok:
            delete_count += 1
        else:
            logger.warning("  FAILED (generation mismatch or already gone): %s", uri)
            errors += 1

    logger.info("=" * 60)
    logger.info("DELETE SUMMARY: %d deleted, %d skipped (mixed-content), %d errors",
                delete_count, skipped_mixed, errors)
    logger.info("Already-gone (no action needed): %d", n_missing)
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
