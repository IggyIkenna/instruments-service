#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod-run confirmed + a re-scan of every GHOST_TO_CANON canonical prefix in
#   instruments-store-defi-prd-central-element-323112 finds 0 rows whose venue/instrument_key
#   embed a ghost-spelled venue token
"""Real, targeted remediation for the ghost-venue-merge contamination bug.

## Background

``legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py`` (shipped as
``instruments-service@11192be2``) merged 33,003 ghost-spelled DeFi venue GCS
objects (e.g. ``AAVEV3-OPTIMISM``, no underscore) into their canonical-spelled
siblings (``AAVE_V3-OPTIMISM``). An adversarial verification pass the same day
found its ``_merge_frames`` fixed the GCS *path* but never rewrote a surviving
ghost-only row's own ``venue``/``instrument_key`` COLUMN values — e.g.
``instrument_key='AAVEV3-OPTIMISM:A_TOKEN:ALINK'`` (no underscore) lived on
inside an otherwise-canonical-path parquet. That script's ``_merge_frames`` is
now fixed (``_rewrite_ghost_venue_columns``, generic over every column) — this
script is the one-time REMEDIATION pass over data already written by the
FIRST (buggy) run, fixing already-migrated rows in place.

## Scope (real, targeted — NOT a fresh full-corpus walk)

The original migration's ``_process_one`` always writes the merged frame to
the DESTINATION bucket only (``instruments-store-defi-prd-{pid}`` —
``dst_bucket = client.bucket(PRD_BUCKET)`` unconditionally, even for the
``VELODROMEV2-OPTIMISM`` cross-bucket case sourced from the legacy env-less
bucket). So every row this bug could have contaminated lives in EXACTLY ONE
bucket, under EXACTLY the 29 canonical venue prefixes in ``GHOST_TO_CANON``'s
value set (imported from the fixed migration script, not re-derived, so scope
can never drift from the original migration's real scope). This script lists
``instrument_availability/by_date/day=*/venue=<canon_venue>/instruments.parquet``
for each of those 29 canonical venues (a scoped per-venue-prefix listing,
exactly mirroring the original migration's own ``_list_ghost_days`` — never a
whole-corpus walk) and, for every day found, downloads the file, tries the
now-fixed ``_rewrite_ghost_venue_columns`` for every ghost token that maps to
that canonical venue, and only writes back (backup-first) if any cell
actually changed. A file where nothing changes (e.g. a day that was never
touched by the original migration, or a collision day where every ghost row
happened to be an exact identity-column duplicate of the canonical row and
was therefore dropped, not carried over) costs one read and is never written.

Mechanics per (canon_venue, day) found:
  1. Download the real canonical object.
  2. Compute ``fixed = _rewrite_ghost_venue_columns(df, ghost_venue, canon_venue)``
     (reused verbatim from the fixed migration script — not re-implemented).
  3. If ``fixed`` is byte-for-byte unchanged from ``df`` (same values in every
     cell), this (canon_venue, day) was never contaminated — skip, no write.
  4. Otherwise: BACKUP the real pre-remediation object (server-side copy) under
     ``_migration_backup/legacy_naming_audit_dexpool_contamination_remediation_2026_07_09/<run_id>/...``,
     write ``fixed`` back to the SAME canonical path, then re-download and
     verify 0 cells still embed the ghost spelling AND the row count is
     unchanged (this is a cell-value fix only, never a row add/drop).

Idempotent: re-running finds 0 contaminated cells once complete.

Usage::

    cd instruments-service
    .venv/bin/python scripts/legacy_naming_audit_dexpool_ghost_venue_contamination_remediation_2026_07_09.py  # dry-run (default, no --apply)
    .venv/bin/python scripts/legacy_naming_audit_dexpool_ghost_venue_contamination_remediation_2026_07_09.py --apply --limit 50  # smoke test
    .venv/bin/python scripts/legacy_naming_audit_dexpool_ghost_venue_contamination_remediation_2026_07_09.py --apply --workers 24

Real evidence + per-venue before/after counts: see the Progress Log in
``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
and ``instruments-service/docs/DEFI_INSTRUMENTS.md``.
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from types import ModuleType

import pandas as pd
from google.cloud import (
    storage,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("legacy_naming_audit_dexpool_contamination_remediation")


def _load_migration_module() -> ModuleType:
    """Dynamically load the fixed migration script so this remediation reuses its
    real constants (``GHOST_TO_CANON``, ``PROJECT_ID``, ``PRD_BUCKET``, ``PREFIX``)
    and its ``_rewrite_ghost_venue_columns``/``_make_client``/``_read_parquet``
    helpers verbatim — never a re-implementation that could silently drift from
    the fix under test. ``scripts/`` has no ``__init__.py`` (not a package), so
    this mirrors the exact loading pattern already used by
    ``tests/scripts/test_legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py``.
    """
    script_path = Path(__file__).resolve().parent / "legacy_naming_audit_dexpool_ghost_venue_merge_2026_07_09.py"
    module_name = "_legacy_naming_audit_dexpool_ghost_venue_merge_reused"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_MIG = _load_migration_module()

PROJECT_ID = _MIG.PROJECT_ID
PRD_BUCKET = _MIG.PRD_BUCKET
PREFIX = _MIG.PREFIX
GHOST_TO_CANON: dict[str, str] = _MIG.GHOST_TO_CANON
CANON_TO_GHOST: dict[str, str] = {canon: ghost for ghost, canon in GHOST_TO_CANON.items()}
RUN_ID = "2026_07_09"
BACKUP_PREFIX = f"_migration_backup/legacy_naming_audit_dexpool_contamination_remediation_{RUN_ID}"


@dataclass
class RemediationResult:
    canon_venue: str
    ghost_venue: str
    day: str
    ok: bool
    contaminated: bool
    rows: int
    cells_fixed: int
    err: str = ""


def _count_ghost_cells(df: pd.DataFrame, ghost_venue: str) -> int:
    """Count cells across every object-dtype column that still embed the ghost
    spelling (exact match or ``<ghost_venue>:`` prefix) — the same convention
    ``_rewrite_ghost_venue_columns`` fixes. Used both to detect contamination
    (pre-write) and to verify the fix (post-write, must be 0)."""
    ghost_prefix = f"{ghost_venue}:"
    total = 0
    for col in df.columns:
        if df[col].dtype != object:
            continue
        s = df[col]
        total += int(s.map(lambda v: isinstance(v, str) and (v == ghost_venue or v.startswith(ghost_prefix))).sum())
    return total


def _list_canon_days(client: storage.Client, bucket_name: str, canon_venue: str) -> list[str]:
    """Real, scoped (single-venue-prefix) GCS listing of the CANONICAL side —
    the mirror image of the original migration's ``_list_ghost_days``."""
    bucket = client.bucket(bucket_name)
    days: list[str] = []
    for blob in client.list_blobs(
        bucket, prefix=PREFIX, match_glob=f"{PREFIX}day=*/venue={canon_venue}/instruments.parquet"
    ):
        part = blob.name.split("day=", 1)[1]
        days.append(part.split("/", 1)[0])
    return sorted(days)


def _backup_path(original_path: str) -> str:
    return f"{BACKUP_PREFIX}/{PRD_BUCKET}/{original_path}"


def _process_one(
    client: storage.Client, canon_venue: str, ghost_venue: str, day: str, *, apply: bool
) -> RemediationResult:
    dst_bucket = client.bucket(PRD_BUCKET)
    canon_path = f"{PREFIX}day={day}/venue={canon_venue}/instruments.parquet"
    try:
        df = _MIG._read_parquet(dst_bucket, canon_path)
        if df is None:
            return RemediationResult(canon_venue, ghost_venue, day, False, False, 0, 0, "canonical object vanished")
        pre_ghost_cells = _count_ghost_cells(df, ghost_venue)
        if pre_ghost_cells == 0:
            return RemediationResult(canon_venue, ghost_venue, day, True, False, len(df), 0)

        fixed = _MIG._rewrite_ghost_venue_columns(df, ghost_venue, canon_venue)
        post_ghost_cells = _count_ghost_cells(fixed, ghost_venue)

        if not apply:
            return RemediationResult(canon_venue, ghost_venue, day, True, True, len(df), pre_ghost_cells)

        # Backup BEFORE mutating: server-side copy of the real pre-remediation bytes
        # (same ``Bucket.copy_blob`` idiom as the original migration script).
        dst_bucket.copy_blob(dst_bucket.blob(canon_path), dst_bucket, new_name=_backup_path(canon_path))
        buf = BytesIO()
        fixed.to_parquet(buf, index=False)
        buf.seek(0)
        dst_bucket.blob(canon_path).upload_from_file(buf, content_type="application/octet-stream")

        verify_df = _MIG._read_parquet(dst_bucket, canon_path)
        if verify_df is None or len(verify_df) != len(df):
            return RemediationResult(
                canon_venue,
                ghost_venue,
                day,
                False,
                True,
                len(df),
                pre_ghost_cells,
                "post-write row-count mismatch",
            )
        verify_ghost_cells = _count_ghost_cells(verify_df, ghost_venue)
        if verify_ghost_cells != 0:
            return RemediationResult(
                canon_venue,
                ghost_venue,
                day,
                False,
                True,
                len(df),
                pre_ghost_cells,
                f"post-write verify still has {verify_ghost_cells} ghost cells",
            )
        _ = post_ghost_cells
        return RemediationResult(canon_venue, ghost_venue, day, True, True, len(df), pre_ghost_cells)
    except Exception as exc:  # broad-except-ok: per-shard failure isolation, real remediation over ~30k objects
        return RemediationResult(canon_venue, ghost_venue, day, False, False, 0, 0, str(exc))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true", help="Write for real. Default is dry-run.")
    p.add_argument("--workers", type=int, default=24)
    p.add_argument("--limit", type=int, default=0, help="Cap total (venue,day) pairs processed (0 = unlimited).")
    p.add_argument(
        "--venue",
        action="append",
        default=None,
        help="Restrict to one or more CANONICAL venue tokens (repeatable). Default: all GHOST_TO_CANON values.",
    )
    args = p.parse_args()
    apply = bool(args.apply)

    client = _MIG._make_client(PROJECT_ID, pool_maxsize=max(32, args.workers * 2))
    canon_venues = args.venue or sorted(CANON_TO_GHOST.keys())

    logger.info(
        "Real GCS listing (scoped per-venue prefix, not a whole-corpus walk) for %d canonical venues in %s",
        len(canon_venues),
        PRD_BUCKET,
    )
    jobs: list[tuple[str, str, str]] = []  # (canon_venue, ghost_venue, day)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=min(32, len(canon_venues))) as ex:
        futs = {ex.submit(_list_canon_days, client, PRD_BUCKET, cv): cv for cv in canon_venues}
        for fut in as_completed(futs):
            canon_venue = futs[fut]
            ghost_venue = CANON_TO_GHOST[canon_venue]
            for day in fut.result():
                jobs.append((canon_venue, ghost_venue, day))
    jobs.sort()
    if args.limit:
        jobs = jobs[: args.limit]
    logger.info("Found %d real (canon_venue,day) pairs to check in %.1fs", len(jobs), time.time() - t0)
    if not jobs:
        logger.info("Nothing to check. Exiting.")
        return 0

    mode = "APPLY (real writes)" if apply else "DRY-RUN (read-only)"
    logger.info("Mode: %s. run_id=%s backup_prefix=gs://%s/%s", mode, RUN_ID, PRD_BUCKET, BACKUP_PREFIX)

    ok = 0
    failed = 0
    contaminated = 0
    total_cells_fixed = 0
    completed = 0
    t1 = time.time()
    per_venue_stats: dict[str, dict[str, int]] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(_process_one, client, cv, gv, d, apply=apply) for cv, gv, d in jobs]
        for fut in as_completed(futs):
            r = fut.result()
            completed += 1
            stats = per_venue_stats.setdefault(
                r.canon_venue, {"ok": 0, "failed": 0, "contaminated": 0, "cells_fixed": 0}
            )
            if r.ok:
                ok += 1
                stats["ok"] += 1
                if r.contaminated:
                    contaminated += 1
                    stats["contaminated"] += 1
                    total_cells_fixed += r.cells_fixed
                    stats["cells_fixed"] += r.cells_fixed
            else:
                failed += 1
                stats["failed"] += 1
                logger.warning("FAILED %s day=%s: %s", r.canon_venue, r.day, r.err)
            if completed % 500 == 0 or completed == len(jobs):
                rate = completed / max(0.01, time.time() - t1)
                logger.info(
                    "  %d/%d processed (%.1f/sec, ok=%d failed=%d contaminated=%d, %.0fs elapsed)",
                    completed,
                    len(jobs),
                    rate,
                    ok,
                    failed,
                    contaminated,
                    time.time() - t1,
                )

    logger.info("=== Per-venue summary (only contaminated>0 or failed>0 shown) ===")
    for venue, stats in sorted(per_venue_stats.items()):
        if stats["contaminated"] or stats["failed"]:
            logger.info(
                "  %-24s ok=%-5d failed=%-4d contaminated=%-4d cells_fixed=%-6d",
                venue,
                stats["ok"],
                stats["failed"],
                stats["contaminated"],
                stats["cells_fixed"],
            )
    logger.info(
        "Done. mode=%s total_pairs=%d ok=%d failed=%d contaminated_pairs=%d total_cells_fixed=%d elapsed=%.0fs at %s",
        mode,
        len(jobs),
        ok,
        failed,
        contaminated,
        total_cells_fixed,
        time.time() - t0,
        datetime.now(UTC).isoformat(),
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
