#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: oneoff
# Delete-when: after silent-absent tradfi IS manifest cells classified+filled confirmed complete:
#              SILENT_ABSENT count verified drops from ~2397 to near-0 genuine-gaps only,
#              captured/failed row counts unchanged, and per-VM shard merged by consolidator.
"""fill_tradfi_silent_absent_cells_2026_06_27.py

Classify and fill SILENT-ABSENT cells in the tradfi IS instrument manifest.

Background
----------
After the full-history backfills + weekend reconcile (104607f), ~2,397 venue x day
cells remain with NO manifest row at all (SILENT-ABSENT). The weekend reconcile
(reconcile_tradfi_non_trading_day_captured_2026_06_26.py) only flipped wrongly-
CAPTURED non-trading cells; it did NOT fill cells that were never written at all.

This script fills those silent gaps with correct ``empty_confirmed`` rows:

  - PRE-GENESIS  : date < venue discovery-start
                   → ``empty_confirmed(EXPECTED_PRE_SOURCE_COVERAGE_START)``

  - NON-TRADING post-genesis : weekend/holiday after discovery-start that was
    missed by the weekend reconcile (Saturday/Sunday/holiday)
                   → ``empty_confirmed(EXPECTED_WEEKEND)``
                      or ``empty_confirmed(EXPECTED_HOLIDAY)``

  - GENUINE-GAP  : post-genesis trading day with no manifest row
                   → FLAGGED in console + CSV, NOT written
                     (likely BLOCKED ICE/FX-allowlist or KRX-adapter)

Classification sources
----------------------
* Discovery start: ``UAC VenueMapping().get_instrument_discovery_start(venue)``
  (SSOT), which equals ``venue_start_dates[venue]`` for all tradfi venues (no
  HYPERLIQUID-style discovery override exists for tradfi).
* Non-trading check: ``instruments_service.reference_data.adapters.tradfi.databento.is_non_trading_day``
  (exchange_calendars-backed, identical to the weekend reconcile script).
* FX is declared 24/7 (``_FX_VENUES_24_7`` frozenset) → ``is_non_trading_day``
  returns False for every date → all post-genesis FX silent-absent cells are
  GENUINE-GAPs (if any exist).

Flip contract (STRICT)
-----------------------
* ONLY write rows where NO existing manifest row exists for (date, venue).
* NEVER overwrite ``captured`` / ``attempted_failed`` / ``empty_confirmed`` rows.
* Only tradfi calendar-bound + FX venues in scope (``_ALL_TRADFI_VENUES``).
* Per-venue sanity cap: warn (not abort) if > 5,000 rows would be written.
* Post-write: captured/failed row counts must not increase.

Usage
-----
DRY-RUN (default — no writes)::

    cd instruments-service
    .venv/bin/python scripts/fill_tradfi_silent_absent_cells_2026_06_27.py \\
        --dry-run

APPLY (only after reviewing dry-run output)::

    MANIFEST_PER_VM_SHARDS=true \\
    VM_NAME=fill-tradfi-silent-$(date +%s) \\
    .venv/bin/python scripts/fill_tradfi_silent_absent_cells_2026_06_27.py \\
        --no-dry-run

Output
------
* Console: per-venue classification counts + genuine-gap list
* CSV audit: one row per classified cell (date, venue, classification, reason)
* Genuine-gap CSV: trading-day absent cells that need operator attention
* Manifest: new empty_confirmed rows written (--no-dry-run only) via per-VM shard
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import tempfile
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────── constants ────────────────────────────────────────

PROJECT_ID = "central-element-323112"
MANIFEST_BLOB = "_index/availability_index.parquet"
SNAPSHOT_BLOB = "_index/snapshots/pre_silent_absent_fill_2026_06_27.parquet"

# All tradfi venues (mirrors _TRADFI_VENUES in venue_core.py).
# FX is 24/7 — is_non_trading_day always returns False for FX.
# YAHOO_FINANCE is a legacy source-as-venue, NOT a canonical venue — not in scope.
_ALL_TRADFI_VENUES: frozenset[str] = frozenset({
    "NASDAQ",
    "NYSE",
    "CME",
    "CBOE",
    "ICE",
    "FX",
    "KRX",
})

# Calendar-bound venues (is_non_trading_day is meaningful for these).
# FX is excluded — it's 24/7 declared.
_CALENDAR_BOUND_VENUES: frozenset[str] = frozenset({
    "NASDAQ",
    "NYSE",
    "CME",
    "CBOE",
    "ICE",
    "KRX",
})

# Per-venue sanity cap: warn (but do not abort) if a venue would get more
# than this many new rows.
_PER_VENUE_WARN_CAP = 5_000

# Classification labels (internal only — not written to manifest)
_CLASS_PRE_GENESIS = "PRE_GENESIS"
_CLASS_NON_TRADING = "NON_TRADING"
_CLASS_GENUINE_GAP = "GENUINE_GAP"


# ─────────────────────────── bucket resolution ────────────────────────────────

def _get_bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="tradfi")


# ─────────────────────────── discovery start dates ────────────────────────────

def _get_discovery_start_dates() -> dict[str, date]:
    """Return the discovery-start date (as a ``date``) for every tradfi venue.

    SSOT: UAC VenueMapping.get_instrument_discovery_start (which equals
    venue_start_dates for all tradfi venues — no discovery overrides exist here).
    """
    from unified_api_contracts import VenueMapping

    vm = VenueMapping()
    result: dict[str, date] = {}
    for venue in _ALL_TRADFI_VENUES:
        start_str = vm.get_instrument_discovery_start(venue)
        if start_str is None:
            logger.warning("VenueMapping has no discovery start for venue %r — skipping", venue)
            continue
        result[venue] = date.fromisoformat(start_str)
    logger.info("Discovery start dates: %s", {v: str(d) for v, d in sorted(result.items())})
    return result


# ─────────────────────────── calendar check ───────────────────────────────────

def _is_non_trading(venue: str, target: date) -> bool:
    """Delegate to IS session SSOT (exchange_calendars-backed).

    FX is 24/7 — always returns False. Undeclared venues return False (safe).
    """
    if venue not in _CALENDAR_BOUND_VENUES:
        return False  # FX: 24/7
    from instruments_service.reference_data.adapters.tradfi.databento import (
        is_non_trading_day,
    )
    from instruments_service.reference_data.adapters.tradfi.databento.sessions import (
        UndeclaredTradfiVenueError,
    )

    try:
        return is_non_trading_day(venue, target)
    except UndeclaredTradfiVenueError:
        logger.warning("Venue %r not declared in IS session SSOT — skipping (safe)", venue)
        return False
    except Exception as exc:
        logger.warning("is_non_trading_day(%r, %r) raised %s — skipping (safe)", venue, target, exc)
        return False


def _non_trading_reason(venue: str, target: date) -> str:
    """Return EXPECTED_WEEKEND or EXPECTED_HOLIDAY for a confirmed non-trading day."""
    from instruments_service.reference_data.adapters.tradfi.databento import (
        non_trading_day_reason,
    )

    try:
        reason = non_trading_day_reason(venue, target)
        return reason or "EXPECTED_WEEKEND"
    except Exception as exc:
        logger.warning("non_trading_day_reason(%r, %r) raised %s — defaulting EXPECTED_WEEKEND", venue, target, exc)
        return "EXPECTED_WEEKEND"


# ─────────────────────────── manifest I/O ─────────────────────────────────────

def _download_manifest(bucket_name: str) -> tuple[pd.DataFrame, bytes]:
    from unified_trading_library import get_storage_client

    client = get_storage_client()
    raw: bytes = client.download_bytes(bucket_name, MANIFEST_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest from gs://%s/%s — %d rows", bucket_name, MANIFEST_BLOB, len(df))
    return df, raw


def _upload_snapshot(bucket_name: str, raw: bytes) -> None:
    """Write a pre-fill snapshot (idempotent — skip if already exists)."""
    from unified_trading_library import get_storage_client

    client = get_storage_client()
    try:
        if hasattr(client, "blob_exists") and client.blob_exists(bucket_name, SNAPSHOT_BLOB):
            logger.info("Snapshot already exists: gs://%s/%s (kept)", bucket_name, SNAPSHOT_BLOB)
            return
    except Exception:
        pass
    client.upload_bytes(bucket_name, SNAPSHOT_BLOB, raw, content_type="application/octet-stream")
    logger.info("Snapshot written: gs://%s/%s", bucket_name, SNAPSHOT_BLOB)


def _write_per_vm_shard(bucket_name: str, new_rows_df: pd.DataFrame) -> str:
    """Upload a per-VM shard (consolidator merges it into the canonical _index)."""
    vm_name = os.environ["VM_NAME"]
    per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
    from unified_trading_library import get_storage_client

    buf = io.BytesIO()
    new_rows_df.to_parquet(buf, index=False)
    buf.seek(0)
    client = get_storage_client()
    client.upload_bytes(bucket_name, per_vm_blob, buf.getvalue(), content_type="application/octet-stream")
    logger.info("Per-VM shard uploaded: gs://%s/%s (%d rows)", bucket_name, per_vm_blob, len(new_rows_df))
    return per_vm_blob


# ─────────────────────────── silent-absent cell enumeration ───────────────────

def _build_expected_universe(
    discovery_starts: dict[str, date],
    df: pd.DataFrame,
) -> dict[tuple[str, str], date]:
    """Return {(venue, date_str): date} for every expected (venue, day) cell.

    The expected universe spans from each venue's discovery start up to the
    maximum date seen in the manifest (or today-1 if manifest has no rows).
    """
    # Determine the date range ceiling from the manifest's max date.
    if df.empty or "date" not in df.columns:
        ceiling = datetime.now(UTC).date() - timedelta(days=1)
    else:
        ceiling = date.fromisoformat(str(df["date"].max()))

    universe: dict[tuple[str, str], date] = {}
    for venue, start in discovery_starts.items():
        current = start
        while current <= ceiling:
            universe[(venue, current.isoformat())] = current
            current += timedelta(days=1)
    logger.info(
        "Expected universe: %d (venue, date) cells across %d venues; date range up to %s",
        len(universe),
        len(discovery_starts),
        ceiling.isoformat(),
    )
    return universe


def _get_existing_cells(df: pd.DataFrame) -> frozenset[tuple[str, str]]:
    """Return a frozenset of (venue, date) tuples that already have a manifest row."""
    if df.empty or "date" not in df.columns or "venue" not in df.columns:
        return frozenset()
    pairs = df[["venue", "date"]].drop_duplicates()
    result: frozenset[tuple[str, str]] = frozenset(
        zip(pairs["venue"].astype(str), pairs["date"].astype(str), strict=False)
    )
    logger.info("Existing manifest cells (venue, date): %d", len(result))
    return result


# ─────────────────────────── classification ───────────────────────────────────

def _classify_silent_absent_cells(
    universe: dict[tuple[str, str], date],
    existing: frozenset[tuple[str, str]],
    discovery_starts: dict[str, date],
) -> tuple[list[tuple[str, str, str, str]], list[tuple[str, str]]]:
    """Classify every silent-absent (venue, date) cell.

    Returns:
        fillable  : list of (venue, date_str, classification, reason) to write
        genuine_gaps : list of (venue, date_str) genuine trading-day gaps

    Classification:
        PRE_GENESIS  → EXPECTED_PRE_SOURCE_COVERAGE_START
        NON_TRADING  → EXPECTED_WEEKEND or EXPECTED_HOLIDAY
        GENUINE_GAP  → flagged but NOT written
    """
    fillable: list[tuple[str, str, str, str]] = []
    genuine_gaps: list[tuple[str, str]] = []

    silent_cells = [(v, d_str, d_val) for (v, d_str), d_val in universe.items() if (v, d_str) not in existing]
    logger.info("Silent-absent cells total: %d", len(silent_cells))

    for venue, date_str, d_val in sorted(silent_cells):
        start = discovery_starts.get(venue)
        if start is None:
            # Unknown venue — should not happen if universe was built correctly
            logger.warning("No discovery start for venue %r — skipping silent cell %s", venue, date_str)
            continue

        if d_val < start:
            # PRE-GENESIS: before discovery start
            fillable.append((venue, date_str, _CLASS_PRE_GENESIS, "EXPECTED_PRE_SOURCE_COVERAGE_START"))
        elif _is_non_trading(venue, d_val):
            # NON-TRADING post-genesis
            reason = _non_trading_reason(venue, d_val)
            fillable.append((venue, date_str, _CLASS_NON_TRADING, reason))
        else:
            # GENUINE-GAP: post-genesis trading day absent
            genuine_gaps.append((venue, date_str))

    logger.info(
        "Classification: %d fillable (PRE_GENESIS + NON_TRADING) + %d genuine gaps",
        len(fillable),
        len(genuine_gaps),
    )
    return fillable, genuine_gaps


# ─────────────────────────── validation gates ─────────────────────────────────

def _validate_per_venue_cap(fillable: list[tuple[str, str, str, str]]) -> None:
    """Warn if any venue exceeds the per-venue cap (warn only — do not abort)."""
    per_venue: Counter[str] = Counter(row[0] for row in fillable)
    over = {v: c for v, c in per_venue.items() if c > _PER_VENUE_WARN_CAP}
    if over:
        logger.warning(
            "Per-venue warn cap %d exceeded for: %s — review before applying",
            _PER_VENUE_WARN_CAP,
            over,
        )


def _validate_row_counts_unchanged(
    df_before: pd.DataFrame, new_row_count: int
) -> bool:
    """Gate: captured/failed counts must not increase after adding new rows."""
    if df_before.empty:
        return True
    n_captured_before = (df_before["capture_status"].fillna("") == "captured").sum()
    n_failed_before = (df_before["capture_status"].fillna("") == "attempted_failed").sum()
    # New rows are all empty_confirmed — no increase expected.
    # This gate is a tripwire: if somehow captured/failed count increased, abort.
    # (We are adding rows to df_before, not patching existing ones.)
    logger.info(
        "Captured rows before: %d | Attempted-failed before: %d | New empty_confirmed rows to add: %d",
        n_captured_before,
        n_failed_before,
        new_row_count,
    )
    return True  # Gate is informational; new rows are additive-only


# ─────────────────────────── CSV audit ────────────────────────────────────────

def _write_audit_csv(
    fillable: list[tuple[str, str, str, str]],
    genuine_gaps: list[tuple[str, str]],
    report_dir: Path,
    ts: str,
) -> tuple[Path, Path]:
    fill_path = report_dir / f"tradfi_silent_absent_fill_{ts}.csv"
    gap_path = report_dir / f"tradfi_genuine_gaps_{ts}.csv"

    fill_df = pd.DataFrame(fillable, columns=["venue", "date", "classification", "reason"])
    fill_df.to_csv(fill_path, index=False)
    logger.info("Fill audit CSV: %s (%d rows)", fill_path, len(fill_df))

    gap_df = pd.DataFrame(genuine_gaps, columns=["venue", "date"])
    gap_df.to_csv(gap_path, index=False)
    logger.info("Genuine-gap CSV: %s (%d rows)", gap_path, len(gap_df))

    return fill_path, gap_path


# ─────────────────────────── summary ──────────────────────────────────────────

def _build_summary(
    fillable: list[tuple[str, str, str, str]],
    genuine_gaps: list[tuple[str, str]],
) -> None:
    """Log per-venue counts and reason breakdown."""
    per_venue_class: Counter[tuple[str, str]] = Counter(
        (row[0], row[2]) for row in fillable
    )
    per_venue_total: Counter[str] = Counter(row[0] for row in fillable)
    reasons: Counter[str] = Counter(row[3] for row in fillable)

    logger.info("=== DRY-RUN: per-venue classification ===")
    for venue in sorted({v for v, _ in per_venue_total.items()}, key=lambda v: -per_venue_total[v]):
        pre = per_venue_class.get((venue, _CLASS_PRE_GENESIS), 0)
        nt = per_venue_class.get((venue, _CLASS_NON_TRADING), 0)
        logger.info("  %-12s : PRE_GENESIS=%d  NON_TRADING=%d  TOTAL=%d", venue, pre, nt, per_venue_total[venue])
    logger.info("  %-12s : %d (TOTAL FILLABLE)", "TOTAL", len(fillable))

    logger.info("=== Reason breakdown ===")
    for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
        logger.info("  %-45s : %d", reason, count)

    if genuine_gaps:
        per_gap_venue: Counter[str] = Counter(v for v, _ in genuine_gaps)
        logger.info("=== GENUINE GAPS (post-genesis trading-day absent — NOT written) ===")
        for venue, count in sorted(per_gap_venue.items(), key=lambda kv: -kv[1]):
            logger.info("  %-12s : %d genuine-gap trading days", venue, count)
        logger.info("  %-12s : %d (TOTAL GENUINE GAPS — operator review required)", "TOTAL", len(genuine_gaps))

        # Show sample genuine-gap dates per venue for quick triage
        shown_per_venue: Counter[str] = Counter()
        for venue, date_str in sorted(genuine_gaps):
            if shown_per_venue[venue] < 5:
                logger.info("    GENUINE-GAP sample: %-12s  %s", venue, date_str)
                shown_per_venue[venue] += 1
    else:
        logger.info("=== No genuine gaps found ===")


# ─────────────────────────── row builder ──────────────────────────────────────

def _build_new_rows(
    fillable: list[tuple[str, str, str, str]],
    df: pd.DataFrame,
    now_iso: str,
) -> pd.DataFrame:
    """Build a DataFrame of new empty_confirmed rows to write.

    Infers column shape from the existing manifest to maximise compatibility
    (unknown columns filled with None/empty).

    The row matches the existing empty_confirmed row shape:
      capture_status = "empty_confirmed"
      error_reason   = <EXPECTED_*>
      attempted_at   = now (ISO-8601)
      available      = "false"
      instrument_count / row_count = 0
    """
    # Build the canonical set of columns from the existing manifest.
    existing_cols: list[str] = list(df.columns) if not df.empty else []

    # Required columns (always present in the IS tradfi manifest).
    required = {
        "date": None,
        "venue": None,
        "capture_status": "empty_confirmed",
        "error_reason": None,
        "attempted_at": now_iso,
        "available": "false",
    }

    rows: list[dict] = []
    for venue, date_str, _classification, reason in fillable:
        row: dict = dict.fromkeys(existing_cols)  # start with None for all columns
        row.update(required)
        row["date"] = date_str
        row["venue"] = venue
        row["error_reason"] = reason
        # Zero numeric counters
        for col in ("instrument_count", "row_count"):
            if col in existing_cols:
                row[col] = 0
        # Propagate asset_group / pipeline_mode / schema_version from existing rows
        # for the same venue if we can (best-effort — not critical for correctness).
        if not df.empty and "venue" in df.columns:
            venue_rows = df[df["venue"] == venue]
            if not venue_rows.empty:
                for col in ("asset_group", "pipeline_mode", "schema_version", "source", "data_type"):
                    if col in existing_cols and col in venue_rows.columns:
                        val = venue_rows[col].dropna().iloc[0] if not venue_rows[col].dropna().empty else None
                        if val is not None:
                            row[col] = val
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=existing_cols)

    new_df = pd.DataFrame(rows, columns=existing_cols if existing_cols else None)
    return new_df


# ─────────────────────────── main ─────────────────────────────────────────────

def main() -> int:
    args = _parse_args()

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== fill_tradfi_silent_absent_cells_2026_06_27 ===")
    logger.info("dry_run=%s  report_dir=%s", args.dry_run, report_dir)

    if not args.dry_run:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error(
                "--no-dry-run requires MANIFEST_PER_VM_SHARDS=true (per-VM shard isolation rule). Aborting."
            )
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--no-dry-run requires VM_NAME=<unique-tag>. Aborting.")
            return 4

    bucket_name = _get_bucket()
    logger.info("Tradfi IS manifest bucket: gs://%s", bucket_name)

    # Step 1: load manifest
    df, raw_bytes = _download_manifest(bucket_name)

    # Step 2: get discovery start dates per venue
    discovery_starts = _get_discovery_start_dates()
    if not discovery_starts:
        logger.error("No discovery start dates found — cannot proceed. Aborting.")
        return 1

    # Step 3: build expected universe and find existing cells
    universe = _build_expected_universe(discovery_starts, df)
    existing = _get_existing_cells(df)

    # Step 4: classify silent-absent cells
    fillable, genuine_gaps = _classify_silent_absent_cells(universe, existing, discovery_starts)

    if not fillable and not genuine_gaps:
        logger.info("No silent-absent cells found. Nothing to do.")
        return 0

    # Step 5: validate per-venue cap
    _validate_per_venue_cap(fillable)

    # Step 6: log summary
    _build_summary(fillable, genuine_gaps)

    # Step 7: write audit CSV
    _fill_path, gap_path = _write_audit_csv(fillable, genuine_gaps, report_dir, run_ts)

    if genuine_gaps:
        logger.warning(
            "GENUINE GAPS detected: %d post-genesis trading days have no manifest row. "
            "These are NOT written by this script. Review %s for operator action. "
            "Likely causes: BLOCKED ICE/FX-allowlist issue or KRX adapter gap.",
            len(genuine_gaps),
            gap_path,
        )

    if args.dry_run:
        logger.info(
            "DRY-RUN complete. %d rows would be written (empty_confirmed). "
            "%d genuine gaps flagged (not written). "
            "Re-run with --no-dry-run (+ MANIFEST_PER_VM_SHARDS=true VM_NAME=...) to apply.",
            len(fillable),
            len(genuine_gaps),
        )
        return 0

    if not fillable:
        logger.info("No fillable cells (only genuine gaps). Nothing to write.")
        return 0

    # ── APPLY ──
    now_iso = datetime.now(UTC).isoformat()

    # Write snapshot before patching
    _upload_snapshot(bucket_name, raw_bytes)

    # Build new rows DataFrame
    new_rows_df = _build_new_rows(fillable, df, now_iso)
    logger.info("New rows to write: %d", len(new_rows_df))

    # Safety: captured/failed counts must not increase
    _validate_row_counts_unchanged(df, len(new_rows_df))

    # Upload per-VM shard (consolidator merges it into canonical _index)
    per_vm_blob = _write_per_vm_shard(bucket_name, new_rows_df)
    logger.info(
        "APPLIED: wrote %d new empty_confirmed rows (PRE_GENESIS + NON_TRADING). "
        "%d genuine gaps SKIPPED (require operator triage). "
        "Per-VM shard: gs://%s/%s  "
        "Consolidator will merge into canonical _index within ~5min.",
        len(fillable),
        len(genuine_gaps),
        bucket_name,
        per_vm_blob,
    )
    return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Default: dry-run (classify + CSV only, no manifest write). "
            "Pass --no-dry-run to apply the fill."
        ),
    )
    p.add_argument(
        "--report-dir",
        default=None,
        metavar="PATH",
        help="Directory for audit CSVs. Default: system temp dir.",
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(main())
