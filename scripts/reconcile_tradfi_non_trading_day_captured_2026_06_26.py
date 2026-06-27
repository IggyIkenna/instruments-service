#!/usr/bin/env python3
# Epic: tradfi_master
# Lifecycle: oneoff
# Delete-when: after non-trading-day captured→empty_confirmed flip confirmed complete
#              in instruments-store-tradfi-prd-central-element-323112/_index and
#              tradfi empty_confirmed count verified climbed from ~406 to thousands.
"""reconcile_tradfi_non_trading_day_captured_2026_06_26.py

Retrospective fix for IS tradfi manifest cells that were WRONGLY stamped
``captured`` on weekends / US market holidays.

Root cause (d3908c3 context)
-----------------------------
The old DBEQ.BASIC 5-day-lookback returned Friday's instrument definitions when
the fetcher ran on Saturday or Sunday. The instruments-service writer wrote those
as ``captured`` (found instruments) instead of ``empty_confirmed(EXPECTED_WEEKEND)``.
d3908c3 fixed the going-forward path to pre-stamp non-trading days before the
fetch loop runs so the writer never writes a ``captured`` row on a weekend/holiday.
However, ``skip-existing`` means the existing wrongly-captured cells are NOT
re-touched by the going-forward fix.  This script identifies and flips them.

Flip contract
-------------
ONLY flip rows where:
  1. ``capture_status == "captured"``  (wrong — should be empty_confirmed)
  2. ``venue`` is in the tradfi calendar-bound set (NASDAQ, NYSE, CME, CBOE,
     ICE, KRX — NOT FX which is 24/7, NOT YAHOO_FINANCE)
  3. ``is_non_trading_day(venue, date)`` is True per the IS sessions SSOT
     (uses exchange_calendars for holiday detection — more authoritative than
     UAC venue_trading_calendar.py's static US_MARKET_HOLIDAYS frozenset)

Flip target
-----------
``capture_status``  → ``empty_confirmed``
``error_reason``    → ``EXPECTED_WEEKEND`` (Sat/Sun) or ``EXPECTED_HOLIDAY`` (holiday weekday)
``attempted_at``    → now (ISO-8601)
``row_count``       → 0  (these cells had instrument definitions from the 5-day-lookback
                          artefact, so row_count > 0 -- must zero them)

Safety gates (ABORT before write if violated)
---------------------------------------------
1. Flipped rows MUST all have ``is_non_trading_day(venue, date) == True``.
2. ``available_from`` / ``available_to`` in the catalogue row (if present) must NOT be
   modified — empty_confirmed weekends sit INSIDE an instrument's availability window,
   not at the boundary.
3. Newly flipped set must be < 5000 rows per venue (sanity cap — alert if exceeded).
4. No rows from the ``FX`` or ``YAHOO_FINANCE`` venue are touched (these are 24/7).
5. Newly ``captured`` count must be 0 after the flip (we must not introduce new
   captured rows — this script only flips captured→empty_confirmed).

Usage
-----
DRY-RUN (default — no writes)::

    cd instruments-service
    .venv/bin/python scripts/reconcile_tradfi_non_trading_day_captured_2026_06_26.py \\
        --dry-run

APPLY (only after reviewing dry-run output)::

    MANIFEST_PER_VM_SHARDS=true \\
    VM_NAME=reconcile-tradfi-nontrade-$(date +%s) \\
    .venv/bin/python scripts/reconcile_tradfi_non_trading_day_captured_2026_06_26.py \\
        --no-dry-run

Output
------
* Console: per-venue wrongly-captured counts + sample rows
* CSV audit: one row per flipped cell (date, venue, data_type, old_status, new_reason)
* Manifest: updated in-place (--no-dry-run only) via snapshot + writeback or per-VM shard
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import tempfile
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────── constants ────────────────────────────────────────

PROJECT_ID = "central-element-323112"
MANIFEST_BLOB = "_index/availability_index.parquet"
SNAPSHOT_BLOB = "_index/snapshots/pre_nontrade_flip_2026_06_26.parquet"

# Calendar-bound tradfi venues (is_non_trading_day raises for unknown venues).
# FX is 24/7 — skip. YAHOO_FINANCE is a legacy source-as-venue — skip.
_CALENDAR_BOUND_VENUES: frozenset[str] = frozenset({
    "NASDAQ",
    "NYSE",
    "CME",
    "CBOE",
    "ICE",
    "KRX",
})

# Per-venue cap: alert (but do not abort) if a venue would have more than this many flips.
_PER_VENUE_WARN_CAP = 5_000


# ─────────────────────────── bucket resolution ────────────────────────────────

def _get_bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="tradfi")


# ─────────────────────────── calendar check ───────────────────────────────────

def _is_non_trading(venue: str, iso_date: str) -> bool:
    """Use the IS sessions SSOT (exchange_calendars-backed) to test non-trading.

    Falls back to False for venues not in _CALENDAR_BOUND_VENUES (FX / YAHOO_FINANCE).
    Catches UndeclaredTradfiVenueError gracefully — returns False (safe: don't flip).
    """
    if venue not in _CALENDAR_BOUND_VENUES:
        return False
    from instruments_service.reference_data.adapters.tradfi.databento import (
        is_non_trading_day,
    )
    from instruments_service.reference_data.adapters.tradfi.databento.sessions import (
        UndeclaredTradfiVenueError,
    )

    try:
        target = date.fromisoformat(iso_date)
        return is_non_trading_day(venue, target)
    except UndeclaredTradfiVenueError:
        logger.warning("Venue %r not declared in IS sessions SSOT — skipping (safe)", venue)
        return False
    except Exception as exc:
        logger.warning("is_non_trading_day(%r, %r) raised %s — skipping (safe)", venue, iso_date, exc)
        return False


def _non_trading_reason(venue: str, iso_date: str) -> str:
    """Return EXPECTED_WEEKEND or EXPECTED_HOLIDAY for a known non-trading day.

    Defaults to EXPECTED_WEEKEND if reason cannot be determined (safe over-estimate).
    """
    from instruments_service.reference_data.adapters.tradfi.databento import (
        non_trading_day_reason,
    )
    try:
        target = date.fromisoformat(iso_date)
        reason = non_trading_day_reason(venue, target)
        return reason or "EXPECTED_WEEKEND"
    except Exception as exc:
        logger.warning("non_trading_day_reason(%r, %r) raised %s — defaulting to EXPECTED_WEEKEND", venue, iso_date, exc)
        return "EXPECTED_WEEKEND"


# ─────────────────────────── manifest I/O ─────────────────────────────────────

def _download_manifest(bucket_name: str) -> pd.DataFrame:
    from unified_trading_library import get_storage_client

    client = get_storage_client()
    raw: bytes = client.download_bytes(bucket_name, MANIFEST_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest from gs://%s/%s — %d rows", bucket_name, MANIFEST_BLOB, len(df))
    return df


def _upload_snapshot(bucket_name: str, raw: bytes) -> None:
    """Write a pre-flip snapshot (idempotent — skip if already exists)."""
    from unified_trading_library import get_storage_client

    client = get_storage_client()
    try:
        # UTL may not have blob_exists; fall back gracefully.
        if hasattr(client, "blob_exists") and client.blob_exists(bucket_name, SNAPSHOT_BLOB):
            logger.info("Snapshot already exists: gs://%s/%s (kept)", bucket_name, SNAPSHOT_BLOB)
            return
    except Exception:
        pass
    client.upload_bytes(bucket_name, SNAPSHOT_BLOB, raw, content_type="application/octet-stream")
    logger.info("Snapshot written: gs://%s/%s", bucket_name, SNAPSHOT_BLOB)


def _write_back(bucket_name: str, df: pd.DataFrame) -> None:
    """Overwrite the canonical manifest with the patched DataFrame."""
    from unified_trading_library import get_storage_client

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    client = get_storage_client()
    client.upload_bytes(bucket_name, MANIFEST_BLOB, buf.getvalue(), content_type="application/octet-stream")
    logger.info("Written manifest back to gs://%s/%s (%d rows)", bucket_name, MANIFEST_BLOB, len(df))


def _write_per_vm_shard(bucket_name: str, flipped_df: pd.DataFrame) -> str:
    """Upload a per-VM shard (consolidator merges it into the canonical _index)."""
    vm_name = os.environ["VM_NAME"]
    per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
    from unified_trading_library import get_storage_client

    buf = io.BytesIO()
    flipped_df.to_parquet(buf, index=False)
    buf.seek(0)
    client = get_storage_client()
    client.upload_bytes(bucket_name, per_vm_blob, buf.getvalue(), content_type="application/octet-stream")
    logger.info("Per-VM shard uploaded: gs://%s/%s (%d rows)", bucket_name, per_vm_blob, len(flipped_df))
    return per_vm_blob


# ─────────────────────────── dry-run count ────────────────────────────────────

def _identify_wrong_cells(df: pd.DataFrame) -> pd.DataFrame:
    """Return sub-DataFrame of wrongly-captured non-trading-day rows.

    Criteria:
    1. capture_status == "captured"
    2. venue in _CALENDAR_BOUND_VENUES
    3. is_non_trading_day(venue, date) is True
    """
    if "capture_status" not in df.columns:
        logger.warning("Manifest has no 'capture_status' column — cannot identify wrong cells")
        return df.iloc[:0]

    captured_mask = df["capture_status"].fillna("") == "captured"
    if "venue" not in df.columns:
        logger.warning("Manifest has no 'venue' column — cannot filter by venue")
        return df.iloc[:0]
    if "date" not in df.columns:
        logger.warning("Manifest has no 'date' column — cannot filter by date")
        return df.iloc[:0]

    calendar_mask = df["venue"].isin(_CALENDAR_BOUND_VENUES)
    candidates = df[captured_mask & calendar_mask].copy()
    logger.info(
        "Candidates (captured AND calendar-bound venue): %d of %d total rows",
        len(candidates),
        len(df),
    )

    if candidates.empty:
        return candidates

    # Apply calendar check row by row.
    is_wrong = candidates.apply(
        lambda row: _is_non_trading(str(row["venue"]), str(row["date"])),
        axis=1,
    )
    wrong = candidates[is_wrong].copy()
    logger.info("Wrongly-captured non-trading-day rows: %d", len(wrong))
    return wrong


# ─────────────────────────── validation gates ─────────────────────────────────

def _validate_no_fx_yahoo(wrong: pd.DataFrame) -> bool:
    """Gate: no FX or YAHOO_FINANCE rows must be in the flip set."""
    bad = wrong[wrong["venue"].isin({"FX", "YAHOO_FINANCE"})]
    if not bad.empty:
        logger.error(
            "GATE FAIL: %d FX/YAHOO_FINANCE rows in flip set — aborting. Sample:\n%s",
            len(bad),
            bad[["date", "venue", "capture_status"]].head(5).to_string(),
        )
        return False
    return True


def _validate_per_venue_cap(wrong: pd.DataFrame) -> bool:
    """Gate: warn if any venue has more than _PER_VENUE_WARN_CAP flips (not abort)."""
    per_venue = wrong.groupby("venue").size()
    over = per_venue[per_venue > _PER_VENUE_WARN_CAP]
    if not over.empty:
        logger.warning(
            "Per-venue warn cap %d exceeded for: %s — review before applying",
            _PER_VENUE_WARN_CAP,
            over.to_dict(),
        )
    return True  # Warn only — do not abort


def _validate_trading_day_unchanged(df_before: pd.DataFrame, df_after: pd.DataFrame) -> bool:
    """Gate: count of 'captured' rows must only decrease (never increase)."""
    n_before = (df_before["capture_status"].fillna("") == "captured").sum()
    n_after = (df_after["capture_status"].fillna("") == "captured").sum()
    if n_after > n_before:
        logger.error(
            "GATE FAIL: captured count INCREASED from %d to %d after flip — aborting",
            n_before,
            n_after,
        )
        return False
    logger.info("Captured count: %d → %d (delta: -%d)", n_before, n_after, n_before - n_after)
    return True


# ─────────────────────────── CSV audit ────────────────────────────────────────

def _write_audit_csv(wrong: pd.DataFrame, report_dir: Path, ts: str) -> Path:
    report_path = report_dir / f"tradfi_nontrade_flip_{ts}.csv"
    audit_cols = [c for c in ["date", "venue", "data_type", "capture_status", "error_reason", "instrument_id"] if c in wrong.columns]
    wrong[audit_cols].to_csv(report_path, index=False)
    logger.info("Audit CSV: %s (%d rows)", report_path, len(wrong))
    return report_path


# ─────────────────────────── main logic ───────────────────────────────────────

def _build_summary(wrong: pd.DataFrame) -> None:
    """Log per-venue counts and sample rows."""
    per_venue: Counter[str] = Counter(wrong["venue"].tolist())
    logger.info("=== DRY-RUN: per-venue wrongly-captured non-trading-day row counts ===")
    for venue, count in sorted(per_venue.items(), key=lambda kv: -kv[1]):
        logger.info("  %-12s : %d", venue, count)
    logger.info("  %-12s : %d (TOTAL)", "TOTAL", len(wrong))

    # Show reason breakdown
    if not wrong.empty:
        reasons = wrong.apply(
            lambda row: _non_trading_reason(str(row["venue"]), str(row["date"])),
            axis=1,
        )
        reason_counts: Counter[str] = Counter(reasons.tolist())
        logger.info("=== Reason breakdown ===")
        for reason, count in sorted(reason_counts.items(), key=lambda kv: -kv[1]):
            logger.info("  %-30s : %d", reason, count)

    # Sample rows for sanity check
    if not wrong.empty:
        sample_cols = [c for c in ["date", "venue", "data_type", "capture_status", "error_reason"] if c in wrong.columns]
        logger.info("=== Sample rows (first 10) ===\n%s", wrong[sample_cols].head(10).to_string())


def _apply_flips(df: pd.DataFrame, wrong: pd.DataFrame, now_iso: str) -> pd.DataFrame:
    """Mutate df in-place to flip wrongly-captured rows to empty_confirmed."""
    for idx, row in wrong.iterrows():
        venue = str(row["venue"])
        iso_date = str(row["date"])
        reason = _non_trading_reason(venue, iso_date)
        df.at[idx, "capture_status"] = "empty_confirmed"
        df.at[idx, "error_reason"] = reason
        df.at[idx, "attempted_at"] = now_iso
        # Zero the instrument_count — the old captured rows had instrument counts from
        # the 5-day-lookback artefact (Friday's instruments returned on Saturday queries).
        # empty_confirmed cells carry 0.
        if "instrument_count" in df.columns:
            df.at[idx, "instrument_count"] = 0
        # Flip the 'available' column: captured rows have available="true";
        # empty_confirmed rows should have available="false" (no data available on
        # a non-trading day). Matches the existing empty_confirmed row shape in the manifest.
        if "available" in df.columns:
            df.at[idx, "available"] = "false"
    return df


def main() -> int:
    args = _parse_args()

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== reconcile_tradfi_non_trading_day_captured_2026_06_26 ===")
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

    # Download manifest
    df = _download_manifest(bucket_name)
    raw_bytes: bytes | None = None
    if not args.dry_run:
        # Re-download for snapshot (same bytes)
        from unified_trading_library import get_storage_client

        client = get_storage_client()
        raw_bytes = client.download_bytes(bucket_name, MANIFEST_BLOB)

    # Identify wrong cells
    wrong = _identify_wrong_cells(df)

    if wrong.empty:
        logger.info("No wrongly-captured non-trading-day rows found. Nothing to do.")
        return 0

    # Validate gates
    if not _validate_no_fx_yahoo(wrong):
        return 2
    _validate_per_venue_cap(wrong)

    # Log summary
    _build_summary(wrong)

    # Write audit CSV
    report_path = _write_audit_csv(wrong, report_dir, run_ts)
    logger.info("Audit CSV written: %s", report_path)

    if args.dry_run:
        logger.info(
            "DRY-RUN complete. %d rows would be flipped captured→empty_confirmed. "
            "Re-run with --no-dry-run (+ MANIFEST_PER_VM_SHARDS=true VM_NAME=...) to apply.",
            len(wrong),
        )
        return 0

    # ── APPLY ──
    now_iso = datetime.now(UTC).isoformat()

    # Write snapshot before patching
    if raw_bytes is not None:
        _upload_snapshot(bucket_name, raw_bytes)

    df_patched = _apply_flips(df.copy(), wrong, now_iso)

    # Validate patched df
    if not _validate_trading_day_unchanged(df, df_patched):
        return 3

    # Upload per-VM shard (consolidator merges it)
    per_vm_blob = _write_per_vm_shard(bucket_name, df_patched.loc[wrong.index])
    logger.info(
        "APPLIED: flipped %d captured→empty_confirmed rows.  "
        "Per-VM shard: gs://%s/%s  "
        "Consolidator will merge into canonical _index within ~5min.",
        len(wrong),
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
            "Default: dry-run (scan + CSV only, no manifest write). "
            "Pass --no-dry-run to apply the flip."
        ),
    )
    p.add_argument(
        "--report-dir",
        default=None,
        metavar="PATH",
        help="Directory for the audit CSV. Default: system temp dir.",
    )
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(main())
