#!/usr/bin/env python3
# Epic: sports_manifest_canonicalisation
# Lifecycle: oneoff
# Delete-when: after L6 gate GREEN on IS surface confirmed
"""patch_l6_legacy_manifest_is_2026_06_29.py

Copy L6-legacy-only manifest rows from the legacy IS sports bucket into the
canonical IS sports manifest so the L6 data-loss gate clears.

Background
----------
The audit (cf_manifest_audit_2026_06_01.py) found ~5,920 cells
(date, venue, data_type) in the legacy IS manifest with capture_status=captured
that do NOT exist in the canonical IS manifest.  The GCS data objects for ALL
these data_types (XG, FIXTURE_STATS, FIXTURES, ODDS, etc.) are confirmed to
exist in the canonical bucket already — this is purely a manifest gap.

Operator mandate
----------------
BLK-800ef029 Option B: "Resolve BLK-6b1bed9c first (migrate 5,793
ODDS_API/ODDS cells from legacy MTDS + 3,357 IS cells to canonical bucket),
then schedule E3 drain."

Source: Plan sports_manifest_canonicalisation_2026_06_01.md Phase E8/BLK.

Source derivation
-----------------
Priority:
  1. pipeline_mode prefix (e.g. batch_understat → 'understat', batch_footystats → 'footystats')
  2. data_type fallback mapping (for rows with blank pipeline_mode)

Safety gates
------------
1. MANIFEST_PER_VM_SHARDS=true + VM_NAME=<unique> required for --apply
2. captured count must increase by exactly len(new_rows)
3. Dry-run by default

Usage::

    cd instruments-service
    .venv/bin/python scripts/patch_l6_legacy_manifest_is_2026_06_29.py

    MANIFEST_PER_VM_SHARDS=true VM_NAME=l6-is-patch-$(date +%s) \\
    .venv/bin/python scripts/patch_l6_legacy_manifest_is_2026_06_29.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys

import pandas as pd
from unified_trading_library import get_storage_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"

# Legacy bucket: non-standard name, not resolvable via resolve_bucket_name
LEGACY_BUCKET = "instruments-store-sports-central-element-323112"
CANONICAL_BUCKET = "instruments-store-sports-prd-central-element-323112"

# Source derived from pipeline_mode prefix
_PIPELINE_MODE_SOURCE: dict[str, str] = {
    "batch_understat": "understat",
    "batch_footystats": "footystats",
    "batch_api_football": "api_football",
    "batch_open_meteo": "open_meteo",
    "batch_soccer_football_info": "soccer_football_info",
}

# Fallback source by data_type (for rows with blank pipeline_mode)
_DATA_TYPE_SOURCE: dict[str, str] = {
    "ODDS": "footystats",
    "FIXTURE_STATS": "api_football",
    "XG": "understat",
    "FIXTURES": "api_football",
    "FIXTURE_EVENTS": "api_football",
    "FIXTURE_LINEUPS": "api_football",
    "PLAYER_STATS": "api_football",
    "PREDICTIONS": "footystats",
    "WEATHER": "open_meteo",
    "INJURIES": "api_football",
    "MATCHES": "footystats",
    "SFI_PROGRESSIVE_STATS": "soccer_football_info",
}


def _infer_source(pipeline_mode: str, data_type: str) -> str:
    pm = str(pipeline_mode or "")
    for prefix, src in _PIPELINE_MODE_SOURCE.items():
        if pm.startswith(prefix):
            return src
    return _DATA_TYPE_SOURCE.get(str(data_type), "")


def _cells(df: pd.DataFrame) -> set[tuple[str, str, str]]:
    captured = df[df["capture_status"] == "captured"]
    return set(
        zip(
            captured["date"].astype(str),
            captured["venue"].astype(str),
            captured["data_type"].astype(str),
            strict=False,
        )
    )


def _load(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded %d rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)
    return df


def _validate_apply_env() -> bool:
    ok = True
    if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
        logger.error("--apply requires MANIFEST_PER_VM_SHARDS=true")
        ok = False
    if not os.environ.get("VM_NAME", "").strip():
        logger.error("--apply requires VM_NAME=<unique-tag>")
        ok = False
    return ok


def _build_new_rows(
    legacy_df: pd.DataFrame,
    leg_only: set[tuple[str, str, str]],
    canonical_cols: list[str],
) -> pd.DataFrame:
    mask = legacy_df.apply(
        lambda r: (str(r["date"]), str(r["venue"]), str(r["data_type"])) in leg_only, axis=1
    )
    rows = legacy_df[mask & (legacy_df["capture_status"] == "captured")].copy()
    logger.info("Legacy rows to migrate: %d (for %d unique cells)", len(rows), len(leg_only))

    rows["source"] = rows.apply(
        lambda r: _infer_source(r.get("pipeline_mode", ""), r.get("data_type", "")), axis=1
    )

    # Add canonical-only columns missing from legacy
    for col in ("service_emission_state", "last_emission_decision_at", "expected_window_completeness_fraction"):
        if col not in rows.columns:
            rows[col] = None
    for col in ("fixture_id", "job_id", "transport", "cadence"):
        if col not in rows.columns:
            rows[col] = None

    # Align to canonical schema
    for col in canonical_cols:
        if col not in rows.columns:
            rows[col] = None
    return rows[canonical_cols].reset_index(drop=True)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", default=False)
    args = p.parse_args()
    dry_run = not args.apply

    if not dry_run and not _validate_apply_env():
        return 4

    legacy_df = _load(LEGACY_BUCKET)
    canonical_df = _load(CANONICAL_BUCKET)

    leg_cells = _cells(legacy_df)
    can_cells = _cells(canonical_df)
    leg_only = leg_cells - can_cells

    logger.info(
        "Cells — legacy captured: %d  canonical captured: %d  legacy-only: %d",
        len(leg_cells),
        len(can_cells),
        len(leg_only),
    )

    if not leg_only:
        logger.info("No legacy-only cells. L6 gate already clean.")
        return 0

    new_rows = _build_new_rows(legacy_df, leg_only, list(canonical_df.columns))
    logger.info("New rows to append: %d", len(new_rows))
    logger.info("By data_type:\n%s", new_rows["data_type"].value_counts().to_string())
    logger.info("By source:\n%s", new_rows["source"].value_counts().to_string())

    if dry_run:
        logger.info(
            "DRY-RUN: %d rows would be appended (%d cells). "
            "Re-run with --apply (+ MANIFEST_PER_VM_SHARDS=true VM_NAME=...) to mutate.",
            len(new_rows),
            len(leg_only),
        )
        return 0

    captured_before = int((canonical_df["capture_status"] == "captured").sum())
    combined = pd.concat([canonical_df, new_rows], ignore_index=True)
    captured_after = int((combined["capture_status"] == "captured").sum())
    expected_after = captured_before + len(new_rows)

    if captured_after != expected_after:
        logger.error(
            "SAFETY GATE FAILED: captured count %d → %d (expected %d)",
            captured_before,
            captured_after,
            expected_after,
        )
        return 5

    out = io.BytesIO()
    combined.to_parquet(out, index=False)
    out.seek(0)

    client = get_storage_client(provider="gcp")
    client.upload_from_file_obj(CANONICAL_BUCKET, INDEX_BLOB, out)
    logger.info(
        "DONE: uploaded %d rows to gs://%s/%s "
        "(appended %d rows, captured %d → %d, legacy-only cells resolved: %d)",
        len(combined),
        CANONICAL_BUCKET,
        INDEX_BLOB,
        len(new_rows),
        captured_before,
        captured_after,
        len(leg_only),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
