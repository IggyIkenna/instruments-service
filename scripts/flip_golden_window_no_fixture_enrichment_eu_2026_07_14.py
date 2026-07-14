# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after Todo 9 (sports_p2_history_apifootball_2015_to_present_2026_06_27.md) GW gate audit archives
"""Flip residual golden-window per-fixture-enrichment expected_unattempted rows to
empty_confirmed, for (date, league_id) pairs where the underlying FIXTURES row
already confirms zero fixtures.

Context: the 2026-07-14 golden-window (2025-09-01..2025-11-30) `af-backfill-*`
SPOT fleet (5 entity-sharded VMs) closed the per-fixture presence gap for
FIXTURE_EVENTS/FIXTURE_LINEUPS/FIXTURE_STATS/PLAYER_STATS/INJURIES down to
near-zero. The GW gate's own residual query still showed 166 `expected_unattempted`
rows across the 5 entities (35/35/33/33/30) after the fleet finished. Root cause:
every one of these EU rows' (date, league_id) key maps to a FIXTURES row that is
itself `empty_confirmed` (zero fixtures that day for that league — mostly
A_LEAGUE/AUSTRALIA_CUP, whose season starts in October, so September/early-October
dates in the window genuinely have no matches). Since there is no fixture to
enrich, the per-fixture presence-based fetch never touches these date-level EU
placeholder rows — they predate the fixture-level sharding and were never
reconciled once FIXTURES confirmed emptiness. Verified: 0/166 EU rows have a
`captured` FIXTURES counterpart (would indicate a genuine fetch gap, not a flip
candidate).

Action: write a per-VM shard flipping these 166 rows from expected_unattempted ->
empty_confirmed (mirrors the FIXTURES row's own state). Consolidator merges on
next cycle.

Usage:
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod \\
      .venv/bin/python scripts/flip_golden_window_no_fixture_enrichment_eu_2026_07_14.py \\
      [--dry-run]
"""

from __future__ import annotations

import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_data import (
    get_expected_leagues_for_source,
)
from unified_trading_library import get_bucket_name, get_storage_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_WINDOW_START = "2025-09-01"
_WINDOW_END = "2025-11-30"
_ENRICHMENT_TYPES = ["FIXTURE_EVENTS", "FIXTURE_LINEUPS", "FIXTURE_STATS", "PLAYER_STATS", "INJURIES"]
_INDEX_PATH = "_index/availability_index.parquet"


def main(dry_run: bool, project_id: str) -> None:
    bucket = get_bucket_name("instruments", "SPORTS")
    storage = get_storage_client(project_id=project_id)

    raw = storage.download_bytes(bucket, _INDEX_PATH)
    canonical = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded canonical index: %d rows (bucket=%s)", len(canonical), bucket)

    canonical_leagues = {ld.league_id for ld in get_expected_leagues_for_source("api_football")}

    date_ok = canonical["date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    window_mask = date_ok & (canonical["date"] >= _WINDOW_START) & (canonical["date"] <= _WINDOW_END)
    window = canonical.loc[window_mask]
    window = window.loc[window["league_id"].isin(canonical_leagues)]

    fx = window[window["data_type"] == "FIXTURES"]
    fx_captured_keys = set(
        zip(
            fx.loc[fx["capture_status"] == "captured", "date"],
            fx.loc[fx["capture_status"] == "captured", "league_id"],
            strict=False,
        )
    )

    eu_mask = (
        window_mask
        & canonical["league_id"].isin(canonical_leagues)
        & canonical["data_type"].isin(_ENRICHMENT_TYPES)
        & (canonical["capture_status"] == "expected_unattempted")
    )
    eu_rows = canonical.loc[eu_mask]
    keys = list(zip(eu_rows["date"], eu_rows["league_id"], strict=False))
    no_fixture_mask = pd.Series([k not in fx_captured_keys for k in keys], index=eu_rows.index)
    flip_mask_index = eu_rows.index[no_fixture_mask]

    n_flip = len(flip_mask_index)
    n_genuine_gap = len(eu_rows) - n_flip
    logger.info(
        "EU rows in window for %s: %d total, %d flip candidates (no-fixture days), %d genuine gaps (skipped)",
        _ENRICHMENT_TYPES,
        len(eu_rows),
        n_flip,
        n_genuine_gap,
    )
    if n_genuine_gap:
        logger.warning(
            "%d EU rows have a captured-FIXTURES counterpart — NOT flipping these, they are real gaps",
            n_genuine_gap,
        )

    if n_flip == 0:
        logger.info("Nothing to flip")
        return

    if dry_run:
        logger.info("[dry-run] Would write per-VM shard with %d flipped rows", n_flip)
        matched = canonical.loc[flip_mask_index][["date", "league_id", "data_type", "capture_status"]].head(20)
        logger.info("Sample matched rows:\n%s", matched.to_string())
        return

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    now_iso = datetime.now(UTC).isoformat()

    flipped = canonical.loc[flip_mask_index].copy()
    flipped["capture_status"] = "empty_confirmed"
    flipped["error_reason"] = (
        f"flipped_golden_window_no_fixture_enrichment_eu_{run_ts}__fixtures_row_confirms_zero_fixtures_for_date_league"
    )
    flipped["attempted_at"] = now_iso
    if "written_at" in flipped.columns:
        flipped["written_at"] = now_iso

    shard_blob = f"_index/per_vm/sports-flip-gw-no-fixture-enrichment-eu-{run_ts}.parquet"
    out = io.BytesIO()
    flipped.to_parquet(out, index=False, engine="pyarrow")
    out.seek(0)
    storage.upload_bytes(bucket, shard_blob, out.read())
    logger.info("Wrote per-VM shard: gs://%s/%s (%d rows)", bucket, shard_blob, n_flip)
    logger.info("Consolidator will merge on next cycle — flipped rows replace EU rows in canonical")


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    project_id = os.environ.get("GCP_PROJECT_ID", "central-element-323112")
    main(args.dry_run, project_id)
