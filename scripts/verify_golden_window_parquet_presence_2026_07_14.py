# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after Todo 9 (sports_p2_history_apifootball_2015_to_present_2026_06_27.md) GW gate audit archives
"""Parquet-level content re-verify for the post-fix golden-window api-football
enrichment re-run (2025-09-01..2025-11-30).

Context: the naive manifest-level gate (capture_status counts) read GREEN on
2026-07-14 while parquet-content cross-check found 3,720 FALSE-EMPTY cells
(manifest said `empty_confirmed`, parquet data actually existed) — see
`plans/active/issues/sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14.md`.
A 3-leg write-path fix shipped (instruments-service@0d9ffabd) and the SAME
5-entity GW fleet was re-run. This script repeats the ORIGINAL parquet-presence
cross methodology (not the naive gate) to confirm the false-empty count has
actually dropped, rather than trusting the manifest alone a second time.

Scope: the 1,848 deduped captured-FIXTURES (date, league_id) GW cells (86
leagues x 91 days) that session 20 established as the GW scoping — reproduced
here by re-deriving from the canonical index's own FIXTURES captured rows
within the window, so this script has no dependency on a prior run's cached
cell list.

For each of the 4 per-fixture entities, this does ONE prefix listing per date
(91 dates x 4 entities = 364 GCS list calls, not a whole-corpus walk) to
determine which leagues have an actual parquet object, then cross-references
against the canonical manifest's per-league capture_status for that
(date, entity) to classify every GW cell.

Usage:
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod \\
      .venv/bin/python scripts/verify_golden_window_parquet_presence_2026_07_14.py
"""

from __future__ import annotations

import io
import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_data import (
    get_expected_leagues_for_source,
)
from unified_trading_library import get_bucket_name, get_storage_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_WINDOW_START = "2025-09-01"
_WINDOW_END = "2025-11-30"
_ENTITIES = ["fixture_events", "fixture_lineups", "fixture_stats", "player_stats"]
_ENTITY_DT = {
    "fixture_events": "FIXTURE_EVENTS",
    "fixture_lineups": "FIXTURE_LINEUPS",
    "fixture_stats": "FIXTURE_STATS",
    "player_stats": "PLAYER_STATS",
}
_PM = "batch_api_football"
_INDEX_PATH = "_index/availability_index.parquet"


def _date_range(start: str, end: str) -> list[str]:
    d0 = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=UTC)
    d1 = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=UTC)
    out = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def main(project_id: str) -> None:
    bucket = get_bucket_name("instruments", "SPORTS")
    storage = get_storage_client(project_id=project_id)

    raw = storage.download_bytes(bucket, _INDEX_PATH)
    canonical = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded canonical index: %d rows (bucket=%s)", len(canonical), bucket)

    canonical_leagues = {ld.league_id for ld in get_expected_leagues_for_source("api_football")}

    date_ok = canonical["date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    window_mask = date_ok & (canonical["date"] >= _WINDOW_START) & (canonical["date"] <= _WINDOW_END)
    window = canonical.loc[window_mask & canonical["league_id"].isin(canonical_leagues)]

    # Re-derive the GW cell set: (date, league_id) pairs with a captured FIXTURES row.
    fx = window[window["data_type"] == "FIXTURES"]
    fx_captured = fx.loc[fx["capture_status"] == "captured", ["date", "league_id"]].drop_duplicates()
    gw_cells: set[tuple[str, str]] = set(zip(fx_captured["date"], fx_captured["league_id"], strict=False))
    logger.info("GW cells (captured-FIXTURES date,league pairs): %d", len(gw_cells))

    dates = _date_range(_WINDOW_START, _WINDOW_END)

    results: dict[str, dict[str, int]] = {}
    false_empty_samples: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for entity_name in _ENTITIES:
        data_type = _ENTITY_DT[entity_name]
        ent_window = window[window["data_type"] == data_type]
        # Dedup manifest at (date, league_id) grain with precedence captured >
        # empty_confirmed > attempted_failed > expected_unattempted (matches
        # the original session-31 verification's dedup rule).
        precedence = {"captured": 3, "empty_confirmed": 2, "attempted_failed": 1, "expected_unattempted": 0}
        ent_window = ent_window.copy()
        ent_window["_prec"] = ent_window["capture_status"].map(precedence).fillna(-1)
        ent_window = ent_window.sort_values("_prec").drop_duplicates(subset=["date", "league_id"], keep="last")
        manifest_status: dict[tuple[str, str], str] = dict(
            zip(
                zip(ent_window["date"], ent_window["league_id"], strict=False),
                ent_window["capture_status"],
                strict=False,
            )
        )

        parquet_present: set[tuple[str, str]] = set()
        for date in dates:
            prefix = f"sports_reference/by_date/day={date}/pipeline_mode={_PM}/entity={entity_name}/"
            blobs = storage.list_blobs(bucket=bucket, prefix=prefix, max_results=None)
            for b in blobs:
                if not b.name.endswith(f"{entity_name}.parquet"):
                    continue
                # path shape: .../entity={entity}/league={L}/{entity}.parquet
                parts = b.name.split("/league=")
                if len(parts) != 2:
                    continue
                league = parts[1].split("/")[0]
                parquet_present.add((date, league))

        manifest_captured = 0
        false_empty = 0
        parquet_absent_and_empty = 0
        attempted_failed = 0
        pending_fetch = 0
        phantom_captured = 0
        for cell in gw_cells:
            status = manifest_status.get(cell, "expected_unattempted")
            present = cell in parquet_present
            if status == "captured":
                manifest_captured += 1
                if not present:
                    phantom_captured += 1
            elif status == "empty_confirmed":
                if present:
                    false_empty += 1
                    if len(false_empty_samples[entity_name]) < 10:
                        false_empty_samples[entity_name].append(cell)
                else:
                    parquet_absent_and_empty += 1
            elif status == "attempted_failed":
                attempted_failed += 1
            else:
                pending_fetch += 1

        results[entity_name] = {
            "gw_cells": len(gw_cells),
            "manifest_captured": manifest_captured,
            "parquet_present_total": len(parquet_present & gw_cells),
            "false_empty": false_empty,
            "parquet_absent_and_empty": parquet_absent_and_empty,
            "attempted_failed": attempted_failed,
            "pending_fetch_eu": pending_fetch,
            "phantom_captured": phantom_captured,
        }
        logger.info("=== %s (%s) ===", entity_name, data_type)
        for k, v in results[entity_name].items():
            logger.info("  %s: %s", k, v)

    total_false_empty = sum(r["false_empty"] for r in results.values())
    total_pending = sum(r["pending_fetch_eu"] for r in results.values())
    total_phantom = sum(r["phantom_captured"] for r in results.values())
    logger.info("=== TOTALS across 4 per-fixture entities ===")
    logger.info(
        "false_empty=%d pending_fetch_eu=%d phantom_captured=%d", total_false_empty, total_pending, total_phantom
    )
    for entity_name, samples in false_empty_samples.items():
        if samples:
            logger.warning("Sample residual false-empty cells for %s: %s", entity_name, samples)

    # INJURIES: check the 30 A_LEAGUE September cells that were blank-reason.
    inj_window = window[window["data_type"] == "INJURIES"]
    inj_a_league_sep = inj_window[
        (inj_window["league_id"] == "A_LEAGUE")
        & (inj_window["date"] >= "2025-09-01")
        & (inj_window["date"] <= "2025-09-30")
    ]
    status_counts = inj_a_league_sep["capture_status"].value_counts().to_dict()
    reason_counts = inj_a_league_sep.get("error_reason", pd.Series(dtype=object)).value_counts().to_dict()
    logger.info("=== INJURIES A_LEAGUE September cells (was: 30 blank-reason expected_unattempted) ===")
    logger.info("capture_status counts: %s", status_counts)
    logger.info("error_reason counts: %s", reason_counts)
    blank_reason = inj_a_league_sep[
        (inj_a_league_sep["capture_status"] == "expected_unattempted")
        & (inj_a_league_sep.get("error_reason", "").fillna("") == "")  # noqa: qg-empty-fallback -- "" IS the blank-reason signal being tested for, not a swallowed-error placeholder
    ]
    logger.info("Still-blank-reason count: %d", len(blank_reason))


if __name__ == "__main__":
    import os

    project_id = os.environ.get("GCP_PROJECT_ID", "central-element-323112")
    main(project_id)
