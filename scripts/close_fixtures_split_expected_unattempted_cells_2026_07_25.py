# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: the sports_post_backfill_relabel_premise_resolved_residual_gap_2026_07_25
#   issue doc is archived and this sweep has been run at least once against production
"""Close the provably-closeable subset of FIXTURES_OUTCOMES/FIXTURES_SCHEDULE
``expected_unattempted`` cells in the 2026-02-20..2026-06-19 window.

Plan: ``unified-trading-pm/plans/active/issues/
sports_post_backfill_relabel_premise_resolved_residual_gap_2026_07_25.md`` (todo 2).

Root cause (diagnosed by slot 11, same issue doc, todo 1): the
FIXTURES_OUTCOMES/FIXTURES_SCHEDULE writer runs on every date in range (proven —
real ``captured`` rows scattered across the WHOLE window, not clustered
post-cutover) but simply never writes anything (not even an empty-confirmation)
on a day with zero real fixtures for that league. The legacy ``FIXTURES`` entity
correctly marks those same days ``empty_confirmed``/``instrument_count=0``.

Why this is safe (not a blind relabel): a cell is closed ONLY when the legacy
``FIXTURES`` row for the IDENTICAL ``(date, league_id)`` already independently
proves — via its own ``empty_confirmed`` state — that no fixture existed that
day. This mirrors ``close_stale_enrichment_expected_unattempted_cells_2026_07_19.py``'s
provable-closure discipline (never guess; only reuse an already-proven fact from
the corpus). ``SOURCE_RETURNED_ZERO`` is EXCLUDED from the mirror for the same
reason that script excludes it: the manifest writer's Phase-1 KEYSTONE
honest-absence gate requires real ``FetchEvidence`` for that specific reason,
which this classification-only closer never has (mirroring it would hard-crash
with ``UnprovenHonestAbsenceError``). Every other ``EXPECTED_*`` calendar/
coverage reason on FIXTURES is exempt (no fetch was attempted, so no evidence is
required) and is safe to mirror unconditionally.

Deliberately does NOT reuse ``_close_stale_enrichment_expected_unattempted_cells``
(the 2026-07-19 enrichment closer): that function's first branch checks
``is_league_entity_covered(league, entity)``, which is conservatively ``False``
for any entity never added to ``LEAGUE_ENTITY_COVERAGE`` — including
FIXTURES_OUTCOMES/FIXTURES_SCHEDULE (the 2026-07-14+ split, not yet in that
registry). Reusing it here would route every cell through
``EXPECTED_NO_PROVIDER_COVERAGE``, which is FALSE for this diagnosis (the
writer demonstrably IS covering these leagues — it just skips empty days).

Single manifest read (single-walk discipline): one ``read_availability_index``
call, slim columns, filter-pushdown on the diagnosed date window — no
whole-corpus GCS walk.

Usage
-----
::

  # Dry-run (default) — read-only manifest scan, reports how many cells WOULD
  # close and how many are left untouched, NO manifest writes:
  python scripts/close_fixtures_split_expected_unattempted_cells_2026_07_25.py --dry-run

  # Apply — real manifest record_empty writes for the provably-closeable subset,
  # then a fresh separate read verifying the before/after delta:
  python scripts/close_fixtures_split_expected_unattempted_cells_2026_07_25.py --apply
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import PipelineMode
from unified_api_contracts.canonical.crosscutting.honest_coverage import EmptyConfirmedReason
from unified_trading_library import ManifestWriter, read_availability_index, resolve_bucket_name

from instruments_service.engine.orchestrator import _sports_ref_source

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_WINDOW_START = "2026-02-20"
_WINDOW_END = "2026-06-19"
_SPLIT_ENTITIES = ["FIXTURES_OUTCOMES", "FIXTURES_SCHEDULE"]
_SOURCE_RETURNED_ZERO = EmptyConfirmedReason.SOURCE_RETURNED_ZERO.value


def _read_manifest_slices(bucket: str) -> tuple[pd.DataFrame, dict[tuple[str, str], str]]:
    """One manifest read -> (stuck split-entity cells, FIXTURES empty-reason lookup)."""
    logger.info("Reading manifest (slim columns, window-filtered) from gs://%s/ ...", bucket)
    df: pd.DataFrame = read_availability_index(
        bucket,
        columns=["date", "data_type", "source", "capture_status", "error_reason", "league_id", "instrument_count"],
        filters=[("date", ">=", _WINDOW_START), ("date", "<=", _WINDOW_END)],
    )
    logger.info("  %d total rows read in window", len(df))
    af = df[df["source"] == "api_football"].copy()

    split = af[af["data_type"].isin(_SPLIT_ENTITIES)]
    stuck_cells = split[split["capture_status"] == "expected_unattempted"].copy()
    logger.info("  %d stuck (expected_unattempted) FIXTURES_OUTCOMES/FIXTURES_SCHEDULE cells", len(stuck_cells))

    fixtures = af[
        (af["data_type"] == "FIXTURES")
        & (af["capture_status"] == "empty_confirmed")
        & (af["instrument_count"].fillna(0) == 0)
    ]
    fixtures_empty_reason_by_date_league = {
        (str(row["date"]), str(row["league_id"])): str(row["error_reason"])
        for _, row in fixtures[["date", "league_id", "error_reason"]].dropna(subset=["date", "league_id"]).iterrows()
    }
    logger.info(
        "  %d FIXTURES empty_confirmed/instrument_count=0 (date, league) cells available as the safety cross-check",
        len(fixtures_empty_reason_by_date_league),
    )
    return stuck_cells, fixtures_empty_reason_by_date_league


def _classify(
    stuck_cells: pd.DataFrame, fixtures_empty_reason_by_date_league: dict[tuple[str, str], str]
) -> dict[str, list[tuple[str, str, str, str]]]:
    """Split stuck cells into closeable (with mirrored reason) vs untouched."""
    closeable: list[tuple[str, str, str, str]] = []  # (date, data_type, league_id, reason)
    untouched = 0
    zero_excluded = 0
    for _, row in stuck_cells.iterrows():
        key = (str(row["date"]), str(row["league_id"]))
        reason = fixtures_empty_reason_by_date_league.get(key)
        if reason is None:
            untouched += 1
            continue
        if reason == _SOURCE_RETURNED_ZERO:
            zero_excluded += 1
            continue
        closeable.append((str(row["date"]), str(row["data_type"]), str(row["league_id"]), reason))
    return {"closeable": closeable, "untouched_count": untouched, "zero_excluded_count": zero_excluded}


def _dry_run_report(classification: dict[str, list[tuple[str, str, str, str]]]) -> None:
    closeable = classification["closeable"]
    logger.info(
        "DRY-RUN: %d cell(s) would close (mirroring FIXTURES' own proven-empty reason), "
        "%d left untouched (no FIXTURES proof yet — genuine pending-fetch gap), "
        "%d excluded (FIXTURES reason is SOURCE_RETURNED_ZERO — no FetchEvidence for this closer)",
        len(closeable),
        classification["untouched_count"],
        classification["zero_excluded_count"],
    )


def _apply(bucket: str, closeable: list[tuple[str, str, str, str]]) -> int:
    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket, per_vm_shards=True)
    attempt_ts = datetime.now(UTC)
    closed = 0
    for date, data_type, league_id, reason in closeable:
        manifest.record_empty(
            row_key={"date": date, "data_type": data_type, "league_id": league_id},
            attempted_at=attempt_ts,
            reason=reason,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
            source=_sports_ref_source(data_type.lower()),
        )
        closed += 1
    manifest.flush()
    return closed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Write the closeable cells (default: dry-run report only)."
    )
    parser.add_argument("--project", default="central-element-323112", help="GCP project ID")
    args = parser.parse_args()

    os.environ.setdefault("CLOUD_PROVIDER", "gcp")
    os.environ.setdefault("GCP_PROJECT_ID", args.project)

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    logger.info("bucket=%s window=%s..%s", bucket, _WINDOW_START, _WINDOW_END)

    stuck_cells, fixtures_empty_reason_by_date_league = _read_manifest_slices(bucket)
    classification = _classify(stuck_cells, fixtures_empty_reason_by_date_league)

    if not args.apply:
        _dry_run_report(classification)
        return

    closed = _apply(bucket, classification["closeable"])
    logger.info("APPLY DONE — %d cell(s) closed. Re-reading manifest to verify by content...", closed)

    # The raw index is append-only between consolidator cycles — a just-written
    # record_empty adds a NEW row rather than overwriting the prior
    # expected_unattempted row for the same (date, data_type, league_id) key.
    # A naive un-deduped count would see BOTH rows and wrongly report delta=0.
    # Keep only the latest `written_at` per key before counting (mirrors what
    # the consolidator itself resolves to on its own cycle).
    verify_df: pd.DataFrame = read_availability_index(
        bucket,
        columns=["date", "data_type", "source", "capture_status", "league_id", "written_at"],
        filters=[("date", ">=", _WINDOW_START), ("date", "<=", _WINDOW_END)],
    )
    verify_af = verify_df[verify_df["source"] == "api_football"]
    verify_split = verify_af[verify_af["data_type"].isin(_SPLIT_ENTITIES)].copy()
    verify_split["written_at"] = pd.to_datetime(verify_split["written_at"], utc=True)
    latest = verify_split.sort_values("written_at").drop_duplicates(
        subset=["date", "data_type", "league_id"], keep="last"
    )
    remaining_stuck = len(latest[latest["capture_status"] == "expected_unattempted"])
    logger.info(
        "VERIFY (fresh separate read, deduped by latest written_at per key) — "
        "%d stuck cells before, %d remaining after (delta=%d, expected=%d)",
        len(stuck_cells),
        remaining_stuck,
        len(stuck_cells) - remaining_stuck,
        closed,
    )


if __name__ == "__main__":
    main()
