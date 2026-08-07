# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the sports_manifest_2026_h1_vs_2025_h1_enumeration_grain_persists
#   issue's P3 league_id investigation todo is checked off — this is a point-in-time
#   read-only classification, not a recurring job.
"""Investigate whether the FIXTURES/FIXTURES_OUTCOMES/ODDS-specific distinct-``league_id``
growth (88->924, 88->926, 51->384 respectively) is genuine coverage expansion or a
duplicate/near-duplicate league_id seeding artifact.

READ-ONLY. Reads the live ``_index/availability_index.parquet`` manifest for
``instruments-store-sports-prd-*`` ONCE, projects only 5 columns needed
(``date``, ``data_type``, ``capture_status``, ``league_id``, ``source``), and
performs three classification analyses:

1. **Set comparison** — distinct league_ids in 2025 H1 vs 2026 H1 per data_type,
   with intersection/new/retired counts.
2. **Cross-data_type overlap** — do new league_ids in FIXTURES/FIXTURES_OUTCOMES/ODDS
   also appear in WEATHER/MATCHES? If a league is genuinely new, it should appear
   across multiple data_types. Target-only appearance is an artifact signal.
3. **Near-duplicate detection** — within each target data_type's new league_ids, look
   for pairs differing only by case/whitespace/punctuation or a small edit distance.
4. **Capture-status breakdown** — are new leagues actually capturing data or just
   ``expected_unattempted`` seeds?

Plan: ``unified-trading-pm/plans/active/issues/
sports_manifest_2026_h1_vs_2025_h1_enumeration_grain_persists_2026_07_27.md``
(todo "[DATA] P3. Investigate the FIXTURES/FIXTURES_OUTCOMES/ODDS-specific
distinct-league_id growth").

Usage
-----
::

  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    .venv/bin/python scripts/sports_league_id_growth_investigation_2026_08_05.py \\
    --report /tmp/sports_league_id_growth_report.json
"""

from __future__ import annotations

import argparse
import io
import json
import logging
from collections import defaultdict
from datetime import UTC, datetime
from difflib import SequenceMatcher
from typing import Any

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
WINDOW_2025 = ("2025-01-01", "2025-06-30")
WINDOW_2026 = ("2026-01-01", "2026-06-30")

TARGET_DATA_TYPES = {"FIXTURES", "FIXTURES_OUTCOMES", "ODDS"}
CONTROL_DATA_TYPES = {"WEATHER", "MATCHES"}
ALL_INVESTIGATED = TARGET_DATA_TYPES | CONTROL_DATA_TYPES

# Near-duplicate threshold: ratio >= this is a candidate duplicate pair
DUPLICATE_SIMILARITY_THRESHOLD = 0.85


def _instruments_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _load_manifest_slim(client, bucket: str) -> pd.DataFrame:
    data = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(
        io.BytesIO(data),
        columns=["date", "data_type", "capture_status", "league_id", "source"],
    )
    df["date"] = df["date"].astype(str)
    return df


def _window_mask(df: pd.DataFrame, start: str, end: str) -> pd.Series:
    return (df["date"] >= start) & (df["date"] <= end)


def _distinct_leagues(df: pd.DataFrame, start: str, end: str, data_type: str) -> set[str]:
    """Return distinct league_id values for a data_type within a date window."""
    mask = _window_mask(df, start, end) & (df["data_type"] == data_type)
    return set(df.loc[mask, "league_id"].dropna().unique())


def _normalize_for_dedup(s: str) -> str:
    """Normalize a league_id string for near-duplicate comparison."""
    return s.strip().lower()


def _find_near_duplicates(league_ids: set[str]) -> list[dict[str, Any]]:
    """Find near-duplicate pairs among a set of league_id strings.

    Returns a list of candidate duplicate pairs with similarity ratio.
    """
    candidates: list[dict[str, Any]] = []
    ids = sorted(league_ids)

    # First pass: case-insensitive duplicates (exact match after normalization)
    norm_map: dict[str, list[str]] = defaultdict(list)
    for lid in ids:
        norm_map[_normalize_for_dedup(lid)].append(lid)
    case_dupes = {k: v for k, v in norm_map.items() if len(v) > 1}
    for norm, variants in case_dupes.items():
        candidates.append(
            {
                "type": "case_insensitive_duplicate",
                "normalized": norm,
                "variants": sorted(variants),
                "similarity": 1.0,
            }
        )

    # Second pass: high-similarity pairs (SequenceMatcher)
    # Only compare pairs not already caught by case-insensitive check
    already_flagged: set[str] = set()
    for variants in case_dupes.values():
        already_flagged.update(v.lower() for v in variants)

    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            a, b = ids[i], ids[j]
            na, nb = a.lower(), b.lower()
            if na == nb:
                continue  # already caught above
            if na in already_flagged or nb in already_flagged:
                continue
            ratio = SequenceMatcher(None, na, nb).ratio()
            if ratio >= DUPLICATE_SIMILARITY_THRESHOLD:
                candidates.append(
                    {
                        "type": "high_similarity",
                        "league_a": a,
                        "league_b": b,
                        "similarity": round(ratio, 4),
                    }
                )

    return candidates


def _capture_status_breakdown(
    df: pd.DataFrame, start: str, end: str, data_type: str, league_ids: set[str]
) -> dict[str, dict[str, int]]:
    """Return {league_id: {capture_status: count}} for a set of leagues."""
    mask = _window_mask(df, start, end) & (df["data_type"] == data_type) & (df["league_id"].isin(league_ids))
    subset = df.loc[mask]
    out: dict[str, dict[str, int]] = {}
    for lid, grp in subset.groupby("league_id"):
        out[str(lid)] = grp["capture_status"].value_counts().to_dict()
    return out


def _source_breakdown(
    df: pd.DataFrame, start: str, end: str, data_type: str, league_ids: set[str]
) -> dict[str, dict[str, int]]:
    """Return {league_id: {source: count}} for a set of leagues."""
    mask = _window_mask(df, start, end) & (df["data_type"] == data_type) & (df["league_id"].isin(league_ids))
    subset = df.loc[mask]
    out: dict[str, dict[str, int]] = {}
    for lid, grp in subset.groupby("league_id"):
        out[str(lid)] = grp["source"].value_counts().to_dict()
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=str, default="", help="path to write the JSON report")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = get_storage_client(provider="gcp")
    bucket = _instruments_bucket()

    logger.info("loading manifest (5-column projection) from gs://%s/%s", bucket, INDEX_BLOB)
    df = _load_manifest_slim(client, bucket)
    logger.info("loaded: %d total rows, %.1f MB", len(df), df.memory_usage(deep=True).sum() / 1024 / 1024)

    # ── 1. Set comparison per data_type ──────────────────────────────────
    set_analysis: dict[str, dict[str, Any]] = {}
    for dt in sorted(ALL_INVESTIGATED):
        l25 = _distinct_leagues(df, *WINDOW_2025, dt)
        l26 = _distinct_leagues(df, *WINDOW_2026, dt)
        intersection = l25 & l26
        new_in_2026 = l26 - l25
        retired = l25 - l26

        set_analysis[dt] = {
            "distinct_leagues_2025_h1": len(l25),
            "distinct_leagues_2026_h1": len(l26),
            "intersection_count": len(intersection),
            "new_in_2026_count": len(new_in_2026),
            "retired_count": len(retired),
            "growth_ratio": round(len(l26) / len(l25), 2) if len(l25) > 0 else None,
            "new_in_2026_ids": sorted(new_in_2026),
            "retired_ids": sorted(retired),
            "persisting_ids": sorted(intersection),
        }
        logger.info(
            "%s: %d (2025) -> %d (2026), intersection=%d, new=%d, retired=%d, ratio=%.2f",
            dt,
            len(l25),
            len(l26),
            len(intersection),
            len(new_in_2026),
            len(retired),
            set_analysis[dt]["growth_ratio"] or 0.0,
        )

    # ── 2. Cross-data_type overlap ───────────────────────────────────────
    # For each target data_type's new league_ids, check presence in other data_types
    cross_overlap: dict[str, dict[str, Any]] = {}
    for dt in sorted(TARGET_DATA_TYPES):
        new_ids = set(set_analysis[dt]["new_in_2026_ids"])

        # In 2026 H1, do these new leagues appear in the OTHER investigated data_types?
        presence_in_2026: dict[str, list[str]] = {}
        for other_dt in sorted(ALL_INVESTIGATED - {dt}):
            other_leagues_2026 = _distinct_leagues(df, *WINDOW_2026, other_dt)
            overlap = sorted(new_ids & other_leagues_2026)
            presence_in_2026[other_dt] = overlap if overlap else []  # empty list if none

        # In 2025 H1, did these leagues appear in ANY data_type at all?
        # (Were they truly new to the entire catalogue, or just new to this data_type?)
        all_2025_leagues: set[str] = set()
        for other_dt_2025 in sorted(ALL_INVESTIGATED):
            all_2025_leagues |= _distinct_leagues(df, *WINDOW_2025, other_dt_2025)

        appeared_in_2025_elsewhere = sorted(new_ids & all_2025_leagues)
        entirely_new_to_catalogue = sorted(new_ids - all_2025_leagues)

        cross_overlap[dt] = {
            "new_league_count": len(new_ids),
            "present_in_other_data_types_2026": {
                k: {"count": len(v), "league_ids": v[:50]}  # cap at 50 for report size
                for k, v in presence_in_2026.items()
            },
            "appeared_in_any_data_type_2025": {
                "count": len(appeared_in_2025_elsewhere),
                "league_ids": appeared_in_2025_elsewhere[:50],
            },
            "entirely_new_to_catalogue": {
                "count": len(entirely_new_to_catalogue),
                "league_ids": entirely_new_to_catalogue[:50],
            },
        }

        # Summary: % of new leagues that appear in at least one control data_type
        control_2026_leagues: set[str] = set()
        for ctrl_dt in CONTROL_DATA_TYPES:
            control_2026_leagues |= _distinct_leagues(df, *WINDOW_2026, ctrl_dt)
        overlap_with_controls = new_ids & control_2026_leagues
        cross_overlap[dt]["overlap_with_control_data_types_2026"] = {
            "count": len(overlap_with_controls),
            "pct_of_new": round(100 * len(overlap_with_controls) / len(new_ids), 1) if new_ids else 0,
            "league_ids": sorted(overlap_with_controls)[:50],
        }

    # ── 3. Near-duplicate detection ──────────────────────────────────────
    near_dupes: dict[str, list[dict[str, Any]]] = {}
    for dt in sorted(TARGET_DATA_TYPES):
        new_ids = set(set_analysis[dt]["new_in_2026_ids"])
        candidates = _find_near_duplicates(new_ids)
        near_dupes[dt] = candidates
        logger.info("%s: %d near-duplicate candidates among %d new leagues", dt, len(candidates), len(new_ids))

    # ── 4. Capture-status breakdown of new leagues ───────────────────────
    status_breakdowns: dict[str, dict[str, Any]] = {}
    for dt in sorted(TARGET_DATA_TYPES):
        new_ids = set(set_analysis[dt]["new_in_2026_ids"])
        per_league_status = _capture_status_breakdown(df, *WINDOW_2026, dt, new_ids)
        per_league_source = _source_breakdown(df, *WINDOW_2026, dt, new_ids)

        # Aggregate: how many new leagues have at least one 'captured' row?
        leagues_with_captured = sum(1 for statuses in per_league_status.values() if "captured" in statuses)
        leagues_with_only_expected = sum(
            1 for statuses in per_league_status.values() if set(statuses.keys()) == {"expected_unattempted"}
        )
        leagues_with_only_empty = sum(
            1 for statuses in per_league_status.values() if set(statuses.keys()) == {"empty_confirmed"}
        )

        # Per-league detail (cap at 20 for report size)
        detail_sample = sorted(per_league_status.items())[:20]

        # Source distribution: count how many new leagues are attributed to each source
        source_counts: dict[str, int] = defaultdict(int)
        for _lid, sources in per_league_source.items():
            for src in sources:
                source_counts[src] += 1
        source_distribution = dict(sorted(source_counts.items(), key=lambda x: -x[1]))

        status_breakdowns[dt] = {
            "new_league_count": len(new_ids),
            "leagues_with_captured_data": leagues_with_captured,
            "leagues_with_only_expected_unattempted": leagues_with_only_expected,
            "leagues_with_only_empty_confirmed": leagues_with_only_empty,
            "leagues_with_mixed_statuses": len(new_ids) - leagues_with_only_expected - leagues_with_only_empty,
            "source_distribution": source_distribution,
            "sample_league_details": detail_sample,
        }

    # ── 5. Classification ────────────────────────────────────────────────
    classification: dict[str, dict[str, Any]] = {}
    for dt in sorted(TARGET_DATA_TYPES):
        cd = cross_overlap[dt]
        nd = near_dupes[dt]
        sb = status_breakdowns[dt]

        signals_genuine: list[str] = []
        signals_artifact: list[str] = []

        # Signal: overlap with control data_types suggests genuine expansion
        ctrl_overlap_pct = cd["overlap_with_control_data_types_2026"]["pct_of_new"]
        if ctrl_overlap_pct >= 50:
            signals_genuine.append(
                f"{ctrl_overlap_pct:.0f}% of new leagues also in WEATHER/MATCHES (cross-data_type consistent)"
            )
        elif ctrl_overlap_pct < 20:
            signals_artifact.append(
                f"only {ctrl_overlap_pct:.0f}% of new leagues in WEATHER/MATCHES (data_type-isolated)"
            )

        # Signal: near-duplicates found
        if nd:
            case_dupes = [c for c in nd if c["type"] == "case_insensitive_duplicate"]
            near_dupes_count = len(nd) - len(case_dupes)
            if case_dupes:
                signals_artifact.append(f"{len(case_dupes)} case-insensitive duplicate pair(s): {case_dupes[:3]}")
            if near_dupes_count:
                signals_artifact.append(f"{near_dupes_count} high-similarity near-duplicate pair(s)")

        # Signal: leagues with actual captured data = genuine
        if sb["leagues_with_captured_data"] > 0:
            pct = round(100 * sb["leagues_with_captured_data"] / sb["new_league_count"], 1)
            signals_genuine.append(
                f"{sb['leagues_with_captured_data']} new leagues ({pct}%) have captured data (not just seeds)"
            )

        # Signal: all expected_unattempted only = could be artifact
        if sb["leagues_with_only_expected_unattempted"] > 0:
            pct = round(100 * sb["leagues_with_only_expected_unattempted"] / sb["new_league_count"], 1)
            if pct > 50:
                signals_artifact.append(
                    f"{sb['leagues_with_only_expected_unattempted']} new leagues ({pct}%) are "
                    f"expected_unattempted-only (never captured)"
                )

        # Signal: overlap with 2025 — leagues that existed in other dtypes in 2025
        # but are new to THIS dtype in 2026 (expected growth)
        appeared_2025 = cd["appeared_in_any_data_type_2025"]["count"]
        if appeared_2025 > 0:
            pct = round(100 * appeared_2025 / sb["new_league_count"], 1)
            signals_genuine.append(
                f"{appeared_2025} new leagues ({pct}%) existed in other sports data_types in 2025 "
                f"(league known, just new to this data_type)"
            )

        # Verdict
        if signals_artifact and not signals_genuine:
            verdict = "ARTIFACT"
            confidence = "high" if len(signals_artifact) >= 2 else "medium"
        elif signals_genuine and not signals_artifact:
            verdict = "GENUINE_EXPANSION"
            confidence = "high" if len(signals_genuine) >= 2 else "medium"
        elif signals_genuine and signals_artifact:
            verdict = "MIXED"
            confidence = "medium"
        else:
            verdict = "INCONCLUSIVE"
            confidence = "low"

        classification[dt] = {
            "verdict": verdict,
            "confidence": confidence,
            "signals_genuine_expansion": signals_genuine,
            "signals_artifact": signals_artifact,
        }

    # ── Compile report ───────────────────────────────────────────────────
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest_blob": f"gs://{bucket}/{INDEX_BLOB}",
        "manifest_total_rows": len(df),
        "window_2025": WINDOW_2025,
        "window_2026": WINDOW_2026,
        "set_analysis": set_analysis,
        "cross_data_type_overlap": cross_overlap,
        "near_duplicates": near_dupes,
        "capture_status_breakdowns": status_breakdowns,
        "classification": classification,
        "summary": {
            dt: {
                "growth": f"{set_analysis[dt]['distinct_leagues_2025_h1']} -> {set_analysis[dt]['distinct_leagues_2026_h1']} ({set_analysis[dt]['growth_ratio']}x)",
                "verdict": classification[dt]["verdict"],
                "confidence": classification[dt]["confidence"],
            }
            for dt in sorted(TARGET_DATA_TYPES)
        },
    }

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("wrote report to %s", args.report)
    else:
        print(json.dumps(report, indent=2, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
