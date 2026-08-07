# Epic: infrastructure_master
# Lifecycle: oneoff
# Delete-when: after the sports_manifest_2026_h1_vs_2025_h1_enumeration_grain_persists
#   issue's P3 cross-AG measurement todo is checked off and the finding filed —
#   this is a point-in-time read-only measurement, not a recurring job.
"""Re-measure the static-default ``expected_universe_start_date`` boundary artifact
for cefi/defi/tradfi/prediction — the same pattern already confirmed for sports.

READ-ONLY. Reads each AG's live ``_index/availability_index.parquet`` manifest ONCE
(one blob download per AG, no corpus walk), projects only the columns needed
(``date``, ``data_type``, ``capture_status``), and counts:

1. Total cells seeded per ``data_type`` for matched H1 windows (2025-01-01..2025-06-30
   vs 2026-01-01..2026-06-30) — same method as the original sports measurement.
2. Per-window ``capture_status`` breakdown (the key diagnostic: pre-2026-02-20 dates
   should show ZERO ``expected_unattempted`` rows if the static-default boundary
   artifact affects this AG).

Plan: ``unified-trading-pm/plans/active/issues/
sports_manifest_2026_h1_vs_2025_h1_enumeration_grain_persists_2026_07_27.md``
(todo "[DATA] P3. Re-measure the same static-default expected_universe_start_date
pattern for cefi/defi/tradfi/prediction").

Usage
-----
::

  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    .venv/bin/python scripts/cross_ag_manifest_enumeration_grain_check_2026_08_05.py \\
    --asset-group cefi --report /tmp/cefi_enum_grain_report.json
"""

from __future__ import annotations

import argparse
import io
import json
import logging
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
WINDOW_2025 = ("2025-01-01", "2025-06-30")
WINDOW_2026 = ("2026-01-01", "2026-06-30")

# Asset groups and their manifest bucket kinds (from
# deployment-service/terraform/gcp/expected_universe_v2_scheduler.tf lines 108-114).
# sports uses instruments-store; all others use market-data-tick.
# Resolved from existing per-AG scripts (cefi→measure_cefi_catalogue_enumeration_gap,
# defi→reclassify_defi_orphan_eu_notlisted, tradfi→migrate_tradfi_combo_manifest_casing,
# prediction→snapshot_prediction_index_pre_final_cleanup, sports→the 2026-07-27
# measurement). Each (kind, asset_group) pair is the one the manifest (market-data)
# bucket resolves to for that AG.
_BUCKET_KINDS: dict[str, tuple[str, str | None]] = {
    "cefi": ("market-data", "cefi"),
    "defi": ("market-data", "defi"),
    "tradfi": ("market-data", "tradfi"),
    "sports": ("instruments-store", "sports"),
    "prediction": ("market-data-tick-prediction", None),
}


def _manifest_bucket(asset_group: str) -> str:
    entry = _BUCKET_KINDS.get(asset_group)
    if entry is None:
        valid = ", ".join(sorted(_BUCKET_KINDS))
        raise SystemExit(f"unknown asset_group: {asset_group!r}. valid: {valid}")
    kind, ag = entry
    return resolve_bucket_name(cloud="gcp", kind=kind, asset_group=ag)


def _load_manifest_slim(client, bucket: str) -> pd.DataFrame:
    data = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(data), columns=["date", "data_type", "capture_status"])
    df["date"] = df["date"].astype(str)
    return df


def _window_mask(df: pd.DataFrame, start: str, end: str) -> pd.Series:
    return (df["date"] >= start) & (df["date"] <= end)


def _cell_counts(df: pd.DataFrame, start: str, end: str) -> pd.Series:
    return df.loc[_window_mask(df, start, end)].groupby("data_type").size()


def _status_breakdown(df: pd.DataFrame, start: str, end: str) -> dict[str, dict[str, int]]:
    """Return {data_type: {capture_status: count}} for the window."""
    w = df.loc[_window_mask(df, start, end)]
    out: dict[str, dict[str, int]] = {}
    for dt, grp in w.groupby("data_type"):
        out[str(dt)] = grp["capture_status"].value_counts().to_dict()  # type: ignore[union-attr]
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--asset-group",
        required=True,
        choices=sorted(_BUCKET_KINDS),
        help="Asset group to measure",
    )
    parser.add_argument("--report", type=str, default="", help="path to write the JSON report")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    ag: str = args.asset_group
    client = get_storage_client(provider="gcp")
    bucket = _manifest_bucket(ag)

    logger.info("loading manifest for %s from gs://%s/%s", ag, bucket, INDEX_BLOB)
    df = _load_manifest_slim(client, bucket)
    logger.info("loaded: %d total rows", len(df))

    counts_2025 = _cell_counts(df, *WINDOW_2025)
    counts_2026 = _cell_counts(df, *WINDOW_2026)
    status_2025 = _status_breakdown(df, *WINDOW_2025)
    status_2026 = _status_breakdown(df, *WINDOW_2026)

    # --- per-data_type cell-count table ---
    data_types = sorted(set(counts_2025.index) | set(counts_2026.index))
    per_type: list[dict[str, Any]] = []
    for dt in data_types:
        c25 = int(counts_2025.get(dt, 0))
        c26 = int(counts_2026.get(dt, 0))
        ratio = (c26 / c25) if c25 > 0 else (float("inf") if c26 > 0 else 0.0)
        s25 = status_2025.get(dt, {})
        s26 = status_2026.get(dt, {})
        has_expected_unattempted_2025 = sum(v for k, v in s25.items() if "expected_unattempted" in str(k).lower())
        has_expected_unattempted_2026 = sum(v for k, v in s26.items() if "expected_unattempted" in str(k).lower())
        per_type.append(
            {
                "data_type": dt,
                "cells_2025_h1": c25,
                "cells_2026_h1": c26,
                "ratio_2026_over_2025": ratio,
                "capture_status_2025": s25,
                "capture_status_2026": s26,
                "expected_unattempted_in_2025": has_expected_unattempted_2025 > 0,
                "expected_unattempted_in_2026": has_expected_unattempted_2026 > 0,
            }
        )

    total_2025 = int(counts_2025.sum())
    total_2026 = int(counts_2026.sum())
    overall_ratio = (total_2026 / total_2025) if total_2025 > 0 else float("inf")

    # --- boundary-artifact diagnosis ---
    # Same logic as the sports measurement: a data_type with a non-trivial 2025
    # baseline (>=100 cells) and a >=5x 2026/2025 ratio AND zero expected_unattempted
    # in 2025 but non-zero in 2026 is a strong signal of the static-default artifact.
    boundary_candidates = [
        r
        for r in per_type
        if r["ratio_2026_over_2025"] >= 5.0
        and r["cells_2025_h1"] >= 100
        and not r["expected_unattempted_in_2025"]
        and r["expected_unattempted_in_2026"]
    ]
    high_ratio_types = [r for r in per_type if r["ratio_2026_over_2025"] >= 5.0 and r["cells_2025_h1"] >= 100]

    report = {
        "asset_group": ag,
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest_blob": f"gs://{bucket}/{INDEX_BLOB}",
        "manifest_total_rows": len(df),
        "window_2025": WINDOW_2025,
        "window_2026": WINDOW_2026,
        "total_cells_2025_h1": total_2025,
        "total_cells_2026_h1": total_2026,
        "overall_ratio_2026_over_2025": overall_ratio,
        "per_data_type": per_type,
        "data_types_with_ratio_ge_5x": high_ratio_types,
        "boundary_artifact_candidates": boundary_candidates,
        "verdict": (
            f"BOUNDARY ARTIFACT CONFIRMED ({len(boundary_candidates)} data_type(s) show "
            f">=5x 2026/2025 ratio with zero expected_unattempted in 2025 but present in 2026)"
            if boundary_candidates
            else (
                f"RATIO ELEVATED BUT NOT BOUNDARY-ARTIFACT ("
                f"{len(high_ratio_types)} data_type(s) with >=5x ratio, "
                f"but none show the expected_unattempted split pattern)"
                if high_ratio_types
                else "CLEAN (no data_type shows a >=5x ratio with a non-trivial 2025 baseline)"
            )
        ),
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
