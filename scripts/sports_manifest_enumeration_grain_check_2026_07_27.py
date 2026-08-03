# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the 2026-07-27 batch-3 DIAG todo's measurement result has been
#   recorded (plan checkbox annotated "resolved as side effect" OR a scoped issue doc
#   filed) — this is a point-in-time re-verification, not a recurring job.
"""Re-measure whether the sports manifest's 2026-vs-prior-year enumeration-grain
inconsistency (diagnosed 2026-06-23 in `data_completion_sports_2026_07_24.md` as
"~120k/data_type vs ~8-30k/prior-year", i.e. up to ~10x-15x) still persists after the
2026-06-23 `enumerate_expected_universe.py` denominator fix (instruments-service@0bcf727)
and the subsequent write-gate/dereg/canonicalize program.

READ-ONLY. Reads the live `_index/availability_index.parquet` manifest for
`instruments-store-sports-prd-*` ONCE (single-walk discipline: one blob download, no
corpus walk), projects only the 3 columns needed (`date`, `data_type`,
`capture_status`) to bound memory, and counts total manifest rows ("cells seeded",
across every capture_status -- captured/attempted_failed/empty_confirmed/
expected_unattempted all count, since the diagnosed defect was over-SEEDING the
universe, not any one capture_status split) per (data_type, year) for a matched H1
sample window (Jan 1 - Jun 30) in 2025 vs 2026.

Plan: ``unified-trading-pm/plans/active/sports_satellite_ao_dispatch_batch3_2026_07_25.md``
(todo "[DIAG] P1. Verify whether the sports manifest's 2026-vs-prior-year
enumeration-grain inconsistency ... still persists").

Usage
-----
::

  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    .venv/bin/python scripts/sports_manifest_enumeration_grain_check_2026_07_27.py \\
    --report /tmp/sports_enum_grain_report.json
"""

from __future__ import annotations

import argparse
import io
import json
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
WINDOW_2025 = ("2025-01-01", "2025-06-30")
WINDOW_2026 = ("2026-01-01", "2026-06-30")


def _instruments_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _load_manifest_slim(client, bucket: str) -> pd.DataFrame:
    data = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(data), columns=["date", "data_type", "capture_status"])
    df["date"] = df["date"].astype(str)
    return df


def _window_counts(df: pd.DataFrame, start: str, end: str) -> pd.Series:
    mask = (df["date"] >= start) & (df["date"] <= end)
    return df.loc[mask].groupby("data_type").size()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=str, default="", help="path to write the JSON report")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = get_storage_client(provider="gcp")
    bucket = _instruments_bucket()

    df = _load_manifest_slim(client, bucket)
    logger.info("loaded manifest: %d total rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)

    counts_2025 = _window_counts(df, *WINDOW_2025)
    counts_2026 = _window_counts(df, *WINDOW_2026)

    data_types = sorted(set(counts_2025.index) | set(counts_2026.index))
    per_type: list[dict] = []
    for dt in data_types:
        c25 = int(counts_2025.get(dt, 0))
        c26 = int(counts_2026.get(dt, 0))
        ratio = (c26 / c25) if c25 > 0 else (float("inf") if c26 > 0 else 0.0)
        per_type.append(
            {
                "data_type": dt,
                "cells_2025_h1": c25,
                "cells_2026_h1": c26,
                "ratio_2026_over_2025": ratio,
            }
        )

    total_2025 = int(counts_2025.sum())
    total_2026 = int(counts_2026.sum())
    overall_ratio = (total_2026 / total_2025) if total_2025 > 0 else float("inf")

    high_ratio_types = [r for r in per_type if r["ratio_2026_over_2025"] >= 5.0 and r["cells_2025_h1"] >= 100]

    report = {
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
        "verdict": (
            "PERSISTS (>=5x class inconsistency found on >=1 data_type with a non-trivial 2025 baseline)"
            if high_ratio_types
            else "RESOLVED (no data_type shows a >=5x 2026-vs-2025 H1 cell-seeding ratio "
            "with a non-trivial 2025 baseline)"
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
