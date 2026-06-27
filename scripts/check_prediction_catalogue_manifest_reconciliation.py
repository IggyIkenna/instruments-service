#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""Logical-reconciliation check: prediction catalogue-active-on-D == manifest-captured-on-D.

Convention (SSOT: codex/02-data/prediction-settlement-availability-convention.md):
  available_to = settlement date (inclusive). A market is active on day D iff
  ``available_from <= D <= available_to``, and that set equals the manifest-captured set.

Usage:
  cd instruments-service
  GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd \\
    .venv/bin/python3 scripts/check_prediction_catalogue_manifest_reconciliation.py \\
    --dates 2026-06-25 2026-06-26 2026-06-27 \\
    --tolerance 50

Exit codes:
  0  All checked dates reconcile within tolerance.
  1  At least one date fails reconciliation (diff > tolerance).
  2  Configuration / data error.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

import pandas as pd
from unified_trading_library import get_config, get_storage_client, resolve_bucket_name


def _load_catalogue(bucket: str, storage: object) -> pd.DataFrame:
    """Download and load the prediction catalogue parquet."""
    import io

    payload = storage.download_bytes(bucket, "prod/catalog.parquet")
    return pd.read_parquet(io.BytesIO(payload))


def _load_manifest(bucket: str, storage: object) -> pd.DataFrame:
    """Download and load the prediction IS-manifest availability_index parquet."""
    import io

    payload = storage.download_bytes(bucket, "_index/availability_index.parquet")
    df = pd.read_parquet(io.BytesIO(payload))
    df["instrument_count"] = pd.to_numeric(df.get("instrument_count"), errors="coerce")
    return df


def _catalogue_active_on(cat: pd.DataFrame, d: date, grain: str) -> dict[str, int]:
    """Count catalogue rows active on date ``d`` at the given data_type grain.

    Args:
        cat: full catalogue DataFrame.
        d: the date to check.
        grain: data_type filter (e.g. ``"prediction_canonical_question_group"``).

    Returns:
        ``{venue: count}`` of catalogue rows active on ``d`` at ``grain``.
    """
    d_ts = pd.Timestamp(d)
    sub = cat[cat["data_type"] == grain].copy()
    af = pd.to_datetime(sub["available_from"], errors="coerce")
    at = pd.to_datetime(sub["available_to"], errors="coerce")
    # available_to = None means open-ended (still active)
    active = sub[(af <= d_ts) & ((at.isna()) | (at >= d_ts))]
    return active.groupby("venue").size().to_dict()


def _manifest_captured_on(mani: pd.DataFrame, d: date, data_type: str) -> dict[str, int]:
    """Sum instrument_count for ``capture_status == captured`` on date ``d`` at ``data_type``.

    The IS manifest records per-CQG ``instrument_count`` for the prediction CQG grain.
    This gives the total number of conditionId-level markets captured per CQG per day.

    For the reconciliation against CQG-grain catalogue rows, we count the number of CQG
    manifest rows (each represents one active CQG captured on that day).

    Args:
        mani: IS manifest DataFrame.
        d: the date to check.
        data_type: manifest ``data_type`` filter.

    Returns:
        ``{venue: cqg_rows_captured}`` — number of CQG rows with ``captured`` status.
    """
    iso = d.isoformat()
    sub = mani[(mani["date"] == iso) & (mani["data_type"] == data_type) & (mani["capture_status"] == "captured")]
    return sub.groupby("venue").size().to_dict()


def _check_date(
    cat: pd.DataFrame,
    mani: pd.DataFrame,
    d: date,
    tolerance: int,
) -> tuple[bool, dict[str, object]]:
    """Check reconciliation for one date.

    Grain: ``prediction_canonical_question_group`` rows in both catalogue and manifest.
    The catalogue count = CQG rows active on D.
    The manifest count  = CQG rows captured on D (capture_status=captured).

    Returns:
        (passed, details_dict)
    """
    cqg_dt = "prediction_canonical_question_group"
    cat_counts = _catalogue_active_on(cat, d, cqg_dt)
    mani_counts = _manifest_captured_on(mani, d, cqg_dt)

    all_venues = sorted(set(cat_counts) | set(mani_counts))
    details: dict[str, object] = {"date": d.isoformat(), "venues": {}}
    passed = True
    for venue in all_venues:
        c = cat_counts.get(venue, 0)
        m = mani_counts.get(venue, 0)
        diff = abs(c - m)
        ok = diff <= tolerance
        if not ok:
            passed = False
        details["venues"][venue] = {  # type: ignore[index]
            "catalogue_active": c,
            "manifest_captured": m,
            "diff": diff,
            "ok": ok,
        }
    return passed, details


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dates",
        nargs="+",
        required=True,
        help="ISO dates to check (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--tolerance",
        type=int,
        default=50,
        help="Max acceptable diff between catalogue-active and manifest-captured counts (default: 50)",
    )
    args = parser.parse_args(argv)

    dates_to_check = [date.fromisoformat(d) for d in args.dates]

    try:
        storage = get_storage_client(cloud="gcp")
        cat_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store-prediction")
        mani_bucket = cat_bucket  # IS manifest lives in the same prediction bucket
    except Exception as exc:
        print(f"ERROR: could not resolve bucket / storage: {exc}", file=sys.stderr)
        return 2

    print(f"Loading prediction catalogue from gs://{cat_bucket}/prod/catalog.parquet ...")
    try:
        cat = _load_catalogue(cat_bucket, storage)
    except Exception as exc:
        print(f"ERROR: could not load catalogue: {exc}", file=sys.stderr)
        return 2

    print(f"Loading IS manifest from gs://{mani_bucket}/_index/availability_index.parquet ...")
    try:
        mani = _load_manifest(mani_bucket, storage)
    except Exception as exc:
        print(f"ERROR: could not load manifest: {exc}", file=sys.stderr)
        return 2

    print(
        f"\nReconciliation check: catalogue-active-on-D == manifest-captured-on-D "
        f"(grain: prediction_canonical_question_group, tolerance: {args.tolerance})"
    )
    print("=" * 70)

    all_passed = True
    for d in dates_to_check:
        passed, details = _check_date(cat, mani, d, args.tolerance)
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
        print(f"\n[{status}] {d.isoformat()}")
        for venue, v in details["venues"].items():  # type: ignore[union-attr]
            mark = "OK" if v["ok"] else "MISMATCH"
            print(
                f"  {venue:12s}: catalogue={v['catalogue_active']:5d}  "
                f"manifest={v['manifest_captured']:5d}  "
                f"diff={v['diff']:4d}  [{mark}]"
            )

    print("\n" + "=" * 70)
    if all_passed:
        print("RESULT: ALL DATES PASS — catalogue and manifest reconcile within tolerance.")
        return 0
    else:
        print("RESULT: SOME DATES FAIL — catalogue and manifest do NOT reconcile within tolerance.")
        print(
            "  If diffs are large, re-run the catalogue regen (build_instrument_catalogue.py "
            "--asset-group prediction) and retry."
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
