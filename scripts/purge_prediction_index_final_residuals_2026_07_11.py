#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after E7 CF-verify confirms GREEN on the prediction pred-prd _index (this cleanup's gate below is 0)
"""Purge the two verified-safe residual classes in the prediction pred-prd `_index`
(E7 CF-verify final residuals, 2026-07-11) — classes A and C from the operator brief.
Class B (13,292 phantom rows) is handled separately by
``reconcile_phantom_manifest_rows_all.py --asset-group prediction --unphantom-only``
(existing tool; --unphantom-only is safe-by-construction, phantom->captured only).

Class A — 6,760 pre-canonical v4/v5 POLYMARKET per-object rows (`trades` /
`prediction_trades`, blank `source`, `capture_status=captured`). VERIFIED (read-only,
2026-07-11): 100% (6,760/6,760) have a `captured` v9
`prediction_canonical_question_group` bundle row for the SAME date + venue=POLYMARKET
— i.e. fully SUPERSEDED by the now-correctly-captured bundle atom (the earlier
"no captured v9 bundle exists" hypothesis in
plans/active/issues/prediction_phantom_reconciler_wipes_bundle_atom_2026_07_10.md is
now resolved: the bundle-atom reconciler exemption landed and the rebuild re-emitted
the bundles). Deletion is LOSSLESS (the aggregate cell the row claimed is now
represented by the bundle).

Class C — 124 lowercase `venue="kalshi"` rows (`data_type=
prediction_canonical_question_group`, `capture_status` in
{empty_confirmed, expected_unattempted}). VERIFIED (read-only, 2026-07-11): 124/124
have an EXACT key-match (date, data_type, capture_status, pipeline_mode, source,
error_reason, underlying, instrument_type) among canonical `venue="KALSHI"` rows —
byte-duplicate, zero object-backing (both are non-`captured` cells; confirmed in
plans/active/issues/prediction_phantom_reconciler_wipes_bundle_atom_2026_07_10.md).

STOP-ON-SURPRISE exits 1 when actual counts drift materially from the verified
counts above, or when either mask selects a `captured`-with-DATA row it shouldn't
(guards against corrupt index state / a re-run after upstream data changed).

Usage::

    cd instruments-service

    # dry-run (default, read-only)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/purge_prediction_index_final_residuals_2026_07_11.py

    # apply (snapshot + delete + verify gate)
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prd DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/purge_prediction_index_final_residuals_2026_07_11.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"

# Class A expectations
_A_SCHEMA_VERSIONS = frozenset({"4", "4.0", "5", "5.0"})
_A_DATA_TYPES = frozenset({"trades", "prediction_trades"})
_A_EXPECTED_MIN = 6_000
_A_EXPECTED_MAX = 7_500

# Class C expectations
_C_EXPECTED_MIN = 100
_C_EXPECTED_MAX = 200

_KEY_COLS = [
    "date",
    "data_type",
    "capture_status",
    "pipeline_mode",
    "source",
    "error_reason",
    "underlying",
    "instrument_type",
]


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Delete ALL matching rows (snapshot first).")
    p.add_argument("--bucket", default=None, help="Override bucket (default: resolve_bucket_name).")
    return p


def _load_index(storage: object, bucket: str) -> pd.DataFrame:
    logger.info("Reading consolidated prediction manifest gs://%s/%s", bucket, INDEX_BLOB)
    raw = storage.download_bytes(bucket, INDEX_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded %d total manifest rows", len(df))
    return df


def _class_a_mask(df: pd.DataFrame) -> pd.Series:  # type: ignore[type-arg]
    sv = df["schema_version"].astype(str) if "schema_version" in df.columns else pd.Series([""] * len(df))
    dt = df["data_type"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)
    src = df["source"].fillna("").astype(str) if "source" in df.columns else pd.Series([""] * len(df))
    cs = df["capture_status"].fillna("").astype(str)
    return (
        sv.isin(_A_SCHEMA_VERSIONS)
        & dt.isin(_A_DATA_TYPES)
        & (venue.str.upper() == "POLYMARKET")
        & (src.str.strip() == "")
        & (cs == "captured")
    )


def _class_c_mask(df: pd.DataFrame) -> pd.Series:  # type: ignore[type-arg]
    venue = df["venue"].fillna("").astype(str)
    return venue == "kalshi"


def _verify_class_a_superseded(df: pd.DataFrame, mask_a: pd.Series) -> None:  # type: ignore[type-arg]
    """STOP-ON-SURPRISE: every Class-A row must have a captured v9 POLYMARKET bundle
    on the SAME date, else it is NOT superseded and must not be deleted."""
    dt = df["data_type"].fillna("").astype(str)
    cs = df["capture_status"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)
    bundle_mask = (
        (dt == "prediction_canonical_question_group") & (cs == "captured") & (venue.str.upper() == "POLYMARKET")
    )
    bundle_dates = set(df.loc[bundle_mask, "date"].astype(str).unique())
    a_dates = df.loc[mask_a, "date"].astype(str)
    not_superseded = ~a_dates.isin(bundle_dates)
    n_not_superseded = int(not_superseded.sum())
    if n_not_superseded:
        logger.error(
            "STOP-ON-SURPRISE: %d Class-A rows have NO same-date captured v9 POLYMARKET bundle — "
            "NOT superseded, refusing to delete. Sample dates: %s",
            n_not_superseded,
            sorted(a_dates[not_superseded].unique())[:20],
        )
        sys.exit(1)
    logger.info(
        "Class-A supersession re-verified: 100%% (%d/%d) have a same-date captured v9 POLYMARKET bundle.",
        int((~not_superseded).sum()),
        len(a_dates),
    )


def _verify_class_c_duplicate(df: pd.DataFrame, mask_c: pd.Series) -> None:  # type: ignore[type-arg]
    """STOP-ON-SURPRISE: every Class-C row must exact-key-match a canonical KALSHI row."""
    venue = df["venue"].fillna("").astype(str)
    upper_mask = venue == "KALSHI"
    key_cols = [c for c in _KEY_COLS if c in df.columns]
    upper_keys = set(map(tuple, df.loc[upper_mask, key_cols].fillna("").astype(str).values.tolist()))
    lower_keys = df.loc[mask_c, key_cols].fillna("").astype(str).values.tolist()
    non_match = [i for i, k in enumerate(lower_keys) if tuple(k) not in upper_keys]
    if non_match:
        logger.error(
            "STOP-ON-SURPRISE: %d Class-C rows have NO exact-key match among canonical KALSHI rows — "
            "refusing to delete (not a confirmed byte-duplicate).",
            len(non_match),
        )
        sys.exit(1)
    # Also require: no Class-C row is itself `captured` (would be real data, not a dup marker).
    cs = df.loc[mask_c, "capture_status"].fillna("").astype(str)
    bad_status = cs[cs == "captured"]
    if len(bad_status):
        logger.error(
            "STOP-ON-SURPRISE: %d Class-C rows carry capture_status=='captured' — refusing to delete "
            "(would risk destroying real data).",
            len(bad_status),
        )
        sys.exit(1)
    logger.info(
        "Class-C duplicate re-verified: 100%% (%d/%d) exact-key-match canonical KALSHI rows.",
        len(lower_keys) - len(non_match),
        len(lower_keys),
    )


def _stop_on_surprise_counts(n_a: int, n_c: int) -> None:
    if not (_A_EXPECTED_MIN <= n_a <= _A_EXPECTED_MAX):
        logger.error(
            "STOP-ON-SURPRISE: Class-A row count %d outside expected range [%d, %d]. Diagnose before --apply.",
            n_a,
            _A_EXPECTED_MIN,
            _A_EXPECTED_MAX,
        )
        sys.exit(1)
    if not (_C_EXPECTED_MIN <= n_c <= _C_EXPECTED_MAX):
        logger.error(
            "STOP-ON-SURPRISE: Class-C row count %d outside expected range [%d, %d]. Diagnose before --apply.",
            n_c,
            _C_EXPECTED_MIN,
            _C_EXPECTED_MAX,
        )
        sys.exit(1)


def _write_index(storage: object, bucket: str, df: pd.DataFrame, *, snapshot: bool) -> None:
    if snapshot:
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        snap_blob = f"_index/snapshots/pre_class_a_c_delete_{ts}.parquet"
        logger.info("Snapshotting pre-delete manifest to gs://%s/%s", bucket, snap_blob)
        sbuf = io.BytesIO()
        df.to_parquet(sbuf, index=False)
        storage.upload_bytes(bucket, snap_blob, sbuf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]

    logger.info("Writing purged manifest back to gs://%s/%s (%d rows)", bucket, INDEX_BLOB, len(df))
    obuf = io.BytesIO()
    df.to_parquet(obuf, index=False)
    storage.upload_bytes(bucket, INDEX_BLOB, obuf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Write complete.")


def _verify_gate(
    storage: object,
    bucket: str,
    *,
    captured_before: int,
    attempted_failed_before: int,
    empty_confirmed_before: int,
    expected_unattempted_before: int,
    n_a: int,
    n_c: int,
) -> int:
    logger.info("Verifying post-apply gate...")
    raw_after = storage.download_bytes(bucket, INDEX_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    df_after = pd.read_parquet(io.BytesIO(raw_after))

    residual_a = df_after[_class_a_mask(df_after)]
    residual_c = df_after[_class_c_mask(df_after)]
    if len(residual_a) > 0:
        logger.error("GATE FAIL: %d Class-A rows still remain.", len(residual_a))
        return 1
    if len(residual_c) > 0:
        logger.error("GATE FAIL: %d Class-C rows still remain.", len(residual_c))
        return 1

    cs_after = df_after["capture_status"].fillna("").astype(str)
    captured_after = int((cs_after == "captured").sum())
    attempted_failed_after = int((cs_after == "attempted_failed").sum())
    empty_confirmed_after = int((cs_after == "empty_confirmed").sum())
    expected_unattempted_after = int((cs_after == "expected_unattempted").sum())

    # Class A deletes `captured` rows (by design) — captured must drop by exactly n_a.
    if captured_after != captured_before - n_a:
        logger.error(
            "GATE FAIL: captured count changed unexpectedly %d -> %d (expected %d)",
            captured_before,
            captured_after,
            captured_before - n_a,
        )
        return 1
    # attempted_failed must be untouched (neither class touches it).
    if attempted_failed_after != attempted_failed_before:
        logger.error(
            "GATE FAIL: attempted_failed count changed %d -> %d (expected unchanged)",
            attempted_failed_before,
            attempted_failed_after,
        )
        return 1
    # empty_confirmed + expected_unattempted combined must drop by exactly n_c (class C spans both statuses).
    ec_eu_before = empty_confirmed_before + expected_unattempted_before
    ec_eu_after = empty_confirmed_after + expected_unattempted_after
    if ec_eu_after != ec_eu_before - n_c:
        logger.error(
            "GATE FAIL: empty_confirmed+expected_unattempted changed unexpectedly %d -> %d (expected %d)",
            ec_eu_before,
            ec_eu_after,
            ec_eu_before - n_c,
        )
        return 1

    logger.info(
        "GATE PASSED: 0 Class-A/C residuals; captured %d->%d (-%d), attempted_failed unchanged (%d), "
        "empty_confirmed+expected_unattempted %d->%d (-%d).",
        captured_before,
        captured_after,
        n_a,
        attempted_failed_after,
        ec_eu_before,
        ec_eu_after,
        n_c,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data-tick-prediction")
    storage = get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]

    df = _load_index(storage, bucket)
    cs = df["capture_status"].fillna("").astype(str)
    captured_before = int((cs == "captured").sum())
    attempted_failed_before = int((cs == "attempted_failed").sum())
    empty_confirmed_before = int((cs == "empty_confirmed").sum())
    expected_unattempted_before = int((cs == "expected_unattempted").sum())

    mask_a = _class_a_mask(df)
    mask_c = _class_c_mask(df)
    n_a = int(mask_a.sum())
    n_c = int(mask_c.sum())
    logger.info("Class A (v4/v5 POLYMARKET trades/prediction_trades, blank source, captured): %d rows", n_a)
    logger.info("Class C (lowercase venue='kalshi' duplicate rows): %d rows", n_c)

    _stop_on_surprise_counts(n_a, n_c)
    if n_a:
        _verify_class_a_superseded(df, mask_a)
    if n_c:
        _verify_class_c_duplicate(df, mask_c)

    overlap = int((mask_a & mask_c).sum())
    if overlap:
        logger.error("STOP-ON-SURPRISE: %d rows match BOTH Class A and Class C masks (should be disjoint).", overlap)
        return 1

    if not args.apply:
        logger.info(
            "DRY-RUN (default) — no write. Pass --apply to delete %d Class-A + %d Class-C = %d rows.",
            n_a,
            n_c,
            n_a + n_c,
        )
        return 0

    df_purged = df.loc[~(mask_a | mask_c)].reset_index(drop=True)
    _write_index(storage, bucket, df_purged, snapshot=True)

    rc = _verify_gate(
        storage,
        bucket,
        captured_before=captured_before,
        attempted_failed_before=attempted_failed_before,
        empty_confirmed_before=empty_confirmed_before,
        expected_unattempted_before=expected_unattempted_before,
        n_a=n_a,
        n_c=n_c,
    )
    if rc == 0:
        logger.info("APPLY COMPLETE: %d Class-A + %d Class-C rows deleted (%d total).", n_a, n_c, n_a + n_c)
    return rc


if __name__ == "__main__":
    sys.exit(main())
