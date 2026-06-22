#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: campaign
# Delete-when: source_data_latency.py has been re-pinned from >=2 weeks of empirical
#   forward observations (observed p50/p95/max per source committed to UAC), the
#   data_completion_to_100_all_ag_2026_06_21.md Source-latency-validation item is flipped,
#   and the forward poller has accrued a stable steady-state distribution that no longer
#   moves the constants — at which point re-validation is a periodic spot-check, not a campaign.
"""Aggregate empirical source-publish-lag observations → recomputed p50/p95/max per source.

**Context.** The per-source p95 lag constants in
``unified-api-contracts/.../registry/source_data_latency.py`` (SFI 300 s,
api_football 1800 s, footystats 3600 s, understat 7200 s, open_meteo 3600 s)
are **UNVALIDATABLE FROM BACKFILL** (every captured ``available_at`` is the lag
arithmetic itself or the backfill write wall-clock). The sports forward
scheduler (``deployment_service/sports_latency_observation.py``) writes a real
``observed_publish_lag_s = first_fetch_utc - match_end_utc`` per (fixture,
data_type, source) to
``instruments-store-sports-<env>/_index/latency_observations/day=*/*.parquet``.

**This tool** reads those observation parquets, aggregates per ``source`` (and
per ``data_type``), and prints the empirical **p50 / p95 / max / count** vs the
``assumed_lag_constant_s`` carried on each row — so after ~1-2 weeks of accrual
the operator can re-pin the constants from REAL data. ``--emit-constants``
prints a ready-to-paste ``source_data_latency.py`` constant block computed from
the observed p95 (ceil to the nearest minute, never below the current assumed
value unless ``--allow-lower`` — fail-safe: an under-sampled window must not
LOWER a lag floor and risk a too-early read).

**Read-only.** This tool NEVER mutates the manifest, ``available_at``, or
``source_data_latency.py`` — it is the measurement+recommendation step; the
re-pin is a human-reviewed UAC edit (the constants feed
``CanonicalFixture.report_time``, a cross-repo contract).

Usage (from workspace root, workspace venv):
    python3 instruments-service/scripts/aggregate_source_latency_observations.py \
        [--project-id central-element-323112] [--min-samples 20] \
        [--first-success-only] [--emit-constants] [--allow-lower]
"""

from __future__ import annotations

import argparse
import io
import logging
import math
from typing import TYPE_CHECKING

import pandas as pd
from unified_trading_library import get_bucket_name, get_storage_client

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

_OBS_PREFIX = "_index/latency_observations"

# Current assumed constants, transcribed from source_data_latency.py, keyed by
# the ``source`` string the observation rows carry. Printed alongside the
# observed distribution as the yardstick.
_ASSUMED_CONSTANTS_S: dict[str, int] = {
    "sfi": 300,
    "api_football": 1800,
    "footystats": 3600,
    "understat": 7200,
    "open_meteo": 3600,
}

# source → the source_data_latency.py constant NAME (for --emit-constants).
_SOURCE_TO_CONST_NAME: dict[str, str] = {
    "sfi": "SFI_DATA_LAG_P95_SECONDS",
    "api_football": "API_FOOTBALL_RESULT_LAG_P95_SECONDS",
    "footystats": "FOOTYSTATS_DATA_LAG_P95_SECONDS",
    "understat": "UNDERSTAT_DATA_LAG_P95_SECONDS",
    "open_meteo": "OPEN_METEO_HISTORICAL_LAG_SECONDS",
}


def _load_observations(storage: object, bucket: str) -> pd.DataFrame:  # pyright: ignore[reportAny]
    """Union all ``_index/latency_observations/day=*/*.parquet`` into one frame."""
    native = getattr(storage, "_client", None)  # pyright: ignore[reportAny]
    if native is None:
        logger.error("Could not access native GCS client for listing observations.")
        return pd.DataFrame()
    blobs = list(native.bucket(bucket).list_blobs(prefix=_OBS_PREFIX))
    parquet_blobs = [b.name for b in blobs if b.name.endswith(".parquet")]
    logger.info("Observation parquets under gs://%s/%s: %d", bucket, _OBS_PREFIX, len(parquet_blobs))
    frames: list[pd.DataFrame] = []
    for name in parquet_blobs:
        try:
            raw = storage.download_bytes(bucket, name)  # pyright: ignore[reportAny]
            frames.append(pd.read_parquet(io.BytesIO(raw)))
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning("Failed to read %s: %s", name, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _percentile(values: Sequence[float], pct: float) -> float:
    """Nearest-rank percentile (no interpolation) over non-negative lags."""
    s = sorted(v for v in values if v is not None and v >= 0)
    if not s:
        return float("nan")
    k = max(0, min(len(s) - 1, math.ceil(pct / 100.0 * len(s)) - 1))
    return s[k]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", default="central-element-323112")
    parser.add_argument("--min-samples", type=int, default=20, help="Min obs per source to trust the p95.")
    parser.add_argument(
        "--first-success-only",
        action="store_true",
        help="Only count rows with first_success=True AND fetched_rows>0 (genuine publish observations).",
    )
    parser.add_argument("--emit-constants", action="store_true", help="Print a source_data_latency.py block.")
    parser.add_argument(
        "--allow-lower",
        action="store_true",
        help="Permit a recomputed constant BELOW the current assumed value (default: floor at assumed).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bucket = get_bucket_name("instruments", "SPORTS")
    storage = get_storage_client(project_id=args.project_id)
    df = _load_observations(storage, bucket)
    if df.empty:
        logger.warning("No observations found — the forward scheduler has not accrued any yet.")
        return 0

    if args.first_success_only and "first_success" in df.columns:
        df = df[(df["first_success"]) & (df.get("fetched_rows", 0) > 0)]
        logger.info("Filtered to first-success observations: %d rows", len(df))

    logger.info("Total observations: %d across %d sources", len(df), df["source"].nunique())
    print("\n=== Observed source-publish lag (seconds) vs assumed p95 constant ===")
    print(f"{'source':<14} {'n':>6} {'p50':>10} {'p95':>10} {'max':>10} {'assumed':>10} {'verdict':<20}")

    recomputed: dict[str, int] = {}
    for source, grp in df.groupby("source"):
        lags = [float(v) for v in grp["observed_publish_lag_s"].tolist() if v is not None]  # pyright: ignore[reportAny]
        n = len(lags)
        p50 = _percentile(lags, 50)
        p95 = _percentile(lags, 95)
        mx = max((v for v in lags if v >= 0), default=float("nan"))
        assumed = _ASSUMED_CONSTANTS_S.get(str(source), 0)
        if n < args.min_samples:
            verdict = f"UNDER-SAMPLED (<{args.min_samples})"
        elif not math.isnan(p95) and p95 > assumed * 1.25:
            verdict = "TOO-LOW (raise)"
        elif not math.isnan(p95) and p95 < assumed * 0.75:
            verdict = "TOO-HIGH (lower?)"
        else:
            verdict = "CONFIRM (within 25%)"
        print(f"{source!s:<14} {n:>6} {p50:>10.0f} {p95:>10.0f} {mx:>10.0f} {assumed:>10} {verdict:<20}")

        if n >= args.min_samples and not math.isnan(p95):
            # ceil to the nearest minute; floor at assumed unless --allow-lower
            recompute = int(math.ceil(p95 / 60.0) * 60)
            if not args.allow_lower:
                recompute = max(recompute, assumed)
            recomputed[str(source)] = recompute

    if args.emit_constants:
        print("\n=== Recomputed source_data_latency.py block (review before committing) ===")
        for source, val in sorted(recomputed.items()):
            name = _SOURCE_TO_CONST_NAME.get(source)
            if name is None:
                continue
            print(
                f"{name}: Final[int] = {val}  # recomputed p95 (ceil-min) from {df[df['source'] == source].shape[0]} obs"
            )
        if not recomputed:
            print("(no source met --min-samples — accrue more forward observations first)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
