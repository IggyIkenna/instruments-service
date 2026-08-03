#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: the fixture_events re-fetch campaign closes (0 non-13-col objects remaining)
"""Census the schema variant of every captured canonical FIXTURE_EVENTS object.

Epic: sports_master
Lifecycle: one-off, part of the fixture_events re-fetch campaign
  (`issues/canonical_player_stats_fixture_events_quality_2026_07_16.md` finding 2,
  `sports_satellite_ao_dispatch_batch2_2026_07_24.md` todo-031).
Delete-when: the re-fetch campaign closes (0 non-13-col objects remaining).

Reads column headers ONLY (pyarrow ParquetFile schema, no row-group load) for
every object the manifest lists as captured FIXTURE_EVENTS, classifies each by
its column set (canonical 13-col / 5-col degenerate stub / 9-col named /
10-col af_-prefixed / other), and writes a recovery-fixture-ids parquet
(canonical_league_id, af_fixture_id) for every non-canonical object so the
existing `--recovery-fixture-ids` CLI mechanism can drive the re-fetch through
the real writer path (read-modify-write merge, canonical schema enforced) --
no hand-rolled GCS write in this script.

Single-walk discipline: this is the ONE full-corpus schema read for this
campaign; the resulting recovery-ids parquet is the durable artifact re-used
by every subsequent re-fetch batch (do not re-walk to regenerate it).
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from unified_trading_library import get_storage_client

BUCKET = "instruments-store-sports-prd-central-element-323112"


CANONICAL_COLS = {
    "event_type",
    "event_detail",
    "player_id",
    "player_name",
    "team_id",
    "team_name",
    "time_elapsed",
}


def classify(cols: frozenset[str]) -> str:
    if CANONICAL_COLS.issubset(cols) and len(cols) >= 12:
        return "canonical_13col"
    if "type" in cols and "detail" in cols and "fixture_id" in cols and len(cols) <= 6:
        return "degenerate_5col_stub"
    if any(c.startswith("af_") for c in cols):
        return "af_prefixed_10col"
    if "assist" in cols or "player" in cols:
        return "named_9col"
    return "other"


def read_schema_cols(object_path: str) -> frozenset[str] | None:
    """Return the object's column set, or None if it genuinely does not exist.

    Retries transient read failures (network blips) up to 3x before giving
    up — a bare except-and-skip here is exactly what produced the pilot
    run's false-positive 'missing' classifications for objects that were
    actually present (traced to the raw google-cloud-storage client's
    default connection-pool exhaustion under many concurrent workers; the
    UTL cloud-agnostic client's ``download_bytes`` doesn't expose that pool,
    but the retry stays as defense against ordinary network blips).
    """
    client = get_storage_client()
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            if not client.blob_exists(BUCKET, object_path):
                return None
            raw = client.download_bytes(BUCKET, object_path)
            pf = pq.ParquetFile(io.BytesIO(raw))
            return frozenset(pf.schema_arrow.names)
        except Exception as exc:
            last_exc = exc
            time.sleep(0.2 * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="Cap the number of objects censused (pilot runs).")
    ap.add_argument("--workers", type=int, default=40)
    ap.add_argument(
        "--out-recovery-ids",
        default="scripts/_fixture_events_recovery_ids_2026_07_25.parquet",
    )
    ap.add_argument("--out-census", default="scripts/_fixture_events_census_2026_07_25.json")
    args = ap.parse_args()

    # Read via the UTL storage client (same credential path every other read in
    # this script already uses) rather than pandas/pyarrow's native gs:// reader,
    # which resolves ADC independently and can silently go stale mid-session
    # (2026-08-03: this bare pd.read_parquet("gs://...") call started failing
    # with a NOT_FOUND on an object confirmed to exist via `gcloud storage
    # objects describe`, while every UTL-backed read in this same script kept
    # working -- an ADC/credential-path mismatch, not a real absence).
    _manifest_client = get_storage_client()
    _manifest_bytes = _manifest_client.download_bytes(BUCKET, "_index/availability_index.parquet")
    manifest = pd.read_parquet(io.BytesIO(_manifest_bytes))
    fe = manifest[(manifest["data_type"] == "FIXTURE_EVENTS") & (manifest["capture_status"] == "captured")]
    fe = fe.drop_duplicates(subset=["date", "league_id"])
    if args.limit:
        fe = fe.sample(n=min(args.limit, len(fe)), random_state=7)
    rows = fe[["date", "league_id"]].to_dict("records")
    print(f"censusing {len(rows)} candidate objects", file=sys.stderr)

    from unified_api_contracts.canonical.domain.sports.gcs_paths import candidate_parquet_paths

    counts: dict[str, int] = {}
    recovery_rows: list[dict[str, str]] = []
    errors = 0

    def _work(row: dict[str, str]) -> tuple[str, str, list[str] | None]:
        day, league = row["date"], row["league_id"]
        paths = candidate_parquet_paths("FIXTURE_EVENTS", day, league, pipeline_mode="batch_api_football")
        read_error = False
        for p in paths:
            try:
                cols = read_schema_cols(p)
            except Exception:
                read_error = True
                continue
            if cols is None:
                continue
            variant = classify(cols)
            fixture_ids: list[str] | None = None
            if variant != "canonical_13col":
                # The fixture-id column name varies by variant (af_prefixed_10col
                # carries af_fixture_id, not fixture_id) -- probing the actual
                # schema instead of hardcoding "fixture_id" avoids silently
                # dropping every af_prefixed_10col object from the recovery set
                # (2026-08-03 finding: this exact bug left 2,383 objects
                # permanently untargeted across every prior census in this
                # campaign, since the hardcoded-column read always raised and
                # was swallowed into an empty fixture_ids list).
                id_col = next((c for c in ("fixture_id", "af_fixture_id") if c in cols), None)
                if id_col is not None:
                    try:
                        df = pd.read_parquet(f"gs://{BUCKET}/{p}", columns=[id_col])
                        fixture_ids = sorted({str(v) for v in df[id_col].dropna().unique()})
                    except Exception:
                        fixture_ids = []
                else:
                    fixture_ids = []
            return league, variant, fixture_ids
        return league, ("read_error" if read_error else "missing"), None

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_work, r): r for r in rows}
        done = 0
        for fut in as_completed(futures):
            done += 1
            if done % 2000 == 0:
                print(f"  {done}/{len(rows)} censused, counts={counts}", file=sys.stderr)
            try:
                league, variant, fixture_ids = fut.result()
            except Exception:
                errors += 1
                continue
            counts[variant] = counts.get(variant, 0) + 1
            if fixture_ids:
                for fid in fixture_ids:
                    recovery_rows.append({"canonical_league_id": league, "af_fixture_id": fid})

    print(f"final counts: {counts}, errors={errors}", file=sys.stderr)
    Path(args.out_census).write_text(json.dumps({"counts": counts, "errors": errors, "n_objects": len(rows)}, indent=2))

    if recovery_rows:
        rec_df = pd.DataFrame(recovery_rows).drop_duplicates()
        rec_df.to_parquet(args.out_recovery_ids, index=False)
        print(f"wrote {len(rec_df)} recovery rows -> {args.out_recovery_ids}", file=sys.stderr)
    else:
        print("no recovery rows to write", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
