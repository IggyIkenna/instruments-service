#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after the 24-league de-registration run (2026-07-13) is confirmed applied in prod
"""De-registration step 2 — PARK the captured rows of the 24 de-registered league_ids.

Per the 2026-07-13 operator ruling: captured rows whose identity is ambiguous
(RFPL understat XG, numeric-id INJURIES/WEATHER scatter) or whose re-key path is
content-blocked (LA_LIGA_2 diff-content collisions, SCOTTISH_LEAGUE_CUP_185
no-per-league-object) are NEVER deleted un-exported — they are parked with full
row fidelity to
``_audits/parked_league_rows_20260713.parquet`` in the sports instruments bucket.
GCS data objects are left untouched. Only after the parked export is verified
(re-downloaded, readable, row-count match) may the live index rows be dropped
(scripts/dereg_purge_24_leagues_2026_07_13.py asserts against this parquet).

Excluded from the park set: the ONE re-keyed atom
(LA_LIGA_2 / 2019-06-04 / MATCHES / footystats — content-verified at the
canonical SEGUNDA_DIVISION path + captured row staged via the
league-rekey-20260713 per-VM shard); its purge justification is path (a)
re-keyed, not path (b) parked.

Reads the LIVE canonical index (fresh download, generation logged) — not the
recon snapshot — and hard-aborts if the live captured set under the 24 differs
from the recon-derived expectation (1,652 raw rows; 1 re-keyed + 1,651 parked).

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    .venv/bin/python scripts/dereg_park_captured_rows_2026_07_13.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

import pandas as pd
from unified_trading_library import (
    gcs_read_object_with_generation,
    get_storage_client,
    resolve_bucket_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dereg_park_captured_rows")

INDEX_BLOB = "_index/availability_index.parquet"
PARKED_BLOB = "_audits/parked_league_rows_20260713.parquet"

DEREG_IDS = frozenset(
    {
        "110",
        "119",
        "122",
        "15066",
        "235",
        "236",
        "239",
        "244",
        "253",
        "254",
        "283",
        "315",
        "32",
        "357",
        "358",
        "362",
        "365",
        "408",
        "493",
        "71",
        "850",
        "LA_LIGA_2",
        "RFPL",
        "SCOTTISH_LEAGUE_CUP_185",
    }
)

#: The one re-keyed atom — excluded from the park set (purge justification (a)).
REKEYED_ATOMS = frozenset({("LA_LIGA_2", "2019-06-04", "MATCHES", "footystats")})

EXPECTED_CAPTURED_RAW = 1652
EXPECTED_PARKED_RAW = 1651


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Default dry-run. Pass to upload the parked parquet.")
    args = parser.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    index_uri = f"gs://{bucket}/{INDEX_BLOB}"  # noqa: gs-uri — one-off script, bucket via resolve_bucket_name
    raw, generation = gcs_read_object_with_generation(index_uri)
    if raw is None:
        logger.error("HARD-ABORT: canonical index absent at %s", index_uri)
        return 3
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Live index: %d rows (generation=%d).", len(df), generation)

    lid = df["league_id"].fillna("").astype(str)
    cap = df[(lid.isin(DEREG_IDS)) & (df["capture_status"] == "captured")].copy()
    logger.info("Captured rows under the 24: %d (expected %d).", len(cap), EXPECTED_CAPTURED_RAW)
    if len(cap) != EXPECTED_CAPTURED_RAW:
        logger.error(
            "HARD-ABORT: live captured count %d != recon expectation %d — index changed since recon; "
            "re-run recon before parking.",
            len(cap),
            EXPECTED_CAPTURED_RAW,
        )
        return 3

    atom_keys = list(
        zip(
            cap["league_id"].astype(str),
            cap["date"].astype(str),
            cap["data_type"].astype(str),
            cap["source"].fillna("").astype(str),
            strict=True,
        )
    )
    rekey_mask = pd.Series([k in REKEYED_ATOMS for k in atom_keys], index=cap.index)
    park = cap.loc[~rekey_mask]
    logger.info(
        "Park set: %d rows (expected %d); re-keyed rows excluded: %d (expected 1).",
        len(park),
        EXPECTED_PARKED_RAW,
        int(rekey_mask.sum()),
    )
    if len(park) != EXPECTED_PARKED_RAW or int(rekey_mask.sum()) != 1:
        logger.error("HARD-ABORT: park/re-key split mismatch.")
        return 3

    logger.info("Per-league park counts:\n%s", park.groupby(park["league_id"].astype(str)).size().to_string())

    if not args.apply:
        logger.info("DRY-RUN complete — no upload performed.")
        return 0

    out = io.BytesIO()
    park.to_parquet(out, index=False)
    payload = out.getvalue()
    storage = get_storage_client()
    storage.upload_bytes(bucket, PARKED_BLOB, payload)
    logger.info("Uploaded parked parquet: gs://%s/%s (%d bytes).", bucket, PARKED_BLOB, len(payload))

    # Verify: re-download, parse, row-count + per-league counts match.
    verify = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, PARKED_BLOB)))
    ok_rows = len(verify) == len(park)
    src_counts = park.groupby(park["league_id"].astype(str)).size().to_dict()
    dst_counts = verify.groupby(verify["league_id"].astype(str)).size().to_dict()
    ok_counts = src_counts == dst_counts
    logger.info("VERIFY re-download: rows=%d match=%s per-league-match=%s", len(verify), ok_rows, ok_counts)
    if not (ok_rows and ok_counts):
        logger.error("HARD-ABORT: parked parquet verification failed.")
        return 3
    logger.info("PARK COMPLETE (source index generation at export: %d).", generation)
    return 0


if __name__ == "__main__":
    sys.exit(main())
