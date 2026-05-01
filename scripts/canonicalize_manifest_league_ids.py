#!/usr/bin/env python3
"""Canonicalize numeric ``league_id`` values in the SPORTS manifest.

Walks every captured per-league row, and any row whose ``league_id`` is an
all-digits string (a legacy api_football_id like ``39`` for EPL) gets
rewritten to the canonical league_id (``EPL``) via UAC's
``get_league_by_api_football_id`` reverse lookup.

Writes the rewrites to a per-VM shard so the consolidator daemon merges
them into canonical (direct canonical writes get overwritten on next
consolidator cycle).

Companion to ``canonicalize_gcs_league_paths.py`` (which renames the
on-disk paths) and to the orchestrator's ``_canonical_league_id`` helper
(which prevents new writes from accumulating numeric paths).

Idempotent.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import tempfile
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage
from unified_api_contracts.sports import get_league_by_api_football_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_SHARD_BLOB = "_index/per_vm/manifest-canonicalize-league-ids.parquet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    client = storage.Client(project="central-element-323112")
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(INDEX_BLOB)
    blob.download_to_filename(f"{tempfile.gettempdir()}/canon_pre_normalize.parquet")
    df = pd.read_parquet(f"{tempfile.gettempdir()}/canon_pre_normalize.parquet")
    logger.info("Manifest rows: %d", len(df))

    captured = df[df["capture_status"] == "captured"]
    per_lg = captured[captured["league_id"].fillna("").astype(str) != ""]
    logger.info("Per-league captured rows: %d", len(per_lg))

    numeric_mask = per_lg["league_id"].astype(str).str.fullmatch(r"\d+")
    numeric_rows = per_lg[numeric_mask]
    logger.info("Numeric league_id rows to canonicalize: %d", len(numeric_rows))
    if numeric_rows.empty:
        logger.info("Nothing to canonicalize.")
        return 0

    by_dt = numeric_rows.groupby("data_type").size().sort_values(ascending=False)
    logger.info("Numeric rows by data_type:\n%s", by_dt.to_string())

    distinct_numerics = sorted(numeric_rows["league_id"].astype(str).unique())
    logger.info("Distinct numeric league_ids: %d", len(distinct_numerics))

    af_to_canonical: dict[str, str] = {}
    unmapped: list[str] = []
    for raw in distinct_numerics:
        try:
            league = get_league_by_api_football_id(int(raw))
        except Exception:
            league = None
        if league is not None:
            af_to_canonical[raw] = league.league_id
        else:
            unmapped.append(raw)
    logger.info("Mapped: %d  unmapped: %d", len(af_to_canonical), len(unmapped))
    if unmapped:
        logger.info("Sample unmapped numeric IDs: %s", unmapped[:30])

    rewritten_rows: list[dict[str, object]] = []
    now_iso = datetime.now(UTC).isoformat()
    for _, row in numeric_rows.iterrows():
        raw = str(row["league_id"]).strip()
        canonical = af_to_canonical.get(raw)
        if canonical is None:
            continue
        new_row = row.to_dict()
        new_row["league_id"] = canonical
        new_row["error_reason"] = "canonicalized_from_numeric_af_league_id"
        new_row["written_at"] = now_iso
        new_row["attempted_at"] = now_iso
        rewritten_rows.append(new_row)
    logger.info("Will write %d canonicalized per-league rows", len(rewritten_rows))

    if args.dry_run:
        logger.info("DRY RUN — no manifest changes")
        return 0

    if rewritten_rows:
        new_df = pd.DataFrame(rewritten_rows)
        out = io.BytesIO()
        new_df.to_parquet(out, index=False)
        out.seek(0)
        bucket.blob(PER_VM_SHARD_BLOB).upload_from_file(out, content_type="application/octet-stream")
        logger.info(
            "Per-VM shard written: %d canonicalized rows at gs://%s/%s",
            len(new_df),
            BUCKET,
            PER_VM_SHARD_BLOB,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
