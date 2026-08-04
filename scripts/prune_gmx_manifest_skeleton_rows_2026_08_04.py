# Epic: defi_master
# Lifecycle: one-off
# Delete-when: after confirmed prod-run of this script (2026-08-04)
"""Prune 4 stale venue=GMX expected_unattempted skeleton rows from the defi manifest.

Issue: plans/active/issues/defi_gmx_expected_skeleton_rows_still_enumerated_2026_08_04.md [DATA] P1

Root-cause: an older enumerator run seeded these rows before the IS catalog was cleaned (2026-07-25).
The incremental manifest consolidator preserves them since no shard has since overwritten them.
The clean catalog means the daily 01:30 UTC enumerator will NOT re-create them after pruning.

Exact rows (verified by AO slot-16 [DIAG] P1, 2026-08-04):
  venue=GMX, chain=ARBITRUM, instrument_type=pool, date=2026-08-04,
  instrument_id=0x489ee077994b6658eafa855c308275ead8097c4a,
  data_type in {dex_pool_state, dex_pool_swaps, governance_events, position_data}

Implementation: DuckDB streaming (avoids OOM — full pandas load of 1.75GB parquet peaks ~7GB RSS;
DuckDB processes row groups in a bounded buffer, <400MB peak RSS).

Safety: dry-run by default; pass --apply to write. Asserts exactly 4 matching rows with
  correct column values before writing. Preserves captured-count invariant.

Reversibility: manifest bucket soft-delete retention = 604800s (>=604800s, per [DATA] P1 note)
  — reversed by restoring the parquet from GCS version history within 7 days.

Run via:
  cd instruments-service
  GCP_PROJECT_ID=central-element-323112 \\
  bash ../unified-trading-pm/scripts/dev/run-bounded-analysis.sh -- \\
      .venv/bin/python scripts/prune_gmx_manifest_skeleton_rows_2026_08_04.py [--apply]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile

import duckdb

from unified_trading_library.core.client_factory import get_storage_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Bucket name hardcoded per the measure_honest_coverage.py convention:
# scripts/ excluded from inline-URI QG ratchet (instruments-service/scripts/quality-gates.sh CLOUD_SDK_EXCLUDE_GLOBS)
_BUCKET = "market-data-tick-defi-prd-central-element-323112"
_INDEX_BLOB = "_index/availability_index.parquet"

# Precise predicate for the 4 stale rows
_TARGET_VENUE = "GMX"
_TARGET_CAPTURE_STATUS = "expected_unattempted"
_EXPECTED_ROW_COUNT = 4

# Column-level assertions (belt-and-suspenders)
_EXPECTED_CHAIN = "ARBITRUM"
_EXPECTED_INSTRUMENT_TYPE = "pool"
_EXPECTED_INSTRUMENT_ID = "0x489ee077994b6658eafa855c308275ead8097c4a"
_EXPECTED_DATA_TYPES = frozenset({"dex_pool_state", "dex_pool_swaps", "governance_events", "position_data"})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the pruned manifest back (default: dry-run)")
    args = parser.parse_args()
    dry_run = not args.apply

    storage = get_storage_client()
    log.info("Downloading manifest gs://%s/%s ...", _BUCKET, _INDEX_BLOB)
    raw = storage.download_bytes(_BUCKET, _INDEX_BLOB)
    log.info("Downloaded %d bytes (%.1f MB)", len(raw), len(raw) / 1024 / 1024)

    # Write to temp file — DuckDB parquet scanning works best from a file path
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_in:
        tmp_in.write(raw)
        tmp_in_path = tmp_in.name
    del raw  # release the raw bytes immediately

    try:
        conn = duckdb.connect()

        # 1. Count total rows
        total_before = conn.execute(f"SELECT count(*) FROM read_parquet('{tmp_in_path}')").fetchone()[0]  # type: ignore[index]
        log.info("Manifest total rows: %d", total_before)

        # 2. Find the target rows
        gmx_rows = conn.execute(
            f"""
            SELECT venue, capture_status, date, chain, instrument_type, instrument_id, data_type
            FROM read_parquet('{tmp_in_path}')
            WHERE venue = '{_TARGET_VENUE}' AND capture_status = '{_TARGET_CAPTURE_STATUS}'
            """
        ).fetchall()
        n_drop = len(gmx_rows)

        if n_drop == 0:
            log.info("No venue=%s / capture_status=%s rows found — already pruned.", _TARGET_VENUE, _TARGET_CAPTURE_STATUS)
            return 0

        log.info("Candidate rows to prune: %d", n_drop)
        for row in gmx_rows:
            log.info("  %s", row)

        # Safety gate 1: exact row count
        if n_drop != _EXPECTED_ROW_COUNT:
            log.error(
                "Expected exactly %d rows but found %d — aborting (unexpected GMX rows)",
                _EXPECTED_ROW_COUNT, n_drop,
            )
            return 1

        # Safety gate 2: column-value assertions
        actual_chains = {str(r[3]) for r in gmx_rows}
        actual_instrument_types = {str(r[4]) for r in gmx_rows}
        actual_instrument_ids = {str(r[5]) for r in gmx_rows}
        actual_data_types = {str(r[6]) for r in gmx_rows}

        for col, actual, expected in (
            ("chain", actual_chains, {_EXPECTED_CHAIN}),
            ("instrument_type", actual_instrument_types, {_EXPECTED_INSTRUMENT_TYPE}),
            ("instrument_id", actual_instrument_ids, {_EXPECTED_INSTRUMENT_ID}),
            ("data_type", actual_data_types, _EXPECTED_DATA_TYPES),
        ):
            if actual != expected:
                log.error("Column %s: found %s, expected %s — aborting", col, sorted(actual), sorted(expected))
                return 1

        log.info("All safety gates passed (exact row count=%d, column values match expected).", n_drop)

        # Safety gate 3: captured-count invariant (none of the target rows should be captured)
        captured_before = conn.execute(
            f"SELECT count(*) FROM read_parquet('{tmp_in_path}') WHERE capture_status = 'captured'"
        ).fetchone()[0]  # type: ignore[index]

        if dry_run:
            log.info(
                "DRY-RUN: would prune %d rows from gs://%s/%s (manifest would have %d rows). "
                "Re-run with --apply to write.",
                n_drop, _BUCKET, _INDEX_BLOB, total_before - n_drop,
            )
            return 0

        # Apply: write filtered parquet to temp output file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_out:
            tmp_out_path = tmp_out.name

        try:
            conn.execute(
                f"""
                COPY (
                    SELECT * FROM read_parquet('{tmp_in_path}')
                    WHERE NOT (venue = '{_TARGET_VENUE}' AND capture_status = '{_TARGET_CAPTURE_STATUS}')
                )
                TO '{tmp_out_path}' (FORMAT PARQUET)
                """
            )

            # Verify output counts
            total_after = conn.execute(f"SELECT count(*) FROM read_parquet('{tmp_out_path}')").fetchone()[0]  # type: ignore[index]
            captured_after = conn.execute(
                f"SELECT count(*) FROM read_parquet('{tmp_out_path}') WHERE capture_status = 'captured'"
            ).fetchone()[0]  # type: ignore[index]

            if total_after != total_before - n_drop:
                log.error(
                    "Row count mismatch after prune: expected %d but got %d — aborting, not uploading",
                    total_before - n_drop, total_after,
                )
                return 1

            if captured_after != captured_before:
                log.error(
                    "Captured-count invariant violated: %d -> %d — aborting, not uploading",
                    captured_before, captured_after,
                )
                return 1

            # Upload the pruned parquet
            with open(tmp_out_path, "rb") as f:
                pruned_bytes = f.read()
            storage.upload_bytes(_BUCKET, _INDEX_BLOB, pruned_bytes)
            log.info(
                "APPLIED: pruned %d rows. Manifest now has %d rows (was %d). Captured count preserved at %d.",
                n_drop, total_after, total_before, captured_after,
            )
        finally:
            if os.path.exists(tmp_out_path):
                os.unlink(tmp_out_path)

    finally:
        if os.path.exists(tmp_in_path):
            os.unlink(tmp_in_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
