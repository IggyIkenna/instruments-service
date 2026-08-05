#!/usr/bin/env python3
# Epic: manifest_master
# Lifecycle: oneoff
# Delete-when: after the EXTENDED/LIGHTER defi MTDS _index expected_unattempted purge is verified in prod
"""Purge EXTENDED/LIGHTER expected_unattempted rows from the MTDS defi manifest _index.

The root cause (stale defi-bucket instrument catalogue) was fixed at
instruments-service@14746732.  This cleans up the 4,980 (EXTENDED) + 5,976
(LIGHTER) already-materialised expected_unattempted rows in the MTDS defi
market-data-tick bucket's _index — inert placeholders with zero captured data.

Mirrors purge_cefi_perp_defi_contamination_2026_06_25.py's pattern but uses
a temp-file + memory-mapped PyArrow read to bound memory on the 1.69 GB manifest:
  1. SNAPSHOT the current _index -> _index/snapshots/pre_extended_lighter_purge_2026_08_05.parquet
  2. BACKUP the live blob -> _index/availability_index.parquet.extended_lighter_purge.bak
  3. REMOVE rows where asset_group==defi AND venue~(EXTENDED|LIGHTER)
     AND capture_status==expected_unattempted (safety: never touch captured rows)
  4. WRITE back + round-trip VERIFY (0 defi expected_unattempted rows for the 2 venues).

Safe-idempotent: only touches expected_unattempted rows (zero captured data);
snapshot + .bak written before the overwrite; bucket soft-delete retention >=7d.
"""

from __future__ import annotations

import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from unified_trading_library import get_storage_client

PID = "central-element-323112"
BUCKET = f"market-data-tick-defi-prd-{PID}"
BLOB = "_index/availability_index.parquet"
VENUES = ("EXTENDED", "LIGHTER")
APPLY = "--apply" in sys.argv

COL_VENUE = "venue"
COL_ASSET_GROUP = "asset_group"
COL_CAPTURE_STATUS = "capture_status"


def count_purge_targets(pf: pq.ParquetFile) -> tuple[int, int, int, int]:
    """Pass 1: column-projected count of purge targets (lightweight)."""
    total_rows = 0
    purge_count = 0
    cefi_venue_count = 0
    captured_defi_count = 0

    for rg_idx in range(pf.metadata.num_row_groups):
        rg = pf.read_row_group(rg_idx, columns=[COL_VENUE, COL_ASSET_GROUP, COL_CAPTURE_STATUS])
        venue_arr = rg.column(COL_VENUE).cast(pa.string())
        ag_arr = rg.column(COL_ASSET_GROUP).cast(pa.string())
        status_arr = rg.column(COL_CAPTURE_STATUS).cast(pa.string())

        is_venue = pa.array([False] * len(venue_arr))
        for v in VENUES:
            is_venue = pa.compute.or_(is_venue, pa.compute.equal(venue_arr, v))
        is_defi = pa.compute.equal(ag_arr, "defi")
        is_expected = pa.compute.equal(status_arr, "expected_unattempted")
        is_cefi = pa.compute.equal(ag_arr, "cefi")

        total_rows += len(rg)
        purge_count += int(pa.compute.sum(pa.compute.and_(pa.compute.and_(is_venue, is_defi), is_expected)).as_py())
        cefi_venue_count += int(pa.compute.sum(pa.compute.and_(is_venue, is_cefi)).as_py())
        captured_defi_count += int(
            pa.compute.sum(pa.compute.and_(pa.compute.and_(is_venue, is_defi), pa.compute.invert(is_expected))).as_py()
        )

    return total_rows, purge_count, captured_defi_count, cefi_venue_count


def purge_and_write(pf: pq.ParquetFile, schema: pa.Schema, output_path: str) -> tuple[int, int]:
    """Pass 2: read row groups, filter, write incrementally to temp file.

    Uses ParquetWriter to write each filtered row group immediately,
    so peak memory stays bounded by the largest row group (~5MB), not the full 1.7GB corpus.
    """
    kept_rows = 0
    removed_rows = 0
    writer: pq.ParquetWriter | None = None

    try:
        for rg_idx in range(pf.metadata.num_row_groups):
            rg = pf.read_row_group(rg_idx)
            venue_arr = rg.column(COL_VENUE).cast(pa.string())
            ag_arr = rg.column(COL_ASSET_GROUP).cast(pa.string())
            status_arr = rg.column(COL_CAPTURE_STATUS).cast(pa.string())

            is_venue = pa.array([False] * len(venue_arr))
            for v in VENUES:
                is_venue = pa.compute.or_(is_venue, pa.compute.equal(venue_arr, v))
            is_defi = pa.compute.equal(ag_arr, "defi")
            is_expected = pa.compute.equal(status_arr, "expected_unattempted")
            purge_in_rg = pa.compute.and_(pa.compute.and_(is_venue, is_defi), is_expected)
            keep_in_rg = pa.compute.invert(purge_in_rg)

            surviving = rg.filter(keep_in_rg)

            if writer is None:
                writer = pq.ParquetWriter(output_path, schema, compression="snappy")
            writer.write_table(surviving)

            kept_rows += len(surviving)
            removed_rows += int(pa.compute.sum(purge_in_rg).as_py())
    finally:
        if writer is not None:
            writer.close()

    return kept_rows, removed_rows


def main() -> int:
    st = get_storage_client(project_id=PID)

    # Download to a temp file to avoid keeping 1.7GB raw bytes in memory.
    # PyArrow's ParquetFile memory-maps the file, so only actively-read
    # row groups consume RSS — peak memory ~largest row group, not ~full file.
    # Use worktree-relative temp dir — /tmp is a small tmpfs (8GB, shared)
    worktree_tmp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".tmp")
    os.makedirs(worktree_tmp, exist_ok=True)
    tmpdir = tempfile.mkdtemp(prefix="mtds_purge_", dir=worktree_tmp)
    tmp_path = os.path.join(tmpdir, "availability_index.parquet")
    try:
        print(f"Downloading _index to temp file ({tmp_path})...")
        raw = st.download_bytes(BUCKET, BLOB)
        print(f"Downloaded: {len(raw):,} bytes ({len(raw) / 1024 / 1024:.1f} MB)")
        with open(tmp_path, "wb") as f:
            f.write(raw)
        del raw  # Release — ParquetFile will mmap the temp file

        pf = pq.ParquetFile(tmp_path)
        schema = pf.schema_arrow
        print(
            f"Schema: {len(schema)} columns, {pf.metadata.num_row_groups} row groups, {pf.metadata.num_rows:,} total rows"
        )

        # Pass 1: lightweight column-projected count
        print("Pass 1: column-projected count...")
        total_rows, purge_count, captured_defi_count, cefi_venue_count = count_purge_targets(pf)
        print(f"  _index rows={total_rows:,}")
        print(f"  purge (defi+{VENUES}+expected_unattempted)={purge_count:,}")
        print(f"  KEEP defi+{VENUES}+captured={captured_defi_count:,}")
        print(f"  KEEP cefi+{VENUES}={cefi_venue_count:,}")

        if purge_count == 0:
            print("\nNothing to purge — already clean.")
            return 0

        # Verify counts match plan expectations (4,980 EXTENDED + 5,976 LIGHTER = 10,956)
        expected = 4980 + 5976
        if purge_count == expected:
            print(f"  ✅ count matches plan expectation ({expected:,})")
        else:
            print(
                f"  ⚠️  count {purge_count:,} differs from plan expectation {expected:,} — proceeding anyway (trust the data)"
            )

        if not APPLY:
            print("\nDRY-RUN (pass --apply to write). Snapshot + backup + write skipped.")
            return 0

        # Pass 2: read row groups, filter, write incrementally to a second temp file
        print(f"\nPass 2: incremental row-group write to temp file ({total_rows:,} rows)...")
        out_path = os.path.join(tmpdir, "purged_index.parquet")
        kept_rows, removed_rows = purge_and_write(pf, schema, out_path)
        print(f"  kept={kept_rows:,}  removed={removed_rows:,}")
        assert removed_rows == purge_count, f"removed={removed_rows} != purge_count={purge_count}"

        # Close the mmap before snapshot re-read
        del pf

        # 1. snapshot  2. backup  (from the original temp file)
        print("\nWriting snapshot + backup (re-reading from temp file)...")
        with open(tmp_path, "rb") as f:
            raw_for_snapshot = f.read()
        st.upload_bytes(BUCKET, "_index/snapshots/pre_extended_lighter_purge_2026_08_05.parquet", raw_for_snapshot)
        st.upload_bytes(BUCKET, BLOB + ".extended_lighter_purge.bak", raw_for_snapshot)
        del raw_for_snapshot
        print("Snapshot + .bak written")

        # 3. write purged _index
        with open(out_path, "rb") as f:
            out_bytes = f.read()
        print(f"Writing purged _index ({len(out_bytes):,} bytes, {len(out_bytes) / 1024 / 1024:.1f} MB)...")
        st.upload_bytes(BUCKET, BLOB, out_bytes)
        del out_bytes
        print("APPLIED: _index rewritten.")

        # Round-trip verify
        print("\nRound-trip verify...")
        chk_raw = st.download_bytes(BUCKET, BLOB)
        chk = pq.read_table(pa.BufferReader(chk_raw))
        chk_venue = chk.column(COL_VENUE).cast(pa.string())
        chk_ag = chk.column(COL_ASSET_GROUP).cast(pa.string())
        chk_status = chk.column(COL_CAPTURE_STATUS).cast(pa.string())

        is_venue_chk = pa.array([False] * len(chk_venue))
        for v in VENUES:
            is_venue_chk = pa.compute.or_(is_venue_chk, pa.compute.equal(chk_venue, v))
        chk_defi_unattempted = int(
            pa.compute.sum(
                pa.compute.and_(
                    pa.compute.and_(is_venue_chk, pa.compute.equal(chk_ag, "defi")),
                    pa.compute.equal(chk_status, "expected_unattempted"),
                )
            ).as_py()
        )
        chk_cefi = int(pa.compute.sum(pa.compute.and_(is_venue_chk, pa.compute.equal(chk_ag, "cefi"))).as_py())

        print(
            f"VERIFY live: rows={len(chk):,} | defi-{VENUES}-expected_unattempted={chk_defi_unattempted} | cefi-{VENUES}={chk_cefi}"
        )
        assert chk_defi_unattempted == 0, f"defi expected_unattempted rows still present: {chk_defi_unattempted}"
        assert chk_cefi == cefi_venue_count, f"cefi count changed: {chk_cefi} != {cefi_venue_count}"
        print("✅ VERIFY passed — 0 remaining defi expected_unattempted rows for", VENUES)
        return 0

    finally:
        # Clean up temp files
        for p in (tmp_path, os.path.join(tmpdir, "purged_index.parquet")):
            if os.path.exists(p):
                os.unlink(p)
        if os.path.exists(tmpdir):
            os.rmdir(tmpdir)


if __name__ == "__main__":
    raise SystemExit(main())
