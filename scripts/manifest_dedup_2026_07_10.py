#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after every asset-group's availability_index.parquet is confirmed
#   durably deduplicated under this tool AND the root-cause (manifest_consolidator's
#   incremental-merge `survivors` CTE lacking self-dedup, unified-trading-library@0de04b6e)
#   has stayed fixed for a full audit cycle.
"""One-off, per-asset-group: remove genuine (identical-grain AND identical-
capture_status) duplicate rows from a manifest.

Generalized from the DeFi-only `defi_manifest_dedup_2026_07_10.py` (now deleted)
per the tracking doc's own P2 follow-up. DeFi's incident found 1,789,793 genuine
duplicate rows (of 4,630,138 same-key rows; the other 2,840,345 had differing
capture_status — legitimate state-transition history, left untouched) caused by
the shared consolidator's incremental merge streaming untouched canonical rows
through with zero self-dedup — fixed at the source in
`unified_trading_library/manifest_consolidator.py` (`unified-trading-library@0de04b6e`).
That fix stops NEW duplicates fleet-wide, but pre-existing duplicate rows already
sitting in each asset group's manifest needed this same one-off cleanup treatment
independently, since the fix only prevents future accumulation.

A 2026-07-10 cross-asset-group scan (correct per-schema grain, see `_GRAIN_EXCLUDE`
below) found: cefi 0, defi 0 (already cleaned by the DeFi-specific run), sports 0,
tradfi 346 rows / 173 groups, prediction 6,284 rows / 3,142 groups. An earlier,
naive scan using DeFi's grain columns falsely flagged ~1.79M "duplicate" sports
rows — sports data legitimately varies by `league_id`/`timeframe`, columns that
naive key omitted. **Lesson embedded in this tool**: the dedup grain key is
auto-detected per manifest as "every present column except known write-metadata"
(`_GRAIN_EXCLUDE`), not a hardcoded DeFi-shaped list, specifically to avoid
repeating that false-positive class of mistake against a different asset group's
schema.

2026-07-10 (later): rewritten from pandas to DuckDB after the defi backlog-resume
merge (18.7M new rows landing in one window) grew the canonical to 27.5M rows —
pandas groupby/drop_duplicates over that many rows repeatedly OOM'd or stalled for
20+ minutes on a laptop-class host; DuckDB (already proven in
`manifest_consolidator.py` handling this exact bucket in ~4 minutes) does the same
work in a bounded-memory, vectorized engine. Also fixed a false-negative in the
original pandas version: NULL and "" were not both normalized to the same sentinel,
missing 61,372 genuine duplicate rows where two different producers (e.g.
market-tick-data-service vs instruments-service) wrote the same logical fact using
different empty-value representations.

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python scripts/manifest_dedup_2026_07_10.py --asset-group tradfi --dry-run
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python scripts/manifest_dedup_2026_07_10.py --asset-group tradfi --apply

SSOT: unified-trading-pm/plans/active/issues/defi_manifest_consolidator_duplicate_race_2026_07_10.md
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
from datetime import UTC, datetime

from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_MANIFEST_PATH = "_index/availability_index.parquet"
_QUARANTINE_PREFIX = "_migration_backup/manifest_dedup_2026_07_10/"
_DUCKDB_MEMORY_LIMIT = os.environ.get("MANIFEST_DEDUP_DUCKDB_MEMORY_LIMIT", "8GB")  # noqa: qg-os-environ — operational tunable for this CLI one-off, not service config
_SENTINEL = "__MANIFEST_DEDUP_NULL_4e8a2__"

# Pure write-metadata/metrics columns: never grain-defining, always excluded from
# the dedup key. capture_status is handled separately (matched, not grouped-out).
_GRAIN_EXCLUDE = frozenset(
    {
        "written_at",
        "attempted_at",
        "last_emission_decision_at",
        "instrument_count",
        "expected",
        "available",
        "row_count",
        "expected_window_completeness_fraction",
        "error_reason",
        "service_name",
        "schema_version",
        "service_emission_state",
        "source",
        "transport",
        "job_id",
        "cadence",
        "enumerator_run_id",
        "capture_status",
    }
)

_ASSET_GROUPS = ("cefi", "defi", "tradfi", "sports", "prediction")


def _resolve_bucket(asset_group: str) -> str:
    if asset_group == "prediction":
        # Dedicated flat-key bucket naming (not kind="market-data" + asset_group).
        return resolve_bucket_name(cloud="gcp", kind="market-data-tick-prediction")
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group=asset_group)


def _key_sql(col_expr: str) -> str:
    # NULL and "" both mean "not applicable" for an unset optional dimension —
    # different producers write different representations for the SAME logical
    # absence. Collapse both to one sentinel so a genuine duplicate straddling two
    # producers is caught (2026-07-10, found via a 61,372-row false-negative).
    return f"COALESCE(NULLIF(CAST({col_expr} AS VARCHAR), ''), '{_SENTINEL}')"


def main(argv: list[str] | None = None) -> int:
    import duckdb  # noqa: imports-inside-functions — deferred: duckdb is a heavy dep, only needed on this CLI path

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--asset-group", required=True, choices=_ASSET_GROUPS)
    ap.add_argument("--apply", action="store_true", help="Write the deduplicated manifest (default: report only).")
    args = ap.parse_args(argv)

    bucket = _resolve_bucket(args.asset_group)
    manifest_uri = f"gs://{bucket}/{_MANIFEST_PATH}"
    logger.info("asset_group=%s bucket=%s manifest=%s", args.asset_group, bucket, manifest_uri)

    st = get_storage_client()
    with tempfile.TemporaryDirectory(prefix="manifest-dedup-") as tmp:
        local_in = os.path.join(tmp, "in.parquet")
        raw = st.download_bytes(bucket, _MANIFEST_PATH)
        with open(local_in, "wb") as fh:
            fh.write(raw)
        logger.info("downloaded manifest: %d bytes", len(raw))

        con = duckdb.connect()
        con.execute(f"SET memory_limit='{_DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET temp_directory='{tmp}'")

        total_rows = con.execute(f"SELECT count(*) FROM read_parquet('{local_in}')").fetchone()[0]  # nosec B608
        logger.info("total manifest rows: %d", total_rows)

        columns = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{local_in}')").fetchall()]  # nosec B608
        grain_cols = [c for c in columns if c not in _GRAIN_EXCLUDE]
        logger.info("auto-detected grain key (%d cols): %s", len(grain_cols), grain_cols)

        part_norm = ", ".join(_key_sql(c) for c in grain_cols)
        order_by = "cast(written_at AS VARCHAR) DESC NULLS LAST"

        local_out = os.path.join(tmp, "out.parquet")
        con.execute("SET preserve_insertion_order=false")
        con.execute(
            f"""COPY (
                WITH scoped AS (
                    SELECT * FROM read_parquet('{local_in}') WHERE asset_group = '{args.asset_group}'
                ),
                deduped AS (
                    SELECT * EXCLUDE (rn) FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY {part_norm}, capture_status ORDER BY {order_by}
                        ) AS rn
                        FROM scoped
                    ) WHERE rn = 1
                )
                SELECT * FROM deduped
            ) TO '{local_out}' (FORMAT parquet)"""  # nosec B608
        )

        scoped_rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{local_in}') WHERE asset_group = '{args.asset_group}'"
        ).fetchone()[0]  # nosec B608
        deduped_rows = con.execute(f"SELECT count(*) FROM read_parquet('{local_out}')").fetchone()[0]  # nosec B608
        removed = scoped_rows - deduped_rows
        logger.info(
            "%s rows before=%d after=%d removed=%d genuine duplicates",
            args.asset_group,
            scoped_rows,
            deduped_rows,
            removed,
        )

        # Safety: row count for (grain)-DIFFERING-status groups must be unchanged (we
        # only ever collapse identical (grain, capture_status) groups, never touch
        # legitimate state-transition rows).
        multi_status_before = con.execute(
            f"""SELECT count(*) FROM (
                SELECT {part_norm} FROM read_parquet('{local_in}') WHERE asset_group = '{args.asset_group}'
                GROUP BY {part_norm} HAVING count(DISTINCT capture_status) > 1
            )"""  # nosec B608
        ).fetchone()[0]
        multi_status_after = con.execute(
            f"""SELECT count(*) FROM (
                SELECT {part_norm} FROM read_parquet('{local_out}')
                GROUP BY {part_norm} HAVING count(DISTINCT capture_status) > 1
            )"""  # nosec B608
        ).fetchone()[0]
        con.close()

        if multi_status_before != multi_status_after:
            logger.error(
                "SAFETY CHECK FAILED: multi-status key-group count changed (%d -> %d) — "
                "legitimate state-transition rows may have been touched. Aborting.",
                multi_status_before,
                multi_status_after,
            )
            return 1
        logger.info("safety check OK: multi-status (legitimate transition) groups unchanged at %d", multi_status_before)

        if not args.apply:
            logger.info("DRY-RUN: would remove %d genuine duplicate rows. Pass --apply to write.", removed)
            return 0

        if removed == 0:
            logger.info("Nothing to remove.")
            return 0

        ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        backup_dest = f"{_QUARANTINE_PREFIX}{args.asset_group}_availability_index_pre_dedup_{ts}.parquet"
        logger.info("Backing up pre-dedup manifest to gs://%s/%s", bucket, backup_dest)
        st.upload_bytes(bucket, backup_dest, raw)
        verify_raw = st.download_bytes(bucket, backup_dest)
        if len(verify_raw) != len(raw):
            logger.error("Backup verify failed (size mismatch) — aborting before touching the live manifest.")
            return 1
        logger.info("Backup verified (%d bytes).", len(raw))

        con2 = duckdb.connect()
        con2.execute(f"SET memory_limit='{_DUCKDB_MEMORY_LIMIT}'")
        con2.execute(f"SET temp_directory='{tmp}'")
        con2.execute("SET preserve_insertion_order=false")
        local_final = os.path.join(tmp, "final.parquet")
        con2.execute(
            f"""COPY (
                SELECT * FROM read_parquet('{local_in}') WHERE asset_group != '{args.asset_group}'
                UNION ALL
                SELECT * FROM read_parquet('{local_out}')
            ) TO '{local_final}' (FORMAT parquet)"""  # nosec B608
        )
        con2.close()

        with open(local_final, "rb") as fh:
            out_bytes = fh.read()
        logger.info("writing deduplicated manifest: removed %d rows", removed)
        st.upload_bytes(bucket, _MANIFEST_PATH, out_bytes)
        logger.info("DONE. Removed %d genuine duplicate rows. Backup: gs://%s/%s", removed, bucket, backup_dest)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
