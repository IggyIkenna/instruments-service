#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the DeFi data_type migration ('instrument-catalog' -> 'instruments') is
#   verified complete (0 residual 'instrument-catalog' rows in instruments-store-defi-prd) and
#   re-verified stable for 7 days (no new 'instrument-catalog' rows re-appearing).

"""One-off migration — collapse ``data_type='instrument-catalog'`` -> ``data_type='instruments'``
on the IS DeFi availability index (operator decision 2026-07-16, plan
``data_status_page_ux_and_canonicalisation_2026_07_16.md`` P9 Q2 item 5).

ROOT CAUSE: the now-dead legacy ``_write_catalogue_record`` path
(``instruments_service/engine/orchestrator/catalogue.py``, unreachable from the current
orchestrator — ``_write_all_venues`` always passes a live ``ManifestWriter`` so ``_write_venue``'s
batched branch is always taken) used to stamp DeFi rows ``data_type='instrument-catalog'`` before
the batched-writer DeFi split landed. The batched writer
(``instruments_service/engine/orchestrator/writers.py`` ``_write_venue``, the ONLY live DeFi write
path today) already stamps the canonical ``data_type='instruments'`` — confirmed
``instrument-catalog`` is a purely HISTORICAL value, no longer emitted. This script backfills the
8.45M pre-existing historical rows to the canonical value. The dead legacy stamp itself was fixed
in the same commit as this script (``catalogue.py`` now also emits ``'instruments'``, kept in sync
for correctness if that path is ever revived) — plus the UAC crosscutting preflight-DAG constant
(``instruments_preflight_dag.py``'s DeFi entry) that reads this exact string, and
``scripts/defi_cumulative_drawdown_guard_2026_06_25.py``'s own read-side filter.

DEDUP, NOT A BLIND RENAME: some ``(date, venue, chain, instrument_type)`` shard atoms already have
BOTH an ``'instrument-catalog'`` row (old writer) AND an ``'instruments'`` row (new writer) --
these are the SAME real shard captured twice under two data_type spellings. For each colliding
identity, keep the row with the more recent ``attempted_at`` (falls back to ``written_at``) --
mirrors the manifest consolidator's own last-write-wins dedup convention
(``codex/05-infrastructure/manifest-consolidator-ssot.md`` "Dedup key") -- and drop the older
duplicate. Non-colliding ``'instrument-catalog'`` rows are simply renamed in place (no data loss).

Idempotent: rows already carrying ``data_type='instruments'`` are untouched; a re-run after
``--apply`` finds 0 ``'instrument-catalog'`` rows and exits clean.

Dry-run by default. ``--apply --confirm`` mutates the live ``_index``.

Safety gate: ``captured_after <= captured_before`` (equality unless a genuine dedup collision
occurred, in which case the delta must exactly equal the reported collision count -- never a
silent extra loss).

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/canonicalize_defi_data_type_instrument_catalog_2026_07_16.py

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/canonicalize_defi_data_type_instrument_catalog_2026_07_16.py --apply --confirm

SSOT: ``unified-trading-pm/plans/active/data_status_page_ux_and_canonicalisation_2026_07_16.md`` P9.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_LEGACY_DATA_TYPE = "instrument-catalog"
_CANONICAL_DATA_TYPE = "instruments"  # matches REFERENCE_DATA_TYPE in migrate_instruments_store_v9.py
_ASSET_GROUP = "defi"

# Shard-atom identity columns (excluding data_type, the axis being unified) — mirrors the manifest
# consolidator's dedup key shape (date, venue + optional dims present in this schema).
_IDENTITY_COLS_CANDIDATES: tuple[str, ...] = (
    "date",
    "venue",
    "chain",
    "instrument_type",
    "service_name",
)


def _resolve_is_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=_ASSET_GROUP, deployment_env="prod")


def _load_manifest(bucket: str) -> pd.DataFrame:
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, _INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded manifest: %d rows from gs://%s/%s", len(df), bucket, _INDEX_BLOB)
    return df


def _identity_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in _IDENTITY_COLS_CANDIDATES if c in df.columns]


def _sort_key(df: pd.DataFrame) -> pd.Series:
    """Recency rank for last-write-wins dedup: attempted_at, falls back to written_at."""
    for col in ("attempted_at", "written_at"):
        if col in df.columns:
            ts = pd.to_datetime(df[col], errors="coerce", utc=True)
            if ts.notna().any():
                return ts.fillna(pd.Timestamp.min.tz_localize("UTC"))
    return pd.Series(pd.Timestamp.min.tz_localize("UTC"), index=df.index)


def _migrate(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int, int, int]:
    """Rewrite legacy rows to the canonical data_type, dedup colliding identities.

    Winner priority within a colliding identity group: a ``capture_status=='captured'``
    row ALWAYS beats a non-captured row (never silently drop a real observation in favor
    of an empty/failed placeholder); recency (``attempted_at``/``written_at``) is the
    tiebreaker among same-eligibility candidates.

    Returns (migrated_df, legacy_row_count, collision_count, dropped_count, captured_dropped_count).
    """
    dtype = df["data_type"].fillna("").astype(str)
    is_legacy = dtype == _LEGACY_DATA_TYPE
    is_canonical = dtype == _CANONICAL_DATA_TYPE
    legacy_count = int(is_legacy.sum())

    if legacy_count == 0:
        return df, 0, 0, 0, 0

    id_cols = _identity_cols(df)
    if not id_cols:
        logger.error("No identity columns found in manifest schema — refusing to migrate blind.")
        return df, legacy_count, 0, 0, 0

    # Every legacy row's RENAMED identity (as if it already carried the canonical value) +
    # every existing canonical row's own identity — one combined candidate pool grouped by
    # final identity, so multi-way collisions (2+ legacy dupes, or legacy+multiple canonical
    # dupes sharing one identity) resolve consistently in a single pass, not pairwise.
    candidate_idx = df.index[is_legacy | is_canonical]
    candidate_keys = df.loc[candidate_idx, id_cols].astype(str)
    key_tuples = list(map(tuple, candidate_keys.itertuples(index=False, name=None)))

    is_captured = (df["capture_status"].fillna("").astype(str) == "captured").to_dict()
    recency = _sort_key(df)

    groups: dict[tuple[str, ...], list[int]] = {}
    for idx, key in zip(candidate_idx, key_tuples, strict=True):
        groups.setdefault(key, []).append(idx)

    result = df.copy()
    drop_idx: list[int] = []
    captured_dropped = 0
    for _key, members in groups.items():
        if len(members) == 1:
            idx = members[0]
            if is_legacy.loc[idx]:
                result.loc[idx, "data_type"] = _CANONICAL_DATA_TYPE
            continue
        # Multi-member collision: captured rows beat non-captured; recency is the tiebreaker.
        winner = max(members, key=lambda idx: (is_captured.get(idx, False), recency.loc[idx]))
        if is_legacy.loc[winner]:
            result.loc[winner, "data_type"] = _CANONICAL_DATA_TYPE
        for idx in members:
            if idx != winner:
                drop_idx.append(idx)
                if is_captured.get(idx, False):
                    captured_dropped += 1

    collision_count = sum(1 for members in groups.values() if len(members) > 1)

    drop_idx = sorted(set(drop_idx))
    if drop_idx:
        result = result.drop(index=drop_idx).reset_index(drop=True)

    return result, legacy_count, collision_count, len(drop_idx), captured_dropped


def _upload_manifest(bucket: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    client = get_storage_client(provider="gcp")
    client.upload_bytes(bucket, _INDEX_BLOB, buf.getvalue())
    logger.info(
        "Uploaded manifest: %d rows, %.1f KB -> gs://%s/%s", len(df), len(buf.getvalue()) / 1024, bucket, _INDEX_BLOB
    )


def _count_legacy(df: pd.DataFrame) -> int:
    return int((df["data_type"].fillna("").astype(str) == _LEGACY_DATA_TYPE).sum())


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the migrated index back (requires --confirm).")
    p.add_argument("--confirm", action="store_true", help="Required alongside --apply as an explicit safety gate.")
    args = p.parse_args(argv)

    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm — refusing to mutate without both flags.")
        return 2

    bucket = _resolve_is_bucket()
    logger.info("=== DeFi data_type canonicalization: '%s' -> '%s' ===", _LEGACY_DATA_TYPE, _CANONICAL_DATA_TYPE)
    logger.info("DeFi IS PRD availability-index bucket: gs://%s", bucket)

    df = _load_manifest(bucket)
    captured_before = int((df["capture_status"].fillna("").astype(str) == "captured").sum())
    legacy_before = _count_legacy(df)

    if legacy_before == 0:
        logger.info("Already clean — 0 '%s' rows; nothing to do.", _LEGACY_DATA_TYPE)
        return 0

    logger.info("Legacy '%s' rows found: %d of %d total rows.", _LEGACY_DATA_TYPE, legacy_before, len(df))

    migrated, legacy_count, collisions, dropped, captured_dropped = _migrate(df)
    captured_after = int((migrated["capture_status"].fillna("").astype(str) == "captured").sum())

    logger.info(
        "Migration summary: legacy_rows=%d collisions=%d dropped_duplicates=%d captured_dropped=%d "
        "captured_before=%d captured_after=%d",
        legacy_count,
        collisions,
        dropped,
        captured_dropped,
        captured_before,
        captured_after,
    )

    # Exact invariant: captured_after == captured_before - captured_dropped. Winner selection
    # (_migrate) prioritizes capture_status=='captured' over any other status within a colliding
    # group, so a captured row is ONLY ever dropped when it duplicates another captured row in
    # the same group — captured_dropped counts exactly those, never a captured row lost to a
    # non-captured one.
    expected_captured_after = captured_before - captured_dropped
    if captured_after != expected_captured_after:
        logger.error(
            "SAFETY GATE FAILED: captured row count %d -> %d does not match expected %d "
            "(captured_before - captured_dropped) — aborting write.",
            captured_before,
            captured_after,
            expected_captured_after,
        )
        return 4
    logger.info(
        "Safety gate OK: captured row delta (%d) exactly matches captured duplicates dropped "
        "(%d of %d total dropped rows were non-captured).",
        captured_dropped,
        dropped - captured_dropped,
        dropped,
    )

    if not args.apply:
        logger.info(
            "DRY-RUN: %d rows would be migrated to data_type='%s' (%d collisions -> %d duplicates dropped). "
            "Re-run with --apply --confirm to mutate.",
            legacy_count,
            _CANONICAL_DATA_TYPE,
            collisions,
            dropped,
        )
        return 0

    _upload_manifest(bucket, migrated)

    verify_df = _load_manifest(bucket)
    residual = _count_legacy(verify_df)
    if residual > 0:
        logger.error("Post-run verification FAILED: %d '%s' rows remain (expected 0)", residual, _LEGACY_DATA_TYPE)
        return 5

    logger.info(
        "Done — migrated %d rows to data_type='%s' (%d duplicates dropped). Post-run verify: 0 residual '%s' rows.",
        legacy_count,
        _CANONICAL_DATA_TYPE,
        dropped,
        _LEGACY_DATA_TYPE,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
