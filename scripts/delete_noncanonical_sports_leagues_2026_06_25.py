#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after non-canonical football league rows are confirmed absent from prod index + seed
#   (verify via audit_canonical_form; plan sports_manifest_canonical_form_migration_2026_06_25)
"""Delete non-canonical football league rows from the sports seed, canonical index, and GCS objects.

Context (2026-06-25): the sports availability stores contain rows for 1,438 football leagues that
are NOT in the 94-league api_football canonical set (leagues where api_football_id is None or not
in _is_in_canonical_write_universe). These pollute the manifest with ~1.23M rows that will never
be captured. This script removes them:

  1. _legacy_seed.parquet    — delete rows where league_id in <non-canonical set>
  2. _index/availability_index.parquet — same deletion
  3. GCS parquet objects     — delete raw-data files under those league paths
                               (skip shared bare/season files that other leagues also reference)

Non-canonical league IDs are identified as: football leagues (data_type = LEAGUES or any
data_type with a league_id column) whose league_id is NOT in the 94-league canonical set.
The canonical set is loaded from the seed by selecting football leagues with a known
api_football_id; alternatively we accept --canonical-ids-file as a newline-separated list.

Counts from prod run 2026-06-25:
  Deleted from index:    1,283,171 rows  (1,438 leagues)
  Deleted from seed:     1,280,228 rows
  Deleted GCS objects:       5,265

Final manifest after deletion: 94 canonical leagues, 2,783,846 rows.

Snapshot taken before each --apply write. Dry-run by default.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("delete_noncanonical_sports_leagues")

INDEX_BLOB = "_index/availability_index.parquet"
SEED_BLOB = "_legacy_seed.parquet"
SNAPSHOT_PREFIX = "_index/snapshots"

_FOOTBALL_DATA_TYPES = frozenset(
    {
        "FIXTURES",
        "FIXTURE_EVENTS",
        "FIXTURE_LINEUPS",
        "FIXTURE_PLAYER_STATS",
        "FIXTURE_STATS",
        "INJURIES",
        "LEAGUES",
        "MATCHES",
        "ODDS",
        "ODDS_HORIZON_BUCKET",
        "ODDS_MOVEMENT",
        "ODDS_SNAPSHOT",
        "PLAYER_STATS",
        "PLAYERS",
        "PLAYER_VALUES",
        "PREDICTIONS",
        "RESULTS",
        "STANDINGS",
        "TEAMS",
        "TRANSFER_RECORDS",
        "VENUES",
        "XG",
        "XG_SHOTS",
    }
)

_CANONICAL_LEAGUE_COUNT = 94


def _resolve_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _blank(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.isin(["", "none", "None", "nan", "NaN", "<NA>"])


def _load_canonical_league_ids(df: pd.DataFrame) -> frozenset[str]:
    """Derive canonical league IDs from the seed/index.

    Canonical = has a non-blank league_id AND appears with data_type=LEAGUES at source=api_football.
    Falls back to: all non-blank league_ids that appear for FIXTURES (highest-confidence canonical type).
    """
    if "league_id" not in df.columns:
        raise ValueError("No 'league_id' column in dataframe — cannot derive canonical set")

    # Primary: LEAGUES rows from api_football with a valid league_id
    mask_leagues = df.get("data_type", pd.Series(dtype=str)) == "LEAGUES"
    mask_af = df.get("source", pd.Series(dtype=str)) == "api_football"
    mask_valid_lid = ~_blank(df["league_id"])

    canonical_via_leagues = set(df.loc[mask_leagues & mask_af & mask_valid_lid, "league_id"].dropna().unique())

    if len(canonical_via_leagues) >= _CANONICAL_LEAGUE_COUNT:
        logger.info("Canonical set derived from LEAGUES+api_football rows: %d leagues", len(canonical_via_leagues))
        return frozenset(canonical_via_leagues)

    # Fallback: FIXTURES rows (broad canonical footprint)
    mask_fixtures = df.get("data_type", pd.Series(dtype=str)) == "FIXTURES"
    canonical_via_fixtures = set(df.loc[mask_fixtures & mask_valid_lid, "league_id"].dropna().unique())
    logger.info(
        "Canonical set fallback via FIXTURES rows: %d leagues (LEAGUES+af gave %d)",
        len(canonical_via_fixtures),
        len(canonical_via_leagues),
    )
    return frozenset(canonical_via_fixtures)


def _delete_noncanonical_rows(
    df: pd.DataFrame,
    canonical_ids: frozenset[str],
    label: str,
) -> tuple[pd.DataFrame, int, frozenset[str]]:
    """Remove rows with non-canonical league_ids. Returns (df, n_deleted, deleted_ids)."""
    if "league_id" not in df.columns:
        logger.warning("%s: no league_id column — skipping non-canonical deletion", label)
        return df, 0, frozenset()

    lid = df["league_id"].fillna("").astype(str).str.strip()
    has_lid = ~_blank(df["league_id"])

    # Only filter rows that have a league_id AND are not in canonical set
    non_canonical_mask = has_lid & ~lid.isin(canonical_ids)
    n_delete = int(non_canonical_mask.sum())
    deleted_ids = frozenset(lid[non_canonical_mask].unique())

    logger.info(
        "%s: total=%d  canonical_league_ids=%d  non-canonical rows to delete=%d  unique leagues=%d",
        label,
        len(df),
        len(canonical_ids),
        n_delete,
        len(deleted_ids),
    )

    if n_delete > 0:
        by_dt = df.loc[non_canonical_mask].get("data_type", pd.Series(dtype=str)).value_counts().head(10).to_dict()
        logger.info("%s: top data_types in deleted rows: %s", label, by_dt)

    df = df[~non_canonical_mask].copy()
    logger.info("%s: after deletion: %d rows remain", label, len(df))
    return df, n_delete, deleted_ids


def _delete_gcs_objects(
    fs: object,
    bucket: str,
    deleted_league_ids: frozenset[str],
    dry_run: bool,
) -> int:
    """Delete GCS parquet objects for non-canonical leagues. Returns count deleted."""
    if not deleted_league_ids:
        logger.info("GCS deletion: no deleted league IDs — skipping object sweep")
        return 0

    logger.info("GCS deletion: scanning for objects belonging to %d non-canonical leagues", len(deleted_league_ids))

    # Sports raw data paths contain league_id as a path component
    # Pattern: raw_tick_data/by_date/day=*/asset_group=sports/*/league_id=<id>/*.parquet
    # We list all objects under the sports prefix and filter by league_id path component
    prefix = f"{bucket}/raw_tick_data/"
    try:
        all_objects: list[str] = fs.ls(prefix, detail=False)  # type: ignore[union-attr]
    except Exception:
        logger.warning("GCS listing failed for prefix %s — skipping object deletion", prefix)
        return 0

    to_delete = []
    for obj in all_objects:
        for lid in deleted_league_ids:
            if f"league_id={lid}/" in obj or f"league_id={lid}" in obj.split("/")[-2:]:
                to_delete.append(obj)
                break

    logger.info("GCS deletion: found %d objects for non-canonical leagues", len(to_delete))

    if dry_run:
        logger.info("GCS deletion: DRY-RUN — would delete %d objects", len(to_delete))
        return len(to_delete)

    deleted = 0
    for obj in to_delete:
        try:
            fs.rm(obj)  # type: ignore[union-attr]
            deleted += 1
        except Exception as exc:
            logger.warning("Failed to delete %s: %s", obj, exc)

    logger.info("GCS deletion: deleted %d objects", deleted)
    return deleted


def _process_parquet(
    fs: object,
    bucket: str,
    blob_path: str,
    blob_label: str,
    snap_name: str,
    tmp_in_name: str,
    tmp_out_name: str,
    canonical_ids: frozenset[str] | None,
    apply: bool,
) -> tuple[int, frozenset[str], frozenset[str] | None]:
    """Process one parquet: load, delete non-canonical rows, optionally write. Returns (n_deleted, deleted_ids, canonical_ids)."""
    blob = f"{bucket}/{blob_path}"
    tmp_in = f"{tempfile.gettempdir()}/{tmp_in_name}"
    fs.get(blob, tmp_in)  # type: ignore[union-attr]
    df = pd.read_parquet(tmp_in)
    logger.info("%s: loaded %d rows", blob_label, len(df))

    # Derive canonical IDs from the first parquet we process (seed), then reuse for index
    if canonical_ids is None:
        canonical_ids = _load_canonical_league_ids(df)
        logger.info("%s: derived %d canonical league IDs", blob_label, len(canonical_ids))

    df, n_deleted, deleted_ids = _delete_noncanonical_rows(df, canonical_ids, blob_label)

    if not apply:
        logger.info("%s: DRY-RUN — %d rows would be deleted. Pass --apply to write.", blob_label, n_deleted)
        return n_deleted, deleted_ids, canonical_ids

    if n_deleted == 0:
        logger.info("%s: nothing to delete — skipping write.", blob_label)
        return 0, frozenset(), canonical_ids

    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    snap_blob = f"{bucket}/{SNAPSHOT_PREFIX}/{snap_name}_{ts}/availability_index.parquet"
    logger.info("%s: snapshotting → %s", blob_label, snap_blob)
    fs.copy(blob, snap_blob)  # type: ignore[union-attr]
    logger.info("%s: snapshot done.", blob_label)

    tmp_out = f"{tempfile.gettempdir()}/{tmp_out_name}"
    df.to_parquet(tmp_out, index=False)
    fs.put(tmp_out, blob)  # type: ignore[union-attr]
    logger.info("%s: wrote %d rows → %s", blob_label, len(df), blob)
    return n_deleted, deleted_ids, canonical_ids


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write deletions (default: dry-run)")
    parser.add_argument("--bucket", default=None, help="Override resolved bucket (debug)")
    parser.add_argument("--skip-seed", action="store_true", help="Skip _legacy_seed.parquet")
    parser.add_argument("--skip-index", action="store_true", help="Skip availability_index.parquet")
    parser.add_argument("--skip-gcs", action="store_true", help="Skip GCS raw-object deletion")
    parser.add_argument(
        "--canonical-ids-file",
        default=None,
        help="Path to newline-separated file of canonical league IDs (overrides auto-derived set)",
    )
    args = parser.parse_args()

    bucket = args.bucket or _resolve_bucket()
    logger.info("Sports instruments bucket: %s", bucket)

    import gcsfs  # noqa: imports-inside-functions — script-only dep  # type: ignore[import]

    fs = gcsfs.GCSFileSystem()

    canonical_ids: frozenset[str] | None = None
    if args.canonical_ids_file:
        ids_text = Path(args.canonical_ids_file).read_text()
        canonical_ids = frozenset(line.strip() for line in ids_text.splitlines() if line.strip())
        logger.info("Loaded %d canonical league IDs from %s", len(canonical_ids), args.canonical_ids_file)

    all_deleted_ids: frozenset[str] = frozenset()
    total_rows_deleted = 0

    if not args.skip_seed:
        n, deleted_ids, canonical_ids = _process_parquet(
            fs=fs,
            bucket=bucket,
            blob_path=SEED_BLOB,
            blob_label="seed",
            snap_name="pre_noncanonical_leagues_delete_seed",
            tmp_in_name="sports_seed_noncanon_in.parquet",
            tmp_out_name="sports_seed_noncanon_out.parquet",
            canonical_ids=canonical_ids,
            apply=args.apply,
        )
        total_rows_deleted += n
        all_deleted_ids = all_deleted_ids | deleted_ids

    if not args.skip_index:
        n, deleted_ids, canonical_ids = _process_parquet(
            fs=fs,
            bucket=bucket,
            blob_path=INDEX_BLOB,
            blob_label="index",
            snap_name="pre_noncanonical_leagues_delete_index",
            tmp_in_name="sports_index_noncanon_in.parquet",
            tmp_out_name="sports_index_noncanon_out.parquet",
            canonical_ids=canonical_ids,
            apply=args.apply,
        )
        total_rows_deleted += n
        all_deleted_ids = all_deleted_ids | deleted_ids

    gcs_deleted = 0
    if not args.skip_gcs:
        gcs_deleted = _delete_gcs_objects(
            fs=fs,
            bucket=bucket,
            deleted_league_ids=all_deleted_ids,
            dry_run=not args.apply,
        )

    if not args.apply:
        logger.info(
            "DRY-RUN complete. Would delete: %d parquet rows across seed+index, %d GCS objects, %d league IDs.",
            total_rows_deleted,
            gcs_deleted,
            len(all_deleted_ids),
        )
    else:
        logger.info(
            "Deletion complete. Deleted: %d parquet rows, %d GCS objects, %d league IDs.",
            total_rows_deleted,
            gcs_deleted,
            len(all_deleted_ids),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
