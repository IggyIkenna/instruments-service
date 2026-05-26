#!/usr/bin/env python3
"""Reconcile manifest rows after entity (venue/data_type/chain/etc.) add or remove.

--mode remove:
  Walks the canonical availability_index.parquet for the given asset_group.
  Finds all rows where <entity_type> == <entity_key> (the removed entity).
  Flips non-tombstoned rows to attempted_failed / REMOVED_ENTITY_TOMBSTONE.
  Emits an audit CSV to --output-csv (or an auto-generated path under
  unified-trading-pm/audits/entity_lifecycle/by_date/day=YYYY-MM-DD/).
  Idempotent: rows already at REMOVED_ENTITY_TOMBSTONE are skipped.
  Dry-run by default — pass --no-dry-run to write back.

--mode add:
  NOT YET IMPLEMENTED.  Blocked on writegate Phase 3.D.5 Wave 3 v2 enumerator.
  Once the v2 enumerator ships, --add will write record_expected_unattempted rows
  for every newly-registered entity-day combination.
  See plans/epics/infrastructure_master.md "Manifest cleanup HARD RULE" section.

Usage::

    cd instruments-service

    # Dry-run (default) — see what would change:
    .venv/bin/python scripts/reconcile_manifest_after_entity_change.py \\
        --mode remove --asset-group cefi \\
        --entity-type venue --entity-key OLD_VENUE

    # Real write:
    .venv/bin/python scripts/reconcile_manifest_after_entity_change.py \\
        --mode remove --asset-group cefi \\
        --entity-type venue --entity-key OLD_VENUE --no-dry-run

Audit CSV is always written (even in dry-run) to:
  unified-trading-pm/audits/entity_lifecycle/by_date/day=YYYY-MM-DD/
  <asset_group>_<entity_type>_<entity_key_safe>_<ts>.csv

PR descriptions for entity-registry changes MUST attach this CSV path
(or include [entity-skip-cleanup] with operator reason) per the workspace
entity-lifecycle HARD RULE (infrastructure_master.md).
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_TOMBSTONE_REASON = "REMOVED_ENTITY_TOMBSTONE"

_ENTITY_TYPE_CHOICES = [
    "venue",
    "data_type",
    "chain",
    "instrument_type",
    "league_id",
]

# Maps asset_group → (bucket-kind, asset_group arg for resolve_bucket_name).
# Mirrors the same constant in reconcile_phantom_manifest_rows_all.py.
_BUCKET_KIND_MAP: dict[str, tuple[str, str | None]] = {
    "cefi": ("market-data", "cefi"),
    "defi": ("market-data", "defi"),
    "tradfi": ("market-data", "tradfi"),
    "sports": ("instruments-store", "sports"),
    "prediction": ("market-data-tick-prediction", None),
}

_INDEX_BLOB = "_index/availability_index.parquet"


def _load_manifest(storage_client: object, bucket_name: str) -> pd.DataFrame:
    raw: bytes = storage_client.download_bytes(bucket_name, _INDEX_BLOB)  # type: ignore[attr-defined]
    return pd.read_parquet(io.BytesIO(raw))


def _write_manifest(storage_client: object, bucket_name: str, df: pd.DataFrame) -> None:
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    storage_client.upload_from_file_obj(bucket_name, _INDEX_BLOB, out)  # type: ignore[attr-defined]


def _default_csv_path(asset_group: str, entity_type: str, entity_key: str) -> Path:
    day = datetime.now(UTC).strftime("%Y-%m-%d")
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    # Walk up from scripts/ → instruments-service/ → .tabs/7/ → workspace root.
    ws_root = Path(__file__).parents[4]
    pm_dir = ws_root / "unified-trading-pm"
    if not pm_dir.is_dir():
        # Fallback: write alongside the script when workspace layout differs.
        pm_dir = Path(__file__).parent
    out_dir = pm_dir / "audits" / "entity_lifecycle" / "by_date" / f"day={day}"
    out_dir.mkdir(parents=True, exist_ok=True)
    key_safe = entity_key.replace("/", "_").replace(":", "_").replace(" ", "_")
    return out_dir / f"{asset_group}_{entity_type}_{key_safe}_{ts}.csv"


def _cmd_remove(args: argparse.Namespace) -> int:
    from unified_trading_library import get_storage_client, resolve_bucket_name

    kind, ag_arg = _BUCKET_KIND_MAP[args.asset_group]
    bucket_name: str = resolve_bucket_name(cloud=args.cloud, kind=kind, asset_group=ag_arg)
    logger.info("Bucket: %s://%s", args.cloud, bucket_name)

    storage_client = get_storage_client(provider=args.cloud)

    logger.info("Loading manifest from %s", _INDEX_BLOB)
    df = _load_manifest(storage_client, bucket_name)
    logger.info("Manifest rows: %d", len(df))

    if args.entity_type not in df.columns:
        logger.error(
            "Column %r not found in manifest.  Available columns: %s",
            args.entity_type,
            sorted(df.columns.tolist()),
        )
        return 1

    entity_mask = df[args.entity_type].astype(str) == args.entity_key
    entity_df = df[entity_mask]
    logger.info("Rows matching %s=%r: %d", args.entity_type, args.entity_key, len(entity_df))

    if entity_df.empty:
        logger.info("No rows found for %s=%r — nothing to tombstone.", args.entity_type, args.entity_key)
        _write_audit_csv(args, entity_df, entity_df.iloc[:0], asset_group=args.asset_group)
        return 0

    already_tombstoned_mask = (df["capture_status"].fillna("") == "attempted_failed") & (
        df["error_reason"].fillna("") == _TOMBSTONE_REASON
    )
    entity_already_mask = already_tombstoned_mask[entity_mask]
    to_tombstone_df = entity_df[~entity_already_mask]
    already_df = entity_df[entity_already_mask]

    logger.info(
        "To tombstone: %d  |  already tombstoned (idempotency skip): %d",
        len(to_tombstone_df),
        len(already_df),
    )

    if not to_tombstone_df.empty:
        dist = to_tombstone_df["capture_status"].value_counts()
        logger.info("Capture-status distribution of rows to tombstone:\n%s", dist.to_string())

    csv_path = _write_audit_csv(args, to_tombstone_df, already_df, asset_group=args.asset_group)
    logger.info("Audit CSV: %s", csv_path)

    if args.dry_run:
        logger.info(
            "DRY RUN — manifest not modified.  "
            "Re-run with --no-dry-run to apply.  "
            "Attach the CSV above to your PR description per the entity-lifecycle HARD RULE."
        )
        return 0

    if to_tombstone_df.empty:
        logger.info("Nothing new to tombstone — manifest unchanged.")
        return 0

    now_iso = datetime.now(UTC).isoformat()
    df.loc[to_tombstone_df.index, "capture_status"] = "attempted_failed"
    df.loc[to_tombstone_df.index, "error_reason"] = _TOMBSTONE_REASON
    df.loc[to_tombstone_df.index, "attempted_at"] = now_iso

    logger.info(
        "Writing back manifest (%d total rows, %d newly tombstoned).",
        len(df),
        len(to_tombstone_df),
    )
    _write_manifest(storage_client, bucket_name, df)
    logger.info("Done.")
    return 0


def _write_audit_csv(
    args: argparse.Namespace,
    to_tombstone_df: pd.DataFrame,
    already_df: pd.DataFrame,
    *,
    asset_group: str,
) -> Path:
    tagged_new = to_tombstone_df.copy()
    tagged_new["_audit_action"] = "TOMBSTONE"
    tagged_skip = already_df.copy()
    tagged_skip["_audit_action"] = "ALREADY_TOMBSTONED_SKIP"
    full_audit = pd.concat([tagged_new, tagged_skip], ignore_index=True)

    csv_path = (
        Path(args.output_csv) if args.output_csv else _default_csv_path(asset_group, args.entity_type, args.entity_key)
    )
    full_audit.to_csv(csv_path, index=False)
    return csv_path


def _cmd_add(_args: argparse.Namespace) -> int:
    raise NotImplementedError(
        "--mode add is BLOCKED-ON writegate Phase 3.D.5 Wave 3 v2 enumerator.\n"
        "Once the v2 enumerator exists, --add will write record_expected_unattempted rows\n"
        "for each newly-registered entity-day combination.\n"
        "See plans/epics/infrastructure_master.md 'Manifest cleanup HARD RULE' section."
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--mode",
        required=True,
        choices=["remove", "add"],
        help=(
            "'remove': tombstone orphan manifest rows for a removed entity.  "
            "'add': write expected_unattempted rows (BLOCKED-ON writegate Phase 3.D.5)."
        ),
    )
    p.add_argument(
        "--asset-group",
        required=True,
        choices=sorted(_BUCKET_KIND_MAP),
        help="Asset group whose canonical manifest to operate on.",
    )
    p.add_argument(
        "--entity-type",
        required=True,
        choices=_ENTITY_TYPE_CHOICES,
        help="Manifest column that identifies the entity (venue / data_type / chain / ...).",
    )
    p.add_argument(
        "--entity-key",
        required=True,
        help="Exact entity value to match, e.g. 'BINANCE-SPOT' or 'funding_rate'.",
    )
    p.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Default: dry-run.  Prints audit and writes CSV; does NOT modify manifest.  "
            "Pass --no-dry-run to apply changes."
        ),
    )
    p.add_argument(
        "--cloud",
        choices=["gcp", "aws"],
        default="gcp",
        help="Cloud provider.  Default: gcp.",
    )
    p.add_argument(
        "--output-csv",
        default=None,
        metavar="PATH",
        help=(
            "Local path for the audit CSV.  Default: auto-generated under unified-trading-pm/audits/entity_lifecycle/."
        ),
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.mode == "add":
        return _cmd_add(args)
    return _cmd_remove(args)


if __name__ == "__main__":
    sys.exit(main())
