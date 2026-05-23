#!/usr/bin/env python3
"""Phase 2.5 — Backfill DeFi metadata onto historical instruments-store-defi parquets.

Phase 2a (`d0d9a0d`) and Phase 2b (concurrent) update the DeFi reference-data
adapters to populate the new ``InstrumentRecord`` metadata fields
(``pool_address``, ``pool_fee_tier``, ``base_asset_*``, ``quote_asset_*``,
``atoken_address``, ``debt_token_address``) going forward. Historical parquets
in ``gs://instruments-store-defi-{pid}/instrument_availability/by_date/...``
were written before that change and have NULL values for those columns.

This script back-fills the metadata cheaply: token decimals / pool fee tiers /
pool addresses / contract addresses are all time-invariant, so a single
recent subgraph snapshot per (venue, chain) is sufficient to "stamp" every
historical day.

Algorithm
---------
1. For each ``(venue_prefix, chain)`` pair declared in UAC ``SUBGRAPH_IDS``
   (resolved via ``get_subgraph_id``), instantiate the corresponding URDI
   adapter and call ``get_instruments()`` once. The returned
   ``InstrumentRecord`` instances now carry the new fields.
2. Build a per-venue lookup ``dict[instrument_key, dict[field, value]]``.
3. List every parquet under
   ``instrument_availability/by_date/day=*/venue={VENUE_PREFIX}-{CHAIN}/instruments.parquet``
   in the DeFi bucket using ``unified_trading_library.get_storage_client``.
4. For each parquet: download, read with pandas, left-merge new columns from
   the lookup, re-upload to the same path (atomic per parquet — old blob
   remains until the new ``upload_bytes`` succeeds).
5. Idempotent skip: if the parquet's first row has ``pool_address IS NOT NULL``
   AND the column exists, skip the rewrite.

Constraints
-----------
- Read-only against subgraph (one snapshot per venue, no per-date queries).
- ``--dry-run`` flag emits diff summary only — no GCS upload.
- ``--venues VENUE_A,VENUE_B`` flag scopes execution to a subset.
- ``--asset-group DEFI`` is the only supported asset_group (this is a DeFi-only migration).
- Reuses the URDI adapter classes so we automatically pick up Phase 2a/2b
  metadata-population fixes.

Usage
-----
    # Dry-run for one venue (recommended first step)
    python3 scripts/migrations/backfill_defi_metadata_2026_04_29.py \
        --venues UNISWAP_V3-ETHEREUM --dry-run

    # Real run for one venue
    python3 scripts/migrations/backfill_defi_metadata_2026_04_29.py \
        --venues UNISWAP_V3-ETHEREUM

    # All venues (after validation)
    python3 scripts/migrations/backfill_defi_metadata_2026_04_29.py
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
from typing import TYPE_CHECKING, cast

import pandas as pd
from unified_api_contracts.registry import get_subgraph_id

if TYPE_CHECKING:
    from unified_api_contracts.internal import InstrumentRecord

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# The new DeFi metadata columns introduced by Phase 1 (UAC `2f039bd`).
# These are the columns we stamp onto historical parquets.
NEW_METADATA_COLUMNS: tuple[str, ...] = (
    "pool_address",
    "pool_fee_tier",
    "base_asset_contract_address",
    "base_asset_decimals",
    "base_asset_symbol_onchain",
    "quote_asset_contract_address",
    "quote_asset_decimals",
    "quote_asset_symbol_onchain",
    "atoken_address",
    "debt_token_address",
)

# Sentinel column we use for the idempotent skip check. If the parquet has
# this column AND the first row is non-null, the parquet has already been
# stamped and we skip the rewrite.
SKIP_SENTINEL_COLUMN = "pool_address"

_DEFAULT_PREFIX = "instrument_availability/by_date"


def _venues_with_subgraph_support() -> list[tuple[str, str, str]]:
    """Enumerate every (venue_prefix, chain, protocol_slug) triple from UAC SUBGRAPH_IDS.

    Mirrors the `_SUBGRAPH_VENUE_PREFIX_TO_PROTOCOL` mapping in
    `instruments_service.reference_data.factory` (SSOT — re-imported below).

    Returns a list of ``(venue_tag, chain, protocol_slug)`` tuples where
    ``venue_tag = f"{venue_prefix}-{chain}"`` is the canonical UAC venue name
    used in GCS paths and the URDI factory.
    """
    # config-bootstrap: deferred import — factory's module-level bootstrap
    # touches UAC capability registry and is only needed for migrations.
    from instruments_service.reference_data.factory import (
        _SUBGRAPH_VENUE_PREFIX_TO_PROTOCOL,
    )

    triples: list[tuple[str, str, str]] = []
    for prefix, protocol_slug in _SUBGRAPH_VENUE_PREFIX_TO_PROTOCOL.items():
        # SUBGRAPH_IDS access via get_subgraph_id() per UAC import-surface rule.
        from unified_api_contracts.registry import SUBGRAPH_IDS

        chains = list(SUBGRAPH_IDS.get(protocol_slug, {}).keys())
        for chain in chains:
            # Skip chains where get_subgraph_id returns None (defensive — keeps
            # us in step with adapter `_resolve_api_url` which gates the same way).
            if get_subgraph_id(protocol_slug, chain) is None:
                continue
            venue_tag = f"{prefix}-{chain}"
            triples.append((venue_tag, chain, protocol_slug))
    return triples


async def _fetch_metadata_lookup_for_venue(
    venue_tag: str,
    chain: str,
    api_keys: dict[str, str],
    project_id: str | None,
) -> dict[str, dict[str, object]]:
    """Fetch one subgraph snapshot for a venue and build the metadata lookup map.

    The map keys are ``instrument_key`` strings exactly as stored in historical
    parquets, so a left-merge against the parquet's ``instrument_key`` column
    fills the new metadata columns for any instrument still present today.
    Delisted / removed instruments stay NULL — that is the honest answer.
    """
    # Lazy import: the factory's module-level bootstrap runs UAC capability
    # discovery; we want this script importable from any cwd without UAC
    # network calls firing during argparse.
    from instruments_service.reference_data.factory import (
        get_adapter_for_canonical_venue,
    )

    adapter = get_adapter_for_canonical_venue(
        canonical_venue=venue_tag,
        api_key=api_keys.get("thegraph"),
        project_id=project_id,
        date=None,
        extra_api_keys=api_keys,
        mode="batch",
    )
    instruments_raw = await adapter.get_instruments()
    instruments = cast("list[InstrumentRecord]", instruments_raw)

    lookup: dict[str, dict[str, object]] = {}
    for inst in instruments:
        row: dict[str, object] = {}
        for col in NEW_METADATA_COLUMNS:
            row[col] = getattr(inst, col, None)
        lookup[inst.instrument_key] = row
    logger.info("[%s|%s] subgraph snapshot: %d instruments", venue_tag, chain, len(lookup))
    return lookup


def _list_historical_parquets(
    storage_client: object,
    bucket: str,
    venue_tag: str,
    prefix: str = _DEFAULT_PREFIX,
) -> list[str]:
    """List every historical parquet path for a given venue tag.

    GCS layout (per `_write_venue` in instruments_service.engine.orchestrator):
        ``{prefix}/day=YYYY-MM-DD/venue={venue_tag}/instruments.parquet``
    """
    # We list all blobs under the per-venue suffix; the simpler approach is
    # to iterate days. Using list_blobs() with a non-narrowing prefix and
    # filtering venue= in the loop is robust to bucket layout changes.
    paths: list[str] = []
    bucket_handle = cast("object", storage_client).bucket(bucket)
    venue_segment = f"venue={venue_tag}/"
    for blob in bucket_handle.list_blobs(prefix=prefix):
        name = str(blob.name)
        if name.endswith("/instruments.parquet") and venue_segment in name:
            paths.append(name)
    paths.sort()
    return paths


def _is_already_stamped(df: pd.DataFrame) -> bool:
    """Idempotent skip check — returns True if the parquet already has stamped metadata."""
    if SKIP_SENTINEL_COLUMN not in df.columns:
        return False
    if len(df) == 0:
        # Empty parquets carry no useful metadata to compare; treat as stamped
        # (re-stamping would no-op anyway).
        return True
    first = df[SKIP_SENTINEL_COLUMN].iloc[0]
    return first is not None and not (isinstance(first, float) and pd.isna(first))


def _merge_metadata(
    df: pd.DataFrame,
    lookup: dict[str, dict[str, object]],
) -> tuple[pd.DataFrame, int]:
    """Left-merge metadata columns from ``lookup`` onto ``df``.

    Returns ``(merged_df, n_filled)`` where ``n_filled`` is the count of rows
    where at least one of the new metadata columns transitioned from NULL
    to a real value. Existing non-null values are preserved (we never
    overwrite — the parquet's columns win if already stamped).
    """
    if df.empty:
        return df, 0
    if "instrument_key" not in df.columns:
        logger.warning("Parquet missing instrument_key column — skipping merge")
        return df, 0

    out = df.copy()
    n_filled = 0
    for col in NEW_METADATA_COLUMNS:
        if col not in out.columns:
            out[col] = None
    for idx, row in out.iterrows():
        ikey = row.get("instrument_key")
        if not isinstance(ikey, str):
            continue
        meta = lookup.get(ikey)
        if not meta:
            continue
        row_filled = False
        for col, val in meta.items():
            if val is None:
                continue
            existing = out.at[idx, col]
            is_missing = existing is None or (isinstance(existing, float) and pd.isna(existing))
            if is_missing:
                out.at[idx, col] = val
                row_filled = True
        if row_filled:
            n_filled += 1
    return out, n_filled


def _rewrite_parquet(
    storage_client: object,
    bucket: str,
    blob_path: str,
    df: pd.DataFrame,
) -> None:
    """Re-upload the merged DataFrame to ``blob_path`` (overwrites the existing parquet).

    Atomic per parquet — the bytes are buffered locally, then uploaded in one
    request. The previous blob remains until the new ``upload_bytes`` finishes
    successfully (server-side replace).
    """
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    cast("object", storage_client).upload_bytes(
        bucket=bucket,
        blob_path=blob_path,
        data=buf.getvalue(),
        content_type="application/octet-stream",
    )


def _process_one_parquet(
    storage_client: object,
    bucket: str,
    blob_path: str,
    lookup: dict[str, dict[str, object]],
    *,
    dry_run: bool,
) -> tuple[str, int, int]:
    """Process a single parquet path. Returns ``(status, n_rows, n_filled)``.

    ``status`` is one of ``"skipped"``, ``"unchanged"``, ``"rewritten"`` (real
    write), or ``"would-rewrite"`` (dry-run preview).
    """
    raw = cast("object", storage_client).download_bytes(bucket, blob_path)
    df = pd.read_parquet(io.BytesIO(raw))
    n_rows = len(df)

    if _is_already_stamped(df):
        return "skipped", n_rows, 0

    merged, n_filled = _merge_metadata(df, lookup)
    if n_filled == 0:
        # Either no instrument_key matched the snapshot (all delisted) or all
        # rows were already non-null. Nothing to write.
        return "unchanged", n_rows, 0

    if dry_run:
        return "would-rewrite", n_rows, n_filled

    _rewrite_parquet(storage_client, bucket, blob_path, merged)
    return "rewritten", n_rows, n_filled


async def _backfill_one_venue(
    storage_client: object,
    bucket: str,
    venue_tag: str,
    chain: str,
    api_keys: dict[str, str],
    project_id: str | None,
    *,
    dry_run: bool,
) -> dict[str, int]:
    """Backfill all historical parquets for one venue. Returns a counts dict."""
    lookup = await _fetch_metadata_lookup_for_venue(venue_tag, chain, api_keys, project_id)
    if not lookup:
        logger.warning("[%s] empty subgraph snapshot — skipping backfill", venue_tag)
        return {"skipped": 0, "unchanged": 0, "rewritten": 0, "would-rewrite": 0, "total": 0}

    paths = _list_historical_parquets(storage_client, bucket, venue_tag)
    logger.info("[%s] historical parquets: %d", venue_tag, len(paths))

    counts = {"skipped": 0, "unchanged": 0, "rewritten": 0, "would-rewrite": 0, "total": len(paths)}
    for blob_path in paths:
        try:
            status, n_rows, n_filled = _process_one_parquet(storage_client, bucket, blob_path, lookup, dry_run=dry_run)
        except (OSError, ValueError) as exc:
            # Shard-level isolation — log and continue. This script is
            # idempotent so a re-run picks up the failed shard cheaply.
            logger.error("[%s] FAILED %s: %s", venue_tag, blob_path, exc)
            continue
        counts[status] = counts.get(status, 0) + 1
        logger.info(
            "[%s] %s %s rows=%d filled=%d",
            venue_tag,
            status.upper(),
            blob_path,
            n_rows,
            n_filled,
        )
    return counts


def _format_summary(per_venue: dict[str, dict[str, int]]) -> str:
    """Build a human-readable summary table from per-venue counts."""
    lines = ["", "=" * 72, "Backfill summary", "=" * 72]
    header = f"{'venue':<28} {'total':>6} {'skipped':>8} {'unchanged':>10} {'rewritten':>10} {'would':>7}"
    lines.append(header)
    lines.append("-" * 72)
    for venue, counts in sorted(per_venue.items()):
        lines.append(
            f"{venue:<28} {counts.get('total', 0):>6} {counts.get('skipped', 0):>8} "
            f"{counts.get('unchanged', 0):>10} {counts.get('rewritten', 0):>10} "
            f"{counts.get('would-rewrite', 0):>7}"
        )
    return "\n".join(lines)


def _parse_venues_filter(raw: str | None) -> set[str] | None:
    if raw is None or raw.strip() == "":
        return None
    return {v.strip().upper() for v in raw.split(",") if v.strip()}


def _resolve_api_keys() -> dict[str, str]:
    """Load API keys from Secret Manager (service-account auth via ADC).

    ``validate_api_keys_for_venues`` expects venue identifiers (e.g.
    ``UNISWAP_V3-ETHEREUM``), not data-source names — it routes through
    UAC ``get_required_secrets()`` which only knows venue keys. We pass
    a representative DeFi venue per data-source to drive the SM lookup;
    the returned dict is keyed on data-source name (``thegraph`` /
    ``balancer_api_v3``) which is what adapters expect.
    """
    # Lazy import — the auth path probes SM on import in some configurations.
    from unified_trading_library import validate_api_keys_for_venues

    keys = validate_api_keys_for_venues(
        [
            "UNISWAP_V3-ETHEREUM",  # drives thegraph
            "AAVE_V3-ETHEREUM",  # also thegraph (idempotent)
            "BALANCER-ETHEREUM",  # drives balancer_api_v3 if mapped
        ]
    )
    return {k: v for k, v in keys.items() if v}


async def _amain(args: argparse.Namespace) -> int:
    from unified_trading_library import (
        UnifiedCloudConfig,
        get_bucket_name,
        get_storage_client,
    )

    if args.asset_group.upper() != "DEFI":
        logger.error("Only --asset-group DEFI is supported (this is a DeFi-only migration)")
        return 2

    project_id = UnifiedCloudConfig().gcp_project_id
    bucket = get_bucket_name("instruments", "DEFI")
    logger.info("Bucket: %s (project=%s)", bucket, project_id)

    storage_client = get_storage_client()
    api_keys = _resolve_api_keys()
    venues_filter = _parse_venues_filter(args.venues)

    triples = _venues_with_subgraph_support()
    if venues_filter is not None:
        triples = [t for t in triples if t[0] in venues_filter]
        missing = venues_filter - {t[0] for t in triples}
        if missing:
            logger.warning("Unknown venues in --venues filter (skipped): %s", sorted(missing))
    if not triples:
        logger.error("No venues to process — exit")
        return 1

    logger.info("Will process %d venue/chain pairs (dry_run=%s)", len(triples), args.dry_run)

    per_venue: dict[str, dict[str, int]] = {}
    for venue_tag, chain, _protocol_slug in triples:
        try:
            counts = await _backfill_one_venue(
                storage_client,
                bucket,
                venue_tag,
                chain,
                api_keys,
                project_id,
                dry_run=args.dry_run,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            # Per-venue isolation: never let one venue kill the whole run.
            logger.error("[%s] venue-level FAILURE: %s", venue_tag, exc)
            counts = {"failed": 1, "total": 0}
        per_venue[venue_tag] = counts

    print(_format_summary(per_venue))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill DeFi metadata onto historical instruments-store-defi parquets."
    )
    parser.add_argument(
        "--asset-group",
        default="DEFI",
        help="Only DEFI is supported (sanity-check on caller intent).",
    )
    parser.add_argument(
        "--venues",
        default=None,
        help="Comma-separated venue filter (e.g. UNISWAP_V3-ETHEREUM,AAVE_V3-ETHEREUM).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Emit diff summary without uploading.",
    )
    args = parser.parse_args()
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
