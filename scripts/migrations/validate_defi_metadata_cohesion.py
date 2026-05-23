#!/usr/bin/env python3
"""Phase 4 — Cohesion validation between DeFi subgraph and instruments-store-defi parquets.

This script is the audit gate before MTDS Phase 3 fully relies on the
``instruments-store-defi`` bucket as the SSOT for DeFi metadata. It compares
per-pool metadata between two sources:

1. The current subgraph response (live truth) — fetched via the URDI adapters
   that Phase 2a/2b updated to populate ``InstrumentRecord`` metadata fields.
2. The instruments-store-defi parquet for a given day — what instruments-service
   emits and what MTDS handlers (Phase 3) now consume.

Read-only. **Never** writes to GCS. **Never** modifies any parquet. Idempotent
and safe to re-run anytime.

Algorithm
---------
For each ``(venue_prefix, chain)`` pair declared in UAC ``SUBGRAPH_IDS``
(resolved via ``get_subgraph_id``):

1. Fetch a fresh subgraph snapshot via the venue's URDI adapter — already
   returns ``InstrumentRecord`` with the new metadata fields (Phases 2a/2b
   shipped).
2. Read the parquet at
   ``gs://instruments-store-defi-{pid}/instrument_availability/by_date/day={day}/venue={VENUE}-{CHAIN}/instruments.parquet``
   for the requested ``--date`` (default = today UTC).
3. For each ``instrument_key`` present in BOTH sources, compare the 7
   high-signal metadata fields:

   - ``pool_address``
   - ``pool_fee_tier``
   - ``base_asset_contract_address``
   - ``base_asset_decimals``
   - ``quote_asset_contract_address``
   - ``quote_asset_decimals``
   - ``atoken_address``

4. Tally matches vs mismatches per field per venue.
5. Emit a markdown report: per-venue total instruments / matched / mismatched
   per field, plus a sample of up to 5 mismatch examples per venue.

Acceptance bar (per plan ``instruments_service_metadata_refactor_2026_04_29``,
Phase 4): 100% match on the latest 30 days for all venues. The script just
emits the report; the operator decides whether cohesion is acceptable.

Constraints
-----------
- Read-only — never writes to GCS, never modifies any parquet.
- Reuses ``_resolve_api_keys()`` pattern from ``backfill_defi_metadata_2026_04_29.py``
  (passes representative venue strings into ``validate_api_keys_for_venues``,
  NOT data-source names — that bug was discovered today).
- ``--venues VENUE_A,VENUE_B`` flag scopes execution to a subset.
- ``--date YYYY-MM-DD`` flag picks the parquet day (default = today UTC).
- UAC import surface only: ``from unified_api_contracts.registry import get_subgraph_id``.

Usage
-----
    # Today's parquet, all venues
    python3 scripts/migrations/validate_defi_metadata_cohesion.py

    # Specific venue and date
    python3 scripts/migrations/validate_defi_metadata_cohesion.py \
        --venues UNISWAP_V3-ETHEREUM --date 2026-04-29

    # Multi-venue subset
    python3 scripts/migrations/validate_defi_metadata_cohesion.py \
        --venues UNISWAP_V3-ETHEREUM,AAVE_V3-ETHEREUM
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pandas as pd
from unified_api_contracts.registry import get_subgraph_id

if TYPE_CHECKING:
    from unified_api_contracts.internal import InstrumentRecord

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# 7 high-signal metadata fields we compare. Subset of the 11 columns the
# adapters populate — these are the ones whose drift would impact MTDS Phase
# 3 handlers (pool addresses, fee tiers, contract addresses + decimals for the
# two-leg tokens, and Aave's aToken). The other 4 (symbol_onchain x2,
# debt_token_address, etc.) are derived/optional and excluded from the
# strict-cohesion bar.
COHESION_FIELDS: tuple[str, ...] = (
    "pool_address",
    "pool_fee_tier",
    "base_asset_contract_address",
    "base_asset_decimals",
    "quote_asset_contract_address",
    "quote_asset_decimals",
    "atoken_address",
)

_DEFAULT_PREFIX = "instrument_availability/by_date"
_MAX_MISMATCH_SAMPLES = 5


def _venues_with_subgraph_support() -> list[tuple[str, str, str]]:
    """Enumerate every (venue_tag, chain, protocol_slug) triple from UAC ``SUBGRAPH_IDS``.

    Mirrors the helper in ``backfill_defi_metadata_2026_04_29.py`` — kept as a
    private copy to avoid cross-script import (scripts/ is not a package).
    """
    # Deferred import — factory module-level bootstrap touches UAC capability
    # registry; only needed once we actually run.
    from instruments_service.reference_data.factory import (
        _SUBGRAPH_VENUE_PREFIX_TO_PROTOCOL,
    )

    triples: list[tuple[str, str, str]] = []
    for prefix, protocol_slug in _SUBGRAPH_VENUE_PREFIX_TO_PROTOCOL.items():
        # SUBGRAPH_IDS access via UAC registry per import-surface rule.
        from unified_api_contracts.registry import SUBGRAPH_IDS

        for chain in SUBGRAPH_IDS.get(protocol_slug, {}):
            if get_subgraph_id(protocol_slug, chain) is None:
                continue
            venue_tag = f"{prefix}-{chain}"
            triples.append((venue_tag, chain, protocol_slug))
    return triples


async def _fetch_subgraph_snapshot(
    venue_tag: str,
    api_keys: dict[str, str],
    project_id: str | None,
) -> dict[str, dict[str, object]]:
    """Fetch one subgraph snapshot for a venue and build the metadata lookup map."""
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
        for col in COHESION_FIELDS:
            row[col] = getattr(inst, col, None)
        lookup[inst.instrument_key] = row
    logger.info("[%s] subgraph snapshot: %d instruments", venue_tag, len(lookup))
    return lookup


def _read_parquet_for_day(
    storage_client: object,
    bucket: str,
    venue_tag: str,
    day: str,
    prefix: str = _DEFAULT_PREFIX,
) -> pd.DataFrame | None:
    """Download and parse the parquet for a venue/day. Returns ``None`` if missing."""
    blob_path = f"{prefix}/day={day}/venue={venue_tag}/instruments.parquet"
    try:
        raw_obj = cast("object", storage_client).download_bytes(bucket, blob_path)
    except (OSError, ValueError) as exc:
        logger.warning("[%s] parquet missing for day=%s (%s) — skipping", venue_tag, day, exc)
        return None
    raw_bytes = cast("bytes", raw_obj)
    return pd.read_parquet(io.BytesIO(raw_bytes))


def _values_match(left: object, right: object) -> bool:
    """Compare two metadata values, treating NaN/None as equal."""
    left_missing = left is None or (isinstance(left, float) and pd.isna(left))
    right_missing = right is None or (isinstance(right, float) and pd.isna(right))
    if left_missing and right_missing:
        return True
    if left_missing != right_missing:
        return False
    # Normalise hex addresses to lowercase strings for the comparison —
    # subgraph and parquet might emit different casings.
    if isinstance(left, str) and isinstance(right, str):
        return left.strip().lower() == right.strip().lower()
    # Numeric comparison: cast both to float when one side is numpy/python int/float.
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)
    return left == right


def _compare_one_venue(
    venue_tag: str,
    subgraph_lookup: dict[str, dict[str, object]],
    parquet_df: pd.DataFrame,
) -> dict[str, object]:
    """Compare one venue's subgraph snapshot against its parquet row-set.

    Returns a dict with:
        - ``total_instruments`` (intersection count)
        - ``per_field`` mapping field → (match_count, mismatch_count)
        - ``mismatch_samples`` list[dict] up to ``_MAX_MISMATCH_SAMPLES``
        - ``subgraph_only`` count (in subgraph but missing from parquet)
        - ``parquet_only`` count (in parquet but missing from subgraph)
    """
    if "instrument_key" not in parquet_df.columns:
        logger.warning("[%s] parquet missing instrument_key column — comparison aborted", venue_tag)
        return {
            "total_instruments": 0,
            "per_field": dict.fromkeys(COHESION_FIELDS, (0, 0)),
            "mismatch_samples": [],
            "subgraph_only": len(subgraph_lookup),
            "parquet_only": 0,
        }

    raw_keys = cast("list[object]", parquet_df["instrument_key"].dropna().tolist())
    parquet_keys: set[str] = {str(k) for k in raw_keys if isinstance(k, str)}
    subgraph_keys = set(subgraph_lookup.keys())
    common = parquet_keys & subgraph_keys
    subgraph_only = subgraph_keys - parquet_keys
    parquet_only = parquet_keys - subgraph_keys

    # Index the parquet by instrument_key for O(1) lookup. We assume a single
    # row per key per day (which is the orchestrator's invariant for the DeFi
    # bucket); duplicate-key drift is out of scope for this audit.
    parquet_indexed = parquet_df.drop_duplicates(subset=["instrument_key"]).set_index("instrument_key")

    per_field: dict[str, tuple[int, int]] = dict.fromkeys(COHESION_FIELDS, (0, 0))
    mismatch_samples: list[dict[str, object]] = []

    for ikey in sorted(common):
        sub_row = subgraph_lookup[ikey]
        if ikey not in parquet_indexed.index:
            continue
        pq_row = parquet_indexed.loc[ikey]
        for field in COHESION_FIELDS:
            sub_val = sub_row.get(field)
            pq_val = pq_row.get(field) if field in parquet_indexed.columns else None
            matches, mismatches = per_field[field]
            if _values_match(sub_val, pq_val):
                per_field[field] = (matches + 1, mismatches)
            else:
                per_field[field] = (matches, mismatches + 1)
                if len(mismatch_samples) < _MAX_MISMATCH_SAMPLES:
                    mismatch_samples.append(
                        {
                            "instrument_key": ikey,
                            "field": field,
                            "subgraph": sub_val,
                            "parquet": pq_val,
                        }
                    )

    return {
        "total_instruments": len(common),
        "per_field": per_field,
        "mismatch_samples": mismatch_samples,
        "subgraph_only": len(subgraph_only),
        "parquet_only": len(parquet_only),
    }


def _format_markdown_report(
    day: str,
    per_venue: dict[str, dict[str, object]],
) -> str:
    """Build a markdown report covering every venue + sample mismatches."""
    lines: list[str] = []
    lines.append(f"# DeFi Metadata Cohesion Report — day={day}")
    lines.append("")
    lines.append(
        f"Compares the live subgraph snapshot against the per-day parquet at "
        f"`gs://instruments-store-defi-{{pid}}/instrument_availability/by_date/day={day}/venue=*/instruments.parquet`.",
    )
    lines.append("")
    lines.append(f"Compared fields: `{'`, `'.join(COHESION_FIELDS)}`.")
    lines.append("")

    # Summary table.
    lines.append("## Per-venue summary")
    lines.append("")
    header_cells = ["venue", "total", "subgraph_only", "parquet_only"]
    header_cells.extend(f"{f}_match" for f in COHESION_FIELDS)
    header_cells.extend(f"{f}_mismatch" for f in COHESION_FIELDS)
    lines.append("| " + " | ".join(header_cells) + " |")
    lines.append("| " + " | ".join("---" for _ in header_cells) + " |")

    for venue in sorted(per_venue.keys()):
        result = per_venue[venue]
        per_field_raw = result.get("per_field", {})
        per_field = cast("dict[str, tuple[int, int]]", per_field_raw)
        row_cells = [
            venue,
            str(result.get("total_instruments", 0)),
            str(result.get("subgraph_only", 0)),
            str(result.get("parquet_only", 0)),
        ]
        for f in COHESION_FIELDS:
            row_cells.append(str(per_field.get(f, (0, 0))[0]))
        for f in COHESION_FIELDS:
            row_cells.append(str(per_field.get(f, (0, 0))[1]))
        lines.append("| " + " | ".join(row_cells) + " |")

    lines.append("")

    # Mismatch sample blocks per venue.
    lines.append("## Mismatch samples")
    lines.append("")
    any_mismatches = False
    for venue in sorted(per_venue.keys()):
        result = per_venue[venue]
        samples_raw = result.get("mismatch_samples", [])
        samples = cast("list[dict[str, object]]", samples_raw)
        if not samples:
            continue
        any_mismatches = True
        lines.append(f"### {venue}")
        lines.append("")
        lines.append("| instrument_key | field | subgraph | parquet |")
        lines.append("| --- | --- | --- | --- |")
        for s in samples:
            lines.append(
                f"| `{s.get('instrument_key', '')}` | `{s.get('field', '')}` | "
                f"`{s.get('subgraph', '')}` | `{s.get('parquet', '')}` |"
            )
        lines.append("")

    if not any_mismatches:
        lines.append("_No mismatches detected — all venues 100% cohesive._")
        lines.append("")

    return "\n".join(lines)


def _parse_venues_filter(raw: str | None) -> set[str] | None:
    if raw is None or raw.strip() == "":
        return None
    return {v.strip().upper() for v in raw.split(",") if v.strip()}


def _resolve_api_keys() -> dict[str, str]:
    """Load API keys from Secret Manager (service-account auth via ADC).

    ``validate_api_keys_for_venues`` expects venue identifiers (e.g.
    ``UNISWAP_V3-ETHEREUM``), not data-source names — it routes through
    UAC ``get_required_secrets()`` which only knows venue keys. We pass a
    representative DeFi venue per data-source to drive the SM lookup; the
    returned dict is keyed on data-source name (``thegraph`` /
    ``balancer_api_v3``) which is what adapters expect.

    This is the same pattern as ``backfill_defi_metadata_2026_04_29._resolve_api_keys`` —
    discovered when the original ``["thegraph", "balancer_api_v3"]`` argument
    list returned an empty dict because UAC only knows venue-keyed secrets.
    """
    # Deferred — auth path may probe SM on import.
    from unified_trading_library import validate_api_keys_for_venues

    keys = validate_api_keys_for_venues(
        [
            "UNISWAP_V3-ETHEREUM",  # drives thegraph
            "AAVE_V3-ETHEREUM",  # also thegraph (idempotent)
            "BALANCER-ETHEREUM",  # drives balancer_api_v3 if mapped
        ]
    )
    return {k: v for k, v in keys.items() if v}


def _today_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


async def _validate_one_venue(
    storage_client: object,
    bucket: str,
    venue_tag: str,
    day: str,
    api_keys: dict[str, str],
    project_id: str | None,
) -> dict[str, object]:
    """Validate cohesion for one venue/day. Returns the per-venue result dict.

    Returns an empty-shaped result if either the subgraph snapshot or the
    parquet is unavailable — the report still renders a row for the venue,
    and the operator sees the gap explicitly.
    """
    subgraph_lookup = await _fetch_subgraph_snapshot(venue_tag, api_keys, project_id)
    parquet_df = _read_parquet_for_day(storage_client, bucket, venue_tag, day)
    if parquet_df is None:
        return {
            "total_instruments": 0,
            "per_field": dict.fromkeys(COHESION_FIELDS, (0, 0)),
            "mismatch_samples": [],
            "subgraph_only": len(subgraph_lookup),
            "parquet_only": 0,
        }
    if not subgraph_lookup:
        return {
            "total_instruments": 0,
            "per_field": dict.fromkeys(COHESION_FIELDS, (0, 0)),
            "mismatch_samples": [],
            "subgraph_only": 0,
            "parquet_only": len(parquet_df),
        }
    return _compare_one_venue(venue_tag, subgraph_lookup, parquet_df)


async def _amain(args: argparse.Namespace) -> int:
    from unified_trading_library import (
        UnifiedCloudConfig,
        get_bucket_name,
        get_storage_client,
    )

    project_id = UnifiedCloudConfig().gcp_project_id
    bucket = get_bucket_name("instruments", "DEFI")
    raw_date = cast("str | None", args.date)
    day = raw_date or _today_utc()
    logger.info("Bucket: %s (project=%s) day=%s", bucket, project_id, day)

    storage_client = get_storage_client()
    api_keys = _resolve_api_keys()
    raw_venues = cast("str | None", args.venues)
    venues_filter = _parse_venues_filter(raw_venues)

    triples = _venues_with_subgraph_support()
    if venues_filter is not None:
        triples = [t for t in triples if t[0] in venues_filter]
        missing = venues_filter - {t[0] for t in triples}
        if missing:
            logger.warning("Unknown venues in --venues filter (skipped): %s", sorted(missing))
    if not triples:
        logger.error("No venues to validate — exit")
        return 1

    logger.info("Will validate %d venue/chain pairs for day=%s", len(triples), day)

    per_venue: dict[str, dict[str, object]] = {}
    for venue_tag, _chain, _protocol_slug in triples:
        try:
            result = await _validate_one_venue(
                storage_client,
                bucket,
                venue_tag,
                day,
                api_keys,
                project_id,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            # Per-venue isolation — never let one failure kill the whole run.
            logger.error("[%s] venue-level FAILURE: %s", venue_tag, exc)
            result = cast(
                "dict[str, object]",
                {
                    "total_instruments": 0,
                    "per_field": dict.fromkeys(COHESION_FIELDS, (0, 0)),
                    "mismatch_samples": [],
                    "subgraph_only": 0,
                    "parquet_only": 0,
                    "error": str(exc),
                },
            )
        per_venue[venue_tag] = result

    report = _format_markdown_report(day, per_venue)
    print(report)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate cohesion between DeFi subgraph and instruments-store-defi parquets."
    )
    parser.add_argument(
        "--venues",
        default=None,
        help="Comma-separated venue filter (e.g. UNISWAP_V3-ETHEREUM,AAVE_V3-ETHEREUM).",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Day to compare (YYYY-MM-DD). Defaults to today UTC.",
    )
    args = parser.parse_args()
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
