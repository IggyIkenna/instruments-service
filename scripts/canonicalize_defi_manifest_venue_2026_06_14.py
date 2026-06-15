#!/usr/bin/env python3
"""Canonicalise the DeFi availability-``_index`` ``venue`` column to the single
canonical ``PROTOCOL-CHAIN`` identity (operator 2026-06-14: "manifest should
migrate to canonical via migration mappings").

The DeFi manifest stores venues in three drifted spellings that all denote the
same protocol-on-chain:

* bare protocol + a separate ``chain`` column  (``UNISWAP_V3`` / ``ETHEREUM``)
* no-underscore protocol-version "ghost"        (``AAVEV3-ARBITRUM``)
* already-canonical                             (``AAVE_V3-ETHEREUM``)

The honest-coverage drilldown joins the manifest venue against the UAC canonical
``ALL_DEFI_VENUES`` / underscore-only ``VENUE_DATA_TYPE_CAPABILITIES``; the drifted
forms don't join → their shards are silently uncredited (the 3.2%-vs-97.6% DeFi
surface split). The deployment-api reader now canonicalises on read, but the
**stored** data stays drifted — so every other consumer (features / ML /
strategy pre-flight) that reads the manifest venue directly inherits the gap.

This one-shot migration rewrites the stored ``venue`` column to the canonical id
using the SAME resolution the readers use (``VenueMapping.normalize_defi_venue``
with a direct ``venue-chain`` concat preferred when it is already canonical, so
classic-vs-versioned forks like ``SUSHISWAP`` + ``ARBITRUM`` →
``SUSHISWAP-ARBITRUM`` are not force-mapped to ``SUSHISWAP_V3``).

Idempotent: a row whose venue is already canonical is left unchanged; a bucket
with zero changes is skipped. Dry-run by default; ``--apply --confirm`` writes
back. ``--projection-out`` writes the canonicalised frame to the CF-20 audit
projection blob instead of the live ``_index`` (for the beta data-status eyeball).

Usage::

    # dry-run diff for the main DeFi market-data bucket
    python scripts/canonicalize_defi_manifest_venue_2026_06_14.py

    # write the beta audit projection (no live write) for the operator eyeball
    python scripts/canonicalize_defi_manifest_venue_2026_06_14.py --projection-out

    # apply to the live _index (gated)
    python scripts/canonicalize_defi_manifest_venue_2026_06_14.py --apply --confirm

SSOT: unified-trading-pm/plans/active/migration_verification_orphan_safety_2026_06_10.md (C).
Composes with: canonicalize_defi_manifest_data_types_2026_05_16.py (data_type column).
"""

from __future__ import annotations

import argparse
import io
import logging

import pandas as pd
from unified_api_contracts.registry import ALL_DEFI_VENUES
from unified_api_contracts.registry.venue_mapping import VenueMapping
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

LIVE_BLOB = "_index/availability_index.parquet"
PROJECTION_BLOB = "_index/audit/projected_index_defi.parquet"


def _canonical_venue(vm: VenueMapping, canon: frozenset[str], venue: str, chain: str) -> str:
    """Resolve ``(venue, chain)`` to its canonical DeFi venue id.

    Prefers a direct ``venue-chain`` concat when that is already canonical
    (classic-vs-versioned forks), else ``normalize_defi_venue`` (bare/ghost/alias
    forms). Returns the input unchanged for non-DeFi / unresolvable rows so they
    are surfaced honestly rather than silently remapped. Mirrors the reader's
    ``DefiStatusMixin._canonical_defi_venue_id`` exactly."""
    venue = str(venue)
    chain = str(chain)
    if chain.strip() and "-" not in venue:
        direct = f"{venue}-{chain}"
        if direct in canon:
            return direct
    try:
        return str(vm.normalize_defi_venue(venue, chain if chain.strip() else None))
    except Exception:
        return venue


def canonicalise_venue_column(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Return ``(df_with_canonical_venue, n_rows_changed)``. No-op if no
    ``venue``/``chain`` columns. Idempotent on already-canonical frames."""
    if "venue" not in df.columns or "chain" not in df.columns or df.empty:
        return df, 0
    vm = VenueMapping()
    canon = frozenset(ALL_DEFI_VENUES)
    pairs = df[["venue", "chain"]].drop_duplicates()
    mapping: dict[tuple[str, str], str] = {}
    for v, c in zip(pairs["venue"].astype(str), pairs["chain"].astype(str), strict=True):
        mapping[(v, c)] = _canonical_venue(vm, canon, v, c)
    orig = df["venue"].astype(str)
    new_venue = [mapping.get((str(v), str(c)), str(v)) for v, c in zip(orig, df["chain"].astype(str), strict=True)]
    n_changed = int((pd.Series(new_venue, index=df.index) != orig).sum())
    out = df.copy()
    out["venue"] = new_venue
    return out, n_changed


def _process_bucket(bucket: str, *, apply: bool, projection_out: bool) -> None:
    client = get_storage_client()
    logger.info("── gs://%s/%s ──", bucket, LIVE_BLOB)
    try:
        raw = client.download_bytes(bucket, LIVE_BLOB)
    except Exception as exc:
        logger.warning("  skip — no live _index (%s)", exc)
        return
    df = pd.read_parquet(io.BytesIO(raw))
    canon_df, n_changed = canonicalise_venue_column(df)
    logger.info(
        "  rows=%d  venue-changes=%d  uniques %d→%d",
        len(df),
        n_changed,
        df["venue"].nunique(),
        canon_df["venue"].nunique(),
    )
    if n_changed:
        sample = (
            pd.DataFrame(
                {"from": df["venue"].astype(str), "to": canon_df["venue"].astype(str), "chain": df["chain"].astype(str)}
            )
            .query("`from` != `to`")
            .drop_duplicates()
            .head(8)
        )
        for _, r in sample.iterrows():
            logger.info("    %-22s + %-10s → %s", r["from"], r["chain"], r["to"])

    if projection_out:
        buf = io.BytesIO()
        canon_df.to_parquet(buf, index=False)
        client.upload_bytes(bucket, PROJECTION_BLOB, buf.getvalue())
        logger.info("  ✎ wrote canonical projection → %s (%d bytes)", PROJECTION_BLOB, len(buf.getvalue()))
        return

    if not n_changed:
        logger.info("  ✓ already canonical — nothing to write")
        return
    if not apply:
        logger.info("  (dry-run — pass --apply --confirm to write the live _index)")
        return
    buf = io.BytesIO()
    canon_df.to_parquet(buf, index=False)
    client.upload_bytes(bucket, LIVE_BLOB, buf.getvalue())
    logger.info("  ✅ APPLIED — wrote canonical venue column to live _index (%d rows changed)", n_changed)


def main() -> None:
    ap = argparse.ArgumentParser(description="Canonicalise the DeFi manifest venue column.")
    ap.add_argument("--cloud", default="gcp", choices=["gcp", "aws"])
    ap.add_argument("--bucket", default=None, help="Override bucket (default: the DeFi market-data bucket)")
    ap.add_argument("--apply", action="store_true", help="Write the live _index (requires --confirm)")
    ap.add_argument("--confirm", action="store_true", help="Confirm a live --apply write")
    ap.add_argument(
        "--projection-out", action="store_true", help="Write the CF-20 beta audit projection instead of the live _index"
    )
    args = ap.parse_args()

    bucket = args.bucket or resolve_bucket_name(cloud=args.cloud, kind="market-data", asset_group="defi")
    if args.apply and not args.confirm:
        ap.error("--apply requires --confirm (live _index write is gated)")
    _process_bucket(bucket, apply=args.apply, projection_out=args.projection_out)


if __name__ == "__main__":
    main()
