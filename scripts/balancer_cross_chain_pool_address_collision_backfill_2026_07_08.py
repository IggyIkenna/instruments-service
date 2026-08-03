#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the defi prod/catalog.parquet has 0 duplicate `instrument_id` values overall
#   (verified 0 by this script's own report) AND
#   unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md's
#   "DEX pools (finding 2)" full migration lands (at which point every DEX-pool `instrument_id`
#   becomes chain-qualified by construction and this narrow patch is subsumed).
"""One-off backfill — disambiguate 5 real cross-chain Balancer pool-address collisions in the
live DeFi catalog (``prod/catalog.parquet``).

Root cause (confirmed by reading the real adapter code + the real persisted catalog directly, NOT
assumed): this is **not** a ``venue_tag``/chain-suffix bug in ``balancer.py`` — the current adapter
already builds a fully chain-qualified key (``venue_tag = f"BALANCER-{self._chain}"``,
``instrument_key = f"{venue_tag}:POOL:{base}-{quote}"``, `balancer.py:225-226`) and the persisted
catalog's own ``glued_pair_id`` column (e.g. ``BALANCER-ETHEREUM:POOL:YFI-UNI:15.0`` vs
``BALANCER-POLYGON:POOL:WBTC-WETH:25.0``) is likewise already correctly chain-differentiated for
every one of the 10 affected rows. The real bug lives one layer up: the catalog's **primary**
``instrument_id`` for every DeFi POOL row (all 13 DEX-pool protocols, not just Balancer) is the
bare on-chain pool contract address alone (``pool_address.lower()``, no chain) — a known,
already-documented architectural gap with an operator-decided target format
(``VENUE-CHAIN:POOL:TOKEN0-TOKEN1[-FEE_TIER]``, i.e. essentially ``glued_pair_id``) that has **not
yet shipped** as a catalog-wide migration (tracked separately:
``instrument_id_format_canonicalization_2026_07_08.md`` finding 2, spanning ~6,180 rows / 13
protocols — deliberately out of scope for this narrow patch).

That chain-agnostic bare-address scheme is silently correct for the other ~7,274 rows in the
catalog (each pool address happens to be globally unique) but breaks for exactly 5 real Balancer
pools whose on-chain contract address is bit-for-bit identical on both Ethereum and Polygon — a
real, benign address coincidence (Balancer's pool factories were deployed to Ethereum and Polygon
within days of each other in 2021 and create pools via sequential ``CREATE`` from the same
canonical factory address per chain; when the Nth-pool nonce happens to line up across the two
chains' independent deployment histories, the resulting contract address collides even though the
two pools are economically unrelated — confirmed real per-row: different token pairs, different
``underlying`` pool names, different ``market_created_at``/``available_from`` dates). Verified
directly against ``prod/catalog.parquet`` (7,284 rows): these 10 rows (5 pairs) are the **only**
duplicate ``instrument_id`` values anywhere in the entire catalog, across every venue.

Fix (surgical, minimal blast-radius — NOT the full finding-2 migration): for only the rows engaged
in an actual collision, disambiguate ``instrument_id`` by appending the row's own already-correct
``chain`` column via the system's established ``[[@CHAIN]]`` suffix grammar (see
``docs/DEFI_INSTRUMENTS.md``'s "general `VENUE:TYPE:PAYLOAD[@CHAIN]` grammar" reference) —
``0x01abc...`` -> ``0x01abc...@ETHEREUM`` / ``0x01abc...@POLYGON``. The other 2,403 non-colliding
Balancer rows (and all other DEX-pool rows) are left untouched — they are not broken today, and
migrating them wholesale is finding 2's separately-tracked job, not this one.

Safety: writes a timestamped ``.balancerchainfix.bak.parquet`` backup before overwriting; refuses
to write if the row count changes or the whole-catalog duplicate-``instrument_id`` count does not
strictly decrease. Dry-run by default; ``--apply --confirm`` mutates the live blob. Idempotent — a
catalog with 0 remaining Balancer collisions reports 0 changes and is a no-op on re-run.

Usage::

    cd instruments-service
    .venv/bin/python scripts/balancer_cross_chain_pool_address_collision_backfill_2026_07_08.py            # dry-run
    .venv/bin/python scripts/balancer_cross_chain_pool_address_collision_backfill_2026_07_08.py --apply --confirm

SSOT: ``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
finding 2; ``instruments-service/docs/DEFI_INSTRUMENTS.md`` "Balancer cross-chain pool-address
collision" section.
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOGUE_BLOB = "prod/catalog.parquet"


def backfill_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Append ``@{chain}`` to every ``instrument_id`` engaged in a real duplicate.

    Scoped to venue ``BALANCER`` (the only venue with real duplicate ``instrument_id`` values in
    the catalog as of 2026-07-08) and only to rows whose ``instrument_id`` collides with at least
    one other row's — a no-op for every non-colliding row, in this venue or any other. Pure +
    idempotent: re-running on an already-fixed catalog finds 0 remaining collisions and changes
    nothing (the ``@CHAIN``-suffixed id no longer collides with anything).
    """
    stats = {"rows_in": len(df), "rows_out": len(df), "balancer_fixed": 0}
    if df.empty or "instrument_id" not in df.columns:
        return df, stats

    ids = df["instrument_id"].astype(str)
    venues = df["venue"].astype(str) if "venue" in df.columns else pd.Series([""] * len(df), index=df.index)
    chains = df["chain"].astype(str) if "chain" in df.columns else pd.Series([""] * len(df), index=df.index)

    is_balancer = venues == "BALANCER"
    is_dupe = ids.duplicated(keep=False)
    to_fix = is_balancer & is_dupe

    stats["balancer_fixed"] = int(to_fix.sum())

    new_ids = ids.copy()
    new_ids.loc[to_fix] = ids.loc[to_fix] + "@" + chains.loc[to_fix]

    work = df.copy()
    work["instrument_id"] = new_ids
    stats["rows_out"] = len(work)
    return work, stats


def _bak(blob: str, ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{ts}.balancerchainfix.bak.parquet"
    return f"{blob}.{ts}.balancerchainfix.bak"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write the fix back to GCS (default: dry-run report only).")
    ap.add_argument("--confirm", action="store_true", help="Required alongside --apply to actually mutate the blob.")
    args = ap.parse_args(argv)
    apply = bool(args.apply and args.confirm)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as well — refusing to mutate without both flags.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi", deployment_env="prd")
    st = get_storage_client(project_id=None)
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    logger.info("Mode=%s bucket=%s blob=%s", "APPLY" if apply else "DRY-RUN", bucket, CATALOGUE_BLOB)

    raw = st.download_bytes(bucket, CATALOGUE_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))

    dup_before = int(df["instrument_id"].astype(str).duplicated().sum())
    out, stats = backfill_frame(df)
    dup_after = int(out["instrument_id"].astype(str).duplicated().sum())

    logger.info(
        "rows %d->%d | balancer_cross_chain_relabeled=%d | dup_instrument_id %d->%d",
        stats["rows_in"],
        stats["rows_out"],
        stats["balancer_fixed"],
        dup_before,
        dup_after,
    )

    if stats["rows_out"] != stats["rows_in"]:
        logger.error("ABORT: row count changed (%d -> %d) — refusing to write", stats["rows_in"], stats["rows_out"])
        return 1

    if not stats["balancer_fixed"]:
        if dup_before == 0:
            logger.info("Nothing to fix — catalog already clean (idempotent no-op).")
            return 0
        logger.error(
            "ABORT: %d duplicate instrument_id row(s) remain but none matched the BALANCER fix — investigate.",
            dup_before,
        )
        return 1

    if dup_after >= dup_before:
        logger.error(
            "ABORT: relabel did not reduce the duplicate instrument_id count (%d -> %d) — refusing to write",
            dup_before,
            dup_after,
        )
        return 1

    if apply:
        st.upload_bytes(bucket, _bak(CATALOGUE_BLOB, ts), raw)
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        st.upload_bytes(bucket, CATALOGUE_BLOB, buf.read())
        logger.info("wrote %s (backup -> %s)", CATALOGUE_BLOB, _bak(CATALOGUE_BLOB, ts))
    else:
        logger.info("Dry-run only — re-run with --apply --confirm to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
