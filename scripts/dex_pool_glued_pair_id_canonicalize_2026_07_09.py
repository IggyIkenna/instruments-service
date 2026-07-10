#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this script's own idempotent re-run reports 0 remaining
#   grammar mismatches against live prod/catalog.parquet AND
#   unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md's
#   finding 2 DEX-pool `instrument_id` migration lands (glued_pair_id becomes
#   redundant with the canonical instrument_id at that point).
"""Real write-back — canonicalize ``glued_pair_id`` in place for all real DEX-pool
``POOL`` rows in ``prod/catalog.parquet`` (13 protocols).

This is the REAL (write) counterpart to the 2026-07-09 dry-run smoke test
documented in ``docs/DEFI_INSTRUMENTS.md`` ("DEX pools" section) — same rebuild
logic, 100% (6,352/6,352) verified parseable with 0 grammar mismatches in the
dry run. This script does NOT touch ``instrument_id`` for POOL rows (that field
is intentionally ``pool_address.lower()`` — a live ``market-tick-data-service``
join key, see ``engine/defi_catalog_reader.py::_canonical_defi_id`` — changing
it needs a coordinated cross-repo migration, out of scope here and NOT
attempted by this script).

Fixes applied to ``glued_pair_id`` (string-level rewrite from already-captured
catalogue columns — no live re-fetch, per the operator's migration-mechanics
decision in the canonicalization issue doc):
  1. Ghost venue-prefix spelling -> canonical: use the catalogue's own
     ``venue`` column (already correctly spelled, e.g. ``UNISWAP_V3``) instead
     of the no-underscore ghost prefix baked into the legacy glued_pair_id
     string (``UNISWAPV3``).
  2. Fee-tier delimiter: colon -> dash (``:POOL:PAIR:FEE`` -> ``:POOL:PAIR-FEE``).
  3. Fee-tier units: raw on-wire feeTier -> real basis points (``raw // 100``)
     for the Uniswap-V3-family + Uniswap V4 protocols. Balancer's fee is
     already bps (int-cast, strips a stray ``.0``). Uniswap V2 / Curve carry no
     fee segment at all (correctly stays absent).

Safety: downloads the live blob once, writes a timestamped
``.gluedpairfix.bak.parquet`` backup BEFORE overwriting, refuses to write if
the row count changes, if ANY POOL row in the 13-protocol scope fails to parse,
or if any rebuilt id fails the target grammar. Re-downloads and re-parses a
sample of the written blob to verify. Dry-run by default; ``--apply --confirm``
mutates the live blob. Idempotent — a catalog whose glued_pair_id already
matches the target grammar reports 0 changes on re-run.

Usage::

    cd instruments-service
    .venv/bin/python scripts/dex_pool_glued_pair_id_canonicalize_2026_07_09.py             # dry-run
    .venv/bin/python scripts/dex_pool_glued_pair_id_canonicalize_2026_07_09.py --apply --confirm

SSOT: ``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
finding 2; ``instruments-service/docs/DEFI_INSTRUMENTS.md`` "DEX pools" section.
"""

from __future__ import annotations

import argparse
import io
import logging
import re
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOGUE_BLOB = "prod/catalog.parquet"

# Protocols whose fee segment is the raw on-wire Uniswap-V3-style feeTier
# (hundredths-of-bps) and needs /100 to become real bps. Covers Uniswap V3
# itself plus the 8 protocols that share UniswapV3ReferenceDataAdapter, and
# Uniswap V4 (separate adapter class, same feeTier convention).
_RAW_FEETIER_PROTOCOLS = {
    "UNISWAP_V3",
    "UNISWAP_V4",
    "PANCAKESWAP_V3",
    "SUSHISWAP_V3",
    "SUSHISWAP",
    "CAMELOT_V3",
    "AERODROME_V3",
    "VELODROME_V2",
    "TRADER_JOE_V2",
    "GMX",
}
# Balancer's fee segment is already basis points (swapFee * 10000) — just
# needs int-cast to drop a stray ".0". Curve / Uniswap V2 carry no fee segment.
_BPS_ALREADY_PROTOCOLS = {"BALANCER"}
_NO_FEE_PROTOCOLS = {"UNISWAP_V2", "CURVE"}

ALL_13_PROTOCOLS = _RAW_FEETIER_PROTOCOLS | _BPS_ALREADY_PROTOCOLS | _NO_FEE_PROTOCOLS

_TARGET_GRAMMAR = re.compile(r"^[A-Z0-9_]+-[A-Z0-9]+:POOL:[^:]+-[^:]+(-\d+)?$")


def rebuild_glued_pair_id(venue: str, chain: str, old_glued: str) -> tuple[str | None, str]:
    """Return ``(new_glued_pair_id_or_None, reason_if_unparseable)``.

    Pure. Same transform validated in the 2026-07-09 dry-run smoke test
    (6,352/6,352 real POOL rows, 0 failures, 0 grammar mismatches).
    """
    marker = ":POOL:"
    idx = old_glued.find(marker)
    if idx < 0:
        return None, "no ':POOL:' marker"
    symbol_part = old_glued[idx + len(marker) :]

    if ":" in symbol_part:
        pair, raw_fee = symbol_part.split(":", 1)
    else:
        pair, raw_fee = symbol_part, ""

    if "-" not in pair:
        # symbol is a bare address fallback (base/quote unknown) — not a real pair.
        return None, "no base-quote pair in glued_pair_id (address fallback)"

    fee_bps: int | None = None
    if raw_fee:
        try:
            raw_fee_f = float(raw_fee)
        except ValueError:
            return None, f"unparseable fee token {raw_fee!r}"
        fee_bps = int(raw_fee_f) // 100 if venue in _RAW_FEETIER_PROTOCOLS else round(raw_fee_f)

    new_id = f"{venue}-{chain}:POOL:{pair}"
    if fee_bps is not None and fee_bps > 0:
        new_id = f"{venue}-{chain}:POOL:{pair}-{fee_bps}"
    return new_id, ""


def rewrite_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    """Rewrite ``glued_pair_id`` for the in-scope POOL rows. Pure + idempotent."""
    stats: dict[str, object] = {
        "rows_in": len(df),
        "rows_out": len(df),
        "pool_rows_in_scope": 0,
        "rewritten": 0,
        "unchanged": 0,
        "failed": 0,
        "fail_reasons": {},
        "grammar_fail": 0,
    }
    work = df.copy()
    if work.empty or "glued_pair_id" not in work.columns:
        return work, stats

    itypes = work["instrument_type"].astype(str).str.upper()
    venues = work["venue"].astype(str)
    is_scope = (itypes == "POOL") & venues.isin(ALL_13_PROTOCOLS)
    stats["pool_rows_in_scope"] = int(is_scope.sum())

    new_col = work["glued_pair_id"].astype(str).copy()
    fail_reasons: dict[str, int] = {}
    for idx in work.index[is_scope]:
        venue = str(work.at[idx, "venue"])
        chain = str(work.at[idx, "chain"])
        old_glued = str(work.at[idx, "glued_pair_id"])
        new_glued, reason = rebuild_glued_pair_id(venue, chain, old_glued)
        if new_glued is None:
            fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
            stats["failed"] = int(stats["failed"]) + 1
            continue
        if not _TARGET_GRAMMAR.match(new_glued):
            stats["grammar_fail"] = int(stats["grammar_fail"]) + 1
        if new_glued != old_glued:
            new_col.at[idx] = new_glued
            stats["rewritten"] = int(stats["rewritten"]) + 1
        else:
            stats["unchanged"] = int(stats["unchanged"]) + 1

    work["glued_pair_id"] = new_col
    stats["fail_reasons"] = fail_reasons
    stats["rows_out"] = len(work)
    return work, stats


def _bak(blob: str, ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{ts}.gluedpairfix.bak.parquet"
    return f"{blob}.{ts}.gluedpairfix.bak"


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

    out, stats = rewrite_frame(df)

    logger.info(
        "rows %d->%d | pool_rows_in_scope=%d | rewritten=%d | unchanged=%d | failed=%d (%s) | grammar_fail=%d",
        stats["rows_in"],
        stats["rows_out"],
        stats["pool_rows_in_scope"],
        stats["rewritten"],
        stats["unchanged"],
        stats["failed"],
        stats["fail_reasons"],
        stats["grammar_fail"],
    )

    if stats["rows_out"] != stats["rows_in"]:
        logger.error("ABORT: row count changed (%d -> %d) — refusing to write", stats["rows_in"], stats["rows_out"])
        return 1

    if stats["failed"]:
        logger.error("ABORT: %d in-scope POOL row(s) failed to parse — refusing to write", stats["failed"])
        return 1

    if stats["grammar_fail"]:
        logger.error("ABORT: %d rebuilt id(s) fail the target grammar — refusing to write", stats["grammar_fail"])
        return 1

    if not stats["rewritten"]:
        logger.info("Nothing to rewrite — catalog already canonical (idempotent no-op).")
        return 0

    if apply:
        st.upload_bytes(bucket, _bak(CATALOGUE_BLOB, ts), raw)
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        written = buf.getvalue()
        st.upload_bytes(bucket, CATALOGUE_BLOB, written)
        logger.info("wrote %s (backup -> %s)", CATALOGUE_BLOB, _bak(CATALOGUE_BLOB, ts))

        # Verify: re-download and re-parse a sample of rewritten rows.
        verify_raw = st.download_bytes(bucket, CATALOGUE_BLOB)
        verify_df = pd.read_parquet(io.BytesIO(verify_raw))
        if len(verify_df) != stats["rows_in"]:
            logger.error("VERIFY FAILED: re-read row count %d != expected %d", len(verify_df), stats["rows_in"])
            return 1
        v_itypes = verify_df["instrument_type"].astype(str).str.upper()
        v_venues = verify_df["venue"].astype(str)
        v_scope = (v_itypes == "POOL") & v_venues.isin(ALL_13_PROTOCOLS)
        v_pool = verify_df[v_scope]
        grammar_ok = v_pool["glued_pair_id"].astype(str).apply(lambda g: bool(_TARGET_GRAMMAR.match(g)))
        logger.info(
            "VERIFY: re-read %d rows, %d/%d in-scope POOL rows match target grammar",
            len(verify_df),
            int(grammar_ok.sum()),
            len(v_pool),
        )
        sample = v_pool[["venue", "chain", "glued_pair_id"]].head(10)
        logger.info("VERIFY sample (post-write, re-read from GCS):\n%s", sample.to_string())
        if int(grammar_ok.sum()) != len(v_pool):
            logger.error("VERIFY FAILED: not all in-scope POOL rows match target grammar after write")
            return 1
    else:
        logger.info("Dry-run only — re-run with --apply --confirm to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
