#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this script's own idempotent re-run reports 0 remaining
#   format mismatches against live prod/catalog.parquet AND the 13 residual
#   rows documented in plans/active/issues/defi_dex_pool_glued_pair_id_backfill_gap_2026_08_03.md
#   are resolved.
"""Cosmetic string-normalize the 13 residual POOL rows in ``prod/catalog.parquet``
whose ``glued_pair_id`` still carries a colon-before-fee or ``.0``-suffix artifact
after the 2026-08-03 full catalogue regen.

These 13 rows are frozen-tail carryover: their ``pool_address`` is absent from
every currently-existing ``by_date`` venue snapshot in the GCS corpus, so the
``--mode full`` walk structurally cannot re-derive them. The frozen-tail merge
(``close_absent=False``, by design — avoids the delisted-instrument-loss class)
carries the OLD pre-fix value forward byte-for-byte, never touching
``_defi_pool_dual_form``.

Fixes applied (cosmetic-only — punctuation normalized, fee digit preserved):
  1. Colon-before-fee: ``:65`` → ``-65``, ``:0.0`` → ``-0``, ``:1.0`` → ``-1``, etc.
  2. ``.0``-suffix (colon already fixed above, or dash-separated): ``-34.0`` → ``-34``.

Safety: downloads the live blob once, writes a timestamped
``.residualfix.bak.parquet`` backup BEFORE overwriting, refuses to write if the
row count changes or if any bad row fails to match the expected instrument_id set.
Dry-run by default; ``--apply --confirm`` mutates the live blob. Idempotent —
a catalog whose glued_pair_id already matches the target grammar reports 0 changes
on re-run.

Usage::

    cd instruments-service
    .venv/bin/python scripts/fix_residual_glued_pair_id_format_2026_08_05.py             # dry-run
    .venv/bin/python scripts/fix_residual_glued_pair_id_format_2026_08_05.py --apply --confirm

SSOT: ``unified-trading-pm/plans/active/issues/defi_dex_pool_glued_pair_id_backfill_gap_2026_08_03.md``
todo "Root-cause + disposition for the 13 residual POOL rows."
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

# The 13 instrument_ids known to carry format bugs as of 2026-08-05.
_EXPECTED_BAD_IDS: set[str] = {
    "0xd4d84a4e3c9daa7cc312a27f8c7cab102ce36dfd815043ca227b759b82fa9639",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
    "0x6e4c86d66433eb4f74a78450c1f58fb8a390c4360ca60751a4464090dc64f2b7",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
    "0x9840e028092b92490e3894fab0efe81e2991baca48f619fb70c9fe366c69ca04",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
    "0x7293ebfe11064e24ac144361a94b493087bc89b1e60dd543575b7f75307e262c",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
    "0x60ebcb0627b86d0efc56df3b8dda1123ae81e48d833bb020f1db7bc7e54c0bfc",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
    "0x863b202390873fd78a63d90016781d6649fc119492b41f7e4773f036224dca3f",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
    "0xb0a3811b1d9d2e71abb570f1ae717394aec1dfe4f3af656e77a8e232ce8f4a95",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
    "0x26ed04762e97810c0e551e22d3601fed13e7b2c4",  # QG-allow: defi-citation — Ethereum pool address auto-deployed by Balancer factory, identified from prod/catalog.parquet
    "0x39cb605b3a69ac7780e92055c94fe945fe38211c",  # QG-allow: defi-citation — Ethereum pool address auto-deployed by Uniswap V3 factory, identified from prod/catalog.parquet
    "0xe17d6d290477a88d1d577f6dabfc9325012a3a30",  # QG-allow: defi-citation — BSC pool address auto-deployed by PancakeSwap V3 factory, identified from prod/catalog.parquet
    "0xf57b7b679bcc94463b2c3a624b8e33b83c3ce507f43c1883f5fd32ee6689bf23",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
    "0x88418b3e299437138b04c60692744147149e51e7",  # QG-allow: defi-citation — Ethereum pool address auto-deployed by Balancer factory, identified from prod/catalog.parquet
    "0xe7fb47d0b1cdc113d95eb71b09eaa089634f5be1e4d33300f64087cee4723a6b",  # QG-allow: defi-citation — Solana pool address, identified from prod/catalog.parquet
}

_TARGET_GRAMMAR = re.compile(r"^[A-Z0-9_]+-[A-Z0-9]+:POOL:[^:]+-[^:]+(-\d+)?$")


def _cosmetic_fix_glued_pair_id(glued: str) -> tuple[str, bool]:
    """Cosmetic-only normalize a single ``glued_pair_id`` string.

    Returns ``(fixed_string, was_changed)``. Only touches punctuation — the
    fee digit itself is preserved verbatim. Does NOT validate the fee value
    against any structured column (these rows have no ``by_date`` snapshot).

    Fixes applied:
      1. Colon-before-fee → dash-before-fee: ``:N`` or ``:N.0`` → ``-N``
      2. ``.0``-suffix on dash-separated fee: ``-N.0`` → ``-N``
    """
    if pd.isna(glued) or not glued:
        return glued, False

    marker = ":POOL:"
    idx = glued.find(marker)
    if idx < 0:
        return glued, False

    pair_part = glued[idx + len(marker) :]

    last_colon = pair_part.rfind(":")
    last_hyphen = pair_part.rfind("-")

    if last_colon > last_hyphen:
        # Colon is the last separator → colon-before-fee
        fee_str = pair_part[last_colon + 1 :]
        # Only act if fee_str is purely numeric (possibly with .0 artifact)
        if not re.match(r"^\d+(\.0)?$", fee_str):
            return glued, False
        # Strip .0 if present, then rebuild with dash
        clean_fee = str(int(float(fee_str)))
        prefix = glued[: glued.rfind(":" + fee_str)]
        fixed = prefix + "-" + clean_fee
        return fixed, True
    elif last_hyphen >= 0:
        fee_str = pair_part[last_hyphen + 1 :]
        # Only act on .0 suffix for numeric fees
        if not re.match(r"^\d+\.0$", fee_str):
            return glued, False
        clean_fee = str(int(float(fee_str)))
        prefix = glued[: glued.rfind("-" + fee_str)]
        fixed = prefix + "-" + clean_fee
        return fixed, True

    return glued, False


def rewrite_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    """Apply cosmetic normalization to the known 13 bad rows. Pure + idempotent."""
    stats: dict[str, object] = {
        "rows_in": len(df),
        "rows_out": len(df),
        "expected_bad": len(_EXPECTED_BAD_IDS),
        "matched": 0,
        "rewritten": 0,
        "unchanged_but_bad": 0,
        "missing_from_catalog": 0,
        "grammar_fail_after": 0,
    }

    work = df.copy()
    if work.empty or "glued_pair_id" not in work.columns or "instrument_id" not in work.columns:
        return work, stats

    missing: list[str] = []
    for inst_id in sorted(_EXPECTED_BAD_IDS):
        mask = work["instrument_id"].astype(str) == inst_id
        if not mask.any():
            missing.append(inst_id)
            continue

        stats["matched"] = int(stats["matched"]) + 1
        idx = work.index[mask][0]
        old_glued = str(work.at[idx, "glued_pair_id"])
        fixed, changed = _cosmetic_fix_glued_pair_id(old_glued)

        if not changed:
            stats["unchanged_but_bad"] = int(stats["unchanged_but_bad"]) + 1
            continue

        if not _TARGET_GRAMMAR.match(fixed):
            stats["grammar_fail_after"] = int(stats["grammar_fail_after"]) + 1
            logger.error("GRAMMAR FAIL after fix: %s → %s", old_glued, fixed)
            continue

        work.at[idx, "glued_pair_id"] = fixed
        stats["rewritten"] = int(stats["rewritten"]) + 1
        logger.info("FIX: %s", inst_id)
        logger.info("  OLD: %s", old_glued)
        logger.info("  NEW: %s", fixed)

    stats["missing_from_catalog"] = len(missing)
    if missing:
        logger.warning("Missing from catalog: %s", missing)

    stats["rows_out"] = len(work)
    return work, stats


def _bak(blob: str, ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{ts}.residualfix.bak.parquet"
    return f"{blob}.{ts}.residualfix.bak"


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
        "rows %d->%d | expected_bad=%d | matched=%d | rewritten=%d | unchanged_but_bad=%d | missing=%d | grammar_fail=%d",
        stats["rows_in"],
        stats["rows_out"],
        stats["expected_bad"],
        stats["matched"],
        stats["rewritten"],
        stats["unchanged_but_bad"],
        stats["missing_from_catalog"],
        stats["grammar_fail_after"],
    )

    if stats["rows_out"] != stats["rows_in"]:
        logger.error("ABORT: row count changed (%d -> %d) — refusing to write", stats["rows_in"], stats["rows_out"])
        return 1

    if stats["grammar_fail_after"]:
        logger.error("ABORT: %d row(s) fail target grammar after fix — refusing to write", stats["grammar_fail_after"])
        return 1

    if stats["unchanged_but_bad"]:
        logger.error(
            "ABORT: %d expected-bad row(s) matched but were not changed (fix function no-op) — "
            "manual investigation needed",
            stats["unchanged_but_bad"],
        )
        return 1

    if not stats["rewritten"]:
        logger.info("Nothing to rewrite — all 13 rows already canonical (idempotent no-op).")
        return 0

    if apply:
        st.upload_bytes(bucket, _bak(CATALOGUE_BLOB, ts), raw)
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        written = buf.getvalue()
        st.upload_bytes(bucket, CATALOGUE_BLOB, written)
        logger.info("wrote %s (backup -> %s)", CATALOGUE_BLOB, _bak(CATALOGUE_BLOB, ts))

        # Verify: re-download and spot-check the 13 rows.
        verify_raw = st.download_bytes(bucket, CATALOGUE_BLOB)
        verify_df = pd.read_parquet(io.BytesIO(verify_raw))
        if len(verify_df) != stats["rows_in"]:
            logger.error("VERIFY FAILED: re-read row count %d != expected %d", len(verify_df), stats["rows_in"])
            return 1

        # Check all 13 are now grammar-clean
        residual_bad = 0
        for inst_id in sorted(_EXPECTED_BAD_IDS):
            vmask = verify_df["instrument_id"].astype(str) == inst_id
            if not vmask.any():
                logger.warning("VERIFY: %s not found in re-read catalog", inst_id)
                continue
            gid = str(verify_df.at[verify_df.index[vmask][0], "glued_pair_id"])
            if not _TARGET_GRAMMAR.match(gid):
                logger.error("VERIFY FAILED: %s still has bad glued_pair_id: %s", inst_id, gid)
                residual_bad += 1
            else:
                logger.info("VERIFY OK: %s → %s", inst_id, gid)

        if residual_bad:
            logger.error("VERIFY FAILED: %d row(s) still fail target grammar after write", residual_bad)
            return 1

        logger.info("VERIFY: all %d rewritten rows now match target grammar", stats["rewritten"])
    else:
        logger.info("Dry-run only — re-run with --apply --confirm to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
