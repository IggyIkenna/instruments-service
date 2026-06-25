#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the defi IS _index + catalogue venue column is 0-glued/0-ghost in prod, captured-count preserved (ε=0), and the foundation-completeness defi G1→G3 gates pass
"""Collapse ALL DeFi venue drift forms → the ONE canonical form in the
instruments-service ``_index`` + catalogue (operator 2026-06-25).

The DeFi reference manifest + catalogue carry the venue identity in N drifted
spellings that all denote the same protocol-on-chain:

  * no-underscore GHOST + glued   ``AAVEV3-ARBITRUM``   → venue=AAVE_V3 chain=ARBITRUM
  * underscore GLUED              ``AAVE_V3-ARBITRUM``  → venue=AAVE_V3 chain=ARBITRUM
  * bare already-canonical        ``AAVE_V3`` (+chain col) → unchanged (chain kept)

The single canonical form (codex/02-data/defi-canonical-naming-ssot.md + the
deployment-api drilldown ``_is_legacy_defi_venue_row``): **bare ``venue=PROTOCOL``
+ a separate ``chain=X`` column.** A glued/ghost venue string is the LEGACY form
the cockpit drilldown drops; collapsing it to bare+chain makes those rows COUNT.

After the collapse there must be **0 glued + 0 ghost** ``venue`` values — only
canonical bare protocols. Captured wins on dedup; a captured cell is NEVER
dropped (the ε=0 invariant).

The protocol-part ghost normalisation reuses the UAC authority
``canonicalize_defi_venue_combined`` (``AAVEV3``→``AAVE_V3``,
``COMPOUNDV3``→``COMPOUND_V3``) which fixes the ghost but KEEPS the glued/bare
shape — this script then splits the chain suffix off so the stored form is bare.

Safety: snapshot is taken by ``step1_snapshot`` / the caller before --apply;
this script ALSO writes a per-blob ``.driftcanon.bak`` before overwriting.
Dry-run by default; ``--apply`` writes back. Idempotent (a frame already bare
returns unchanged).

Usage:
  python scripts/collapse_defi_drift_to_canonical_2026_06_25.py --dry-run
  python scripts/collapse_defi_drift_to_canonical_2026_06_25.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts.registry.capability_declarations._defi import canonicalize_defi_venue_combined
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
CATALOGUE_BLOB = "prod/catalog.parquet"
PER_VM_PREFIX = "_index/per_vm/"

# Known chain suffixes (UPPER). A glued venue ends with ``-<CHAIN>``.
KNOWN_CHAINS = frozenset(
    {
        "ETHEREUM",
        "ARBITRUM",
        "BASE",
        "OPTIMISM",
        "POLYGON",
        "BSC",
        "AVALANCHE",
        "SOLANA",
        "ZKSYNC",
        "SCROLL",
        "LINEA",
        "HYPERLIQUID",
        "STARKNET",
        "PLASMA",
    }
)
# Cell key (minus instrument_id) — the dedup grain for the _index.
_CELL_KEY_COLS = ("date", "venue", "chain", "data_type", "instrument_id")


def _split_glued(venue: str) -> tuple[str, str] | None:
    """Return (protocol, chain) if *venue* ends with a known ``-<CHAIN>`` suffix, else None."""
    if "-" in venue:
        for ch in KNOWN_CHAINS:
            if venue.endswith("-" + ch):
                return venue[: -(len(ch) + 1)], ch
    return None


def canonicalise_venue_chain(venue: str, chain: str) -> tuple[str, str]:
    """Map any drift form ``(venue, chain)`` → canonical ``(bare_protocol, chain_upper)``.

    1. ghost-normalise the full venue string (AAVEV3-ARBITRUM → AAVE_V3-ARBITRUM).
    2. if it is glued (ends ``-<CHAIN>``), split → bare protocol + that chain.
    3. else (bare): keep venue; chain from the existing chain column (upper).
    Pure + idempotent.
    """
    v = str(venue).strip()
    c = str(chain).strip().upper()
    if not v:
        return v, c
    normed = canonicalize_defi_venue_combined(v)  # fixes ghost, keeps glued/bare shape
    split = _split_glued(normed)
    if split is not None:
        proto, gchain = split
        return proto, gchain  # bare + chain-from-glued-suffix (authoritative)
    # bare already (no known-chain suffix) — keep the existing chain column
    return normed, c


def collapse_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Collapse venue/chain to canonical bare+chain; dedup captured-wins. Pure + idempotent."""
    stats = {"venue_changed": 0, "chain_filled": 0, "dropped_dup": 0, "rows_in": len(df), "rows_out": len(df)}
    if df.empty or "venue" not in df.columns:
        return df, stats
    has_chain = "chain" in df.columns
    orig_v = df["venue"].astype(str)
    orig_c = df["chain"].astype(str) if has_chain else pd.Series([""] * len(df), index=df.index)

    # vectorised via a per-unique-pair map (the _index is ~190k rows; unique pairs are tiny)
    pairs = pd.unique(list(zip(orig_v, orig_c, strict=True)))
    pmap = {p: canonicalise_venue_chain(p[0], p[1]) for p in pairs}
    new_v = [pmap[(v, c)][0] for v, c in zip(orig_v, orig_c, strict=True)]
    new_c = [pmap[(v, c)][1] for v, c in zip(orig_v, orig_c, strict=True)]

    work = df.copy()
    work["venue"] = new_v
    if has_chain:
        work["chain"] = new_c
    else:
        work["chain"] = new_c
    stats["venue_changed"] = int((pd.Series(new_v, index=df.index) != orig_v).sum())
    stats["chain_filled"] = int((pd.Series(new_c, index=df.index) != orig_c).sum())

    # Dedup: after collapse a glued-twin + bare-twin map onto the SAME canonical cell
    # (e.g. CURVE-ETHEREUM + CURVE both → venue=CURVE chain=ETHEREUM, blank instrument_id).
    # Keep EXACTLY ONE row per canonical cell, by status priority captured > empty_confirmed >
    # attempted_failed > expected_unattempted, and within a status by the richest row (highest
    # instrument_count, then non-blank source/error). "Never lose a captured" = every cell that
    # HAD a captured twin stays captured with the best instrument_count — collapsing two captured
    # twins to one captured row PRESERVES the cell (it does NOT drop a captured cell).
    def _captured_cells(frame: pd.DataFrame, kcols: list[str]) -> int:
        if "capture_status" not in frame.columns:
            return 0
        cap_rows = frame[frame["capture_status"].astype(str) == "captured"]
        return int(cap_rows[kcols].drop_duplicates().shape[0])

    key_cols = [c for c in _CELL_KEY_COLS if c in work.columns]
    if key_cols:
        rows_before = len(work)
        captured_cells_before = _captured_cells(work, key_cols)
        _status_rank = {"captured": 0, "empty_confirmed": 1, "attempted_failed": 2, "expected_unattempted": 3}
        if "capture_status" in work.columns:
            work["_srank"] = work["capture_status"].astype(str).map(_status_rank).fillna(9)
        else:
            work["_srank"] = 9
        if "instrument_count" in work.columns:
            work["_ic"] = pd.to_numeric(work["instrument_count"], errors="coerce").fillna(-1)
        else:
            work["_ic"] = -1
        # best row per cell: lowest _srank, then highest _ic
        work = (
            work.sort_values(["_srank", "_ic"], ascending=[True, False])
            .drop_duplicates(subset=key_cols, keep="first")
            .drop(columns=["_srank", "_ic"])
            .reset_index(drop=True)
        )
        stats["dropped_dup"] = rows_before - len(work)
        captured_cells_after = _captured_cells(work, key_cols)
        # invariant: no captured CELL lost (distinct captured cell-keys preserved)
        stats["captured_cells_before"] = captured_cells_before
        stats["captured_cells_after"] = captured_cells_after
    stats["rows_out"] = len(work)
    return work, stats


def _bak(blob: str, ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{ts}.driftcanon.bak.parquet"
    return f"{blob}.{ts}.driftcanon.bak"


def _glued_ghost_count(df: pd.DataFrame) -> tuple[int, int]:
    if "venue" not in df.columns:
        return 0, 0
    v = df["venue"].astype(str)
    glued = int(v.str.contains("-", regex=False).sum())
    # ghost = a venue whose combined-canonical differs (AAVEV3→AAVE_V3) — count distinct affected rows
    uniq = {x: canonicalize_defi_venue_combined(x) for x in v.unique()}
    ghost = int(v.map(lambda x: uniq[x] != x).sum())
    return glued, ghost


def _process(st, bucket: str, blob: str, ts: str, *, apply: bool, label: str) -> dict[str, int]:
    try:
        raw = st.download_bytes(bucket, blob)
    except Exception as exc:  # pragma: no cover
        logger.warning("  %s: read fail %s", blob, exc)
        return {}
    df = pd.read_parquet(io.BytesIO(raw))
    g0, gh0 = _glued_ghost_count(df)
    cap0 = int((df.get("capture_status", pd.Series(dtype=str)).astype(str) == "captured").sum())
    out, stats = collapse_frame(df)
    g1, gh1 = _glued_ghost_count(out)
    cap1 = int((out.get("capture_status", pd.Series(dtype=str)).astype(str) == "captured").sum())
    # The ε=0 invariant is on captured CELLS (distinct canonical cell-keys), NOT raw captured ROWS
    # (two captured glued+bare twins legitimately MERGE to one captured row for the same cell).
    cells_b = stats.get("captured_cells_before", cap0)
    cells_a = stats.get("captured_cells_after", cap1)
    logger.info(
        "  [%s] %s rows %d→%d | venue_changed=%d chain_filled=%d dropped_dup=%d | "
        "glued %d→%d ghost %d→%d | captured_ROWS %d→%d | captured_CELLS %d→%d",
        label,
        blob,
        stats["rows_in"],
        stats["rows_out"],
        stats["venue_changed"],
        stats["chain_filled"],
        stats["dropped_dup"],
        g0,
        g1,
        gh0,
        gh1,
        cap0,
        cap1,
        cells_b,
        cells_a,
    )
    if cells_a < cells_b:
        logger.error("  ❌ CAPTURED-CELL REGRESSION %d→%d on %s — REFUSING to write this blob", cells_b, cells_a, blob)
        return {**stats, "captured_regression": cells_b - cells_a}
    if apply and (stats["venue_changed"] or stats["chain_filled"] or stats["dropped_dup"]):
        st.upload_bytes(bucket, _bak(blob, ts), raw)
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        st.upload_bytes(bucket, blob, buf.read())
        logger.info("    ✅ wrote %s (bak → %s)", blob, _bak(blob, ts))
    return {**stats, "captured_before": cap0, "captured_after": cap1, "glued_after": g1, "ghost_after": gh1}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    m = ap.add_mutually_exclusive_group(required=True)
    m.add_argument("--dry-run", action="store_true")
    m.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)
    apply = bool(args.apply)

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")
    st = get_storage_client(project_id=None)
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    logger.info("Mode=%s bucket=%s", "APPLY" if apply else "DRY-RUN", bucket)

    any_regression = 0
    # _index + per-VM shards + catalogue
    blobs = [INDEX_BLOB, CATALOGUE_BLOB]
    blobs += [
        b.name
        for b in st.list_blobs(bucket, prefix=PER_VM_PREFIX)
        if b.name.endswith(".parquet") and ".bak" not in b.name
    ]
    for blob in blobs:
        r = _process(st, bucket, blob, ts, apply=apply, label="IS-defi")
        any_regression += r.get("captured_regression", 0)

    if any_regression:
        logger.error("ABORT: %d captured cells would be lost — no further writes", any_regression)
        return 1
    logger.info("Done (mode=%s).", "APPLY" if apply else "DRY-RUN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
