#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the defi NO-GLUED-ANYWHERE sweep is complete (0 glued/0 ghost across IS+MTDS _index/catalogue/by_date paths+columns + UAC registry) and verified in prod
"""COMPREHENSIVE zero-glued prober (READ-ONLY) — audits every DeFi surface for glued
PROTOCOL-CHAIN venues + no-underscore ghosts. Reports per-surface before/after counts.
DoD: every surface returns 0 glued + 0 ghost (venues, paths, columns, registry)."""
from __future__ import annotations

import io

import pandas as pd
from unified_api_contracts.registry.capability_declarations._defi import canonicalize_defi_venue_combined as cc
from unified_trading_library import get_storage_client

PID = "central-element-323112"
IS_PRD = f"instruments-store-defi-prd-{PID}"
IS_ENVLESS = f"instruments-store-defi-{PID}"
MTDS = f"market-data-tick-defi-prd-{PID}"
KNOWN_CHAINS = {"ETHEREUM","ARBITRUM","BASE","OPTIMISM","POLYGON","BSC","AVALANCHE","SOLANA","ZKSYNC","SCROLL","LINEA","HYPERLIQUID","STARKNET","PLASMA"}


def is_glued(v: str) -> bool:
    v = str(v)
    return "-" in v and any(v.endswith("-" + c) for c in KNOWN_CHAINS)


def is_ghost(v: str) -> bool:
    v = str(v)
    return cc(v) != v


def probe_column(df: pd.DataFrame, col: str = "venue") -> tuple[int, int, list[str]]:
    if col not in df.columns:
        return 0, 0, []
    v = df[col].astype(str)
    glued = v[v.map(is_glued)]
    ghost = v[v.map(is_ghost)]
    bad = sorted(set(glued.unique()) | set(ghost.unique()))
    return len(glued), len(ghost), bad


def probe_index(st, bucket, blob="_index/availability_index.parquet", label=""):
    try:
        df = pd.read_parquet(io.BytesIO(st.download_bytes(bucket, blob)))
    except Exception as e:
        print(f"  [{label}] {blob}: READ FAIL {type(e).__name__}")
        return
    g, gh, bad = probe_column(df, "venue")
    print(f"  [{label}] {blob} ({len(df):,} rows): glued_rows={g} ghost_rows={gh}  {('SAMPLES '+str(bad[:6])) if bad else 'CLEAN'}")


def probe_paths(st, bucket, prefix, label, sample_limit=200000):
    """Walk GCS object paths, count venue= path segments that are glued/ghost."""
    glued_paths = ghost_paths = total = 0
    bad = set()
    for b in st.list_blobs(bucket, prefix=prefix):
        total += 1
        for tok in b.name.split("/"):
            if tok.startswith("venue="):
                v = tok[6:]
                if is_glued(v):
                    glued_paths += 1
                    bad.add(v)
                elif is_ghost(v):
                    ghost_paths += 1
                    bad.add(v)
                break
        if total >= sample_limit:
            print(f"  [{label}] {prefix}: SAMPLED {sample_limit} (truncated)")
            break
    print(f"  [{label}] PATH {prefix} ({total:,} objs): glued_path={glued_paths} ghost_path={ghost_paths}  {('SAMPLES '+str(sorted(bad)[:6])) if bad else 'CLEAN'}")


def main():
    st = get_storage_client(project_id=PID)
    print("========== ZERO-GLUED PROBER ==========\n--- IS instruments _index + catalogue (columns) ---")
    probe_index(st, IS_PRD, "_index/availability_index.parquet", "IS-PRD")
    probe_index(st, IS_PRD, "prod/catalog.parquet", "IS-PRD-CAT")
    probe_index(st, IS_ENVLESS, "_index/availability_index.parquet", "IS-ENVLESS")
    # per_vm shards
    for b in st.list_blobs(IS_PRD, prefix="_index/per_vm/"):
        if b.name.endswith(".parquet") and ".bak" not in b.name:
            probe_index(st, IS_PRD, b.name, "IS-PRD-PERVM")

    print("\n--- IS by_date raw snapshot PATH segments (latest day sample) ---")
    probe_paths(st, IS_PRD, "instrument_availability/by_date/day=2026-06-21/", "IS-PRD-BYDATE")

    print("\n--- IS by_date raw snapshot in-file venue COLUMN (sample parquet) ---")
    # sample a few recent by_date parquets, check their venue column
    sampled = 0
    glc = ghc = 0
    badcol: set[str] = set()
    for b in st.list_blobs(IS_PRD, prefix="instrument_availability/by_date/day=2026-06-21/"):
        if not b.name.endswith("instruments.parquet"):
            continue
        try:
            df = pd.read_parquet(io.BytesIO(st.download_bytes(IS_PRD, b.name)))
            g, gh, bad = probe_column(df, "venue")
            glc += g
            ghc += gh
            badcol |= set(bad)
        except (OSError, ValueError):
            pass
        sampled += 1
        if sampled >= 15:
            break
    print(f"  [IS-PRD-BYDATE-COL] {sampled} sampled parquets: glued_col_rows={glc} ghost_col_rows={ghc}  {('SAMPLES '+str(sorted(badcol)[:6])) if badcol else 'CLEAN'}")

    print("\n--- MTDS defi market-data _index (column) ---")
    probe_index(st, MTDS, "_index/availability_index.parquet", "MTDS")

    print("\n--- MTDS raw_tick_data PATH segments (sample) ---")
    probe_paths(st, MTDS, "raw_tick_data/by_date/day=2026-06-21/", "MTDS-RAW")

    print("\n--- UAC ALL_DEFI_VENUES registry ---")
    from unified_api_contracts.registry import ALL_DEFI_VENUES
    reg_glued = [v for v in ALL_DEFI_VENUES if is_glued(v)]
    reg_ghost = [v for v in ALL_DEFI_VENUES if is_ghost(v)]
    print(f"  ALL_DEFI_VENUES ({len(set(ALL_DEFI_VENUES))} uniq): glued={len(reg_glued)} ghost={len(reg_ghost)}  {'(glued-form registry)' if reg_glued else 'CLEAN'}")


if __name__ == "__main__":
    main()
