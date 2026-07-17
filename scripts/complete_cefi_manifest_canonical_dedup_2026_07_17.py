#!/usr/bin/env python3
# Epic: cefi_master
# Lifecycle: oneoff
# Delete-when: after the CeFi canonical-completeness Phase-1 manifest migration is
#              verified live in the cefi -prd _index (canonical-fraction >= projected,
#              _verify_gate green) and the drain window is closed.
"""D4 SCRIPT 3 — complete the CeFi manifest to canonical instrument_id + de-dup coexisting forms.

Provenance: plans/active/issues/_cefi_canonical_blueprint_2026_07_17.md § 3 "SCRIPT 3 —
Manifest completion + de-dup (instruments-service)". Fork of
``relabel_cefi_tardis_raw_symbol_to_canonical_2026_07_15.py``.

WHAT THIS FIXES (three deltas vs the fork base):

  (i)  KEY OFF THE ROLLED-UP CATALOGUE VIA THE UAC 3-TUPLE MAP. The fork built a
       2-tuple ``(venue, raw_symbol)`` map from the ``instrument_availability/by_date``
       snapshots. That 2-tuple key EXCLUDES the marquee majors — ``(BYBIT, BTCUSDT)``
       resolves to BOTH ``BYBIT:SPOT_PAIR:BTC-USDT`` and ``BYBIT:PERPETUAL:BTC-USDT@LIN``,
       so the ambiguous-exclusion left the active BYBIT/OKX/BINANCE-FUTURES perps RAW.
       This script builds the ONE shared 3-tuple map
       ``(venue, instrument_type, raw_symbol) -> instrument_id`` from the rolled-up
       ``prod/catalog.parquet`` ``instrument_id`` column, via ``CeFiWireCanonicalMap``
       (UAC SSOT; NEVER ``canonical_instrument_id`` — that column is a raw-glued trap).
       ``instrument_type`` disambiguates the spot/perp clash, recovering the majors.

  (ii) ROLLED-UP (all-lifecycle) CATALOGUE. ``prod/catalog.parquet`` carries every
       instrument that ever listed, so delisted/expired contracts the by-date snapshot
       missed now resolve too.

  (iii) NEW DE-DUP PASS. After the relabel, coexisting spellings of ONE instrument
       (``…@LIN`` / bare ``…:BASE-QUOTE`` no-marker / bare-wire / wrapped-wire) are
       normalized onto the catalogue ``instrument_id`` and collapsed by
       ``drop_duplicates`` on the PINNED shard atom
       ``[date, venue, data_type, instrument_type, instrument_id, pipeline_mode]``
       (WITH ``pipeline_mode``), keeping the best ``capture_status`` via ``_STATUS_RANK``
       (captured > empty_confirmed > attempted_failed > expected_unattempted).

The fork's expected_unattempted reconcile pass is RETAINED (drops eu rows in the main
index whose 5-col shard key now matches a relabeled ``captured`` key — the 5-col key
deliberately excludes ``pipeline_mode`` so a ``batch_tardis`` eu skeleton reconciles
against the canonical ``captured`` twin regardless of which lane fulfilled it).

Scope: ``asset_group=cefi`` (the whole cefi tick manifest). Inputs: the main
``_index/availability_index.parquet`` + every ``_index/per_vm/*.parquet`` shard.

SNAPSHOT-FIRST: every blob this script rewrites gets a pre-write copy under
``_index/snapshots/pre_d4_<ts>/`` before any write. Default mode is DRY-RUN (read-only);
``--apply`` mutates and is GATED by the Phase -1 catalogue verify gate (0 ``:PERP:`` ids,
0 ``instrument_id != canonical_instrument_id`` in the cefi catalogue).

STOP-ON-SURPRISE: the raw-captured candidate population (captured rows whose
``instrument_id`` is not yet ``{venue}:``-prefixed) must land in the ~490k band
(``[_CANDIDATE_MIN, _CANDIDATE_MAX]``) — the pre-rebuild ~490k raw remainder is the
upper bound. Halt outside the band and diagnose before ``--apply``.

Usage::

    cd instruments-service

    # dry-run (default, read-only) — prints all migration counts
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/complete_cefi_manifest_canonical_dedup_2026_07_17.py

    # apply (snapshot + relabel + de-dup + eu-reconcile + verify gate) — DRAIN-GATED
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/complete_cefi_manifest_canonical_dedup_2026_07_17.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import CeFiWireCanonicalMap
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"
CATALOG_BLOB = "prod/catalog.parquet"

# PINNED shard atom (WITH pipeline_mode) — the SSOT de-dup key (blueprint § 1 Phase 0a).
PIN_ATOM = ["date", "venue", "data_type", "instrument_type", "instrument_id", "pipeline_mode"]
# 5-col shard key for the RETAINED fork eu-reconcile pass. Excludes pipeline_mode by
# design so an eu skeleton written by one lane reconciles against the canonical captured
# twin written by another (native on-chain vs batch_tardis).
SHARD_KEY_COLS = ["date", "venue", "data_type", "instrument_type", "instrument_id"]

# Best-status wins on a shard-atom collision.
_STATUS_RANK = {"captured": 0, "empty_confirmed": 1, "attempted_failed": 2, "expected_unattempted": 3}

# STOP-ON-SURPRISE: raw-captured candidate band across ALL blobs. Measured live
# 2026-07-17 against the rebuilt -prd manifest: main index = 490,490 (matches the
# blueprint's ~490k main-index upper bound) + the `_legacy_seed` per-VM shard = 67,560
# => 558,050 total. Bound generously around the total; halt outside.
_CANDIDATE_MIN = 400_000
_CANDIDATE_MAX = 700_000

# Majors that the 2-tuple 2026-07-16 relabel left RAW — the dry-run confirms these now
# resolve through the 3-tuple map (review blocking-risk #1).
_MAJORS_CHECK: list[tuple[str, str, str]] = [
    ("BYBIT", "SPOT_PAIR", "BTCUSDT"),
    ("BYBIT", "PERPETUAL", "BTCUSDT"),
    ("BYBIT", "SPOT_PAIR", "ETHUSDT"),
    ("BYBIT", "PERPETUAL", "ETHUSDT"),
    ("BINANCE-FUTURES", "PERPETUAL", "BTCUSDT"),
    ("BINANCE-FUTURES", "PERPETUAL", "ETHUSDT"),
]


def _load_catalog(storage: object, inst_bucket: str) -> pd.DataFrame:
    """Download the rolled-up cefi ``prod/catalog.parquet`` (all-lifecycle)."""
    raw = storage.download_bytes(inst_bucket, CATALOG_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    return pd.read_parquet(
        io.BytesIO(raw),
        columns=["venue", "instrument_type", "raw_symbol", "instrument_id", "canonical_instrument_id"],
    )


def _build_wire_map(cat: pd.DataFrame) -> CeFiWireCanonicalMap:
    """Build the UAC 3-tuple wire->canonical map off the catalogue ``instrument_id`` column."""
    cols = cat[["venue", "instrument_type", "raw_symbol", "instrument_id"]].fillna("").astype(str)
    rows = zip(cols["venue"], cols["instrument_type"], cols["raw_symbol"], cols["instrument_id"], strict=True)
    return CeFiWireCanonicalMap.from_rows(rows)


def _build_marker_base_map(cat: pd.DataFrame) -> dict[str, str]:
    """Map ``strip_marker(instrument_id) -> instrument_id`` (catalogue), excluding collisions.

    Normalizes a legacy no-marker spelling (``BYBIT:PERPETUAL:BTC-USDT``) onto the
    catalogue's marker-bearing canonical id (``BYBIT:PERPETUAL:BTC-USDT@LIN``). Two
    catalogue ids stripping to the same base (e.g. ``@LIN`` and ``@INV``) are ambiguous
    and excluded (never guessed).
    """
    base_to_id: dict[str, str] = {}
    conflict: set[str] = set()
    for cid in cat["instrument_id"].fillna("").astype(str).unique():
        if not cid:
            continue
        base = cid.split("@", 1)[0]
        if base in conflict:
            continue
        if base in base_to_id and base_to_id[base] != cid:
            conflict.add(base)
            del base_to_id[base]
            continue
        base_to_id[base] = cid
    return base_to_id


def _catalogue_gate(cat: pd.DataFrame) -> tuple[bool, int, int]:
    """Phase -1 verify gate: 0 ``:PERP:`` ids, 0 ``instrument_id != canonical_instrument_id``."""
    iid = cat["instrument_id"].fillna("").astype(str)
    cid = cat["canonical_instrument_id"].fillna("").astype(str)
    n_perp = int(iid.str.contains(":PERP:", regex=False).sum())
    n_mismatch = int((iid != cid).sum())
    return (n_perp == 0 and n_mismatch == 0), n_perp, n_mismatch


def _normalize_id(
    venue: str,
    instrument_type: str,
    current: str,
    wire_map: CeFiWireCanonicalMap,
    marker_base: dict[str, str],
) -> str | None:
    """Resolve ``current`` (bare-wire / wrapped-wire / no-marker) to the catalogue id, or None.

    Three ordered paths; a miss on all three returns None (honest — left as-is):
      1. FORWARD wire lookup: ``canonical_for(venue, itype, current)`` — recovers bare
         and colon-wire symbols (``BTCUSDT``, ``ADAF0:USTF0``) including the majors.
      2. MARKER-BASE: strip ``@marker`` and match the catalogue base — normalizes a
         legacy no-marker canonical spelling onto its marker-bearing catalogue id.
      3. WRAPPED-WIRE: peel ``VENUE:TYPE:`` and forward-resolve the raw tail — recovers
         ``BITFINEX-FUTURES:PERPETUAL:ADAF0:USTF0`` / ``BYBIT:SPOT_PAIR:BTCUSDT``.
    """
    fwd = wire_map.canonical_for(venue, instrument_type, current)
    if fwd:
        return fwd
    base = current.split("@", 1)[0]
    mb = marker_base.get(base)
    if mb:
        return mb
    if current.count(":") >= 2:
        return wire_map.canonical_for(venue, instrument_type, current.split(":", 2)[2])
    return None


def _empty_keys() -> pd.DataFrame:
    return pd.DataFrame(columns=SHARD_KEY_COLS)


def _relabel_blob(
    df: pd.DataFrame,
    wire_map: CeFiWireCanonicalMap,
    marker_base: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Normalize ``captured`` rows' ``instrument_id`` to canonical. Returns (df, new_keys, stats).

    ``candidates`` = captured rows whose id is not yet ``{venue}:``-prefixed (the ~490k
    raw population). ``relabeled`` = candidates that resolved. ``unresolved`` = the honest
    remainder (delisted/ambiguous — left as-is + counted). ``normalized_extra`` = already
    ``{venue}:``-prefixed captured rows whose no-marker/wrapped spelling was normalized to
    the catalogue id (feeds the de-dup collapse).
    """
    zero = {"candidates": 0, "relabeled": 0, "unresolved": 0, "normalized_extra": 0}
    required = {"capture_status", "venue", "instrument_type", "instrument_id", *SHARD_KEY_COLS}
    if not required.issubset(df.columns):
        return df, _empty_keys(), zero

    captured = df["capture_status"] == "captured"
    if not bool(captured.any()):
        return df, _empty_keys(), zero

    venue = df["venue"].fillna("").astype(str)
    itype = df["instrument_type"].fillna("").astype(str)
    cur = df["instrument_id"].fillna("").astype(str)

    cap_idx = df.index[captured]
    triples = list(zip(venue.loc[cap_idx], itype.loc[cap_idx], cur.loc[cap_idx], strict=True))
    memo: dict[tuple[str, str, str], str | None] = {}
    for tr in set(triples):
        memo[tr] = _normalize_id(tr[0], tr[1], tr[2], wire_map, marker_base)
    new_vals = [memo[tr] for tr in triples]

    prefixed_cap = [c.upper().startswith(v.upper() + ":") for v, _t, c in triples]
    resolved_cap = [(nv is not None and nv != c) for nv, (_v, _t, c) in zip(new_vals, triples, strict=True)]

    candidates = sum(1 for p in prefixed_cap if not p)
    relabeled = sum(1 for p, r in zip(prefixed_cap, resolved_cap, strict=True) if (not p) and r)
    unresolved = candidates - relabeled
    normalized_extra = sum(1 for p, r in zip(prefixed_cap, resolved_cap, strict=True) if p and r)

    df = df.copy()
    changed_idx = [ix for ix, r in zip(cap_idx, resolved_cap, strict=True) if r]
    changed_vals = [nv for nv, r in zip(new_vals, resolved_cap, strict=True) if r]
    df.loc[changed_idx, "instrument_id"] = pd.Series(changed_vals, index=changed_idx, dtype="object")

    new_keys = (
        df.loc[changed_idx, SHARD_KEY_COLS].drop_duplicates().reset_index(drop=True) if changed_idx else _empty_keys()
    )
    stats = {
        "candidates": candidates,
        "relabeled": relabeled,
        "unresolved": unresolved,
        "normalized_extra": normalized_extra,
    }
    return df, new_keys, stats


def _dedup_blob(df: pd.DataFrame) -> tuple[pd.DataFrame, int, dict[str, int]]:
    """Collapse coexisting shard-atom spellings via drop_duplicates(PIN_ATOM, keep best status)."""
    if not set(PIN_ATOM).issubset(df.columns) or "capture_status" not in df.columns:
        return df, 0, {}
    rank = df["capture_status"].map(_STATUS_RANK).fillna(9).astype(int)
    ordered = df.assign(_rank=rank).sort_values("_rank", kind="stable")
    before = len(ordered)
    kept = ordered.drop_duplicates(subset=PIN_ATOM, keep="first")
    collapsed = before - len(kept)
    breakdown: dict[str, int] = {}
    if collapsed:
        dropped = ordered.loc[~ordered.index.isin(kept.index)]
        breakdown = {str(k): int(v) for k, v in dropped["capture_status"].value_counts().to_dict().items()}
    kept = kept.drop(columns="_rank").sort_index()
    return kept, collapsed, breakdown


def _reconcile_eu_duplicates(df: pd.DataFrame, all_new_keys: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """RETAINED fork pass: drop eu rows whose 5-col shard key now matches a relabeled captured row."""
    if all_new_keys.empty or not set(SHARD_KEY_COLS).issubset(df.columns) or "capture_status" not in df.columns:
        return df, 0
    eu_mask = df["capture_status"] == "expected_unattempted"
    if not bool(eu_mask.any()):
        return df, 0
    eu_keys = df.loc[eu_mask, SHARD_KEY_COLS].reset_index().rename(columns={"index": "_orig_index"})
    merged = eu_keys.merge(all_new_keys.drop_duplicates().assign(_hit=True), on=SHARD_KEY_COLS, how="left")
    hit = merged["_hit"].astype("boolean").fillna(False)
    drop_idx = merged.loc[hit, "_orig_index"].tolist()
    if drop_idx:
        df = df.drop(index=drop_idx)
    return df, len(drop_idx)


def _canon_captured(df: pd.DataFrame) -> tuple[int, int]:
    """(venue-prefixed captured, total captured) — for the canonical-fraction metric."""
    if "capture_status" not in df.columns or "instrument_id" not in df.columns or "venue" not in df.columns:
        return 0, 0
    cap = df[df["capture_status"] == "captured"]
    if cap.empty:
        return 0, 0
    iid = cap["instrument_id"].fillna("").astype(str)
    ven = cap["venue"].fillna("").astype(str)
    prefixed = sum(1 for s, v in zip(iid, ven, strict=True) if s.upper().startswith(v.upper() + ":"))
    return prefixed, len(cap)


def _load(storage: object, bucket: str, blob: str) -> pd.DataFrame:
    raw = storage.download_bytes(bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
    return pd.read_parquet(io.BytesIO(raw))


def _write(storage: object, bucket: str, blob: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, blob, buf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]


def _snapshot(storage: object, bucket: str, blob: str, run_ts: str) -> str:
    label = blob.rsplit("/", 1)[-1].removesuffix(".parquet")
    snap_blob = f"_index/snapshots/pre_d4_{run_ts}/{label}.parquet"
    logger.info("Snapshotting gs://%s/%s -> gs://%s/%s", bucket, blob, bucket, snap_blob)
    _write(storage, bucket, snap_blob, _load(storage, bucket, blob))
    return snap_blob


def _verify_gate(
    storage: object,
    bucket: str,
    blobs: list[str],
    wire_map: CeFiWireCanonicalMap,
    marker_base: dict[str, str],
) -> int:
    """Post-apply: 0 further-resolvable captured rows and 0 eu/captured 5-col collisions remain."""
    logger.info("Verifying post-apply gate on gs://%s ...", bucket)
    residual_resolvable = 0
    for blob in blobs:
        df = _load(storage, bucket, blob)
        if "capture_status" not in df.columns or "venue" not in df.columns:
            continue
        cap = df[df["capture_status"] == "captured"]
        venue = cap["venue"].fillna("").astype(str)
        itype = cap["instrument_type"].fillna("").astype(str)
        cur = cap["instrument_id"].fillna("").astype(str)
        still = sum(
            1
            for v, t, c in zip(venue, itype, cur, strict=True)
            if (nv := _normalize_id(v, t, c, wire_map, marker_base)) is not None and nv != c
        )
        residual_resolvable += still
        if still:
            logger.error("GATE FAIL: gs://%s/%s still has %d further-resolvable captured rows.", bucket, blob, still)

    main_df = _load(storage, bucket, INDEX_BLOB)
    residual_eu = 0
    if set(SHARD_KEY_COLS).issubset(main_df.columns) and "capture_status" in main_df.columns:
        cap_keys = main_df.loc[main_df["capture_status"] == "captured", SHARD_KEY_COLS].drop_duplicates()
        eu_keys = main_df.loc[main_df["capture_status"] == "expected_unattempted", SHARD_KEY_COLS]
        merged = eu_keys.merge(cap_keys.assign(_hit=True), on=SHARD_KEY_COLS, how="left")
        residual_eu = int(merged["_hit"].astype("boolean").fillna(False).sum())
        if residual_eu:
            logger.error("GATE FAIL: main index still has %d eu rows colliding with a captured 5-col key.", residual_eu)

    if residual_resolvable or residual_eu:
        return 1
    logger.info("GATE PASSED: 0 further-resolvable captured rows; 0 eu/captured 5-col collisions.")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--apply", action="store_true", help="Relabel + de-dup + reconcile (snapshot first). Default: dry-run."
    )
    p.add_argument("--bucket", default=None, help="Override the cefi market-data bucket.")
    p.add_argument("--instruments-bucket", default=None, help="Override the cefi instruments-store bucket.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    inst_bucket = args.instruments_bucket or resolve_bucket_name(
        cloud="gcp", kind="instruments-store", asset_group="cefi"
    )
    storage = get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    cat = _load_catalog(storage, inst_bucket)
    wire_map = _build_wire_map(cat)
    marker_base = _build_marker_base_map(cat)
    logger.info(
        "3-tuple wire map: %d forward keys, %d ambiguous (honest-unresolved); marker-base map: %d bases.",
        len(wire_map.canonical_by_wire),
        len(wire_map.ambiguous_wire_keys),
        len(marker_base),
    )

    gate_ok, n_perp, n_mismatch = _catalogue_gate(cat)
    logger.info(
        "Phase -1 catalogue gate: ':PERP:'=%d, instrument_id!=canonical=%d, GREEN=%s", n_perp, n_mismatch, gate_ok
    )
    if args.apply and not gate_ok:
        logger.error(
            "PRECONDITION GUARD: catalogue verify gate is RED — refusing --apply. Rebuild the catalogue first."
        )
        return 1

    for v, t, r in _MAJORS_CHECK:
        resolved = wire_map.canonical_for(v, t, r)
        marker = "RESOLVES" if resolved else "STILL-RAW"
        logger.info("  majors-check [%s]: (%s, %s, %s) -> %s", marker, v, t, r, resolved)

    per_vm_names = [
        meta.name
        for meta in storage.list_blobs(bucket, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if meta.name.endswith(".parquet") and "/snapshots/" not in meta.name
    ]
    blobs = [INDEX_BLOB, *per_vm_names]

    dfs: dict[str, pd.DataFrame] = {}
    all_new_keys: list[pd.DataFrame] = []
    totals = {"candidates": 0, "relabeled": 0, "unresolved": 0, "normalized_extra": 0}
    main_candidates = 0
    total_collapsed = 0
    collapse_breakdown: dict[str, int] = {}
    before_pref = before_cap = 0

    # Pass 1 — relabel captured rows off the 3-tuple map, per blob; collect new canonical keys.
    for blob in blobs:
        df = _load(storage, bucket, blob)
        bp, bc = _canon_captured(df)
        before_pref += bp
        before_cap += bc

        df, new_keys, stats = _relabel_blob(df, wire_map, marker_base)
        dfs[blob] = df
        if not new_keys.empty:
            all_new_keys.append(new_keys)
        for k in totals:
            totals[k] += stats[k]
        if blob == INDEX_BLOB:
            main_candidates = stats["candidates"]
        logger.info(
            "gs://%s/%s: candidates=%d relabeled=%d unresolved=%d normalized_extra=%d",
            bucket,
            blob,
            stats["candidates"],
            stats["relabeled"],
            stats["unresolved"],
            stats["normalized_extra"],
        )

    # Pass 2 — RETAINED fork eu-reconcile: drop eu twins of relabeled captured rows (main only).
    combined_new_keys = pd.concat(all_new_keys, ignore_index=True).drop_duplicates() if all_new_keys else _empty_keys()
    dfs[INDEX_BLOB], n_eu_dropped = _reconcile_eu_duplicates(dfs[INDEX_BLOB], combined_new_keys)

    # Pass 3 — de-dup coexisting shard-atom spellings (PINNED 6-col atom, keep best status).
    after_pref = after_cap = 0
    for blob in blobs:
        deduped, collapsed, breakdown = _dedup_blob(dfs[blob])
        dfs[blob] = deduped
        total_collapsed += collapsed
        for k, val in breakdown.items():
            collapse_breakdown[k] = collapse_breakdown.get(k, 0) + val
        ap, ac = _canon_captured(deduped)
        after_pref += ap
        after_cap += ac

    before_frac = 100.0 * before_pref / before_cap if before_cap else 0.0
    after_frac = 100.0 * after_pref / after_cap if after_cap else 0.0

    logger.info("=" * 78)
    logger.info("SUMMARY across %d blob(s):", len(blobs))
    logger.info(
        "  raw-captured candidates : %d  (main index=%d + per-VM=%d)",
        totals["candidates"],
        main_candidates,
        totals["candidates"] - main_candidates,
    )
    logger.info("  relabeled (majors etc.) : %d", totals["relabeled"])
    logger.info("  honest-unresolved       : %d  (delisted/ambiguous, left as-is)", totals["unresolved"])
    logger.info(
        "  normalized_extra        : %d  (already-prefixed no-marker/wrapped -> catalogue id)",
        totals["normalized_extra"],
    )
    logger.info(
        "  eu-reconcile dropped    : %d  (main index only, 5-col twin of a relabeled captured row)", n_eu_dropped
    )
    logger.info("  de-dup collapsed        : %d  by status: %s", total_collapsed, collapse_breakdown)
    logger.info("      (captured collapse = coexisting-spelling of ONE instrument; empty/eu collapse is dominated")
    logger.info("       by pre-existing exact-atom manifest dupes, safe under keep-best-status)")
    logger.info(
        "  eu rows removed (total) : %d  (reconcile %d + de-dup %d)",
        n_eu_dropped + collapse_breakdown.get("expected_unattempted", 0),
        n_eu_dropped,
        collapse_breakdown.get("expected_unattempted", 0),
    )
    logger.info("  canonical-fraction      : %.2f%% -> %.2f%% (captured venue-prefixed)", before_frac, after_frac)
    logger.info("=" * 78)

    if not (_CANDIDATE_MIN <= totals["candidates"] <= _CANDIDATE_MAX):
        logger.error(
            "STOP-ON-SURPRISE: raw-captured candidate count %d outside band [%d, %d]. Diagnose before --apply.",
            totals["candidates"],
            _CANDIDATE_MIN,
            _CANDIDATE_MAX,
        )
        return 1

    if not args.apply:
        logger.info(
            "DRY-RUN (default) — no write. --apply would relabel %d, collapse %d dupes, drop %d eu rows (snapshot-first).",
            totals["relabeled"],
            total_collapsed,
            n_eu_dropped,
        )
        return 0

    if totals["relabeled"] == 0 and total_collapsed == 0 and n_eu_dropped == 0:
        logger.info("Nothing to change.")
        return 0

    for blob in blobs:
        _snapshot(storage, bucket, blob, run_ts)
        _write(storage, bucket, blob, dfs[blob])
        logger.info("gs://%s/%s: written (%d rows).", bucket, blob, len(dfs[blob]))

    rc = _verify_gate(storage, bucket, blobs, wire_map, marker_base)
    if rc == 0:
        logger.info(
            "APPLY COMPLETE: relabeled=%d, de-dup-collapsed=%d, eu-dropped=%d, canonical-fraction %.2f%%->%.2f%%.",
            totals["relabeled"],
            total_collapsed,
            n_eu_dropped,
            before_frac,
            after_frac,
        )
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
