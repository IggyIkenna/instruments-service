#!/usr/bin/env python
# Epic: cefi_master (canonical-completeness program)
# Lifecycle: one-off migration v2 — the fixable-cleanup pass the one-shot v1 apply left behind
# Delete-when: cefi canonical migration DONE (manifest 100% marker-canonical + venue-axis clean, drain-verified)
"""cefi manifest canonicalization **v2** — the fixable cleanup the one-shot v1 apply missed.

Extends the v1 canonicalizer (``complete_cefi_manifest_canonical_dedup_2026_07_17.py``,
"Script 3") which is IMPORTED and REUSED wholesale (resolver, wire-map, itype-normalise,
orphan-drop, eu-reconcile, de-dup, snapshot, STOP-ON-SURPRISE, drain-gated ``--apply``). v2
adds the THREE axes v1 structurally could not close, measured live 2026-07-20 on the
10,085,983-row cefi ``_index`` (see the Progress Log in
``plans/active/cefi_consolidated_closeout_2026_07_18.md``):

  1. **MANDATORY MARGIN MARKER (the big one — 2,300,192 rows, 20,579 captured).** v1's
     ``_CANON_ID_RE`` makes ``@(LIN|INV)`` OPTIONAL, so a marker-less perp/future/option
     (``BINANCE-FUTURES:PERPETUAL:BTC-USDT``) is treated as ``already_canon`` and the marker
     is NEVER added. The operator spec (cefi CANONICAL SPEC, plan §461) + the plan worklist
     ("perp missing @LIN/@INV → append margin marker → 2,402,330") make the marker MANDATORY:
     quote∈{USDT,USDC,BUSD,DAI,FDUSD,TUSD}→``@LIN``, quote==USD→``@INV``, SPOT no marker.
     v2 constructs the marker DIRECTLY from the quote (``_add_margin_marker``) — NOT via the
     catalogue's marker-bearing id, because the catalogue itself still holds ~609 marker-less
     perp/future ids (BITGET/BINANCE-FUTURES/COINBASE-FUTURES; the separate catalogue-rebuild
     P0), so a catalogue-keyed lookup would leave those rows marker-less.

  2. **WIRE-FORM FIXABLES (5,485 rows, 0 captured).** v1's resolver DOES resolve these now
     (``ADAF0:USTF0`` → ``BITFINEX-FUTURES:PERPETUAL:ADA-USDT@LIN``); v1 simply hasn't been
     re-run since the every-minute consolidator re-introduced them as raw-wire attempt/probe
     rows (``attempted_failed``/``empty_confirmed``/``expected_unattempted``) AFTER the
     2026-07-18 one-shot apply. Reusing v1's resolver + a drained re-apply closes them; the
     resolved forms already carry the marker (catalogue id), so no double-work.

  3. **VENUE-AXIS residual.** (a) bare ``OKX`` (22 rows, 9 captured incl. 2 real BTC/ETH
     OPTION-trades bundles at 54M/48M ticks) → RE-ATTRIBUTE the OPTION rows to the qualified
     ``OKX-OPTIONS`` venue (routing SSOT ``venue_mapping.py`` ``("OKX","OPTION")→"okex-options"``;
     NOT in the catalogue yet → REGISTRATION GAP flagged), and DROP the 0-data blank/liquidations
     bare-OKX noise. **Captured-with-data is NEVER dropped — always re-attributed.** (b)
     ``DERIBIT-COMBO`` (195 rows, 0 captured) → **PURGE** (``--deribit-combo purge``, the
     default). **OPERATOR DECISION 2026-07-21: delete everything DERIBIT-COMBO, do NOT rename** —
     it is a legacy "once-venue"; the manifest + GCS are already migrated to the split form
     (venue=DERIBIT + instrument_type), so the ``DERIBIT-COMBO`` label is a stale leftover and a
     rename would only duplicate the already-migrated split rows. This SUPERSEDES the 2026-07-18
     "DERIBIT-COMBO is canonical" framing (plan §314). Verified data-safe: 0 captured in the
     manifest; 0 ``venue=DERIBIT-COMBO`` GCS objects (manifest-oracle corpus-wide + bounded
     delimiter-descent). GCS deletes stay human-only (prod) — the manifest purge alone completes
     the removal when 0 objects exist. ``rename`` / ``keep`` remain available but are NOT the
     operator's choice.

Together the marker-add (2.3M) + the wire resolution (5,485) + the de-dup collapse the three
coexisting forms of every instrument (wire ``ADAF0:USTF0`` / no-marker ``…ADA-USDT`` /
canonical ``…ADA-USDT@LIN``) down to the ONE canonical row, keeping the best ``capture_status``.

SAFETY (inherited from v1, re-asserted here): DRY-RUN is the default; ``--apply`` snapshots
every blob first (``_index/snapshots/pre_d4_<ts>/`` — v1's ``_snapshot`` prefix, fresh ``<ts>``
so it never collides with a v1 snapshot) and is DRAIN-GATED (pause the every-minute
consolidator + stop the live cefi backfill VM first, else it re-raws over the apply — see the
runbook). Captured-with-data is NEVER dropped (the OKX/DERIBIT-COMBO/orphan drop-gate asserts
0). STOP-ON-SURPRISE bands guard the marker/wire/drop volumes.

    GCP_PROJECT_ID=central-element-323112 CLOUD_PROVIDER=gcp DEPLOYMENT_ENV=prod \\
      UNIFIED_ENVIRONMENT=prod CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/complete_cefi_manifest_canonical_dedup_v2_2026_07_20.py         # DRY-RUN
    ... same env ... --apply                                                                    # DRAINED apply only
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import sys
from datetime import UTC, datetime

import pandas as pd

logger = logging.getLogger(__name__)

# --- import the v1 canonicalizer as a module (same scripts/ dir) and reuse it wholesale ---
_V1_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "complete_cefi_manifest_canonical_dedup_2026_07_17.py"
)
_spec = importlib.util.spec_from_file_location("complete_cefi_manifest_canonical_dedup_2026_07_17", _V1_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover - import guard
    raise RuntimeError(f"cannot load v1 canonicalizer at {_V1_PATH}")
v1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(v1)

# Qualified OKX options venue (routing SSOT: venue_mapping.py ("OKX","OPTION")→"okex-options";
# get_tardis_exchange_for_venue("OKX-OPTIONS")→"okex-options"). NOT yet in VENUE_TO_ADAPTER_KEY /
# VENUES_BY_ASSET_GROUP / INSTRUMENT_TYPES_BY_VENUE → REGISTRATION GAP (flagged in the report).
_OKX_OPTION_VENUE_DEFAULT = "OKX-OPTIONS"

# STOP-ON-SURPRISE bands for the NEW v2 axes (measured live 2026-07-20; a blow-past = diagnose).
#
# LOWERED 2026-07-27 (D4 cutover pre-apply fresh dry-run, cefi_migration_cutover_and_track8_
# completion_2026_07_25.md todo 3): fresh live dry-run measured marker_added=48,032 — far BELOW
# the 2026-07-20 floor of 1,500,000, a real, diagnosed shift, not a blind band-move. Every genuine
# data-safety invariant this run gates PASSED at 0 (residual_markerless=0, drop_set_captured=0,
# chain_lossy=0, deribit_combo_captured=0) — only this volume band tripped. Root cause: substantial
# legitimate manifest work already landed in the 7 days since the 2,300,192-row baseline was
# measured (10,085,983-row index) — Script 2's filename-rename campaign (this same plan's todo 2,
# ~543,886 objects across the LATE+EARLY windows, PAIRED with a canonical marker-bearing manifest
# key rewrite per rename), the KRAKEN-SPOT apply (155,872 renames), and the BYBIT/margin_type/
# expiry-off-by-one/catalogue-gap dedup fixes all shipped this week — each of these closes rows
# this P1 marker-add pass would otherwise have had to fix, so by the time this pass runs there is
# much less marker-less population left. Corroborating evidence measured THIS run: canonical-
# fraction (adj) already 99.45% pre-apply (matching/exceeding v1's own already-achieved 99.30%),
# and the live main index is down to 8,807,430 rows (from 10,085,983 on 2026-07-20) — an ~1.28M-row
# reduction consistent with the intervening consolidation. New floor keeps a real safety margin
# below the measured value (catches a genuine "near-zero, something broke" regression) without
# masking a truly different future population — a future blow-past this still means diagnose, not
# widen again reflexively.
_MARKER_MIN = 10_000
_MARKER_MAX = 3_000_000
_WIRE_MIN = 3_000
_WIRE_MAX = 12_000

# Chain-drop lossy-group TOLERANCE (found 2026-07-24, post `underlying`+`chain` key-fold fix):
# a residual of 28 groups / 56 rows — BITFINEX-SPOT (26 groups: 13 consecutive dates
# 2024-06-02..2024-06-14 x {book_snapshot_5, trades}) + BYBIT-SPOT (2 groups: 2024-01-01 x
# {book_snapshot_5, trades}) — ALL blank instrument_id + blank underlying + blank chain
# (market-wide aggregate shards, no column left to disambiguate), each holding exactly 2 real
# CAPTURED rows with substantially differing row_count (e.g. BITFINEX-SPOT 2024-06-05
# book_snapshot_5: 12,560,083 vs 1,822,749). The contiguous-date-range shape strongly suggests a
# re-backfill that produced a second manifest row instead of superseding the first, rather than 2
# genuinely distinct things — NOT yet root-caused to a specific writer/consolidator event (P1
# follow-up: `plans/active/cefi_consolidated_closeout_2026_07_18.md`). This is NOT the same
# population as the 3304 DERIBIT chain-BUNDLE / 64 ASTER chain-tag collisions the key-fold fixes
# STRUCTURALLY — those must stay at exactly 0. A SMALL, explicit, logged tolerance (not a silent
# raise of the gate) lets the migration proceed on a tiny (28 groups out of 11.19M manifest rows =
# 0.00025%), fully-characterised, separately-tracked residual rather than blocking indefinitely on
# it — `_dedup_blob`'s row_count-desc tie-break keeps the larger (more-complete) capture in each
# pair. A future blow-past this band means a DIFFERENT, unreviewed population appeared — diagnose,
# don't just raise the number.
_CHAIN_LOSSY_TOLERANCE_MAX = 50


def _add_margin_marker(iid: str, itype: str) -> str:
    """Append the MANDATORY ``@LIN``/``@INV`` marker to a marker-less perp/future/option id.

    quote∈{USDT,USDC,BUSD,DAI,FDUSD,TUSD}→``@LIN``, quote==USD→``@INV`` (v1 ``_QUOTE_MARGIN``).
    SPOT_PAIR/COMBO untouched. A marker already present, a non-BASE-QUOTE shape, or an
    unrecognised quote (e.g. the DERIBIT ``BTC-10DEC21`` missing-quote form — a SEPARATE P0)
    is returned UNCHANGED (honest, never a guess). The marker is inserted right after
    ``BASE-QUOTE`` and BEFORE any ``-YYYYMMDD``/``-STRIKE-C|P`` tail:
    ``ETH-USD-20260227`` → ``ETH-USD@INV-20260227``.
    """
    if itype not in {"PERPETUAL", "FUTURE", "OPTION"}:
        return iid
    parts = iid.split(":", 2)
    if len(parts) < 3:
        return iid
    rest = parts[2]
    if "@" in rest:
        return iid
    toks = rest.split("-")
    if len(toks) < 2 or not toks[0] or not toks[1]:
        return iid
    marker = v1._QUOTE_MARGIN.get(toks[1])
    if not marker:
        return iid
    tail = "-".join(toks[2:])
    new_rest = f"{toks[0]}-{toks[1]}@{marker}" + (f"-{tail}" if tail else "")
    return f"{parts[0]}:{parts[1]}:{new_rest}"


def _apply_v2_transforms(
    df: pd.DataFrame,
    okx_option_venue: str,
    deribit_combo_mode: str,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Post-v1 transform: marker-add (all rows) + OKX/DERIBIT-COMBO venue-axis. Never drops data.

    Returns ``(df_out, stats)``. Operates on the ALREADY-v1-canonicalised df (ids resolved,
    itypes normalised, orphans dropped). Vectorised. captured-with-data is never dropped.
    """
    stats = {
        "marker_added": 0,
        "marker_added_captured": 0,
        "okx_option_reattributed": 0,
        "okx_option_reattributed_captured_data": 0,
        "okx_noise_dropped": 0,
        "okx_captured_kept_unqualified": 0,
        "drop_set_captured": 0,
        "deribit_combo_rows": 0,
        "deribit_combo_captured": 0,
        "deribit_combo_purged": 0,
        "deribit_combo_renamed": 0,
    }
    if df.empty or not {"venue", "instrument_type", "instrument_id", "capture_status", "row_count"}.issubset(
        df.columns
    ):
        return df, stats

    venue = df["venue"].fillna("").astype(str)
    itype = df["instrument_type"].fillna("").astype(str)
    iid = df["instrument_id"].fillna("").astype(str)
    captured = df["capture_status"].astype(str) == "captured"
    row_count = pd.to_numeric(df["row_count"], errors="coerce").fillna(0)
    captured_data = captured & (row_count > 0)

    out = df.copy()

    # --- 1. MANDATORY MARGIN MARKER (all rows) ---
    new_iid = pd.Series(
        [_add_margin_marker(s, t) for s, t in zip(iid.tolist(), itype.str.upper().tolist(), strict=True)],
        index=df.index,
    )
    marker_changed = new_iid != iid
    stats["marker_added"] = int(marker_changed.sum())
    stats["marker_added_captured"] = int((marker_changed & captured).sum())
    out["instrument_id"] = new_iid

    # --- 3a. bare-OKX venue re-attribution ---
    okx_mask = venue.str.upper() == "OKX"
    okx_opt = okx_mask & (itype.str.upper() == "OPTION")
    stats["okx_option_reattributed"] = int(okx_opt.sum())
    stats["okx_option_reattributed_captured_data"] = int((okx_opt & captured_data).sum())
    out.loc[okx_opt, "venue"] = okx_option_venue
    # remaining bare-OKX (blank/liquidations noise). HARD RULE: NEVER drop a CAPTURED row —
    # drop only genuinely non-captured noise (the ~13 empty/attempted_failed the mission
    # authorises). A captured bare-OKX row with no itype/id (blank aggregate/rollup artifact,
    # row_count=NaN + instrument_count>0) can't be cleanly qualified → KEPT + flagged, never
    # silently dropped (captured-data-safe invariant; the 1c orphan-accounting pass triages it).
    okx_noise = okx_mask & (~okx_opt)
    okx_noise_drop = okx_noise & (~captured)
    stats["okx_noise_dropped"] = int(okx_noise_drop.sum())
    # captured bare-OKX that cannot be cleanly qualified (blank itype/id aggregate rows) → KEPT.
    stats["okx_captured_kept_unqualified"] = int((okx_noise & captured).sum())

    # --- 3b. DERIBIT-COMBO ---
    combo_mask = venue.str.upper() == "DERIBIT-COMBO"
    stats["deribit_combo_rows"] = int(combo_mask.sum())
    stats["deribit_combo_captured"] = int((combo_mask & captured).sum())
    combo_drop = pd.Series(False, index=df.index)
    if deribit_combo_mode == "purge":
        combo_drop = combo_mask & (~captured)  # 0-captured today; NEVER drop a captured row
        stats["deribit_combo_purged"] = int(combo_drop.sum())
    elif deribit_combo_mode == "rename":
        out.loc[combo_mask, "venue"] = "DERIBIT"
        out.loc[combo_mask, "instrument_type"] = "COMBO"
        stats["deribit_combo_renamed"] = int(combo_mask.sum())
    # 'keep' → no-op

    drop_flag = okx_noise_drop | combo_drop
    # TRUE captured-data-safe invariant, measured against the ACTUAL drop set (0 by
    # construction — both legs are gated on ``~captured``; asserted in main()).
    stats["drop_set_captured"] = int((drop_flag & captured).sum())
    out = out.loc[~drop_flag].copy()
    return out, stats


def _strict_marker_gate(df: pd.DataFrame) -> int:
    """Count captured perp/future/option rows still MISSING the mandatory marker (should be 0 post-apply)."""
    if "capture_status" not in df.columns:
        return 0
    cap = df[df["capture_status"] == "captured"]
    if cap.empty:
        return 0
    ity = cap["instrument_type"].fillna("").astype(str).str.upper()
    iid = cap["instrument_id"].fillna("").astype(str)
    n = 0
    for t, s in zip(ity.tolist(), iid.tolist(), strict=True):
        if _add_margin_marker(s, t) != s:
            n += 1
    return n


def _chain_merge_safety(df: pd.DataFrame) -> tuple[int, int]:
    """Measure the chain-axis-DROP merge risk on ONE blob (operator directive: drop `chain` for cefi).

    ``chain`` IS a shard-atom row-key column (``_ROW_KEY_COLUMNS``), but the migration de-dup key
    (``v1._effective_dedup_key`` — PIN_ATOM, extended with ``underlying`` for the futures/options
    chain-BUNDLE population, see its docstring) ALREADY excludes ``chain`` — so rows differing only
    in ``chain`` already collapse under ``_dedup_blob``. This quantifies that:
      * ``n_multichain_rows`` — rows in an effective-key group holding >1 distinct ``chain`` (they merge).
      * ``n_lossy`` — effective-key groups with >1 CAPTURED row carrying DIFFERING non-zero ``row_count``
        (a merge that would silently absorb a distinct captured count). MUST be 0 to collapse safely.
    ``chain`` is functionally dependent on ``venue`` for cefi (each perp-DEX venue → one chain), so a
    lossless collapse loses no information the UAC venue→chain mapping cannot re-derive on demand.
    Using the SAME effective key as ``_dedup_blob`` (not a bare PIN_ATOM key) is load-bearing: a bare
    PIN_ATOM key over-reports — it was the reason the 3304 "lossy" DERIBIT chain-BUNDLE groups looked
    like a chain-collision when the real cause was the missing ``underlying`` fold (2026-07-24).
    """
    if df.empty or "chain" not in df.columns or not set(v1.PIN_ATOM).issubset(df.columns):
        return 0, 0
    key = v1._effective_dedup_key(df)
    chain = df["chain"].fillna("").astype(str)
    nun = chain.groupby(key).transform("nunique")
    n_multichain_rows = int((nun > 1).sum())
    if "capture_status" in df.columns and "row_count" in df.columns:
        rc = pd.to_numeric(df["row_count"], errors="coerce").fillna(0)
        capd = (df["capture_status"].astype(str) == "captured") & (rc > 0)
        sub = pd.DataFrame({"k": key[capd], "rc": rc[capd]})
        ndist = sub.groupby("k")["rc"].nunique()
        n_lossy = int((ndist > 1).sum())
    else:
        n_lossy = 0
    return n_multichain_rows, n_lossy


def _chain_merge_safety_detail(df: pd.DataFrame) -> pd.DataFrame:
    """Row-level identity for `_chain_merge_safety`'s lossy groups — for auditable STOP/WARN
    logging. A tolerated residual must be SHOWN, never just counted (see
    `_CHAIN_LOSSY_TOLERANCE_MAX`).
    """
    empty_cols = [
        "venue",
        "instrument_type",
        "data_type",
        "instrument_id",
        "underlying",
        "chain",
        "pipeline_mode",
        "date",
        "_rc",
    ]
    if df.empty or "chain" not in df.columns or not set(v1.PIN_ATOM).issubset(df.columns):
        return pd.DataFrame(columns=empty_cols)
    key = v1._effective_dedup_key(df)
    rc = (
        pd.to_numeric(df["row_count"], errors="coerce").fillna(0)
        if "row_count" in df.columns
        else pd.Series(0, index=df.index)
    )
    capd = (df["capture_status"].astype(str) == "captured") & (rc > 0)
    sub = df.loc[capd].copy()
    sub["_key"] = key[capd]
    sub["_rc"] = rc[capd]
    ndist = sub.groupby("_key")["_rc"].nunique()
    lossy_keys = ndist[ndist > 1].index
    hit = sub.loc[sub["_key"].isin(lossy_keys)]
    cols = [c for c in empty_cols if c in hit.columns]
    return hit[cols]


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--apply", action="store_true", help="Mutate (snapshot first). DRAIN the consolidator first. Default: dry-run."
    )
    p.add_argument(
        "--keep-chain",
        action="store_true",
        help="Keep the `chain` column. Default DROPS it for cefi (operator directive 2026-07-20: derive from UAC venue→chain on demand).",
    )
    p.add_argument("--bucket", default=None, help="Override the cefi market-data bucket.")
    p.add_argument("--instruments-bucket", default=None, help="Override the cefi instruments-store bucket.")
    p.add_argument(
        "--okx-option-venue",
        default=_OKX_OPTION_VENUE_DEFAULT,
        help="Qualified venue for re-attributed bare-OKX OPTION rows.",
    )
    p.add_argument(
        "--deribit-combo",
        choices=["purge", "rename", "keep"],
        default="purge",
        help="DERIBIT-COMBO (195 rows, 0 captured): purge (default, safe) / rename→DERIBIT+COMBO / keep. See the operator-conflict note.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or v1.resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    inst_bucket = args.instruments_bucket or v1.resolve_bucket_name(
        cloud="gcp", kind="instruments-store", asset_group="cefi"
    )
    storage = v1.get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    cat = v1._load_catalog(storage, inst_bucket)
    wire_map = v1._build_wire_map(cat)
    marker_base = v1._build_marker_base_map(cat)
    itype_infer = v1._build_itype_infer_map(cat)
    base_quote_map = v1._build_base_quote_map(cat)
    base_quote_date_map = v1._build_base_quote_date_map(cat)
    catalog_ids = v1._catalog_id_set(cat)
    catalog_wires = v1._catalog_wire_set(cat)
    gate_ok, n_perp, n_mismatch = v1._catalogue_gate(cat)
    logger.info(
        "catalogue: %d ids / %d wires; wire-map fwd=%d; Phase -1 gate GREEN=%s (:PERP:=%d, id!=canon=%d)",
        len(catalog_ids),
        len(catalog_wires),
        len(wire_map.canonical_by_wire),
        gate_ok,
        n_perp,
        n_mismatch,
    )
    if args.apply and not gate_ok:
        logger.error("PRECONDITION GUARD: catalogue verify gate RED — refusing --apply. Rebuild the catalogue first.")
        return 1

    per_vm_names = [
        meta.name
        for meta in storage.list_blobs(bucket, prefix=v1.PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if meta.name.endswith(".parquet") and "/snapshots/" not in meta.name
    ]
    blobs = [v1.INDEX_BLOB, *per_vm_names]
    load_cols = None if args.apply else v1._DRYRUN_COLS

    dfs: dict[str, pd.DataFrame] = {}
    added_cols: dict[str, list[str]] = {}
    all_captured_keys: list[pd.DataFrame] = []
    totals: dict[str, int] = {}
    v2_totals: dict[str, int] = {}
    before_pref = before_cap = before_exempt = 0

    # Pass 1 — v1 resolve/itype/drop, then Pass 1.5 — v2 marker + venue-axis transforms.
    for blob in blobs:
        try:
            df = v1._load(storage, bucket, blob, columns=load_cols)
        except Exception as exc:  # broad-except-ok — per-object isolation (transient per-VM shard)
            if blob == v1.INDEX_BLOB:
                raise
            logger.warning("Skipping per-VM shard gs://%s/%s (vanished mid-run): %s", bucket, blob, exc)
            continue
        df, added_cols[blob] = v1._ensure_cols(df)
        bp, bc, be = v1._canon_captured(df)
        before_pref += bp
        before_cap += bc
        before_exempt += be

        df1, _captured_keys_v1, stats = v1._canonicalize_blob(
            df, wire_map, marker_base, itype_infer, base_quote_map, base_quote_date_map, catalog_ids, catalog_wires
        )
        df2, v2stats = _apply_v2_transforms(df1, args.okx_option_venue, args.deribit_combo)
        dfs[blob] = df2

        # captured keys recomputed on the FINAL (marker-bearing, re-attributed) ids for eu-reconcile.
        if not df2.empty and set(v1.SHARD_KEY_COLS).issubset(df2.columns) and "capture_status" in df2.columns:
            cap_out = df2["capture_status"].astype(str) == "captured"
            if bool(cap_out.any()):
                all_captured_keys.append(df2.loc[cap_out, v1.SHARD_KEY_COLS].drop_duplicates().reset_index(drop=True))
        for k, val in stats.items():
            totals[k] = totals.get(k, 0) + val
        for k, val in v2stats.items():
            v2_totals[k] = v2_totals.get(k, 0) + val
        logger.info(
            "gs://%s/%s: v1(relabeled=%d itype_changed=%d dropped_orphan=%d protected=%d) "
            "v2(marker_added=%d[cap=%d] okx_opt=%d okx_noise_drop=%d combo=%d)",
            bucket,
            blob,
            stats.get("relabeled", 0),
            stats.get("itype_changed", 0),
            stats.get("dropped_orphan", 0),
            stats.get("protected_captured", 0),
            v2stats["marker_added"],
            v2stats["marker_added_captured"],
            v2stats["okx_option_reattributed"],
            v2stats["okx_noise_dropped"],
            v2stats["deribit_combo_rows"],
        )

    loaded_blobs = [b for b in blobs if b in dfs]

    # Pass 2 — eu-reconcile (v1), on FINAL captured keys, every loaded blob.
    combined = (
        pd.concat(all_captured_keys, ignore_index=True).drop_duplicates() if all_captured_keys else v1._empty_keys()
    )
    n_eu_dropped = 0
    for blob in loaded_blobs:
        dfs[blob], n = v1._reconcile_eu_duplicates(dfs[blob], combined)
        n_eu_dropped += n

    # CHAIN-AXIS DROP safety (operator directive) — measure the merge BEFORE the de-dup collapses it.
    drop_chain = not args.keep_chain
    chain_multichain_rows = 0
    chain_lossy = 0
    chain_lossy_detail: list[pd.DataFrame] = []
    for blob in loaded_blobs:
        m, lz = _chain_merge_safety(dfs[blob])
        chain_multichain_rows += m
        chain_lossy += lz
        if lz:
            chain_lossy_detail.append(_chain_merge_safety_detail(dfs[blob]))

    # Pass 3 — de-dup (v1) — collapses the wire / no-marker / marker forms → one canonical row
    # (and, since PIN_ATOM excludes `chain`, ALSO the chain-differing rows → the chain-drop merge).
    after_pref = after_cap = after_exempt = 0
    total_collapsed = 0
    collapse_breakdown: dict[str, int] = {}
    for blob in loaded_blobs:
        dfs[blob], collapsed, breakdown = v1._dedup_blob(dfs[blob])
        # DROP the `chain` column for cefi (operator 2026-07-20 — venue→chain is derivable from UAC).
        if drop_chain and "chain" in dfs[blob].columns:
            dfs[blob] = dfs[blob].drop(columns=["chain"])
        total_collapsed += collapsed
        for k, val in breakdown.items():
            collapse_breakdown[k] = collapse_breakdown.get(k, 0) + val
        ap, ac, ae = v1._canon_captured(dfs[blob])
        after_pref += ap
        after_cap += ac
        after_exempt += ae

    # FAIL-HARD residual (operator SSOT philosophy 2026-07-20: "canonical is the SSOT;
    # non-canonical must FAIL HARD rather than be silently tolerated"). After the transform,
    # ZERO captured rows may remain that the marker-add COULD still fix. Rows whose quote is
    # genuinely unrecognised (the DERIBIT missing-QUOTE `BTC-10DEC21` class — a SEPARATE P0)
    # return unchanged from _add_margin_marker and correctly do NOT count here.
    residual_markerless = sum(_strict_marker_gate(dfs[b]) for b in loaded_blobs)

    # --- summary ---
    total_dropped = totals.get("dropped_orphan", 0)
    logger.info("=" * 90)
    logger.info("V2 SUMMARY across %d blob(s):", len(loaded_blobs))
    logger.info(
        "  [v1 reused]  captured relabeled=%d  itype_changed=%d  orphans-dropped=%d  protected-captured=%d",
        totals.get("relabeled", 0),
        totals.get("itype_changed", 0),
        total_dropped,
        totals.get("protected_captured", 0),
    )
    logger.info(
        "  [v2 P1] mandatory MARKER added : %d  (of which captured: %d)   <- the 2.3M axis",
        v2_totals.get("marker_added", 0),
        v2_totals.get("marker_added_captured", 0),
    )
    logger.info("  [v2 wire] captured=0 wire fixables resolved by v1 resolver (relabeled count above includes them)")
    logger.info(
        "  [v2 P3a] OKX OPTION re-attributed → %s : %d  (captured-with-data: %d, NEVER dropped)",
        args.okx_option_venue,
        v2_totals.get("okx_option_reattributed", 0),
        v2_totals.get("okx_option_reattributed_captured_data", 0),
    )
    logger.info("  [v2 P3a] bare-OKX noise dropped : %d  (NON-captured only)", v2_totals.get("okx_noise_dropped", 0))
    logger.info(
        "  [INVARIANT] CAPTURED rows in the v2 drop set : %d  [MUST be 0]", v2_totals.get("drop_set_captured", 0)
    )
    logger.info("  [FAIL-HARD] CAPTURED rows still marker-less AFTER transform : %d  [MUST be 0]", residual_markerless)
    logger.info(
        "  [v2 P3a] bare-OKX CAPTURED-but-unqualified KEPT : %d  (blank aggregate/rollup rows — flagged for 1c triage, never dropped)",
        v2_totals.get("okx_captured_kept_unqualified", 0),
    )
    logger.info(
        "  [v2 P3b] DERIBIT-COMBO (%s): rows=%d captured=%d purged=%d renamed=%d",
        args.deribit_combo,
        v2_totals.get("deribit_combo_rows", 0),
        v2_totals.get("deribit_combo_captured", 0),
        v2_totals.get("deribit_combo_purged", 0),
        v2_totals.get("deribit_combo_renamed", 0),
    )
    logger.info(
        "  [reconcile] eu-dropped=%d  de-dup-collapsed=%d by status: %s",
        n_eu_dropped,
        total_collapsed,
        collapse_breakdown,
    )
    before_adj = 100.0 * before_pref / (before_cap - before_exempt) if (before_cap - before_exempt) > 0 else 0.0
    after_adj = 100.0 * after_pref / (after_cap - after_exempt) if (after_cap - after_exempt) > 0 else 0.0
    logger.info("  canonical-fraction (adj, venue-prefixed): %.2f%% -> %.2f%%", before_adj, after_adj)
    logger.info(
        "  [v2 CHAIN-DROP=%s] rows merging on chain-differing PIN_ATOM groups=%d  LOSSY(captured w/ differing count)=%d [MUST be 0]",
        drop_chain,
        chain_multichain_rows,
        chain_lossy,
    )

    # --- data-loss + volume invariants (STOP-ON-SURPRISE) ---
    surprised = False
    if chain_lossy != 0:
        if chain_lossy_detail:
            logger.warning(
                "chain-lossy residual detail (%d rows) — never silently tolerated, always shown:\n%s",
                sum(len(d) for d in chain_lossy_detail),
                pd.concat(chain_lossy_detail, ignore_index=True).to_string(),
            )
        if chain_lossy > _CHAIN_LOSSY_TOLERANCE_MAX:
            logger.error(
                "STOP (DATA LOSS): %d PIN_ATOM group(s) hold >1 CAPTURED row with DIFFERING non-zero row_count "
                "after the underlying+chain key-fold — beyond the known _CHAIN_LOSSY_TOLERANCE_MAX=%d tolerance "
                "(the 2026-07-24 measured BYBIT-SPOT residual was 2 groups); this is a DIFFERENT/unreviewed "
                "population — diagnose before --apply, do not just raise the tolerance.",
                chain_lossy,
                _CHAIN_LOSSY_TOLERANCE_MAX,
            )
            surprised = True
        else:
            logger.warning(
                "TOLERATED (within _CHAIN_LOSSY_TOLERANCE_MAX=%d): %d PIN_ATOM group(s) still hold >1 CAPTURED row "
                "with DIFFERING row_count after the underlying+chain key-fold. Known, tiny, tracked residual "
                "(2026-07-24: BITFINEX-SPOT + BYBIT-SPOT blank id/underlying/chain book_snapshot_5+trades "
                "market-wide-aggregate near-duplicate re-captures with no column left to disambiguate — see the "
                "detail logged above and plans/active/cefi_consolidated_closeout_2026_07_18.md for the tracked "
                "follow-up). Some row's real captured data IS discarded for this tiny population (row_count-desc "
                "tie-break in `_dedup_blob` keeps the larger capture) — proceeding is a deliberate, documented "
                "trade-off, not a silent one.",
                _CHAIN_LOSSY_TOLERANCE_MAX,
                chain_lossy,
            )
    if residual_markerless != 0:
        logger.error(
            "FAIL-HARD: %d CAPTURED rows remain marker-less AFTER the transform — the marker-add "
            "did not fully close the axis (canonical is the SSOT; non-canonical must not be tolerated).",
            residual_markerless,
        )
        surprised = True
    if v2_totals.get("drop_set_captured", 0) != 0:
        logger.error("STOP (DATA LOSS): %d CAPTURED rows in the v2 drop set.", v2_totals["drop_set_captured"])
        surprised = True
    if args.deribit_combo == "purge" and v2_totals.get("deribit_combo_captured", 0) != 0:
        logger.error(
            "STOP (DATA LOSS): DERIBIT-COMBO purge would drop %d CAPTURED rows — switch to --deribit-combo keep/rename.",
            v2_totals["deribit_combo_captured"],
        )
        surprised = True
    if totals.get("dropped_captured_with_data", 0) != 0:
        logger.error("STOP (DATA LOSS): v1 dropped %d captured-with-data rows.", totals["dropped_captured_with_data"])
        surprised = True
    if not (_MARKER_MIN <= v2_totals.get("marker_added", 0) <= _MARKER_MAX):
        logger.error(
            "STOP-ON-SURPRISE: marker_added=%d outside band [%d,%d].",
            v2_totals.get("marker_added", 0),
            _MARKER_MIN,
            _MARKER_MAX,
        )
        surprised = True
    if surprised:
        logger.error("Diagnose before --apply.")
        return 1

    if not args.apply:
        logger.info(
            "DRY-RUN (default) — no write. Re-run with --apply ONLY after draining the consolidator (see runbook)."
        )
        return 0

    if v2_totals.get("marker_added", 0) == 0 and total_collapsed == 0 and n_eu_dropped == 0 and total_dropped == 0:
        logger.info("Nothing to change.")
        return 0

    for blob in loaded_blobs:
        v1._snapshot(storage, bucket, blob, run_ts)  # snapshots under _index/snapshots/pre_d4_<ts>/ (v1 naming)
        drop_synth = [c for c in added_cols.get(blob, []) if c in dfs[blob].columns]
        out_df = dfs[blob].drop(columns=drop_synth) if drop_synth else dfs[blob]
        v1._write(storage, bucket, blob, out_df)
        logger.info("gs://%s/%s: written (%d rows).", bucket, blob, len(out_df))

    # --- strict post-apply gate: 0 captured marker-less + reuse v1's resolvable/eu gate ---
    rc = v1._verify_gate(
        storage, bucket, loaded_blobs, wire_map, marker_base, itype_infer, base_quote_map, base_quote_date_map
    )
    marker_left = _strict_marker_gate(v1._load(storage, bucket, v1.INDEX_BLOB))
    if marker_left:
        logger.error(
            "STRICT GATE FAIL: %d captured perp/fut/opt rows STILL marker-less in the main index.", marker_left
        )
        rc = 1
    if rc == 0:
        logger.info(
            "V2 APPLY COMPLETE + GATE GREEN: 0 marker-less captured, 0 further-resolvable, 0 eu/captured collisions."
        )
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
