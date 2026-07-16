#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the CeFi availability-index legacy lowercase instrument_type
#   dupes are 0 (verified by this script's own dry-run report mode) AND
#   unified-trading-pm/plans/active/data_status_page_ux_and_canonicalisation_2026_07_16.md
#   the "CeFi legacy lowercase dupes" P9 todo is checked off.
"""One-off migration — collapse the CeFi availability-index legacy lowercase
``instrument_type`` duplicates (``"perpetual"`` / ``"spot"``) into their canonical
UAC ``InstrumentType`` values (``PERPETUAL`` / ``SPOT_PAIR``).

Background (operator-confirmed, live full-stack review 2026-07-16)
--------------------------------------------------------------------
``gs://instruments-store-cefi-prd-central-element-323112/_index/availability_index.parquet``
carries legacy lowercase ``instrument_type`` values sitting ALONGSIDE the canonical
uppercase values for the SAME real instrument type. Real scoping (2026-07-16 read):

* ``"perpetual"`` — 15,802 manifest rows (``sum(row_count)`` = 1,152,860, matching the
  operator's "~1.15M rows" framing), ALL on the 6 multi-type ``*-FUTURES`` Tardis-blend
  venues (BINANCE-FUTURES, COINBASE-FUTURES, KRAKEN-FUTURES, OKX-FUTURES,
  BITGET-FUTURES, BITFINEX-FUTURES), date range 2019-03-30 → 2026-06-28.
* ``"spot"`` — 14,418 manifest rows (``sum(row_count)`` = 502,714, matching "~502K
  rows"), on the 7 ``*-SPOT`` venues (BINANCE-SPOT, BITFINEX-SPOT, COINBASE-SPOT,
  OKX-SPOT, BITGET-SPOT, BYBIT-SPOT, KRAKEN-SPOT), date range 2019-03-30 → 2026-06-23.

Root cause + writer status (already fixed, this script is the backlog cleanup)
--------------------------------------------------------------------------------
The manifest row_key is ``(date, venue, instrument_type, ...)`` and is
PERMANENT/literal-string-keyed (``ManifestWriter.record_captured`` upserts by the
full ``_ROW_KEY_COLUMNS`` tuple) — a stray lowercase value written under an older
code path never self-heals; it just accumulates FOREVER alongside the canonical-cased
row for the same real shard atom once the writer is fixed, because the two literal
key strings never collide. Live evidence (2026-07-16):

* ``InstrumentRecord.instrument_type`` (UAC ``unified_api_contracts.internal.
  reference.instrument``) has been a STRICT ``InstrumentType`` enum field since
  UAC@6f0e0c2e (2026-04-02) — a raw lowercase string now hard-fails Pydantic
  validation at record-construction time (verified live:
  ``InstrumentRecord(instrument_type="perpetual", ...)`` raises
  ``ValidationError``). Every live CeFi adapter (Tardis + CCXT) constructs
  ``InstrumentRecord`` via real ``InstrumentType`` enum members
  (``_TYPE_MAP``/``_map_ccxt_type``), never a raw string.
* The manifest multi-type-split fix (``instruments-service@91fc7bd2``,
  2026-07-07, "emit one manifest row per (date, venue, instrument_type) for
  multi-type venues") is the most recent writer-path change touching this exact
  row_key construction.
* Direct production verification: the per-day snapshot files backing the
  legacy-lowercase manifest rows are THEMSELVES already clean —
  ``instrument_availability/by_date/day=2026-06-28/venue=BINANCE-FUTURES/
  instruments.parquet`` (the file the stale ``"perpetual"`` manifest row purports
  to describe) contains ``{'PERPETUAL': 672, 'FUTURE': 4}`` — 0 lowercase rows.
  Fresh manifest writes for BINANCE-FUTURES from 2026-07-10 through 2026-07-16
  (today) are 100% clean, canonical-cased, and correctly per-type split
  (``PERPETUAL`` + ``FUTURE`` as two rows). The legacy-lowercase manifest rows are
  therefore confirmed STALE BOOKKEEPING ENTRIES, not a sign of an active data
  leak in the underlying parquet corpus.
* ``prod/catalog.parquet`` (the separate rolled-up per-instrument table) was
  checked directly and carries ZERO lowercase instrument_type values (424,633
  rows: only OPTION/COMBO/FUTURE/SPOT_PAIR/PERPETUAL) — no catalog-side
  migration is needed.
* ``instruments_service/engine/orchestrator/writers.py``'s
  ``_split_by_instrument_type`` (the manifest row_key emission site) additionally
  gained a defensive ``_LEGACY_INSTRUMENT_TYPE_ALIASES`` normalization guard in
  this same change-set, so even a future/residual non-``InstrumentRecord``-
  validated write path can never again mint a new permanently-duplicated
  lowercase row_key.

Cross-service coordination (HARD RULE check, 2026-07-16)
------------------------------------------------------------
``instrument_type`` is a SHARD axis for MTDS/MDPS/features (their OWN manifests),
but a DISPLAY axis for instruments-service (this manifest). Checked whether
market-tick-data-service / market-data-processing-service / features-service read
``instrument_type`` FROM this IS manifest as part of THEIR OWN shard key: they do
NOT. MTDS has its own, fully independent ``TardisAdapter`` (a different class in a
different repo) that derives ``instrument_type`` from its own raw Tardis captures
via its own ``_classify_row_instrument_type`` — it does not query or read IS's
``_index/availability_index.parquet`` or ``prod/catalog.parquet`` at all (confirmed
by grep: no MTDS/MDPS/features code references the IS manifest/catalog blobs).
MTDS independently discovered + partially fixed the SAME class of legacy-lowercase
bug in ITS OWN manifest (``market-tick-data-service/scripts/
normalize_instrument_type_casing.py`` +
``relabel_bybit_spot_perpetual_itype_2026_07_07.py``) — a parallel, unrelated
occurrence worth flagging to the operator (see this script's final report), but
this IS-side migration does not need to coordinate with or block on MTDS/MDPS/
features: the two manifests are fully independent surfaces.

Dedup mechanics (mission HARD RULE (e))
------------------------------------------
Collapsing ``"perpetual"`` → ``PERPETUAL`` (same for spot) can collide with an
ALREADY-EXISTING canonical-cased row for the identical real shard atom (the same
instrument captured twice under two type-spellings across the writer's evolution).
Real collision counts at 2026-07-16 scoping: 3,377 ``(date, venue)`` pairs carry
BOTH ``"perpetual"`` and ``PERPETUAL``; 10,003 pairs carry BOTH ``"spot"`` and
``SPOT_PAIR``. This script dedups on the manifest's REAL composite row identity —
the full ``unified_trading_library.manifest_writer._ROW_KEY_COLUMNS`` tuple
(``date, venue, data_type, timeframe, league_id, chain, instrument_type,
underlying, feature_group, feature_family, model_family, training_period,
strategy_id, client_id, instruction_type, instrument_id, quote_asset, margin_type,
combo_type, fixture_id, job_id, pipeline_mode, source, transport, cadence`` — the
SAME key ``ManifestWriter.record_captured`` upserts on), keeping the row with the
LATEST ``attempted_at`` (ties broken by ``written_at``) — last-write-wins, never a
blind ``pd.concat`` that could double-count captured rows in downstream coverage
math. Only row_keys that are ACTUALLY touched by the ``perpetual``/``spot`` remap
are eligible for collapse — every other row in the 93,958-row index (including any
unrelated pre-existing duplicate outside this script's scope) is left byte-for-byte
untouched.

Safety: dry-run by default; ``--apply --confirm`` writes. Backs up the live index
to a timestamped blob before any write. Post-write re-download verification
confirms 0 remaining lowercase ``perpetual``/``spot`` instrument_type rows and the
expected post-collapse row count. Idempotent — a second run finds 0 legacy rows
and is a clean no-op.

Usage::

    cd instruments-service

    # Dry-run (default: report only, no writes)
    .venv/bin/python scripts/canonicalize_cefi_instrument_type_legacy_lowercase_2026_07_16.py

    # Apply (writes backup + migrated index, then re-verifies)
    .venv/bin/python scripts/canonicalize_cefi_instrument_type_legacy_lowercase_2026_07_16.py --apply --confirm

SSOT: unified-trading-pm/plans/active/data_status_page_ux_and_canonicalisation_2026_07_16.md P9.
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

# Reuse the real manifest row-identity SSOT (never hand-copy the key columns —
# codex/06-coding-standards "minimize the change surface").
from unified_trading_library.manifest_writer import _ROW_KEY_COLUMNS  # noqa: qg-deep-import

# Reuse the SAME alias map the writer-side defensive guard uses — one mapping,
# not a duplicated copy that could drift.
from instruments_service.engine.orchestrator.writers import (  # noqa: qg-deep-import
    _LEGACY_INSTRUMENT_TYPE_ALIASES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
BACKUP_PREFIX = "_index/availability_index.legacyinstrumenttypefix."

_KEY_COLS: list[str] = list(_ROW_KEY_COLUMNS)


def _row_key_series(df: pd.DataFrame) -> pd.Series:
    """Build the composite manifest row-identity key string per row.

    Mirrors the SAME columns ``ManifestWriter.record_captured`` upserts on
    (``_ROW_KEY_COLUMNS``). NaN-safe (``fillna("")``) so a missing/None dimension
    matches consistently with how the writer itself treats absent columns.
    """
    blank = pd.Series([""] * len(df), index=df.index)
    cols = [df[c].astype("string").fillna("") if c in df.columns else blank for c in _KEY_COLS]
    key_df = pd.concat(cols, axis=1)
    return key_df.astype(str).agg("\x1f".join, axis=1)


def canonicalize_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Collapse legacy lowercase ``instrument_type`` dupes. Pure + idempotent.

    Returns ``(out_df, stats)``. ``out_df`` is byte-identical to ``df`` for every
    row whose composite row-key is not touched by the remap; touched row-keys keep
    only the most-recently-attempted survivor.
    """
    stats: dict[str, int] = {
        "rows_in": len(df),
        "legacy_perpetual": 0,
        "legacy_spot": 0,
        "touched_rows": 0,
        "collisions_dropped": 0,
        "rows_out": len(df),
    }
    if df.empty or "instrument_type" not in df.columns:
        return df, stats

    itype = df["instrument_type"].astype("string").fillna("")
    legacy_mask = itype.isin(list(_LEGACY_INSTRUMENT_TYPE_ALIASES))
    stats["legacy_perpetual"] = int((itype == "perpetual").sum())
    stats["legacy_spot"] = int((itype == "spot").sum())
    if not bool(legacy_mask.any()):
        return df, stats

    work = df.copy()
    work.loc[legacy_mask, "instrument_type"] = itype.loc[legacy_mask].map(_LEGACY_INSTRUMENT_TYPE_ALIASES)

    key = _row_key_series(work)
    touched_keys = set(key[legacy_mask.to_numpy()])
    in_scope = key.isin(touched_keys)
    stats["touched_rows"] = int(in_scope.sum())

    scope_df = work.loc[in_scope].copy()
    scope_df["_rk"] = key.loc[in_scope]
    # Last-write-wins: sort by attempted_at DESC, written_at DESC, then keep the
    # first survivor per composite key (pandas keeps row ORDER within ties, so the
    # explicit sort makes "first" == "most recently attempted").
    sort_cols = [c for c in ("attempted_at", "written_at") if c in scope_df.columns]
    if sort_cols:
        scope_df = scope_df.sort_values(by=sort_cols, ascending=False, kind="stable")
    survivors = scope_df.drop_duplicates(subset="_rk", keep="first").drop(columns="_rk")

    out = pd.concat([work.loc[~in_scope], survivors], ignore_index=True)
    stats["collisions_dropped"] = stats["touched_rows"] - len(survivors)
    stats["rows_out"] = len(out)
    return out, stats


def _summarize(df: pd.DataFrame, label: str) -> None:
    counts = df["instrument_type"].astype("string").fillna("").value_counts()
    logger.info(
        "%s: rows=%d perpetual=%d spot=%d PERPETUAL=%d SPOT_PAIR=%d",
        label,
        len(df),
        int(counts.get("perpetual", 0)),
        int(counts.get("spot", 0)),
        int(counts.get("PERPETUAL", 0)),
        int(counts.get("SPOT_PAIR", 0)),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="Write the migrated index (default: dry-run report only).")
    ap.add_argument("--confirm", action="store_true", help="Required alongside --apply to mutate the live index.")
    args = ap.parse_args()
    apply = bool(args.apply and args.confirm)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as well — refusing to mutate without both flags.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    st = get_storage_client(project_id=None)
    logger.info("Mode=%s bucket=%s blob=%s", "APPLY" if apply else "DRY-RUN", bucket, INDEX_BLOB)

    raw = st.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    _summarize(df, "BEFORE")

    out, stats = canonicalize_frame(df)
    logger.info(
        "plan: rows_in=%d legacy_perpetual=%d legacy_spot=%d touched_rows=%d "
        "collisions_dropped=%d rows_out=%d (delta=%d)",
        stats["rows_in"],
        stats["legacy_perpetual"],
        stats["legacy_spot"],
        stats["touched_rows"],
        stats["collisions_dropped"],
        stats["rows_out"],
        stats["rows_in"] - stats["rows_out"],
    )
    _summarize(out, "AFTER (planned)")

    if stats["legacy_perpetual"] == 0 and stats["legacy_spot"] == 0:
        logger.info("Idempotent no-op — 0 legacy lowercase rows found.")
        return 0

    if not apply:
        logger.info("DRY-RUN (no writes). Re-run with --apply --confirm to migrate the live index.")
        return 0

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_key = f"{BACKUP_PREFIX}{ts}.bak.parquet"
    st.upload_bytes(bucket, backup_key, raw)
    logger.info("Backup written: gs://%s/%s (%d bytes)", bucket, backup_key, len(raw))

    buf = io.BytesIO()
    out.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    st.upload_bytes(bucket, INDEX_BLOB, buf.getvalue())
    logger.info(
        "APPLIED — gs://%s/%s rewritten (%d bytes, %d -> %d rows)",
        bucket,
        INDEX_BLOB,
        len(buf.getvalue()),
        stats["rows_in"],
        stats["rows_out"],
    )

    verify_raw = st.download_bytes(bucket, INDEX_BLOB)
    verify_df = pd.read_parquet(io.BytesIO(verify_raw))
    verify_itype = verify_df["instrument_type"].astype("string").fillna("")
    remaining_legacy = int(verify_itype.isin(list(_LEGACY_INSTRUMENT_TYPE_ALIASES)).sum())
    if remaining_legacy != 0 or len(verify_df) != stats["rows_out"]:
        logger.error(
            "VERIFICATION FAILED — re-downloaded index has remaining_legacy=%d rows=%d "
            "(expected 0, %d). Restore from backup: gs://%s/%s",
            remaining_legacy,
            len(verify_df),
            stats["rows_out"],
            bucket,
            backup_key,
        )
        return 1
    logger.info(
        "Verification PASSED — 0 remaining legacy lowercase rows, row count matches (%d).",
        stats["rows_out"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
