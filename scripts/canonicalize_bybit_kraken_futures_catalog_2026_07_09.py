#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after every real BYBIT/KRAKEN-FUTURES row in both prod/catalog.parquet AND
#   instrument_availability/by_date/**/venue={BYBIT,KRAKEN-FUTURES}/instruments.parquet carries a
#   correct margin_type AND the @LIN/@INV-YYYYMMDD canonical instrument_id marker (verified 0
#   remaining wrong/legacy-shape rows by this script's own dry-run report mode), AND
#   unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
#   finding 1 is marked resolved for BYBIT/KRAKEN-FUTURES.
"""One-off backfill — two real, related BYBIT/KRAKEN-FUTURES defects in one pass:

1. **Wrong ``margin_type``** (``_infer_margin_type`` had no venue branch for either exchange pre-fix,
   commit ``176d4610``): real inverse (coin-margined) rows silently defaulted to ``linear``. Real,
   live-verified impact against ``prod/catalog.parquet`` (2026-07-09): 279/1,543 BYBIT rows (255
   FUTURE + 24 PERPETUAL) and 382/1,103 KRAKEN-FUTURES rows (378 FUTURE + 4 PERPETUAL) mislabeled.
2. **Legacy ``instrument_id``/``instrument_key`` format** — still the raw Tardis symbol passthrough
   (``KRAKEN-FUTURES:FUTURE:FF_ETHUSD_230728``, ``BYBIT:FUTURE:BTC-01DEC23``), not the
   operator-decided ``@LIN``/``@INV``-``YYYYMMDD`` target
   (``instrument_id_format_canonicalization_2026_07_08.md`` finding 1).

Both defects share one root cause and one fix: re-derive ``(base, quote, margin_type[, expiry])``
from each row's own already-captured ``raw_symbol`` via the corrected, real parsing helpers in
``instruments_service.reference_data.adapters.cefi.tardis.parsing`` (``_split_bybit_symbol``,
``_split_kraken_symbol``, ``_infer_margin_type`` — all already fixed + unit-tested, commit
``176d4610``; imported directly here rather than duplicated, since — unlike ``adapter.py`` — this
module has no concurrent sibling edit in flight this session), then rebuild BOTH the corrected
``margin_type`` value and the target canonical id via the shared
``_build_canonical_perpetual_key``/``_build_canonical_future_key`` builders (same module). One
transform, one row rewrite — never a catalog-only or margin-type-only half-fix.

Two independent real surfaces, same as the Binance/Deribit precedent scripts:

1. ``prod/catalog.parquet`` — the rolled-up table. 1,543 real BYBIT + 1,103 real KRAKEN-FUTURES rows
   (2026-07-09 scoping). Real ``raw_symbol`` column already present and 0% null for both venues —
   no string-splitting of ``instrument_id`` needed (unlike the Binance catalog script), the ground
   truth Tardis-native symbol is already sitting in its own column.
2. ``instrument_availability/by_date/day=*/[pipeline_mode=.../asset_group=cefi/]venue={BYBIT,
   KRAKEN-FUTURES}/instruments.parquet`` — the per-day upstream snapshots ``catalog.parquet`` is
   rolled up FROM (``build_instrument_catalogue.py``). A catalog-only fix is NOT durable — the next
   scheduled catalogue rebuild silently regenerates ``catalog.parquet`` from these still-legacy
   snapshots (same caveat already proven real for the Deribit/finding-7 migrations this session).
   Real full-scan count (2026-07-09): 4,788 BYBIT + 4,752 KRAKEN-FUTURES = 9,540 files. Each
   snapshot row already carries clean ``base_asset``/``quote_asset``/``margin_type``/``expiry``/
   ``raw_symbol`` columns (same shape as the catalog table) — same re-derivation, no metadata gap.

The Bybit no-dash CME-style legacy shape (``BTCUSDH22``, 46 real symbols) is deliberately OUT OF
SCOPE for this script: confirmed 0/46 present in ``prod/catalog.parquet`` today (this bug silently
dropped them at write time, historically, every day — there is nothing to relabel because nothing
was ever captured). Re-capturing them is a separate live re-capture run via the normal adapter path
(``scripts/recapture_bybit_legacy_quarterly_futures_2026_07_09.py``), not a backfill of this script.

Both transforms route final construction through the shared parsing.py builders (which themselves
route through the shared UAC ``build_instrument_id(..., passthrough=True)``) — real callable-builder
enforcement per this workspace's "minimize the change surface" convention.

Safety: writes a timestamped backup blob before overwriting; refuses to write a blob if its row
count changes or a new duplicate ``instrument_id``/``instrument_key`` is introduced; a row that
fails to resolve (base/quote/margin_type/expiry) is left UNCHANGED, never guessed. Dry-run by
default; ``--apply --confirm`` mutates. Idempotent — a blob with 0 remaining fixable rows reports 0
changes and is a no-op on re-run.

Usage::

    cd instruments-service
    .venv/bin/python scripts/canonicalize_bybit_kraken_futures_catalog_2026_07_09.py --catalog
    .venv/bin/python scripts/canonicalize_bybit_kraken_futures_catalog_2026_07_09.py --catalog --apply --confirm

    .venv/bin/python scripts/canonicalize_bybit_kraken_futures_catalog_2026_07_09.py --by-date-sample 10
    .venv/bin/python scripts/canonicalize_bybit_kraken_futures_catalog_2026_07_09.py --by-date-sample 10 --apply --confirm

    # Full historical sweep (9,540 files):
    .venv/bin/python scripts/canonicalize_bybit_kraken_futures_catalog_2026_07_09.py --by-date-all --apply --confirm --workers 16

SSOT: ``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
finding 1; ``instruments-service/docs/CEFI_INSTRUMENTS.md`` "Instrument ID format: current vs.
decided target".
"""

from __future__ import annotations

import argparse
import datetime as _dt
import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from unified_api_contracts.internal import InstrumentType, MarginType
from unified_trading_library import get_storage_client, resolve_bucket_name

from instruments_service.reference_data.adapters.cefi.tardis import parsing as tardis_parsing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOGUE_BLOB = "prod/catalog.parquet"
BY_DATE_PREFIX = "instrument_availability/by_date/"
_TARGET_VENUES = ("BYBIT", "KRAKEN-FUTURES")
_EXCHANGE_FOR_VENUE: dict[str, str] = {"BYBIT": "bybit", "KRAKEN-FUTURES": "cryptofacilities"}


def _resolve_base_quote(venue: str, instrument_type: str, raw_symbol: str) -> tuple[str, str]:
    """Split a real BYBIT/KRAKEN-FUTURES ``raw_symbol`` into ``(base, quote)`` via the real,
    already-fixed parsing.py helpers (no re-implementation — same functions ``adapter.py`` calls).
    """
    upper = raw_symbol.upper()
    if venue == "KRAKEN-FUTURES":
        return tardis_parsing._split_kraken_symbol(upper)
    if venue == "BYBIT":
        if instrument_type == "FUTURE":
            return tardis_parsing._split_bybit_symbol(upper)
        if instrument_type == "PERPETUAL":
            return tardis_parsing._split_symbol(upper)
    return "", ""


def _resolve_expiry(venue: str, raw_symbol: str) -> _dt.datetime | None:
    """Resolve the real expiry for a FUTURE row from its ``raw_symbol`` (DDMMMYY dash-suffix for
    Bybit, underscore-YYMMDD for Kraken-Futures) — same shapes the real captured corpus carries
    today (the Bybit no-dash CME shape is out of scope, see module docstring).
    """
    upper = raw_symbol.upper()
    if venue == "KRAKEN-FUTURES":
        return tardis_parsing._parse_underscore_yymmdd_symbol_expiry(upper)
    if venue == "BYBIT" and "-" in upper:
        _, _, date_part = upper.rpartition("-")
        return tardis_parsing._parse_ddmmmyy(date_part)
    return None


def _canonicalize_row(venue: str, instrument_type: str, raw_symbol: str) -> tuple[str | None, MarginType | None]:
    """Re-derive ``(canonical_symbol_id, margin_type)`` for one real BYBIT/KRAKEN-FUTURES row.

    Returns ``(None, None)`` when the row can't be safely resolved (caller leaves it UNCHANGED —
    never guesses or half-transforms).
    """
    base, quote = _resolve_base_quote(venue, instrument_type, raw_symbol)
    if not base or not quote:
        return None, None
    inst_type_enum = InstrumentType.FUTURE if instrument_type == "FUTURE" else InstrumentType.PERPETUAL
    exchange = _EXCHANGE_FOR_VENUE[venue]
    margin_type = tardis_parsing._infer_margin_type(inst_type_enum, quote, raw_symbol, exchange)
    if margin_type is None:
        return None, None

    if instrument_type == "PERPETUAL":
        key = tardis_parsing._build_canonical_perpetual_key(venue, base, quote, margin_type)
        return key, margin_type

    if instrument_type == "FUTURE":
        expiry = _resolve_expiry(venue, raw_symbol)
        if expiry is None:
            return None, None
        key = tardis_parsing._build_canonical_future_key(venue, base, quote, margin_type, expiry)
        return key, margin_type

    return None, None


def canonicalize_catalog_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Apply the catalog-table transform to every real BYBIT/KRAKEN-FUTURES row. Pure + idempotent."""
    stats = {"rows_in": len(df), "rows_out": len(df), "changed": 0, "margin_fixed": 0, "unresolved": 0}
    if df.empty or "instrument_id" not in df.columns:
        return df, stats

    mask = df["venue"].isin(_TARGET_VENUES)
    orig_id = df["instrument_id"].astype(str)
    orig_margin = df["margin_type"].astype(str)
    new_id = orig_id.copy()
    new_margin = df["margin_type"].copy()

    for idx in df.index[mask]:
        row = df.loc[idx]
        iid = str(row["instrument_id"])
        if "@" in iid:
            continue  # already migrated — idempotent no-op
        venue = str(row["venue"])
        itype = str(row["instrument_type"])
        raw_symbol = str(row.get("raw_symbol", "") or "")
        if not raw_symbol:
            stats["unresolved"] += 1
            continue
        key, margin_type = _canonicalize_row(venue, itype, raw_symbol)
        if key is None or margin_type is None:
            stats["unresolved"] += 1
            continue
        new_id.loc[idx] = key
        new_margin.loc[idx] = margin_type.value

    stats["changed"] = int((new_id != orig_id).sum())
    stats["margin_fixed"] = int((new_margin.astype(str) != orig_margin).sum())
    out = df.copy()
    out["instrument_id"] = new_id
    out["margin_type"] = new_margin
    stats["rows_out"] = len(out)
    return out, stats


def canonicalize_by_date_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Apply the per-day-snapshot transform. Pure + idempotent."""
    id_col = "instrument_key" if "instrument_key" in df.columns else "instrument_id"
    stats = {"rows_in": len(df), "rows_out": len(df), "changed": 0, "margin_fixed": 0, "unresolved": 0}
    if df.empty or id_col not in df.columns:
        return df, stats

    mask = df["venue"].isin(_TARGET_VENUES) if "venue" in df.columns else pd.Series(False, index=df.index)
    orig_id = df[id_col].astype(str)
    orig_margin = df["margin_type"].astype(str) if "margin_type" in df.columns else pd.Series("", index=df.index)
    new_id = orig_id.copy()
    new_margin = df["margin_type"].copy() if "margin_type" in df.columns else None

    for idx in df.index[mask]:
        row = df.loc[idx]
        iid = str(row[id_col])
        if "@" in iid:
            continue
        venue = str(row.get("venue", ""))
        itype = str(row.get("instrument_type", ""))
        raw_symbol = str(row.get("raw_symbol", "") or "")
        if not raw_symbol:
            stats["unresolved"] += 1
            continue
        key, margin_type = _canonicalize_row(venue, itype, raw_symbol)
        if key is None or margin_type is None:
            stats["unresolved"] += 1
            continue
        new_id.loc[idx] = key
        if new_margin is not None:
            new_margin.loc[idx] = margin_type.value

    stats["changed"] = int((new_id != orig_id).sum())
    out = df.copy()
    out[id_col] = new_id
    if new_margin is not None:
        stats["margin_fixed"] = int((new_margin.astype(str) != orig_margin).sum())
        out["margin_type"] = new_margin
    stats["rows_out"] = len(out)
    return out, stats


def _bak(blob: str, ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{ts}.bybitkrakenfix.bak.parquet"
    return f"{blob}.{ts}.bybitkrakenfix.bak"


def _write_blob(st, bucket: str, blob: str, raw: bytes, out: pd.DataFrame, id_col: str, apply: bool, ts: str) -> bool:
    dup_before = int(pd.Series(pd.read_parquet(io.BytesIO(raw))[id_col]).astype(str).duplicated().sum())
    dup_after = int(out[id_col].astype(str).duplicated().sum())
    if dup_after > dup_before:
        logger.error(
            "ABORT %s: relabel introduced %d new duplicate %s value(s) — refusing to write",
            blob,
            dup_after - dup_before,
            id_col,
        )
        return False
    if not apply:
        return True
    st.upload_bytes(bucket, _bak(blob, ts), raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    st.upload_bytes(bucket, blob, buf.read())
    logger.info("wrote %s (backup -> %s)", blob, _bak(blob, ts))
    return True


def run_catalog(st, bucket: str, apply: bool, ts: str) -> None:
    logger.info("=== catalog: %s ===", CATALOGUE_BLOB)
    raw = st.download_bytes(bucket, CATALOGUE_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = canonicalize_catalog_frame(df)
    logger.info(
        "catalog rows=%d id_changed=%d margin_fixed=%d unresolved=%d",
        stats["rows_in"],
        stats["changed"],
        stats["margin_fixed"],
        stats["unresolved"],
    )
    if stats["rows_out"] != stats["rows_in"]:
        logger.error("ABORT: row count changed (%d -> %d) — refusing to write", stats["rows_in"], stats["rows_out"])
        return
    if stats["changed"] == 0 and stats["margin_fixed"] == 0:
        logger.info("catalog: nothing to fix — idempotent no-op.")
        return
    _write_blob(st, bucket, CATALOGUE_BLOB, raw, out, "instrument_id", apply, ts)


def _list_by_date_targets(st, bucket: str, limit: int | None) -> list[str]:
    """List real primary per-day snapshot files only.

    Requires the blob name end exactly ``/instruments.parquet`` — a bare substring match on
    ``venue=BYBIT/``/``venue=KRAKEN-FUTURES/`` also matches this script's own
    ``.bybitkrakenfix.bak.parquet`` backup blobs (real bug found + fixed in the sibling Binance
    migration script's `_list_by_date_targets` during its own pre-sweep validation, 2026-07-09 —
    same fix applied here proactively).
    """
    targets: list[str] = []
    for b in st.list_blobs(bucket, prefix=BY_DATE_PREFIX):
        if not b.name.endswith("/instruments.parquet"):
            continue
        if "venue=BYBIT/" in b.name or "venue=KRAKEN-FUTURES/" in b.name:
            targets.append(b.name)
            if limit is not None and len(targets) >= limit:
                break
    return targets


def _process_one_by_date(st, bucket: str, blob: str, apply: bool, ts: str) -> dict[str, int]:
    raw = st.download_bytes(bucket, blob)
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = canonicalize_by_date_frame(df)
    if stats["rows_out"] != stats["rows_in"]:
        logger.error("ABORT %s: row count changed (%d -> %d)", blob, stats["rows_in"], stats["rows_out"])
        stats["written"] = 0
        return stats
    id_col = "instrument_key" if "instrument_key" in df.columns else "instrument_id"
    stats["written"] = 0
    if stats["changed"] > 0 or stats["margin_fixed"] > 0:
        ok = _write_blob(st, bucket, blob, raw, out, id_col, apply, ts)
        stats["written"] = 1 if ok else 0
    return stats


def run_by_date(st, bucket: str, apply: bool, ts: str, limit: int | None, workers: int) -> None:
    targets = _list_by_date_targets(st, bucket, limit)
    logger.info("=== by_date: %d target file(s) (limit=%s, workers=%d) ===", len(targets), limit, workers)
    total = {"files": 0, "files_changed": 0, "rows_id_changed": 0, "rows_margin_fixed": 0, "unresolved": 0}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process_one_by_date, st, bucket, blob, apply, ts): blob for blob in targets}
        for i, fut in enumerate(as_completed(futures), start=1):
            blob = futures[fut]
            try:
                stats = fut.result()
            except Exception:
                logger.exception("FAILED %s", blob)
                continue
            total["files"] += 1
            total["unresolved"] += stats["unresolved"]
            if stats["written"]:
                total["files_changed"] += 1
                total["rows_id_changed"] += stats["changed"]
                total["rows_margin_fixed"] += stats["margin_fixed"]
            if i % 200 == 0 or i == len(targets):
                elapsed = time.time() - t0
                logger.info(
                    "[%d/%d] elapsed=%.1fs (%.3fs/file) files_changed=%d id_changed=%d margin_fixed=%d unresolved=%d",
                    i,
                    len(targets),
                    elapsed,
                    elapsed / i,
                    total["files_changed"],
                    total["rows_id_changed"],
                    total["rows_margin_fixed"],
                    total["unresolved"],
                )
    elapsed = time.time() - t0
    per_file = elapsed / len(targets) if targets else 0.0
    logger.info(
        "by_date DONE: files=%d files_changed=%d rows_id_changed=%d rows_margin_fixed=%d "
        "unresolved=%d elapsed=%.1fs (%.3fs/file, %d workers)",
        total["files"],
        total["files_changed"],
        total["rows_id_changed"],
        total["rows_margin_fixed"],
        total["unresolved"],
        elapsed,
        per_file,
        workers,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--catalog", action="store_true", help="Migrate prod/catalog.parquet (full, 2,646 BYBIT+KRAKEN-FUTURES rows)."
    )
    ap.add_argument(
        "--by-date-sample", type=int, default=None, metavar="N", help="Migrate the first N by_date files (smoke test)."
    )
    ap.add_argument(
        "--by-date-all", action="store_true", help="Migrate ALL real by_date files (9,540) — full historical sweep."
    )
    ap.add_argument(
        "--workers", type=int, default=8, help="Concurrent thread-pool workers for --by-date-* (default 8)."
    )
    ap.add_argument("--apply", action="store_true", help="Write fixes back to GCS (default: dry-run report only).")
    ap.add_argument("--confirm", action="store_true", help="Required alongside --apply to actually mutate blobs.")
    args = ap.parse_args(argv)
    apply = bool(args.apply and args.confirm)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as well — refusing to mutate without both flags.")
        return 1
    if not (args.catalog or args.by_date_sample or args.by_date_all):
        logger.error("Nothing to do — pass --catalog, --by-date-sample N, and/or --by-date-all.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi", deployment_env="prd")
    st = get_storage_client(project_id=None)
    ts = _dt.datetime.now(_dt.UTC).strftime("%Y%m%d-%H%M%S")
    logger.info("Mode=%s bucket=%s", "APPLY" if apply else "DRY-RUN", bucket)

    if args.catalog:
        run_catalog(st, bucket, apply, ts)
    if args.by_date_sample:
        run_by_date(st, bucket, apply, ts, args.by_date_sample, args.workers)
    if args.by_date_all:
        run_by_date(st, bucket, apply, ts, None, args.workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
