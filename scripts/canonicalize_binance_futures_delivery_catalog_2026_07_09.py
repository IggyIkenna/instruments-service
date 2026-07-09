#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after every real BINANCE-FUTURES/BINANCE-DELIVERY instrument_id in both
#   prod/catalog.parquet AND instrument_availability/by_date/**/venue=BINANCE-{FUTURES,DELIVERY}/
#   instruments.parquet carries the @LIN/@INV marker (verified 0 remaining legacy-shape rows by
#   this script's own --report mode), AND
#   unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
#   finding 1 is marked resolved for BINANCE-FUTURES/BINANCE-DELIVERY.
"""One-off backfill — relabel real BINANCE-FUTURES / BINANCE-DELIVERY ``instrument_id`` strings to
the operator-decided ``@LIN``/``@INV`` canonical target format, in place, per the migration-mechanics
decision (``instrument_id_format_canonicalization_2026_07_08.md``: "rewrite/relabel already-downloaded
data in place ... not a re-download from the source venue").

Target format (finding 1, PERPETUAL scope expanded 2026-07-09)::

    PERPETUAL: VENUE:PERPETUAL:BASE-QUOTE            -> VENUE:PERPETUAL:BASE-QUOTE@LIN|@INV
    FUTURE:    VENUE:FUTURE:BASEQUOTE_YYMMDD (raw)   -> VENUE:FUTURE:BASE-QUOTE@LIN|@INV-YYYYMMDD

Real evidence (2026-07-09, ``prod/catalog.parquet``, cefi asset group,
``instruments-store-cefi-prd-central-element-323112``)::

    BINANCE-FUTURES  PERPETUAL: 837 rows, margin_type=linear  (100%)
    BINANCE-FUTURES  FUTURE:     48 rows, margin_type=linear  (100%)
    BINANCE-DELIVERY PERPETUAL:  45 rows, margin_type=inverse (100%)
    BINANCE-DELIVERY FUTURE:    177 rows, margin_type=inverse (100%)
    Total catalog rows: 1,107 (single prod/catalog.parquet file)

Two independent surfaces, BOTH real "already-downloaded historical GCS rows":

1. ``prod/catalog.parquet`` — the rolled-up multi-grain reference table. Small (1,107 rows in ONE
   file) — ``--catalog`` migrates it in a single pass. This table lacks separate
   ``quote_asset``/``expiry`` columns, so base/quote/expiry are re-derived from the CURRENT
   ``instrument_id`` string itself (PERPETUAL is already dash-joined ``BASE-QUOTE``; FUTURE is the
   raw concatenated ``BASEQUOTE_YYMMDD`` Tardis shape — split via the same longest-first
   quote-currency matcher ``parsing.py::_split_symbol`` uses).

2. ``instrument_availability/by_date/day=*/[pipeline_mode=.../asset_group=cefi/]venue=BINANCE-
   {FUTURES,DELIVERY}/instruments.parquet`` — the per-day-per-venue upstream snapshots
   ``build_instrument_catalogue.py`` rolls ``catalog.parquet`` UP FROM. Real full-scan count
   (2026-07-09): 4,878 BINANCE-FUTURES + 4,356 BINANCE-DELIVERY = 9,234 files, since 2019-11-17.
   **IMPORTANT**: ``catalog.parquet`` is DERIVED from these snapshots — a catalog-only fix is NOT
   durable; the next scheduled catalogue rebuild will silently regenerate ``catalog.parquet`` FROM
   the still-legacy-format snapshots and revert the catalog fix. Unlike the catalog table, each
   snapshot row already carries clean ``base_asset``/``quote_asset``/``margin_type``/``expiry``
   columns — no string re-parsing needed, just routed straight through the shared UAC builder.
   ``--by-date-sample N`` migrates a bounded real sample (smoke test); ``--by-date-all`` runs the
   full real sweep across all 9,234 files (NOT run by the authoring dispatch — smoke-test-derived
   ETA only, full sweep is a deliberate separate go/no-go per this workspace's established staged
   process).

Both transforms route final construction through the shared UAC builder
(``unified_api_contracts.build_instrument_id(..., margin_marker=...)``) — real callable-builder
enforcement, not an ad hoc f-string, per this workspace's "minimize the change surface" convention.

Safety: writes a timestamped backup blob before overwriting; refuses to write a blob if its row
count changes or a new duplicate ``instrument_id``/``instrument_key`` is introduced. Dry-run by
default; ``--apply --confirm`` mutates. Idempotent — a blob with 0 remaining legacy-shape rows
reports 0 changes and is a no-op on re-run.

Usage::

    cd instruments-service
    .venv/bin/python scripts/canonicalize_binance_futures_delivery_catalog_2026_07_09.py --catalog
    .venv/bin/python scripts/canonicalize_binance_futures_delivery_catalog_2026_07_09.py --catalog --apply --confirm

    .venv/bin/python scripts/canonicalize_binance_futures_delivery_catalog_2026_07_09.py --by-date-sample 10
    .venv/bin/python scripts/canonicalize_binance_futures_delivery_catalog_2026_07_09.py --by-date-sample 10 --apply --confirm

    # Full historical sweep (9,234 files), thread-pool concurrent (--workers, default 16):
    .venv/bin/python scripts/canonicalize_binance_futures_delivery_catalog_2026_07_09.py \
        --by-date-all --apply --confirm --workers 16

SSOT: ``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
finding 1; ``instruments-service/docs/CEFI_INSTRUMENTS.md`` "Instrument ID format: current vs.
decided target".
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as _dt
import io
import logging
import threading
import time

import pandas as pd
from unified_api_contracts._instrument_enums import InstrumentType
from unified_api_contracts.internal.reference.canonical_id_builder import build_instrument_id
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOGUE_BLOB = "prod/catalog.parquet"
BY_DATE_PREFIX = "instrument_availability/by_date/"
_TARGET_VENUES = ("BINANCE-FUTURES", "BINANCE-DELIVERY")
_MARGIN_MARKER: dict[str, str] = {"linear": "LIN", "inverse": "INV"}

# Same longest-first quote-currency list as
# instruments_service.reference_data.adapters.cefi.tardis.parsing._QUOTE_CURRENCIES
# (duplicated here deliberately — that module has real concurrent sibling-agent edits in
# flight this session; this one-off script stays self-contained rather than importing from
# a file whose API surface is actively changing mid-session).
_QUOTE_CURRENCIES: tuple[str, ...] = (
    "USDT",
    "USDC",
    "BUSD",
    "TUSD",
    "USDE",
    "BRZ",
    "IDRT",
    "BIDR",
    "USD",
    "BTC",
    "ETH",
    "BNB",
    "XRP",
    "TRX",
    "ADA",
    "SOL",
    "LTC",
    "DOT",
    "LINK",
    "UNI",
    "AVAX",
    "DOGE",
    "SHIB",
    "PEPE",
    "FLOKI",
    "WIF",
    "BONK",
    "MEME",
    "KRW",
    "EUR",
    "GBP",
    "JPY",
    "AUD",
    "TRY",
    "BRL",
    "RUB",
    "NGN",
    "ZAR",
    "DAI",
    "VAI",
    "GEL",
    "CZK",
    "MXN",
    "ARS",
    "COP",
    "CLP",
    "PEN",
    "VES",
    "UAH",
    "PLN",
    "RON",
    "CNY",
    "HKD",
    "AED",
    "SAR",
    "MYR",
    "SGD",
    "IDR",
    "PHP",
    "THB",
    "INR",
    "VND",
    "TWD",
)


def _split_concat_symbol(symbol: str) -> tuple[str, str]:
    """Split a concatenated ``BASEQUOTE`` symbol via the longest-first quote suffix match."""
    for quote in _QUOTE_CURRENCIES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return symbol, ""


def _canonicalize_catalog_instrument_id(instrument_id: str, venue: str, instrument_type: str, margin_type: str) -> str:
    """Rewrite one legacy ``prod/catalog.parquet`` BINANCE-FUTURES/DELIVERY instrument_id.

    Returns the ORIGINAL string unchanged for any row that isn't a real, matching legacy shape
    (wrong venue, unsupported type, unresolved marker, already migrated, unparseable body) — never
    guesses or half-transforms.
    """
    marker = _MARGIN_MARKER.get((margin_type or "").strip().lower())
    if venue not in _TARGET_VENUES or marker is None:
        return instrument_id
    prefix = f"{venue}:{instrument_type}:"
    if not instrument_id.startswith(prefix):
        return instrument_id
    segment = instrument_id[len(prefix) :]
    if "@" in segment:
        return instrument_id  # already migrated — idempotent no-op

    if instrument_type == "PERPETUAL":
        if "-" not in segment:
            return instrument_id
        base, _, quote = segment.partition("-")
        if not base or not quote:
            return instrument_id
        return build_instrument_id(venue, InstrumentType.PERPETUAL, f"{base}-{quote}", margin_marker=marker)

    if instrument_type == "FUTURE":
        if "_" not in segment:
            return instrument_id
        concat, _, date_part = segment.rpartition("_")
        if len(date_part) != 6 or not date_part.isdigit():
            return instrument_id
        base, quote = _split_concat_symbol(concat)
        if not base or not quote:
            return instrument_id
        try:
            expiry_date = _dt.date(2000 + int(date_part[:2]), int(date_part[2:4]), int(date_part[4:6]))
        except ValueError:
            return instrument_id
        return build_instrument_id(
            venue, InstrumentType.FUTURE, f"{base}-{quote}", expiry_date=expiry_date, margin_marker=marker
        )

    return instrument_id


def canonicalize_catalog_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Apply the catalog-table transform to every real BINANCE-FUTURES/DELIVERY row. Pure + idempotent."""
    stats = {"rows_in": len(df), "rows_out": len(df), "changed": 0}
    if df.empty or "instrument_id" not in df.columns:
        return df, stats

    mask = df["venue"].isin(_TARGET_VENUES)
    orig = df["instrument_id"].astype(str)
    new_ids = orig.copy()
    for idx in df.index[mask]:
        row = df.loc[idx]
        new_ids.loc[idx] = _canonicalize_catalog_instrument_id(
            str(row["instrument_id"]), str(row["venue"]), str(row["instrument_type"]), str(row.get("margin_type", ""))
        )

    stats["changed"] = int((new_ids != orig).sum())
    out = df.copy()
    out["instrument_id"] = new_ids
    stats["rows_out"] = len(out)
    return out, stats


def _canonicalize_by_date_instrument_key(
    instrument_key: str,
    venue: str,
    instrument_type: str,
    base_asset: str,
    quote_asset: str,
    margin_type: str,
    expiry: pd.Timestamp | None,
) -> str:
    """Rewrite one legacy per-day-snapshot BINANCE-FUTURES/DELIVERY ``instrument_key``.

    Unlike the catalog table, the per-day snapshot rows already carry clean
    ``base_asset``/``quote_asset``/``margin_type``/``expiry`` columns — no string
    re-parsing of the legacy id, just routed straight through the shared UAC builder.
    """
    marker = _MARGIN_MARKER.get((margin_type or "").strip().lower())
    if venue not in _TARGET_VENUES or marker is None or not base_asset or not quote_asset:
        return instrument_key
    if "@" in instrument_key:
        return instrument_key  # already migrated

    if instrument_type == "PERPETUAL":
        return build_instrument_id(venue, InstrumentType.PERPETUAL, f"{base_asset}-{quote_asset}", margin_marker=marker)

    if instrument_type == "FUTURE":
        if expiry is None or pd.isna(expiry):
            return instrument_key
        expiry_date = expiry.date() if hasattr(expiry, "date") else expiry
        return build_instrument_id(
            venue, InstrumentType.FUTURE, f"{base_asset}-{quote_asset}", expiry_date=expiry_date, margin_marker=marker
        )

    return instrument_key


def canonicalize_by_date_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Apply the per-day-snapshot transform. Pure + idempotent."""
    id_col = "instrument_key" if "instrument_key" in df.columns else "instrument_id"
    stats = {"rows_in": len(df), "rows_out": len(df), "changed": 0}
    if df.empty or id_col not in df.columns:
        return df, stats

    orig = df[id_col].astype(str)
    new_ids = orig.copy()
    for idx, row in df.iterrows():
        new_ids.loc[idx] = _canonicalize_by_date_instrument_key(
            str(row[id_col]),
            str(row.get("venue", "")),
            str(row.get("instrument_type", "")),
            str(row.get("base_asset", "") or ""),
            str(row.get("quote_asset", "") or ""),
            str(row.get("margin_type", "") or ""),
            row.get("expiry"),
        )

    stats["changed"] = int((new_ids != orig).sum())
    out = df.copy()
    out[id_col] = new_ids
    stats["rows_out"] = len(out)
    return out, stats


def _bak(blob: str, ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{ts}.binancefix.bak.parquet"
    return f"{blob}.{ts}.binancefix.bak"


def _write_blob(st, bucket: str, blob: str, raw: bytes, out: pd.DataFrame, id_col: str, apply: bool, ts: str) -> None:
    dup_before = int(pd.Series(pd.read_parquet(io.BytesIO(raw))[id_col]).astype(str).duplicated().sum())
    dup_after = int(out[id_col].astype(str).duplicated().sum())
    if dup_after > dup_before:
        logger.error(
            "ABORT %s: relabel introduced %d new duplicate %s value(s) — refusing to write",
            blob,
            dup_after - dup_before,
            id_col,
        )
        return
    if not apply:
        return
    st.upload_bytes(bucket, _bak(blob, ts), raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    st.upload_bytes(bucket, blob, buf.read())
    logger.info("wrote %s (backup -> %s)", blob, _bak(blob, ts))


def run_catalog(st, bucket: str, apply: bool, ts: str) -> None:
    logger.info("=== catalog: %s ===", CATALOGUE_BLOB)
    raw = st.download_bytes(bucket, CATALOGUE_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = canonicalize_catalog_frame(df)
    logger.info("catalog rows=%d changed=%d", stats["rows_in"], stats["changed"])
    if stats["rows_out"] != stats["rows_in"]:
        logger.error("ABORT: row count changed (%d -> %d) — refusing to write", stats["rows_in"], stats["rows_out"])
        return
    if stats["changed"] == 0:
        logger.info("catalog: nothing to fix — idempotent no-op.")
        return
    _write_blob(st, bucket, CATALOGUE_BLOB, raw, out, "instrument_id", apply, ts)


def _list_by_date_targets(st, bucket: str, limit: int | None) -> list[str]:
    """List real primary ``instruments.parquet`` blobs only.

    Must exclude this script's own ``.binancefix.bak.parquet`` backup blobs — a bare
    ``venue=BINANCE-FUTURES/`` substring match also matches
    ``venue=BINANCE-FUTURES/instruments.<ts>.binancefix.bak.parquet`` (real, found live
    2026-07-09: a prior smoke-test run's backups were sitting in the same prefix). Without
    this exclusion, a sweep would re-migrate + re-backup its own backups, corrupting the
    backup chain (mutates what's supposed to be an immutable pre-fix snapshot) and wildly
    inflating file/changed counts as more backups accumulate across runs.
    """
    targets: list[str] = []
    for b in st.list_blobs(bucket, prefix=BY_DATE_PREFIX):
        if not b.name.endswith("/instruments.parquet"):
            continue
        if "venue=BINANCE-FUTURES/" in b.name or "venue=BINANCE-DELIVERY/" in b.name:
            targets.append(b.name)
            if limit is not None and len(targets) >= limit:
                break
    return targets


def _process_one_by_date_blob(st, bucket: str, blob: str, apply: bool, ts: str) -> dict[str, int]:
    """Download+canonicalize+(optionally write) ONE by_date blob. Returns per-file stats.

    Pure per-file unit of work — safe to call from multiple worker threads concurrently
    (each call only touches its own local ``raw``/``df``/``out`` objects; the shared
    storage client is used read-only-per-blob-path, same pattern as the rest of this
    workspace's shard-level-isolated concurrent GCS I/O).
    """
    raw = st.download_bytes(bucket, blob)
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = canonicalize_by_date_frame(df)
    if stats["rows_out"] != stats["rows_in"]:
        logger.error("ABORT %s: row count changed (%d -> %d)", blob, stats["rows_in"], stats["rows_out"])
        stats["changed"] = 0
        return stats
    id_col = "instrument_key" if "instrument_key" in df.columns else "instrument_id"
    if stats["changed"] > 0:
        _write_blob(st, bucket, blob, raw, out, id_col, apply, ts)
    return stats


def run_by_date(st, bucket: str, apply: bool, ts: str, limit: int | None, workers: int) -> None:
    """Real-concurrency sweep over every target by_date blob (thread-pool, shard-isolated).

    A single file is a serial download+backup-upload+rewrite-upload round trip (I/O-bound,
    not CPU-bound), so a thread pool gives near-linear speedup up to GCS's per-project
    concurrent-request ceiling. Per-file exceptions (network hiccup, malformed row) are
    caught here and logged — one bad file never aborts the sweep (same shard-level-failure-
    isolation convention as the rest of this workspace's per-shard capture loops).
    """
    targets = _list_by_date_targets(st, bucket, limit)
    logger.info("=== by_date: %d target file(s) (limit=%s, workers=%d) ===", len(targets), limit, workers)
    total_rows_changed = 0
    total_files_changed = 0
    total_errors = 0
    completed = 0
    lock = threading.Lock()
    t0 = time.time()

    def _run_one(blob: str) -> tuple[str, dict[str, int] | None]:
        try:
            return blob, _process_one_by_date_blob(st, bucket, blob, apply, ts)
        except Exception:
            logger.exception("ERROR %s — skipping (shard-isolated, sweep continues)", blob)
            return blob, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_run_one, blob) for blob in targets]
        for future in concurrent.futures.as_completed(futures):
            _blob, stats = future.result()
            with lock:
                completed += 1
                if stats is None:
                    total_errors += 1
                elif stats["changed"] > 0:
                    total_files_changed += 1
                    total_rows_changed += stats["changed"]
                if completed % 500 == 0 or completed == len(targets):
                    elapsed_so_far = time.time() - t0
                    logger.info(
                        "[progress] %d/%d done, %.1fs elapsed (%.3fs/file avg) "
                        "files_changed=%d rows_changed=%d errors=%d",
                        completed,
                        len(targets),
                        elapsed_so_far,
                        elapsed_so_far / completed,
                        total_files_changed,
                        total_rows_changed,
                        total_errors,
                    )

    elapsed = time.time() - t0
    per_file = elapsed / len(targets) if targets else 0.0
    logger.info(
        "by_date DONE: files=%d files_changed=%d rows_changed=%d errors=%d elapsed=%.1fs (%.3fs/file, workers=%d)",
        len(targets),
        total_files_changed,
        total_rows_changed,
        total_errors,
        elapsed,
        per_file,
        workers,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--catalog", action="store_true", help="Migrate prod/catalog.parquet (full, 1,107 rows).")
    ap.add_argument(
        "--by-date-sample", type=int, default=None, metavar="N", help="Migrate the first N by_date files (smoke test)."
    )
    ap.add_argument(
        "--by-date-all", action="store_true", help="Migrate ALL real by_date files (9,234) — full historical sweep."
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=16,
        metavar="N",
        help="Thread-pool concurrency for --by-date-* GCS read-modify-write ops (default: 16).",
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
