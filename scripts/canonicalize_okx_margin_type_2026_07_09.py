#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the full OKX-SWAP/OKX-FUTURES margin_type backfill (prod/catalog.parquet +
#   instrument_availability/by_date per-day snapshots) is applied and re-verified, AND
#   docs/CEFI_INSTRUMENTS.md's "Known limitations" OKX row is removed.
"""One-off backfill — re-derive the real, corrected ``margin_type`` for every real OKX-SWAP /
OKX-FUTURES row in ``prod/catalog.parquet`` (and, with ``--by-day``, the per-day
``instrument_availability/by_date/day=*/venue=OKX-{SWAP,FUTURES}/instruments.parquet`` snapshot
corpus catalog.parquet is itself rolled up FROM).

Background (``fix(cefi): correct real OKX-SWAP/OKX-FUTURES margin-type inversion bug``,
2026-07-09, ``instruments-service@554ef058``): ``_infer_margin_type``
(``instruments_service/reference_data/adapters/cefi/tardis/parsing.py``) had one unconditional,
exchange-unscoped check mapping any ``_UM``/``_CM`` infix straight to ``MarginType.INVERSE``, plus no
OKX branch at all for the bare-USD-quoted case (fell through to the generic LINEAR default) — every
real OKX-SWAP/OKX-FUTURES derivative was mislabeled the OPPOSITE of its true margin type. The live
parsing bug is fixed (go-forward captures are correct); this script re-derives ``margin_type`` for
ALREADY-persisted rows from each row's own already-captured ``raw_symbol`` (no re-download from OKX)
and rewrites the store in place, per this workspace's migration-mechanics convention.

Real, live-verified corrected rule (OKX ``okex``/``okex-swap``/``okex-futures``, mirrors
``_infer_margin_type``'s OKX branch exactly): ``"USD_CM"`` in the upper-cased raw symbol -> INVERSE;
``"USD_UM"`` -> LINEAR; else bare ``USD`` quote segment -> INVERSE; anything else (``USDT``, ``USDC``,
...) -> LINEAR.

Two independent real surfaces, BOTH already-downloaded historical GCS data:

1. ``prod/catalog.parquet`` (default target) — the rolled-up reference table. Real scoping
   (2026-07-09): 6,053 real OKX-SWAP (623) + OKX-FUTURES (5,430) rows, single 4.4 MiB file. This
   table carries no separate ``quote_asset`` column — quote is re-derived from the already-captured
   ``raw_symbol`` (``BASE-QUOTE[_UM|_CM]-SUFFIX``, always 2 dashes for both PERPETUAL/SWAP and
   FUTURE rows in the real data). A 21-row smoke test (7 OKX-SWAP + 7 bare-USD OKX-FUTURES + 7
   ``_UM`` OKX-FUTURES) already ran and was verified correct (backup
   ``prod/catalog.okxmarginfix-smoketest.20260709-091832.bak.parquet``) — this script's
   ``--full-sweep`` (default target) migrates the REMAINING real mismatched rows.

2. ``instrument_availability/by_date/day=*/venue=OKX-{SWAP,FUTURES}/instruments.parquet`` — the
   per-day-per-venue snapshots ``catalog.parquet`` is rolled up FROM
   (``instruments_service/engine/orchestrator/writers.py``). Real scoping (2026-07-09, concurrent
   delimited GCS listing, NOT a full recursive walk): 2,381 real OKX-SWAP + 2,381 real OKX-FUTURES
   day-partitions = 4,762 real files, 2020-01-22 to present. **IMPORTANT**: catalog.parquet is
   DERIVED from these snapshots — a catalog-only fix is NOT durable; the next scheduled catalogue
   rebuild will silently regenerate ``catalog.parquet`` FROM the still-wrong snapshots and revert
   the catalog fix. Confirmed by direct sampling (2026-07-09) that the SAME inversion bug is present
   in this corpus (e.g. real ``OKX-FUTURES`` 2023-06-15 row ``BTC-USD-230616`` mislabeled ``linear``,
   should be ``inverse``). Unlike ``catalog.parquet``, each per-day snapshot row already carries a
   clean ``quote_asset`` column — no string re-parsing of a combined quote+infix token needed for
   the quote itself, only the ``raw_symbol`` infix check. ``--by-day-sample N`` migrates a bounded
   real sample (smoke test) across all 4,762 files; ``--by-day-full-sweep`` runs the full real sweep.

Usage::

    cd instruments-service

    # Dry-run against prod/catalog.parquet (default: report only, no writes)
    .venv/bin/python scripts/canonicalize_okx_margin_type_2026_07_09.py

    # Full sweep of prod/catalog.parquet (backup + write + verify)
    .venv/bin/python scripts/canonicalize_okx_margin_type_2026_07_09.py --apply --full-sweep

    # Per-day corpus: bounded real smoke test (backup + write + verify each touched file)
    .venv/bin/python scripts/canonicalize_okx_margin_type_2026_07_09.py --by-day --apply \\
        --by-day-sample 20

    # Per-day corpus: full real sweep (real concurrency, all 4,762 files)
    .venv/bin/python scripts/canonicalize_okx_margin_type_2026_07_09.py --by-day --apply \\
        --by-day-full-sweep --workers 40
"""

from __future__ import annotations

import argparse
import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library.cloud_interface.factory import (  # noqa: qg-deep-import — canonical migration-script GCS-object-op API (codex/05-infrastructure/gcs-object-operations.md)
    get_storage_client,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"
BUCKET = f"instruments-store-cefi-prd-{PROJECT_ID}"
CATALOG_PATH = "prod/catalog.parquet"
CATALOG_BACKUP_PREFIX = "prod/catalog.okxmarginfix-fullsweep."
BY_DATE_PREFIX = "instrument_availability/by_date/"

_VENUES = ("OKX-SWAP", "OKX-FUTURES")


def _expected_margin_type(raw_symbol: str, quote_asset: str | None = None) -> str:
    """Re-derive the corrected margin_type — mirrors ``_infer_margin_type``'s OKX branch in
    ``instruments_service/reference_data/adapters/cefi/tardis/parsing.py`` exactly.
    """
    upper_id = str(raw_symbol).upper()
    if "USD_CM" in upper_id:
        return "inverse"
    if "USD_UM" in upper_id:
        return "linear"
    quote = (quote_asset or "").upper()
    if not quote:
        parts = upper_id.split("-")
        quote = parts[1] if len(parts) > 1 else ""
    if quote == "USD":
        return "inverse"
    return "linear"


# --------------------------------------------------------------------------- catalog.parquet ----


def _migrate_catalog(*, apply: bool, full_sweep: bool, sample_size: int) -> int:
    storage_client = get_storage_client()
    raw = storage_client.download_bytes(BUCKET, CATALOG_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded gs://%s/%s — %d total rows", BUCKET, CATALOG_PATH, len(df))

    mask = df["venue"].isin(_VENUES)
    target = df.loc[mask]
    logger.info("Real OKX-SWAP/OKX-FUTURES rows in scope: %d", len(target))

    expected = target["raw_symbol"].map(_expected_margin_type)
    mismatch_idx = target.index[expected != target["margin_type"]]
    logger.info(
        "Mismatched (needs migration): %d / %d (%.1f%%)",
        len(mismatch_idx),
        len(target),
        100.0 * len(mismatch_idx) / max(1, len(target)),
    )
    by_venue = target.loc[mismatch_idx].groupby("venue").size().to_dict()
    logger.info("By venue: %s", by_venue)

    if not apply:
        logger.info("DRY-RUN (no writes). Re-run with --apply --full-sweep to migrate all rows.")
        for idx in list(mismatch_idx[:10]):
            logger.info(
                "  %s: %s -> %s",
                target.loc[idx, "instrument_id"],
                target.loc[idx, "margin_type"],
                expected.loc[idx],
            )
        return 0

    write_idx = list(mismatch_idx) if full_sweep else list(mismatch_idx[:sample_size])
    if not write_idx:
        logger.warning("Nothing to migrate — no writes performed.")
        return 0

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_key = f"{CATALOG_BACKUP_PREFIX}{ts}.bak.parquet"
    storage_client.upload_bytes(BUCKET, backup_key, raw)  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Backup written: gs://%s/%s (%d bytes)", BUCKET, backup_key, len(raw))

    before_after = [(target.loc[i, "instrument_id"], target.loc[i, "margin_type"], expected.loc[i]) for i in write_idx]
    df = df.copy()
    for idx in write_idx:
        df.loc[idx, "margin_type"] = expected.loc[idx]
    out = io.BytesIO()
    df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(BUCKET, CATALOG_PATH, out.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info(
        "APPLIED — %d row(s) migrated (%s), gs://%s/%s rewritten (%d bytes)",
        len(write_idx),
        "full sweep" if full_sweep else f"bounded sample of {sample_size}",
        BUCKET,
        CATALOG_PATH,
        len(out.getvalue()),
    )
    for iid, old, new in before_after[:10]:
        logger.info("  %s: %s -> %s", iid, old, new)
    if len(before_after) > 10:
        logger.info("  ... and %d more", len(before_after) - 10)

    verify_raw = storage_client.download_bytes(BUCKET, CATALOG_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    verify_df = pd.read_parquet(io.BytesIO(verify_raw))
    ok = all(verify_df.loc[i, "margin_type"] == expected.loc[i] for i in write_idx)
    if not ok:
        logger.error(
            "VERIFICATION FAILED — re-downloaded catalog does not match the intended write. Restore from backup: gs://%s/%s",
            BUCKET,
            backup_key,
        )
        return 1
    logger.info("Verification PASSED — all %d written row(s) confirmed correct by re-download.", len(write_idx))
    return 0


# ------------------------------------------------------------------------------ per-day corpus ----


def _list_okx_day_files(storage_client) -> list[tuple[str, str]]:  # StorageClient (avoid deep-import for type-only use)
    """Enumerate real OKX-SWAP/OKX-FUTURES per-day file paths via delimited (non-recursive) listing.

    Two-level delimited listing (day= partitions, then venue= subdirs per day) — bounded by the
    real day-partition count (2,658 as of 2026-07-09), NOT a full recursive corpus walk.
    """
    bucket = storage_client.bucket(BUCKET)
    it = bucket.list_blobs(prefix=BY_DATE_PREFIX, delimiter="/")
    list(it)
    day_prefixes = sorted(it.prefixes)
    logger.info("Real day partitions: %d", len(day_prefixes))

    def _check_day(day_prefix: str) -> list[tuple[str, str]]:
        it2 = bucket.list_blobs(prefix=day_prefix, delimiter="/")
        list(it2)
        found: list[tuple[str, str]] = []
        for venue in _VENUES:
            sub = f"{day_prefix}venue={venue}/"
            if sub in it2.prefixes:
                found.append((venue, f"{sub}instruments.parquet"))
        return found

    files: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=40) as ex:
        futs = [ex.submit(_check_day, dp) for dp in day_prefixes]
        for fut in as_completed(futs):
            files.extend(fut.result())
    return files


def _migrate_one_by_day_file(storage_client, venue: str, path: str) -> tuple[str, str, int, int, bool]:
    """Migrate one real per-day snapshot file. Returns (venue, path, rows_total, rows_changed, wrote)."""
    raw = storage_client.download_bytes(BUCKET, path)
    df = pd.read_parquet(io.BytesIO(raw))
    if "margin_type" not in df.columns or "raw_symbol" not in df.columns:
        return venue, path, len(df), 0, False
    quote_col = df["quote_asset"] if "quote_asset" in df.columns else pd.Series([None] * len(df), index=df.index)
    expected = [
        _expected_margin_type(raw_symbol, quote_asset)
        for raw_symbol, quote_asset in zip(df["raw_symbol"], quote_col, strict=True)
    ]
    changed_mask = pd.Series(expected, index=df.index) != df["margin_type"]
    n_changed = int(changed_mask.sum())
    if n_changed == 0:
        return venue, path, len(df), 0, False

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = path.replace("instruments.parquet", f"instruments.okxmarginfix.{ts}.bak.parquet")
    storage_client.upload_bytes(BUCKET, backup_path, raw)

    df = df.copy()
    df.loc[changed_mask, "margin_type"] = pd.Series(expected, index=df.index).loc[changed_mask]
    out = io.BytesIO()
    df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(BUCKET, path, out.getvalue())
    return venue, path, len(df), n_changed, True


def _migrate_by_day(*, apply: bool, full_sweep: bool, sample_size: int, workers: int) -> int:
    storage_client = get_storage_client()
    files = _list_okx_day_files(storage_client)
    logger.info("Real OKX-SWAP/OKX-FUTURES per-day files found: %d", len(files))
    by_venue = {}
    for venue, _ in files:
        by_venue[venue] = by_venue.get(venue, 0) + 1
    logger.info("By venue: %s", by_venue)

    if not apply:
        logger.info("DRY-RUN (no writes). Re-run with --apply --by-day-sample N (smoke test) or --by-day-full-sweep.")
        return 0

    scope = files if full_sweep else files[:sample_size]
    logger.info(
        "Migrating %d file(s) (%s) with %d workers...",
        len(scope),
        "full sweep" if full_sweep else f"bounded sample of {sample_size}",
        workers,
    )
    t0 = time.time()
    n_touched = 0
    n_rows_changed = 0
    n_scanned = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(_migrate_one_by_day_file, storage_client, venue, path): (venue, path) for venue, path in scope
        }
        for fut in as_completed(futs):
            _venue, _path, _rows_total, rows_changed, wrote = fut.result()
            n_scanned += 1
            if wrote:
                n_touched += 1
                n_rows_changed += rows_changed
            if n_scanned % 200 == 0:
                logger.info(
                    "  progress %d/%d files scanned, %d touched, %d rows fixed (t=%.1fs)",
                    n_scanned,
                    len(scope),
                    n_touched,
                    n_rows_changed,
                    time.time() - t0,
                )
    elapsed = time.time() - t0
    logger.info(
        "APPLIED — %d/%d file(s) touched, %d row(s) migrated, elapsed=%.1fs (%.3fs/file avg)",
        n_touched,
        len(scope),
        n_rows_changed,
        elapsed,
        elapsed / max(1, len(scope)),
    )
    if not full_sweep and n_touched:
        projected = elapsed / max(1, len(scope)) * len(files)
        logger.info(
            "Projected full-sweep ETA at this measured throughput: ~%.1fs (%.1fmin) for all %d real files",
            projected,
            projected / 60.0,
            len(files),
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Write migrated data (default: dry-run/report only).")
    parser.add_argument("--full-sweep", action="store_true", help="prod/catalog.parquet: migrate ALL mismatched rows.")
    parser.add_argument("--sample-size", type=int, default=21, help="prod/catalog.parquet bounded sample size.")
    parser.add_argument(
        "--by-day",
        action="store_true",
        help="Target the per-day instrument_availability corpus instead of prod/catalog.parquet.",
    )
    parser.add_argument(
        "--by-day-sample", type=int, default=20, help="Per-day corpus bounded real sample (file count)."
    )
    parser.add_argument("--by-day-full-sweep", action="store_true", help="Per-day corpus: migrate ALL real files.")
    parser.add_argument("--workers", type=int, default=40, help="Thread-pool concurrency for per-day corpus ops.")
    args = parser.parse_args()

    if args.by_day:
        return _migrate_by_day(
            apply=args.apply,
            full_sweep=args.by_day_full_sweep,
            sample_size=args.by_day_sample,
            workers=args.workers,
        )
    return _migrate_catalog(apply=args.apply, full_sweep=args.full_sweep, sample_size=args.sample_size)


if __name__ == "__main__":
    raise SystemExit(main())
