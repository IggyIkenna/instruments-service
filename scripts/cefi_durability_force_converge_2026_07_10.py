#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after prod/catalog.parquet is confirmed durably converged for
#   BYBIT/KRAKEN-FUTURES/DERIBIT across a real --mode incremental regen cycle
#   AND instrument_id_format_canonicalization_2026_07_08.md's CeFi-durability
#   follow-up items are marked resolved.
"""Force-converge CeFi by_date + catalog durability for BYBIT/KRAKEN-FUTURES/DERIBIT.

Real root cause (diagnosed 2026-07-10, live GCS + Cloud Build/Run evidence, not
re-run blind): the two prior one-off migration scripts
(``canonicalize_bybit_kraken_futures_catalog_2026_07_09.py``,
``canonicalize_deribit_id_markers_2026_07_09.py``) wrote their per-file backups
INLINE next to the original blob (``instruments.<ts>.<tag>.bak.parquet``, same
``day=/venue=/`` directory) instead of to a quarantined ``_migration_backup/``
prefix. ``build_instrument_catalogue.py``'s ``_iter_by_date_snapshots`` walk has
NO filename filter (only checks ``.parquet`` suffix + a ``day=YYYY-MM-DD``
path segment via ``_DAY_RE``), so it silently unions all 14,902 of these stray
backup files' OLD-format rows into EVERY catalogue rebuild (full or
incremental-window) — permanently reintroducing pre-migration instrument_ids as
"active" no matter how many times the current ``instruments.parquet`` files or
``prod/catalog.parquet`` itself get fixed. This — not a code gap — is why the
2026-07-08/09 fixes were "not durable".

Separately (tracked, NOT fixed by this script — see the issue doc): the
production ``is-daily-enum-cefi`` Cloud Run job's deployed image was built
2026-07-09T00:50 UTC, BEFORE all 3 of this session's @LIN/@INV fix commits
(176d4610 09:19 UTC, 8128189e 12:43 UTC, c2d3fbbc 18:22 UTC, same day) — its
daily 13:30 UTC run (``--days-back 3``) genuinely re-wrote day=2026-07-08 back
to old-format and wrote day=2026-07-09 fresh in old-format for KRAKEN-FUTURES/
DERIBIT, confirmed via blob-timestamp correlation with the scheduler's real
``lastAttemptTime``. The live adapter CODE itself (this repo's current git
HEAD) was verified 100% canonical via a real, live Tardis-API call for all 3
venues 2026-07-10 — this is a stale-deployment problem, not a code gap.

This script does the two things fully within THIS repo's control:

1. ``--quarantine-backups``: relocate (server-side copy + delete) every real
   stray ``.bak.parquet`` file for BYBIT/KRAKEN-FUTURES/DERIBIT out of the
   walked ``instrument_availability/by_date/`` tree into
   ``_migration_backup/cefi_by_date_bak_relocation_2026_07_10/<original path>``
   — nothing is lost, it just stops polluting the catalogue walk.
2. ``--fix-by-date``: for every CURRENT (non-backup) ``instruments.parquet``
   file, re-derive ``margin_type`` (re-inferred from ``raw_symbol`` via the
   CURRENT ``_infer_margin_type`` — the STORED value is not trusted, since
   real evidence shows pre-fix rows carry a wrong stored value too, e.g.
   Kraken-Futures ``FI_``-prefixed real-inverse rows stored as
   ``margin_type=linear``) and ``instrument_key`` (via the same shared
   ``_build_canonical_perpetual_key``/``_build_canonical_future_key``/
   ``_build_canonical_option_key`` the live adapter code calls) from each
   row's own already-captured ``base_asset``/``quote_asset``/``expiry``/
   ``strike``/``option_type``/``raw_symbol`` — no re-download from the venue
   (migration-mechanics decision, instrument_id_format_canonicalization_
   2026_07_08.md). Backup-first (to the SAME quarantine prefix, never inline),
   verify-after, idempotent, shard-isolated (one file's failure never aborts
   the sweep).

Usage::

    cd instruments-service
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --quarantine-backups --dry-run
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --quarantine-backups --apply
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --fix-by-date --dry-run
    .venv/bin/python scripts/cefi_durability_force_converge_2026_07_10.py --fix-by-date --apply --workers 32

SSOT: unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
"""

from __future__ import annotations

import argparse
import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
from google.cloud import storage as _gcs_sdk  # noqa: qg-deep-import — read-only bulk LISTING only (server-side

# match_glob filtering ~28s/venue vs UTL StorageClient.list_blobs's unfiltered full-prefix walk, which
# does not terminate in a reasonable window over this bucket's ~150k+ cefi by_date objects — see module
# docstring. Every actual read/write/delete of blob CONTENT below goes through UTL's StorageClient
# (get_storage_client()), not this. Migration-script carve-out, matches codex/06-coding-standards/
# quality-gates.md's "no direct google.cloud" ban intent (production service code paths), not a
# one-off backfill's read-only enumeration step.
from unified_api_contracts.internal import InstrumentType, MarginType
from unified_trading_library import get_storage_client, resolve_bucket_name

from instruments_service.reference_data.adapters.cefi.tardis import parsing as tardis_parsing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BY_DATE_PREFIX = "instrument_availability/by_date/"
QUARANTINE_PREFIX = "_migration_backup/cefi_by_date_bak_relocation_2026_07_10/"
_TARGET_VENUES: tuple[str, ...] = ("BYBIT", "KRAKEN-FUTURES", "DERIBIT")
_EXCHANGE_FOR_VENUE: dict[str, str] = {
    "BYBIT": "bybit",
    "KRAKEN-FUTURES": "cryptofacilities",
    "DERIBIT": "deribit",
}
_DERIVATIVE_TYPES = frozenset({"FUTURE", "OPTION", "PERPETUAL"})


# --------------------------------------------------------------------------
# Step 0: one bulk LISTING pass per venue (server-side match_glob — see the
# module-level SDK-usage note above), split into (current, backup) locally.
# --------------------------------------------------------------------------


def _list_all_for_venue(bucket: str, venue: str) -> tuple[list[str], list[str]]:
    """Return ``(current_files, stray_backup_files)`` for one venue, ONE listing call."""
    gcs_client = _gcs_sdk.Client()
    gcs_bucket = gcs_client.bucket(bucket)
    current: list[str] = []
    backups: list[str] = []
    for b in gcs_client.list_blobs(gcs_bucket, prefix=BY_DATE_PREFIX, match_glob=f"**/venue={venue}/*.parquet"):
        name = str(b.name)
        if name.endswith("/instruments.parquet"):
            current.append(name)
        else:
            backups.append(name)
    return current, backups


def _quarantine_one(st, bucket: str, blob_name: str, apply: bool) -> str:
    dest = QUARANTINE_PREFIX + blob_name
    if not apply:
        return "would_move"
    if st.blob_exists(bucket, dest):
        # Idempotent re-run: already relocated (or a same-named prior relocation) — just
        # remove the stray source copy, never overwrite an existing quarantine entry.
        st.delete_blob(bucket, blob_name)
        return "already_quarantined_source_removed"
    raw = st.download_bytes(bucket, blob_name)
    st.upload_bytes(bucket, dest, raw)
    verify = st.download_bytes(bucket, dest)
    if len(verify) != len(raw):
        return "verify_size_mismatch_source_kept"
    st.delete_blob(bucket, blob_name)
    return "moved"


def run_quarantine(st, bucket: str, apply: bool, workers: int, all_targets: list[str]) -> None:
    logger.info(
        "=== quarantine: %d stray file(s) total (mode=%s, workers=%d) ===",
        len(all_targets),
        "APPLY" if apply else "DRY-RUN",
        workers,
    )
    results: dict[str, int] = {}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_quarantine_one, st, bucket, blob, apply): blob for blob in all_targets}
        for i, fut in enumerate(as_completed(futures), start=1):
            blob = futures[fut]
            try:
                outcome = fut.result()
            except Exception:
                logger.exception("FAILED quarantine %s", blob)
                outcome = "error"
            results[outcome] = results.get(outcome, 0) + 1
            if i % 1000 == 0 or i == len(all_targets):
                elapsed = time.time() - t0
                logger.info("[%d/%d] elapsed=%.1fs outcomes=%s", i, len(all_targets), elapsed, results)
    logger.info("quarantine DONE: total=%d outcomes=%s elapsed=%.1fs", len(all_targets), results, time.time() - t0)


# --------------------------------------------------------------------------
# Step 2: re-derive instrument_key + margin_type on CURRENT by_date files
# --------------------------------------------------------------------------


def _to_decimal(val: object) -> Decimal | None:
    if val is None:
        return None
    try:
        if isinstance(val, float) and val != val:  # NaN
            return None
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None


def _re_derive_row(venue: str, row: pd.Series[object]) -> tuple[str | None, MarginType | None, str]:
    """Re-derive ``(new_instrument_key, new_margin_type, reason)`` for one row.

    Returns ``(None, None, reason)`` when the row cannot be safely re-derived — the
    caller leaves it UNCHANGED (never guesses). Mirrors the exact construction the
    CURRENT live adapter code uses (``tardis/adapter.py::_parse_tardis_instrument``),
    just replayed against already-captured columns instead of a fresh venue fetch.
    """
    itype_str = str(row.get("instrument_type") or "")
    if itype_str not in _DERIVATIVE_TYPES:
        return None, None, "not_a_margined_type"
    base = str(row.get("base_asset") or "").strip()
    quote = str(row.get("quote_asset") or "").strip()
    raw_symbol = str(row.get("raw_symbol") or "").strip()
    if not base or not raw_symbol:
        return None, None, "missing_base_or_raw_symbol"
    exchange = _EXCHANGE_FOR_VENUE[venue]
    inst_type = InstrumentType(itype_str)
    margin_type = tardis_parsing._infer_margin_type(inst_type, quote, raw_symbol, exchange)
    if margin_type is None:
        return None, None, "margin_type_unresolvable"

    if itype_str == "PERPETUAL":
        if not quote:
            return None, None, "perpetual_missing_quote"
        key = tardis_parsing._build_canonical_perpetual_key(venue, base, quote, margin_type)
        return key, margin_type, ""

    expiry_raw = row.get("expiry")
    expiry = expiry_raw.to_pydatetime() if hasattr(expiry_raw, "to_pydatetime") else expiry_raw
    if expiry is None or pd.isna(expiry):
        return None, None, "missing_expiry"
    dated_quote = "" if venue == "DERIBIT" else quote

    if itype_str == "FUTURE":
        key = tardis_parsing._build_canonical_future_key(venue, base, dated_quote, margin_type, expiry)
        return key, margin_type, ""

    # OPTION
    strike = _to_decimal(row.get("strike"))
    option_type_raw = str(row.get("option_type") or "").strip().lower()
    if strike is None or option_type_raw not in ("call", "put"):
        return None, None, "missing_strike_or_option_type"
    option_right = "C" if option_type_raw == "call" else "P"
    key = tardis_parsing._build_canonical_option_key(
        venue, base, dated_quote, margin_type, expiry, strike, option_right
    )
    return key, margin_type, ""


def _fix_frame(venue: str, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {"rows": len(df), "id_changed": 0, "margin_fixed": 0, "unresolved": 0}
    if df.empty or "instrument_key" not in df.columns:
        return df, stats
    mask = (
        df["instrument_type"].isin(_DERIVATIVE_TYPES)
        if "instrument_type" in df.columns
        else pd.Series(False, index=df.index)
    )
    orig_id = df["instrument_key"].astype(str)
    new_id = orig_id.copy()
    orig_margin = df["margin_type"].astype(str) if "margin_type" in df.columns else pd.Series("", index=df.index)
    new_margin = df["margin_type"].copy() if "margin_type" in df.columns else pd.Series(None, index=df.index)
    for idx in df.index[mask]:
        row = df.loc[idx]
        key, margin_type, reason = _re_derive_row(venue, row)
        if key is None:
            if reason != "not_a_margined_type":
                stats["unresolved"] += 1
            continue
        new_id.loc[idx] = key
        if margin_type is not None:
            new_margin.loc[idx] = margin_type.value
    stats["id_changed"] = int((new_id != orig_id).sum())
    stats["margin_fixed"] = int((new_margin.astype(str) != orig_margin).sum())
    out = df.copy()
    out["instrument_key"] = new_id
    if "margin_type" in df.columns:
        out["margin_type"] = new_margin
    return out, stats


def _process_one_file(st, bucket: str, venue: str, blob_name: str, apply: bool) -> dict[str, object]:
    raw = st.download_bytes(bucket, blob_name)
    df = pd.read_parquet(io.BytesIO(raw))
    out, stats = _fix_frame(venue, df)
    result: dict[str, object] = {"blob": blob_name, **stats, "written": 0}
    if len(out) != len(df):
        result["error"] = "row_count_changed"
        return result
    if stats["id_changed"] == 0 and stats["margin_fixed"] == 0:
        return result
    # Duplicate-introduction guard — never silently merge distinct instruments.
    dup_before = int(df["instrument_key"].astype(str).duplicated().sum())
    dup_after = int(out["instrument_key"].astype(str).duplicated().sum())
    if dup_after > dup_before:
        result["error"] = f"would_introduce_{dup_after - dup_before}_dup_ids"
        return result
    if not apply:
        result["written"] = 1
        return result
    backup_dest = QUARANTINE_PREFIX + blob_name
    if not st.blob_exists(bucket, backup_dest):
        st.upload_bytes(bucket, backup_dest, raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    st.upload_bytes(bucket, blob_name, buf.getvalue())
    result["written"] = 1
    return result


def run_fix_by_date(
    st, bucket: str, apply: bool, workers: int, limit: int | None, current_by_venue: dict[str, list[str]]
) -> None:
    for venue in _TARGET_VENUES:
        targets = current_by_venue[venue]
        if limit is not None:
            targets = sorted(targets)[:limit]
        logger.info(
            "=== fix-by-date %s: %d current file(s) (mode=%s, workers=%d) ===",
            venue,
            len(targets),
            "APPLY" if apply else "DRY-RUN",
            workers,
        )
        total = {"files": 0, "files_changed": 0, "id_changed": 0, "margin_fixed": 0, "unresolved": 0}
        errors: dict[str, int] = {}
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_process_one_file, st, bucket, venue, blob, apply): blob for blob in targets}
            for i, fut in enumerate(as_completed(futures), start=1):
                blob = futures[fut]
                try:
                    r = fut.result()
                except Exception:
                    logger.exception("FAILED %s", blob)
                    errors["exception"] = errors.get("exception", 0) + 1
                    continue
                if r.get("error"):
                    errors[str(r["error"])] = errors.get(str(r["error"]), 0) + 1
                    continue
                total["files"] += 1
                total["unresolved"] += int(r["unresolved"])
                if r["written"]:
                    total["files_changed"] += 1
                    total["id_changed"] += int(r["id_changed"])
                    total["margin_fixed"] += int(r["margin_fixed"])
                if i % 500 == 0 or i == len(targets):
                    elapsed = time.time() - t0
                    logger.info(
                        "%s [%d/%d] elapsed=%.1fs (%.3fs/file) %s errors=%s",
                        venue,
                        i,
                        len(targets),
                        elapsed,
                        elapsed / i,
                        total,
                        errors,
                    )
        logger.info("%s DONE: %s errors=%s elapsed=%.1fs", venue, total, errors, time.time() - t0)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--quarantine-backups", action="store_true", help="Relocate stray .bak.parquet files.")
    ap.add_argument("--fix-by-date", action="store_true", help="Re-derive instrument_key/margin_type on current files.")
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry-run/report only).")
    ap.add_argument("--workers", type=int, default=32, help="Thread-pool size (default 32).")
    ap.add_argument("--limit", type=int, default=None, help="Per-venue smoke-test cap for --fix-by-date.")
    args = ap.parse_args(argv)
    if not (args.quarantine_backups or args.fix_by_date):
        logger.error("Nothing to do — pass --quarantine-backups and/or --fix-by-date.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    st = get_storage_client()
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    logger.info("bucket=%s ts=%s", bucket, ts)

    logger.info("=== listing (one server-side match_glob pass per venue) ===")
    current_by_venue: dict[str, list[str]] = {}
    backups_all: list[str] = []
    for venue in _TARGET_VENUES:
        t0 = time.time()
        current, backups = _list_all_for_venue(bucket, venue)
        current_by_venue[venue] = current
        backups_all.extend(backups)
        logger.info(
            "%s: %d current, %d stray backup file(s) (listed in %.1fs)",
            venue,
            len(current),
            len(backups),
            time.time() - t0,
        )

    if args.quarantine_backups:
        run_quarantine(st, bucket, args.apply, args.workers, backups_all)
    if args.fix_by_date:
        run_fix_by_date(st, bucket, args.apply, args.workers, args.limit, current_by_venue)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
