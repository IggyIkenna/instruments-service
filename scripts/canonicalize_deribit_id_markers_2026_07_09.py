#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the full DERIBIT PERPETUAL/FUTURE/OPTION @LIN/@INV backfill
#   (prod/catalog.parquet + instrument_availability/by_date per-day snapshots) is
#   applied and re-verified, AND the go-forward capture paths (tardis/adapter.py,
#   ccxt_adapter.py) are wired to the same marker convention.
"""Canonicalize DERIBIT PERPETUAL/FUTURE/OPTION instrument_id to the operator-decided
``@LIN``/``@INV`` marker + ``YYYYMMDD`` target format.

Background (instrument_id_format_canonicalization_2026_07_08.md finding 1, PERPETUAL
scope-expanded 2026-07-09): Deribit's real current instrument_id embeds the native
``DDMMMYY`` expiry and carries no margin-type marker —
``DERIBIT:OPTION:BTC-10JUL26-48000-C`` — even though Deribit genuinely runs BOTH
inverse (USD-quoted, BTC/ETH-settled) and linear (USDC-quoted/settled) instruments
side by side and the id alone cannot disambiguate them. Target:
``VENUE:TYPE:BASE[-QUOTE]@LIN|INV-YYYYMMDD[-STRIKE-C|P]`` — PERPETUAL keeps the quote
segment (``DERIBIT:PERPETUAL:BTC-USD@INV``), FUTURE/OPTION drop it
(``DERIBIT:FUTURE:BTC@INV-20260710``, ``DERIBIT:OPTION:BTC@INV-20260710-48000-C``) —
matching the doc's own worked Deribit examples verbatim.

Migration mechanics (operator-decided, same doc): re-derive the corrected
instrument_id from already-captured fields (``instrument_type``, ``base_asset``,
``margin_type``, and — for per-day snapshots — ``quote_asset``/``expiry``/``strike``/
``option_type``; ``prod/catalog.parquet`` only carries ``instrument_id``/``raw_symbol``/
``base_asset``/``margin_type`` for these types, so the DDMMMYY date is re-parsed from
``raw_symbol`` there) and rewrite the parquet/partition in place — NOT a re-download
from Deribit. Real-scoping numbers (2026-07-09, ``prod/catalog.parquet``, 4.4 MiB):
263,979 real DERIBIT PERPETUAL/FUTURE/OPTION rows (26 PERPETUAL + 1,507 FUTURE +
262,446 OPTION), 0 null ``margin_type``, 0 parse failures, 0 id collisions against
this exact transform (verified locally against the full real row set before this
script existed). The much larger historical corpus lives in per-day snapshots —
``instrument_availability/by_date/day=*/venue=DERIBIT/instruments.parquet`` (both the
legacy and ``pipeline_mode=batch_instruments_service``-prefixed path shapes) —
5,342 real files (2019-03-30 to present), NOT touched by this script's ``--apply``
(catalog-only) mode; see the module-level ``PER_DAY_PREFIX_GLOB`` docstring below for
the follow-up scope.

**STAGED ROLLOUT — this script's ``--apply`` writes ONLY the rows named by
``--sample-ids`` (a small, explicit, bounded real sample) or all target rows when
``--full-sweep`` is passed. The full sweep was NOT run as part of shipping this
script (staged-rollout rule: smoke-test + measure + report, full sweep is a separate
go/no-go decision) — see ``docs/CEFI_INSTRUMENTS.md`` "Deribit @LIN/@INV migration"
for the measured throughput/ETA this scoping produced.**

Usage::

    cd instruments-service

    # Dry-run (default) — report what would change, no writes:
    .venv/bin/python scripts/canonicalize_deribit_id_markers_2026_07_09.py

    # Smoke test — migrate a small, explicit real sample in place (backup first):
    .venv/bin/python scripts/canonicalize_deribit_id_markers_2026_07_09.py --apply \\
        --sample-size 30

    # Full sweep (NOT run by this session — separate go/no-go decision):
    .venv/bin/python scripts/canonicalize_deribit_id_markers_2026_07_09.py --apply \\
        --full-sweep
"""

from __future__ import annotations

import argparse
import concurrent.futures
import io
import logging
import random
import re
import time
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
BACKUP_PREFIX = "prod/catalog.deribit-marker-migration."
BY_DATE_PREFIX = "instrument_availability/by_date/"
_BY_DATE_BACKUP_TAG = "deribit-marker-migration"

_VENUE = "DERIBIT"
_TARGET_TYPES = ("PERPETUAL", "FUTURE", "OPTION")

_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_DDMMMYY_RE = re.compile(r"^(\d{1,2})([A-Z]{3})(\d{2})$")


def _ddmmmyy_to_yyyymmdd(token: str) -> str | None:
    """Reformat a Deribit ``DDMMMYY`` raw expiry token (e.g. ``10JUL26``) as ``YYYYMMDD``."""
    m = _DDMMMYY_RE.match(token.upper())
    if not m:
        return None
    day, month_str, year_str = int(m.group(1)), m.group(2), int(m.group(3))
    month = _MONTHS.get(month_str)
    if not month:
        return None
    return f"{2000 + year_str:04d}{month:02d}{day:02d}"


def _marker(margin_type: object) -> str | None:
    """Map a real, already-captured ``margin_type`` string to its ``@LIN``/``@INV`` token."""
    mt = str(margin_type or "").strip().lower()
    if mt == "inverse":
        return "INV"
    if mt == "linear":
        return "LIN"
    return None


def build_target_instrument_id(instrument_type: str, instrument_id: str, margin_type: object) -> tuple[str | None, str]:
    """Re-derive the target ``@LIN``/``@INV`` instrument_id from catalog columns already present.

    Returns ``(new_id_or_None, reason)`` — ``reason`` is empty on success, else a
    short skip-reason string (unclassifiable margin_type / already migrated / an
    unrecognised symbol shape). Never guesses: a row this function can't confidently
    re-derive is left untouched, not silently mismigrated.
    """
    marker = _marker(margin_type)
    if marker is None:
        return None, "no_margin_type"
    prefix, _, symbol = instrument_id.partition(":" + instrument_type + ":")
    if not symbol:
        return None, "unparseable_instrument_id"
    if "@" in symbol:
        return None, "already_migrated"
    venue_type_prefix = f"{prefix}:{instrument_type}:"
    if instrument_type == "PERPETUAL":
        return f"{venue_type_prefix}{symbol}@{marker}", ""
    if instrument_type in ("FUTURE", "OPTION"):
        parts = symbol.split("-")
        if len(parts) < 2:
            return None, "too_few_dash_parts"
        base_part = parts[0].split("_", 1)[0]  # strip _QUOTE (e.g. AVAX_USDC -> AVAX)
        yyyymmdd = _ddmmmyy_to_yyyymmdd(parts[1])
        if yyyymmdd is None:
            return None, f"unparseable_date:{parts[1]}"
        new_symbol = f"{base_part}@{marker}-{yyyymmdd}"
        tail = parts[2:]
        if tail:
            new_symbol += "-" + "-".join(tail)
        return f"{venue_type_prefix}{new_symbol}", ""
    return None, "unsupported_instrument_type"


def _select_sample(target: pd.DataFrame, sample_size: int) -> pd.DataFrame:
    """Pick a small, real, representative bounded sample spanning every
    (instrument_type, margin_type) combination present — mirrors the 2026-07-09
    OKX margin-type smoke test's "spanning all buckets" approach.
    """
    groups = target.groupby(["instrument_type", "margin_type"], observed=True)
    per_group = max(1, sample_size // max(1, len(groups)))
    parts = [g.head(per_group) for _, g in groups]
    sample = pd.concat(parts) if parts else target.head(0)
    return sample.head(sample_size)


def _by_date_backup_key(blob_name: str, ts: str) -> str:
    """Derive a colocated backup blob key for one real per-day snapshot file."""
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{_BY_DATE_BACKUP_TAG}.{ts}.bak.parquet"
    return f"{blob_name}.{_BY_DATE_BACKUP_TAG}.{ts}.bak"


def list_by_date_targets(storage_client, bucket: str, limit: int | None = None) -> list[str]:
    """List every real per-day DERIBIT snapshot blob under ``instrument_availability/by_date/``
    — both the legacy and the ``pipeline_mode=``-prefixed path shapes are real, distinct blobs
    and both are in scope.
    """
    targets: list[str] = []
    for b in storage_client.list_blobs(bucket, prefix=BY_DATE_PREFIX):  # pyright: ignore[reportAttributeAccessIssue]
        if f"venue={_VENUE}/" in b.name and b.name.endswith("instruments.parquet"):
            targets.append(b.name)
            if limit is not None and len(targets) >= limit:
                break
    return targets


def process_one_by_date_file(storage_client, bucket: str, blob_name: str, apply: bool, ts: str) -> dict[str, object]:
    """Migrate one real per-day DERIBIT snapshot file's ``instrument_key`` column in place.

    Reuses ``build_target_instrument_id`` unchanged — a per-day snapshot row's ``instrument_key``
    already carries the exact same raw ``VENUE:TYPE:BASE[-QUOTE|_QUOTE][-DDMMMYY[-STRIKE-C|P]]``
    shape as ``prod/catalog.parquet``'s ``instrument_id`` (verified against real 2019 and 2026
    samples, incl. USDC-linear futures/options and legacy inverse futures/options), so no
    separate re-derivation from ``base_asset``/``quote_asset``/``expiry``/``strike`` columns is
    needed. Never raises — any failure on this file is captured in the returned dict so the
    caller's concurrent sweep can isolate it (shard-level failure isolation), never abort the
    rest of the run over one bad file.
    """
    result: dict[str, object] = {"blob": blob_name, "rows": 0, "changed": 0, "error": None}
    raw = storage_client.download_bytes(bucket, blob_name)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    id_col = "instrument_key" if "instrument_key" in df.columns else "instrument_id"
    result["rows"] = len(df)
    if id_col not in df.columns or "margin_type" not in df.columns or "instrument_type" not in df.columns:
        result["error"] = "missing_columns"
        return result

    orig = df[id_col].astype(str)
    new_ids = orig.copy()
    for idx, row in df.iterrows():
        itype = str(row["instrument_type"])
        if itype not in _TARGET_TYPES:
            continue
        new_id, _reason = build_target_instrument_id(itype, str(row[id_col]), row["margin_type"])
        if new_id is not None:
            new_ids.loc[idx] = new_id

    changed_mask = new_ids != orig
    changed = int(changed_mask.sum())
    result["changed"] = changed
    if changed == 0:
        return result

    dup_before = int(orig.duplicated().sum())
    dup_after = int(new_ids.duplicated().sum())
    if dup_after > dup_before:
        result["error"] = f"would_introduce_{dup_after - dup_before}_duplicate_id(s)"
        result["changed"] = 0
        return result

    if not apply:
        return result

    out = df.copy()
    out[id_col] = new_ids
    backup_key = _by_date_backup_key(blob_name, ts)
    storage_client.upload_bytes(bucket, backup_key, raw)  # pyright: ignore[reportAttributeAccessIssue]
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(bucket, blob_name, buf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    result["backup"] = backup_key
    return result


def run_by_date(
    storage_client, bucket: str, apply: bool, ts: str, limit: int | None, workers: int
) -> dict[str, object]:
    """Concurrent (thread-pool) sweep over every real per-day DERIBIT snapshot file. Each file is
    an isolated shard — one file's failure is logged and counted, never raised out of the pool.
    """
    targets = list_by_date_targets(storage_client, bucket, limit)
    logger.info("=== by_date: %d target file(s) (limit=%s, workers=%d) ===", len(targets), limit, workers)
    total_rows_changed = 0
    total_files_changed = 0
    errors: dict[str, int] = {}
    written: list[str] = []
    t0 = time.time()
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(process_one_by_date_file, storage_client, bucket, blob, apply, ts): blob for blob in targets
        }
        for fut in concurrent.futures.as_completed(futures):
            blob = futures[fut]
            done += 1
            try:
                result = fut.result()
            except Exception as exc:  # broad-except-ok: shard-level isolation, one file must never abort the sweep
                errors[type(exc).__name__] = errors.get(type(exc).__name__, 0) + 1
                logger.error("[%d/%d] FAILED %s: %s", done, len(targets), blob, exc)
                continue
            if result["error"]:
                errors[str(result["error"])] = errors.get(str(result["error"]), 0) + 1
                logger.warning("[%d/%d] SKIP %s: %s", done, len(targets), blob, result["error"])
                continue
            if result["changed"]:
                total_files_changed += 1
                total_rows_changed += int(result["changed"])
                if apply:
                    written.append(blob)
            if done % 250 == 0 or done == len(targets):
                elapsed = time.time() - t0
                logger.info(
                    "progress %d/%d files (%.1f%%), changed_files=%d changed_rows=%d elapsed=%.1fs",
                    done,
                    len(targets),
                    100.0 * done / max(1, len(targets)),
                    total_files_changed,
                    total_rows_changed,
                    elapsed,
                )
    elapsed = time.time() - t0
    per_file = elapsed / len(targets) if targets else 0.0
    logger.info(
        "by_date DONE: files=%d files_changed=%d rows_changed=%d errors=%s elapsed=%.1fs (%.3fs/file, %d workers)",
        len(targets),
        total_files_changed,
        total_rows_changed,
        errors,
        elapsed,
        per_file,
        workers,
    )
    return {
        "targets": targets,
        "written": written,
        "files_changed": total_files_changed,
        "rows_changed": total_rows_changed,
        "errors": errors,
        "elapsed": elapsed,
    }


def verify_by_date_sample(storage_client, bucket: str, written: list[str], sample_size: int) -> bool:
    """Re-download a random real sample of written files and confirm every in-scope row now
    carries the ``@`` marker — the by-date analogue of the catalog path's full re-download verify
    (re-downloading all 5,342 files would roughly double the sweep's GCS traffic, so this checks a
    real random sample instead, per this workspace's "verify a sample after writing" convention).
    """
    if not written:
        logger.info("VERIFY: no files were written — nothing to verify.")
        return True
    sample = random.sample(written, min(sample_size, len(written)))
    ok = True
    for blob in sample:
        raw = storage_client.download_bytes(bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
        df = pd.read_parquet(io.BytesIO(raw))
        id_col = "instrument_key" if "instrument_key" in df.columns else "instrument_id"
        in_scope = df["instrument_type"].isin(_TARGET_TYPES) & df["margin_type"].notna()
        migrated = df.loc[in_scope, id_col].astype(str).str.contains("@")
        if not migrated.all():
            logger.error("VERIFY FAILED %s: %d row(s) still missing @ marker", blob, int((~migrated).sum()))
            ok = False
        else:
            logger.info("VERIFY OK %s (%d target rows all carry @ marker)", blob, int(in_scope.sum()))
    logger.info("VERIFY sample=%d/%d written file(s): %s", len(sample), len(written), "PASSED" if ok else "FAILED")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--apply", action="store_true", help="Write the migrated catalog (default: dry-run/report only)."
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=30,
        help="Bounded real sample size for --apply (default 30; ignored with --full-sweep).",
    )
    parser.add_argument(
        "--full-sweep",
        action="store_true",
        help="Migrate ALL real target rows, not just a bounded sample. NOT used in the 2026-07-09 shipping pass "
        "(staged-rollout: smoke-test + measure + report; full sweep is a separate go/no-go decision).",
    )
    parser.add_argument(
        "--by-date-sample",
        type=int,
        default=None,
        metavar="N",
        help="Migrate the first N real per-day snapshot files (smoke test) instead of the catalog.",
    )
    parser.add_argument(
        "--by-date-all",
        action="store_true",
        help="Migrate ALL real per-day snapshot files (full historical sweep, concurrent).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Thread-pool size for --by-date-sample/--by-date-all (default 32).",
    )
    parser.add_argument(
        "--verify-sample",
        type=int,
        default=20,
        help="Real random sample size to re-download and verify after a by-date --apply (default 20).",
    )
    args = parser.parse_args()

    storage_client = get_storage_client()

    if args.by_date_sample is not None or args.by_date_all:
        limit = None if args.by_date_all else args.by_date_sample
        ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
        sweep = run_by_date(storage_client, BUCKET, args.apply, ts, limit, args.workers)
        if not args.apply:
            logger.info(
                "DRY-RUN (no writes). Re-run with --apply to migrate for real (backs up every changed file first)."
            )
            return 0
        if sweep["errors"]:
            logger.warning("Sweep completed with per-file errors (shard-isolated, not fatal): %s", sweep["errors"])
        ok = verify_by_date_sample(storage_client, BUCKET, sweep["written"], args.verify_sample)  # pyright: ignore[reportArgumentType]
        return 0 if ok else 1

    raw = storage_client.download_bytes(BUCKET, CATALOG_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded gs://%s/%s — %d total rows", BUCKET, CATALOG_PATH, len(df))

    mask = (df["venue"] == _VENUE) & (df["instrument_type"].isin(_TARGET_TYPES))
    target = df.loc[mask]
    logger.info("Real DERIBIT PERPETUAL/FUTURE/OPTION rows in scope: %d", len(target))
    logger.info("By instrument_type: %s", target["instrument_type"].value_counts().to_dict())

    new_ids: dict[int, str] = {}
    reasons: dict[str, int] = {}
    for idx, row in target.iterrows():
        new_id, reason = build_target_instrument_id(row["instrument_type"], row["instrument_id"], row["margin_type"])
        if new_id is not None:
            new_ids[idx] = new_id
        elif reason:
            reasons[reason] = reasons.get(reason, 0) + 1

    logger.info("Re-derivable: %d / %d (%.1f%%)", len(new_ids), len(target), 100.0 * len(new_ids) / max(1, len(target)))
    if reasons:
        logger.info("Skip reasons: %s", reasons)

    collisions = pd.Series(new_ids.values()).duplicated().sum()
    if collisions:
        logger.error(
            "%d collision(s) in re-derived ids — ABORTING (never silently merge distinct instruments)", collisions
        )
        return 1
    logger.info("Collision check: 0 (each re-derived id is unique)")

    if not args.apply:
        logger.info(
            "DRY-RUN (no writes). Re-run with --apply to migrate a bounded sample, or --apply --full-sweep for all rows."
        )
        sample_preview = _select_sample(target, args.sample_size)
        for idx in sample_preview.index:
            if idx in new_ids:
                logger.info("  %s -> %s", target.loc[idx, "instrument_id"], new_ids[idx])
        return 0

    write_idx = list(target.index) if args.full_sweep else list(_select_sample(target, args.sample_size).index)
    write_idx = [i for i in write_idx if i in new_ids]
    if not write_idx:
        logger.warning("Nothing re-derivable in the selected scope — no writes performed.")
        return 0

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_key = f"{BACKUP_PREFIX}{ts}.bak.parquet"
    storage_client.upload_bytes(BUCKET, backup_key, raw)  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Backup written: gs://%s/%s (%d bytes)", BUCKET, backup_key, len(raw))

    before_after = [(target.loc[i, "instrument_id"], new_ids[i]) for i in write_idx]
    df = df.copy()
    # Vectorized assignment — NOT a per-row `.loc[idx, col] = val` loop. A DataFrame `.loc` scalar
    # setitem re-checks block consistency on every call, so looping it over a quarter-million rows
    # is effectively O(n^2) and can hang for many minutes; a single indexed assignment is O(n).
    write_series = pd.Series(new_ids).loc[write_idx]
    df.loc[write_series.index, "instrument_id"] = write_series
    out = io.BytesIO()
    df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(BUCKET, CATALOG_PATH, out.getvalue())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info(
        "APPLIED — %d row(s) migrated (%s), gs://%s/%s rewritten (%d bytes)",
        len(write_idx),
        "full sweep" if args.full_sweep else f"bounded sample of {args.sample_size}",
        BUCKET,
        CATALOG_PATH,
        len(out.getvalue()),
    )
    for old_id, new_id in before_after[:10]:
        logger.info("  %s -> %s", old_id, new_id)
    if len(before_after) > 10:
        logger.info("  ... and %d more", len(before_after) - 10)

    # Verify by re-downloading and re-checking the written rows.
    verify_raw = storage_client.download_bytes(BUCKET, CATALOG_PATH)  # pyright: ignore[reportAttributeAccessIssue]
    verify_df = pd.read_parquet(io.BytesIO(verify_raw))
    ok = all(verify_df.loc[i, "instrument_id"] == new_ids[i] for i in write_idx)
    if not ok:
        logger.error(
            "VERIFICATION FAILED — re-downloaded catalog does not match the intended write. Restore from backup: gs://%s/%s",
            BUCKET,
            backup_key,
        )
        return 1
    logger.info("Verification PASSED — all %d written row(s) confirmed correct by re-download.", len(write_idx))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
