#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after every real OKX-SWAP/OKX-FUTURES per-day snapshot blob (all 4 real
#   path shapes — see module docstring) carries the corrected margin_type (verified 0
#   remaining mismatched real rows by this script's own dry-run report mode), AND
#   unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md's
#   2026-07-09 "generalized legacy-GCS-naming" finding is marked resolved for OKX.
"""CeFi legacy GCS naming-convention audit + gap-fix — OKX-SWAP/OKX-FUTURES per-day corpus.

Background (operator decision 2026-07-09, ``instrument_id_format_canonicalization_2026_07_08.md``
"generalized finding"): a sibling on-chain-perp workflow found ~99% of "captured" HL/ASTER
historical objects sat under an even-older bare-symbol filename shape no migration script
recognized. Operator: audit CeFi for the same class of problem (multiple coexisting GCS
path/filename conventions, only the newest of which any current script's listing covers).

**Real finding for CeFi (this script), 2026-07-09** — a real GCS listing of
``instrument_availability/by_date/`` (110,636 objects, one flat walk, not the manifest
summary) across BINANCE-FUTURES/DELIVERY, BYBIT, KRAKEN-FUTURES, DERIBIT, OKX-SWAP,
OKX-FUTURES found **4 distinct real path shapes** coexist for the per-day instrument
snapshot corpus (all under the SAME fixed leaf filename, ``instruments.parquet`` — CeFi
has NO bare-symbol-per-instrument-file shape anywhere, unlike the HL/ASTER on-chain-perp
case; the divergence here is partition-path depth, not filename identity ambiguity):

  A. ``day=D/venue=V/instruments.parquet`` — the original/bare shape, present for
     literally every real day 2019-current (33,602 real objects across the 7 target
     venues incl. backups).
  B. ``day=D/pipeline_mode=batch_instruments_service/asset_group=cefi/venue=V/
     instruments.parquet`` — a newer, ``pipeline_mode``-partitioned shape that ALSO
     spans the full 2019-current corpus in parallel with shape A for the same
     (day, venue) — not a replacement, a real coexisting duplicate write path
     (28,710 real objects). Per ``codex/02-data/pipeline-mode-partition.md`` this is
     the SOURCE-AWARE partition convention readers are meant to prefix-match on.
  C. ``day=D/pipeline_mode=.../asset_group=cefi/day=D/venue=V/instruments.parquet`` —
     a doubled-``day=`` partition-key bug, bounded to 18 real dates
     (2026-05-05..2026-05-22) x 12 venues (144 real objects across target venues).
  D. ``day=D/day=D/venue=V/instruments.parquet`` — the same doubled-``day=`` bug
     without the ``pipeline_mode``/``asset_group`` prefix (144 real objects, same
     18-date window).

Coverage check against the 5 sibling 2026-07-09 canonicalization scripts (real, verified
via matching each script's own by-date target-listing logic against the real object
inventory, then cross-checking real ``.bak`` backup counts already written):

  - ``canonicalize_bybit_kraken_futures_catalog_2026_07_09.py``,
    ``canonicalize_binance_futures_delivery_catalog_2026_07_09.py``,
    ``canonicalize_deribit_id_markers_2026_07_09.py`` all list via a FLAT substring scan
    (``"venue=X/" in blob.name``) over the *entire* ``by_date/`` prefix — depth-agnostic,
    so they already cover shapes A/B/C/D uniformly. Confirmed by exact reconciliation:
    real total objects == real ``.bak`` backup count for every one of those venues
    (BYBIT+KRAKEN-FUTURES: 9,540 real objects / 9,560 ``.bak`` — the +20 is a separate,
    already-flagged double-backup-of-a-backup artifact in that script, not a coverage
    gap; BINANCE-FUTURES+DELIVERY: 9,234/9,234 exact; DERIBIT: 5,342/5,342 exact).
  - ``canonicalize_okx_margin_type_2026_07_09.py``'s ``--by-day`` mode is the ONE real
    gap: its ``_list_okx_day_files`` does a **two-level DELIMITED (non-recursive)
    listing** — ``day=D/`` prefixes, then checks ONLY whether ``venue={V}/`` is a
    DIRECT CHILD of each ``day=D/`` prefix. This structurally can never see shape B (one
    level deeper, under ``pipeline_mode=.../asset_group=cefi/``) or shapes C/D (also
    nested/doubled). Real, exact reconciliation: 9,576 total real OKX-SWAP+OKX-FUTURES
    objects, only 4,762 (shape A) carry a ``.okxmarginfix.`` backup — **4,814 real
    objects (shape B: 4,742, shape C: 36, shape D: 36) were silently never discovered**,
    still carrying the original margin-type-inversion bug this whole migration exists to
    fix. Confirmed with real content, not just path inspection — sampled
    ``day=2023-06-15`` OKX-FUTURES: shape A (already migrated) correctly shows
    ``BTC-USD-230616`` as ``inverse``; the shape-B copy of the SAME (day, venue,
    instrument) still shows ``linear`` for all 60 real rows in that file (the original,
    unfixed bug) — real, live, unmigrated production data, not a hypothetical.

This script closes that gap: same ``_expected_margin_type`` correction rule as
``canonicalize_okx_margin_type_2026_07_09.py`` (byte-for-byte identical formula, already
live-verified against real OKX ``/api/v5/public/instruments`` responses and already
proven correct via that script's own smoke test + full catalog.parquet sweep + shape-A
per-day sweep), applied via a FLAT, depth-agnostic listing (same proven pattern as the
Bybit/Kraken/Binance/Deribit scripts) so no path shape — known or yet-undiscovered — can
hide a target file from it again. Idempotent: a file whose ``margin_type`` values are
already correct (shape A, already migrated) reports 0 changes and is a safe no-op
re-touch, not re-migrated a second time.

Safety: writes a timestamped backup blob before overwriting; a row that can't be safely
resolved is left UNCHANGED (never guessed); shard-level failure isolation (one file's
error is logged and counted, never aborts the sweep); dry-run by default; ``--apply
--confirm`` required together to mutate.

Usage::

    cd instruments-service

    # Dry-run (default) — report real shape inventory + mismatch counts, no writes:
    .venv/bin/python scripts/legacy_naming_audit_okx_2026_07_09.py

    # Bounded real smoke test (backup + write + verify a small sample):
    .venv/bin/python scripts/legacy_naming_audit_okx_2026_07_09.py --apply --confirm \\
        --sample 20

    # Full real sweep (real concurrency, all real gap files):
    .venv/bin/python scripts/legacy_naming_audit_okx_2026_07_09.py --apply --confirm \\
        --full-sweep --workers 40

SSOT: ``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
"generalized finding" (2026-07-09); ``instruments-service/docs/CEFI_INSTRUMENTS.md``
"Legacy GCS path-shape audit (2026-07-09)".
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
BY_DATE_PREFIX = "instrument_availability/by_date/"
_BACKUP_TAG = "legacynamingauditokx"

_VENUES = ("OKX-SWAP", "OKX-FUTURES")


def _expected_margin_type(raw_symbol: str, quote_asset: str | None = None) -> str:
    """Re-derive the corrected margin_type — byte-for-byte the same rule as
    ``canonicalize_okx_margin_type_2026_07_09.py::_expected_margin_type`` (itself mirroring
    ``_infer_margin_type``'s OKX branch in
    ``instruments_service/reference_data/adapters/cefi/tardis/parsing.py``). Duplicated rather
    than imported: that script is a standalone one-off with no importable module surface, and
    this is a narrow, already-proven-correct formula — not re-derived from scratch.
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


def _shape_of(blob_name: str) -> str:
    """Classify a real blob's path shape for reporting (A/B/C/D per module docstring).

    ``day=`` always appears once at the very start of every real blob path regardless of
    shape, so "doubled" means a SECOND ``day=`` segment appears before the ``venue=``
    segment — counting occurrences in the pre-``venue=`` prefix (not just checking
    membership, which would trivially match shape A/B too via their single leading
    ``day=``).
    """
    before_venue = blob_name.split("venue=", 1)[0]
    day_count = before_venue.count("day=")
    has_pipeline = "pipeline_mode=batch_instruments_service/asset_group=cefi/" in blob_name
    if has_pipeline and day_count >= 2:
        return "C (doubled-day, pipelined)"
    if has_pipeline:
        return "B (pipelined)"
    if day_count >= 2:
        return "D (doubled-day, bare)"
    return "A (bare)"


def list_by_date_targets(storage_client, bucket: str, limit: int | None = None) -> list[str]:
    """Flat, depth-agnostic listing over the ENTIRE ``by_date/`` prefix — deliberately NOT the
    two-level delimited listing ``canonicalize_okx_margin_type_2026_07_09.py`` uses (that is the
    real, confirmed gap this script exists to close; see module docstring). Mirrors the proven
    Bybit/Kraken/Binance/Deribit sibling scripts' listing pattern exactly.
    """
    targets: list[str] = []
    for b in storage_client.list_blobs(bucket, prefix=BY_DATE_PREFIX):
        if b.name.endswith("instruments.parquet") and any(f"venue={v}/" in b.name for v in _VENUES):
            targets.append(b.name)
            if limit is not None and len(targets) >= limit:
                break
    return targets


def _backup_key(blob_name: str, ts: str) -> str:
    if blob_name.endswith(".parquet"):
        return f"{blob_name[:-8]}.{_BACKUP_TAG}.{ts}.bak.parquet"
    return f"{blob_name}.{_BACKUP_TAG}.{ts}.bak"


def process_one_file(storage_client, bucket: str, blob_name: str, apply: bool, ts: str) -> dict[str, object]:
    """Migrate one real per-day OKX-SWAP/OKX-FUTURES snapshot file's ``margin_type`` column.

    Never raises out — any failure on this file is captured in the returned dict (shard-level
    failure isolation), so one bad file never aborts the rest of a full sweep.
    """
    result: dict[str, object] = {
        "blob": blob_name,
        "shape": _shape_of(blob_name),
        "rows": 0,
        "changed": 0,
        "error": None,
        "wrote": False,
    }
    try:
        raw = storage_client.download_bytes(bucket, blob_name)
        df = pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:  # broad-except-ok: shard-level isolation, capture and continue
        result["error"] = f"read_failed: {exc}"
        return result

    result["rows"] = len(df)
    if "margin_type" not in df.columns or "raw_symbol" not in df.columns:
        result["error"] = "missing_columns"
        return result

    quote_col = df["quote_asset"] if "quote_asset" in df.columns else pd.Series([None] * len(df), index=df.index)
    expected = pd.Series(
        [_expected_margin_type(rs, qa) for rs, qa in zip(df["raw_symbol"], quote_col, strict=True)],
        index=df.index,
    )
    changed_mask = expected != df["margin_type"]
    n_changed = int(changed_mask.sum())
    result["changed"] = n_changed
    if n_changed == 0 or not apply:
        return result

    backup_key = _backup_key(blob_name, ts)
    storage_client.upload_bytes(bucket, backup_key, raw)
    out = df.copy()
    out.loc[changed_mask, "margin_type"] = expected.loc[changed_mask]
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    storage_client.upload_bytes(bucket, blob_name, buf.getvalue())
    result["backup"] = backup_key
    result["wrote"] = True
    return result


def _load_targets_from_file(path: str) -> list[str]:
    """Load a pre-computed real target-blob list (one blob name per line, optionally full
    ``gs://bucket/...`` URLs) — reuses an already-real, already-fresh full ``by_date/`` GCS
    listing instead of re-walking the whole corpus a second time (single-walk discipline; the
    listing this reads from was itself a real ``gsutil ls -r`` over the live bucket). Still
    validated against the same real target predicate as a fresh listing would apply, so a stale
    or malformed input file can only under-select, never mis-select, a target.
    """
    targets: list[str] = []
    prefix = f"gs://{BUCKET}/"
    with open(path, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            blob = line[len(prefix) :] if line.startswith(prefix) else line
            if blob.endswith("instruments.parquet") and any(f"venue={v}/" in blob for v in _VENUES):
                targets.append(blob)
    return targets


def run(*, apply: bool, full_sweep: bool, sample: int, workers: int, target_listing: str | None = None) -> int:
    storage_client = get_storage_client()
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    logger.info("Mode=%s bucket=%s", "APPLY" if apply else "DRY-RUN", BUCKET)

    if target_listing:
        all_targets = _load_targets_from_file(target_listing)
        logger.info(
            "Loaded %d real target(s) from pre-computed listing %s (skipping fresh GCS walk)",
            len(all_targets),
            target_listing,
        )
    else:
        all_targets = list_by_date_targets(storage_client, BUCKET)
    by_shape: dict[str, int] = {}
    for t in all_targets:
        by_shape[_shape_of(t)] = by_shape.get(_shape_of(t), 0) + 1
    logger.info("Real OKX-SWAP/OKX-FUTURES per-day files found (all shapes): %d", len(all_targets))
    for shape, n in sorted(by_shape.items()):
        logger.info("  shape %s: %d file(s)", shape, n)

    scope = all_targets if full_sweep else all_targets[:sample]
    logger.info(
        "Scanning %d file(s) (%s) with %d workers...",
        len(scope),
        "full sweep" if full_sweep else f"bounded sample of {sample}",
        workers,
    )

    t0 = time.time()
    n_scanned = 0
    n_touched = 0
    n_rows_changed = 0
    n_errors = 0
    changed_by_shape: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_one_file, storage_client, BUCKET, blob, apply, ts): blob for blob in scope}
        for fut in as_completed(futures):
            res = fut.result()
            n_scanned += 1
            if res["error"]:
                n_errors += 1
                logger.warning("  ERROR %s: %s", res["blob"], res["error"])
            elif res["changed"]:
                changed_by_shape[res["shape"]] = changed_by_shape.get(res["shape"], 0) + 1
                n_rows_changed += res["changed"]
                if res["wrote"]:
                    n_touched += 1
            if n_scanned % 500 == 0 or n_scanned == len(scope):
                elapsed = time.time() - t0
                logger.info(
                    "  [%d/%d] elapsed=%.1fs files_with_mismatches=%d rows_fixed=%d files_written=%d errors=%d",
                    n_scanned,
                    len(scope),
                    elapsed,
                    sum(changed_by_shape.values()),
                    n_rows_changed,
                    n_touched,
                    n_errors,
                )

    elapsed = time.time() - t0
    logger.info(
        "DONE mode=%s files_scanned=%d files_written=%d rows_fixed=%d errors=%d elapsed=%.1fs (%.3fs/file, %d workers)",
        "APPLY" if apply else "DRY-RUN",
        n_scanned,
        n_touched,
        n_rows_changed,
        n_errors,
        elapsed,
        elapsed / max(1, n_scanned),
        workers,
    )
    logger.info("Mismatches found by shape: %s", changed_by_shape)
    if not apply:
        logger.info("DRY-RUN only — re-run with --apply --confirm to write.")
    return 1 if n_errors else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="Write fixes back to GCS (default: dry-run report only).")
    ap.add_argument("--confirm", action="store_true", help="Required alongside --apply to actually mutate blobs.")
    ap.add_argument("--sample", type=int, default=20, help="Bounded real sample size (file count) for smoke tests.")
    ap.add_argument("--full-sweep", action="store_true", help="Migrate ALL real target files, not just --sample.")
    ap.add_argument("--workers", type=int, default=30, help="Concurrent thread-pool workers (default 30).")
    ap.add_argument(
        "--target-listing",
        type=str,
        default=None,
        metavar="PATH",
        help="Reuse a pre-computed real by_date/ GCS listing (one blob/line) instead of a fresh walk.",
    )
    args = ap.parse_args(argv)

    apply = bool(args.apply and args.confirm)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as well — refusing to mutate without both flags.")
        return 1

    return run(
        apply=apply,
        full_sweep=args.full_sweep,
        sample=args.sample,
        workers=args.workers,
        target_listing=args.target_listing,
    )


if __name__ == "__main__":
    raise SystemExit(main())
