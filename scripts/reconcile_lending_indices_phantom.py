#!/usr/bin/env python3
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false
"""Phantom-row reconciler for the lending-indices canonical manifest.

The lending-indices bucket (``lending-indices-{pid}``) has its OWN canonical
manifest at ``_index/availability_index.parquet``, separate from the shared
``market-data-tick-defi-*`` manifest covered by
``reconcile_phantom_manifest_rows_all.py``.

A **phantom row** claims ``capture_status=captured`` but no parquet exists at
the canonical GCS prefix for that ``(date, protocol, chain)`` shard.

This script audits the lending-indices manifest and classifies each phantom into
one of two typed ``empty_confirmed`` reasons:

* ``EXPECTED_PRE_GENESIS_CHAIN`` — the row date is BEFORE the
  ``(chain, protocol)`` launch date per UAC ``PROTOCOL_LAUNCH_DATES`` (via
  ``get_protocol_launch_date``).  These rows should never have been captured;
  the pre-floor short-circuit in the handler now prevents this but old backfill
  runs may have created them.

* ``SOURCE_RETURNED_ZERO`` — the row date is ON or AFTER the protocol launch
  date but no parquet exists on disk.  This is the classic "true phantom": the
  manifest claims captured, the orchestrator will forever skip the shard, and
  the data is silently missing.

GCS path layout (canonical, as written by ``_collect_protocol_chain``):

    ``lending_indices/{protocol}/{chain}/date={YYYY-MM-DD}/{protocol}_{chain}_{ts}.parquet``

e.g. ``lending_indices/aave_v3/ETHEREUM/date=2026-05-07/aave_v3_ETHEREUM_20260507_120000.parquet``

Manifest row key columns (as written by ``DefiManifestRecorder.record_captured``):
    - ``date``            YYYY-MM-DD
    - ``venue``           protocol slug (``aave_v3`` / ``spark`` / ``compound_v3``)
    - ``chain``           uppercase chain (``ETHEREUM`` / ``ARBITRUM`` / ...)
    - ``data_type``       ``lending_indices``
    - ``instrument_type`` ``lending``
    - ``capture_status``  one of the 4-state manifest values

Idempotent: rows already at ``empty_confirmed`` with a typed reason are
skipped.  Re-running after a successful apply-flips is a no-op.

v8-tolerant: manifest read/write via ``pd.read_parquet`` / ``df.to_parquet``;
unknown v8 columns (``pipeline_mode`` / ``service_emission_state`` /
``last_emission_decision_at`` / ``expected_window_completeness_fraction``) pass
through untouched.

Usage::

    cd instruments-service

    # Dry-run (default — no writes)
    .venv/bin/python scripts/reconcile_lending_indices_phantom.py --dry-run

    # Apply flips (safety: requires --confirm + --apply-flips)
    .venv/bin/python scripts/reconcile_lending_indices_phantom.py \\
        --apply-flips --confirm \\
        --protocols aave_v3,compound_v3 \\
        --chains ETHEREUM,ARBITRUM \\
        --max-flips 500

References:
    * Plan: ``plans/active/defi_catalogue_chain_primitives_2026_05_10.md``
      § 3-LENDING.5
    * Sister script: ``reconcile_phantom_manifest_rows_all.py``
    * UAC SSOT: ``unified_api_contracts.registry.chain_env.get_protocol_launch_date``
    * Handler: ``market_tick_data_service.cli.handlers.lending_indices_handler``
"""

from __future__ import annotations

import argparse
import contextlib
import io
import logging
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import pandas as pd
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Grandfathered per reconcile_phantom_manifest_rows_all.py precedent.
PROJECT_ID = "central-element-323112"
BUCKET_NAME = f"lending-indices-{PROJECT_ID}"
MANIFEST_BLOB = "_index/availability_index.parquet"

# Closed set of protocols that write to the lending-indices bucket.
# Must match ``_DEFAULT_PROTOCOLS`` in LendingIndicesHandler.
_DEFAULT_PROTOCOLS: frozenset[str] = frozenset({"aave_v3", "spark", "compound_v3"})

# Protocol slug → canonical venue_prefix used in UAC PROTOCOL_LAUNCH_DATES.
# Derived from PROTOCOL_CAPABILITIES[slug].venue_prefix in UAC _defi.py via
# get_venue_prefix("aave_v3") == "AAVE_V3", etc.
_VENUE_PREFIX: dict[str, str] = {
    "aave_v3": "AAVE_V3",
    "spark": "SPARK",
    "compound_v3": "COMPOUND_V3",
}

# Inverse map: venue_prefix (manifest column value) → protocol slug (GCS path segment).
# The lending-indices bucket's flat-prefix layout uses lowercase + underscored protocol
# slugs (e.g. ``lending_indices/aave_v3/ETHEREUM/...``) while the canonical manifest
# carries venue=AAVE_V3 (uppercase, no underscore). The audit must translate before
# probing GCS — without this fix, every captured row false-positives as phantom
# (path /AAVE_V3/ has 0 parquets; actual path /aave_v3/ has the data).
_VENUE_TO_SLUG: dict[str, str] = {v: k for k, v in _VENUE_PREFIX.items()}

# GCS prefix template for listing parquets under a (protocol, chain, date) shard.
# Written by LendingIndicesHandler._collect_protocol_chain → write_defi_rows → upload.
# Example: ``lending_indices/aave_v3/ETHEREUM/date=2026-05-07/``
_GCS_PREFIX_TPL = "lending_indices/{protocol}/{chain}/date={date}/"


def _shard_prefix(protocol: str, chain: str, date_str: str) -> str:
    """Return the GCS prefix for a single ``(protocol, chain, date)`` shard."""
    return _GCS_PREFIX_TPL.format(protocol=protocol, chain=chain, date=date_str)


def _has_parquet(bucket: storage.Bucket, prefix: str) -> bool:
    """Return True if at least one ``.parquet`` blob exists under *prefix*."""
    for blob in bucket.list_blobs(prefix=prefix, max_results=1):
        if blob.name.endswith(".parquet"):
            return True
    return False


def _classify_phantom(
    venue: str,
    chain: str,
    date_str: str,
) -> str:
    """Return the ``empty_confirmed`` reason string for a phantom shard.

    Takes the canonical venue (uppercase, manifest form — e.g. ``AAVE_V3``) and
    uses ``get_protocol_launch_date(chain, venue_prefix)`` from UAC SSOT to
    distinguish pre-genesis rows (``EXPECTED_PRE_GENESIS_CHAIN``) from
    post-genesis true phantoms (``SOURCE_RETURNED_ZERO``).
    """
    from unified_api_contracts.registry.chain_env import get_protocol_launch_date

    if venue not in _VENUE_TO_SLUG:
        # Unknown venue — cannot determine launch date; default to post-genesis.
        return "SOURCE_RETURNED_ZERO"

    launch_iso = get_protocol_launch_date(chain, venue)
    if launch_iso is not None and date_str < launch_iso:
        return "EXPECTED_PRE_GENESIS_CHAIN"
    return "SOURCE_RETURNED_ZERO"


def _audit_captured_rows(
    bucket: storage.Bucket,
    df: pd.DataFrame,
    captured_idx: "pd.Index[int]",
    workers: int,
) -> dict[int, tuple[bool, str]]:
    """Return per-row audit results: ``{row_index: (is_real, reason)}``.

    ``is_real=True``  → row has a parquet on disk; leave at ``captured``.
    ``is_real=False`` → phantom; ``reason`` is the typed ``empty_confirmed`` reason.

    Only rows with ``capture_status=captured`` are passed in — idempotency is
    guaranteed upstream by the scope mask.
    """
    prefixes_by_idx: dict[int, str] = {}
    venue_to_protocol_warnings: set[str] = set()
    for idx in captured_idx:
        row = df.loc[idx]
        venue = str(row.get("venue", "") or "")
        chain = str(row.get("chain", "") or "")
        date_str = str(row.get("date", "") or "")[:10]
        if not venue or not chain or not date_str:
            logger.warning("Row %d missing venue/chain/date — skipping", idx)
            continue
        # Translate manifest venue (uppercase, e.g. ``AAVE_V3``) to flat-prefix protocol
        # slug (lowercase + underscored, e.g. ``aave_v3``). Without this translation
        # every captured row false-positives as phantom (GCS layout uses slugs).
        protocol = _VENUE_TO_SLUG.get(venue)
        if protocol is None:
            if venue not in venue_to_protocol_warnings:
                logger.warning(
                    "Unknown venue %r in manifest — no slug mapping; rows for this venue will be skipped",
                    venue,
                )
                venue_to_protocol_warnings.add(venue)
            continue
        prefixes_by_idx[idx] = _shard_prefix(protocol, chain, date_str)

    unique_prefixes = sorted(set(prefixes_by_idx.values()))
    logger.info(
        "lending-indices phantom: listing %d unique (protocol, chain, date) prefixes",
        len(unique_prefixes),
    )

    prefix_exists: dict[str, bool] = {}

    def _check(prefix: str) -> tuple[str, bool]:
        try:
            return prefix, _has_parquet(bucket, prefix)
        except Exception as exc:
            logger.warning("list error for %s: %s", prefix, exc)
            return prefix, False

    completed = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_check, p) for p in unique_prefixes]
        for fut in as_completed(futs):
            prefix, exists = fut.result()
            prefix_exists[prefix] = exists
            completed += 1
            if completed % 100 == 0:
                rate = completed / max(0.001, time.time() - t0)
                logger.info(
                    "  %d/%d prefixes checked (%.1f/s)",
                    completed,
                    len(unique_prefixes),
                    rate,
                )

    results: dict[int, tuple[bool, str]] = {}
    for idx, prefix in prefixes_by_idx.items():
        row = df.loc[idx]
        venue = str(row.get("venue", "") or "")
        chain = str(row.get("chain", "") or "")
        date_str = str(row.get("date", "") or "")[:10]
        is_real = prefix_exists.get(prefix, False)
        if is_real:
            results[idx] = (True, "")
        else:
            reason = _classify_phantom(venue, chain, date_str)
            results[idx] = (False, reason)

    return results


def main() -> int:  # noqa: C901
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report findings only; do not modify the manifest (default mode).",
    )
    mode.add_argument(
        "--apply-flips",
        action="store_true",
        default=False,
        help="Flip phantom rows to empty_confirmed with typed reason.",
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        default=False,
        help="Required safety belt when --apply-flips is passed.",
    )
    p.add_argument(
        "--protocols",
        type=str,
        default="",
        help=("Comma-separated protocol slugs to include (default: all — aave_v3,spark,compound_v3)."),
    )
    p.add_argument(
        "--chains",
        type=str,
        default="",
        help="Comma-separated chains to include (default: all).",
    )
    p.add_argument(
        "--max-flips",
        type=int,
        default=1000,
        help="Safety cap: abort if --apply-flips would touch more than N rows (default 1000).",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=16,
        help="GCS listing concurrency (default 16).",
    )
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="Scope audit to dates >= YYYY-MM-DD.",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default="",
        help="Scope audit to dates <= YYYY-MM-DD.",
    )
    p.add_argument(
        "--project-id",
        type=str,
        default=PROJECT_ID,
        help=f"GCP project ID (default: {PROJECT_ID}).",
    )
    args = p.parse_args()

    # --dry-run is the default; ensure at least one mode is set.
    if not args.apply_flips and not args.dry_run:
        args.dry_run = True

    if args.apply_flips and not args.confirm:
        logger.error("--apply-flips requires --confirm as a safety belt. Aborting.")
        return 1

    project_id: str = args.project_id
    bucket_name = f"lending-indices-{project_id}"

    # Protocol filter (default: all declared protocols).
    wanted_protocols: frozenset[str] | None = None
    if args.protocols:
        raw_protos: frozenset[str] = frozenset(q.strip().lower() for q in str(args.protocols).split(",") if q.strip())
        unknown = raw_protos - _DEFAULT_PROTOCOLS
        if unknown:
            logger.warning(
                "Unknown protocols (not in lending-indices handler): %s — skipping",
                unknown,
            )
        wanted_protocols = raw_protos & _DEFAULT_PROTOCOLS
        if not wanted_protocols:
            logger.error("No valid protocols after filter. Aborting.")
            return 1

    # Chain filter (default: all).
    wanted_chains: frozenset[str] | None = None
    if args.chains:
        wanted_chains = frozenset(c.strip().upper() for c in args.chains.split(",") if c.strip())

    # GCS client.
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)

    logger.info("Loading manifest from gs://%s/%s", bucket_name, MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(
        prefix="recon-lending-indices-",
        suffix=".parquet",
        delete=False,
        dir=tempfile.gettempdir(),
    ) as _tf:
        manifest_path = _tf.name
    try:
        blob.download_to_filename(manifest_path)
        df = pd.read_parquet(manifest_path)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(manifest_path)

    logger.info("Manifest rows: %d", len(df))

    # Build the captured-row mask with optional filters.
    # Idempotent: only rows at capture_status="captured" are in scope.
    captured_mask = df["capture_status"].fillna("").astype(str) == "captured"

    if wanted_protocols is not None:
        # Manifest carries venue in uppercase-no-underscore form (e.g. AAVE_V3, COMPOUND_V3).
        # CLI --protocols passes slug form (aave_v3, compound_v3). Translate manifest
        # venue → slug via _VENUE_TO_SLUG before comparing. Pre-fix bug: this used
        # ``.str.lower()`` which produces ``aavev3`` (no underscore) ∉ {aave_v3, ...}.
        slugs_for_row = df["venue"].astype(str).map(_VENUE_TO_SLUG).fillna("")
        captured_mask = captured_mask & slugs_for_row.isin(wanted_protocols)
    if wanted_chains is not None:
        captured_mask = captured_mask & df["chain"].astype(str).str.upper().isin(wanted_chains)
    if args.start_date:
        captured_mask = captured_mask & (df["date"].astype(str) >= args.start_date)
    if args.end_date:
        captured_mask = captured_mask & (df["date"].astype(str) <= args.end_date)

    # Filter to data_type=lending_indices (defensive — accept BOTH the canonical snake
    # form ``lending_indices`` AND the legacy kebab ``lending-indices`` 24,976 pre-rename
    # rows; full audit-trail in
    # plans/active/issues/lending_indices_data_type_vocabulary_drift_2026_05_16.md).
    if "data_type" in df.columns:
        dt_str = df["data_type"].fillna("").astype(str)
        captured_mask = captured_mask & ((dt_str == "lending_indices") | (dt_str == "lending-indices"))

    captured_idx = df[captured_mask].index
    logger.info("Captured rows in scope: %d", len(captured_idx))

    if len(captured_idx) == 0:
        logger.info("Nothing to audit. Manifest is clean (or already fully reconciled).")
        return 0

    audit_results = _audit_captured_rows(bucket, df, captured_idx, args.workers)

    phantom_idx = [i for i, (real, _) in audit_results.items() if not real]
    real_count = sum(1 for real, _ in audit_results.values() if real)

    reason_counts: dict[str, int] = {}
    for idx in phantom_idx:
        _, reason = audit_results[idx]
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    logger.info("=" * 60)
    logger.info("Audit summary — lending-indices manifest phantom check:")
    logger.info("  Total captured rows audited : %d", len(captured_idx))
    logger.info("  Real captures (parquet found): %d", real_count)
    logger.info("  Phantom captures            : %d", len(phantom_idx))
    for reason, cnt in sorted(reason_counts.items()):
        logger.info("    %-40s %d", reason, cnt)
    logger.info("=" * 60)

    if not phantom_idx:
        logger.info("No phantoms found. Manifest is clean.")
        return 0

    # Sample up to 5 phantom row keys.
    logger.info("Sample phantom rows (up to 5):")
    for idx in phantom_idx[:5]:
        row = df.loc[idx]
        logger.info(
            "  date=%s venue=%s chain=%s data_type=%s",
            row.get("date"),
            row.get("venue"),
            row.get("chain"),
            row.get("data_type", "N/A"),
        )

    # Per-protocol / per-chain distribution.
    phantom_df_view = df.loc[phantom_idx]
    if "venue" in phantom_df_view.columns:
        by_venue = phantom_df_view.groupby("venue").size().sort_values(ascending=False)
        logger.info("Phantom distribution by venue (protocol):\n%s", by_venue.to_string())
    if "chain" in phantom_df_view.columns:
        by_chain = phantom_df_view.groupby("chain").size().sort_values(ascending=False)
        logger.info("Phantom distribution by chain:\n%s", by_chain.to_string())

    if args.dry_run:
        logger.info("DRY RUN — manifest not modified. Pass --apply-flips --confirm to commit.")
        return 0

    # Safety cap check before writing.
    if len(phantom_idx) > args.max_flips:
        logger.error(
            "Safety cap: %d phantoms detected > --max-flips %d. "
            "Re-run with a higher --max-flips or narrow scope with --protocols/--chains.",
            len(phantom_idx),
            args.max_flips,
        )
        return 1

    now_iso = datetime.now(UTC).isoformat()
    for idx in phantom_idx:
        _, reason = audit_results[idx]
        df.at[idx, "capture_status"] = "empty_confirmed"
        df.at[idx, "error_reason"] = reason
        df.at[idx, "attempted_at"] = now_iso

    # Write back — v8 emission-tracking columns pass through transparently via
    # pd.DataFrame.to_parquet preserving every column already on the frame.
    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    logger.info(
        "Uploading reconciled manifest (%d rows total, %d phantoms flipped)",
        len(df),
        len(phantom_idx),
    )
    blob.upload_from_file(out, content_type="application/octet-stream")
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
