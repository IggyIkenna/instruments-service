#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this run is confirmed applied against
#   market-data-tick-cefi-prd-central-element-323112 (before/after evidence
#   recorded in plans/active/issues/phantom_captures_cefi_2026_06_28.md) AND
#   one full audit cycle has passed with no DP_RUN_MOSTLY_EMPTY recurrence
#   for this cell.
"""One-off: delete the confirmed-stale, blank-``data_type`` orphan rows from
the CeFi ``_index`` manifest.

Background: ``plans/active/issues/phantom_captures_cefi_2026_06_28.md``
(2026-07-15 investigation section) found 9,757 CeFi manifest rows stuck at
``capture_status=attempted_failed`` with a blank/null ``data_type``. Live
re-verification confirms:

* All matching rows share ONE literal ``attempted_at`` timestamp
  (``2026-06-28T03:12:34.851168+00:00``) and ONE ``error_reason``
  (``phantom_captured_no_parquet_at_canonical_path``) — an undocumented
  ``reconcile_phantom_manifest_rows_all.py --apply`` run flipped them
  ``captured`` -> ``attempted_failed`` on that date, using the tool's
  structural blind spot for blank-``data_type`` rows (see the
  ``_build_forward_captured_idx`` fix in
  ``reconcile_phantom_manifest_rows_all.py``, landed alongside this script,
  which now excludes blank-``data_type`` rows from that forward pass so this
  class of false-positive can't recur).
* 100% of the matched rows carry ``schema_version==4`` (pre-v5 vestigial
  rows — informational "this venue was touched on this date" markers, not
  real shards) and ``pipeline_mode``/``source`` both 100% blank.
* ~99.0% (9,658/9,757) already have a SEPARATE, correctly-typed ``captured``
  row for the same ``(date, venue)`` elsewhere in the manifest — these are
  redundant/orphan duplicates, not evidence of missing market data. The
  remaining 99 (all early-2019 DERIBIT dates) have no captured counterpart
  at all — plausibly pre-genesis/no-data.

These rows can NEVER be un-phantomed (their blank ``data_type`` can never
match a real GCS object at any candidate path) and can never legitimately
convert to ``captured`` — they should be REMOVED, not flipped back.

**Deliberately bypasses ``merge_canonical_with_outstanding_shards`` (unlike
``reconcile_phantom_manifest_rows_all.py``'s own
``_apply_delete_chain_level_defi_phantoms`` / delete pattern it would
otherwise mirror).** Live investigation (2026-07-15, this run) found the
CeFi bucket carries a permanent ``_index/per_vm/_legacy_seed.parquet``
snapshot (frozen 2026-06-24, per ``manifest_consolidator.py``'s "legacy seed
— never pruned" design) that still holds these exact 9,757 rows in their
PRE-flip ``captured`` state. The 2026-07-13 "captured-outranks" merge
tie-break (``_merge_shard_frames`` / the consolidator's own leading
``CASE WHEN capture_status = 'captured' THEN 1 ELSE 0 END DESC``) makes that
frozen ``captured`` row win over the canonical's newer, correct
``attempted_failed`` row REGARDLESS OF RECENCY — so
``merge_canonical_with_outstanding_shards`` currently reports these rows as
still ``captured`` (verified empirically: 0 matches via that path vs. 9,757
via a raw canonical-only read). Using it here would make this script silently
no-op (safe) in the best case, or risk writing a merged frame that resurrects
the phantom rows in the worst case. See
``plans/active/issues/legacy_seed_captured_outranks_resurrection_risk_2026_07_15.md``
for the full cross-bucket finding (affects cefi/defi/tradfi) — NOT fixed by
this script, flagged for a separate follow-up.

Instead, this script reads + writes ONLY the canonical
``_index/availability_index.parquet`` blob directly (no shard merge), and
protects against a concurrent canonical write (e.g. a consolidator cycle
landing mid-run) via true atomic compare-and-set
(``StorageClient.conditional_upload_bytes(..., if_generation_match=...)``,
the SAME GCS generation-precondition primitive ``manifest_consolidator.py``
and ``manifest_writer/_writer_io.py`` use for their own canonical writes) —
rather than the "re-merge and hope nothing raced" pattern, this refuses the
write outright (returns a distinct exit code) if the canonical's generation
changed between read and write, so it can never silently clobber a
concurrent legitimate write. A verified pre-delete backup of the full
canonical blob is snapshotted first (mirrors ``manifest_dedup_2026_07_10
.py``'s quarantine-prefix pattern) so there's a byte-exact restore point
regardless.

This is a one-time, narrowly-scoped cleanup, not a general recurring
reconciliation pass: the delete predicate below pins the EXACT literal
``attempted_at`` + ``error_reason`` values observed in the live 2026-07-15
investigation, so it can only ever match this one historical incident's rows
— never a future or unrelated population, even if some future writer
independently emits a different blank-``data_type`` row (it would carry a
different ``attempted_at``).

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python \\
        scripts/delete_cefi_blank_data_type_orphan_rows_2026_07_15.py --dry-run
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python \\
        scripts/delete_cefi_blank_data_type_orphan_rows_2026_07_15.py --apply

SSOT: unified-trading-pm/plans/active/issues/phantom_captures_cefi_2026_06_28.md
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import StorageClient, get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_QUARANTINE_PREFIX = "_migration_backup/delete_cefi_blank_data_type_orphan_rows_2026_07_15/"

# SSOT delete predicate — the EXACT, confirmed-stale population from the
# 2026-07-15 live investigation. Every discriminating field is pinned to the
# literal value observed live so this predicate can only ever match this one
# historical incident's rows.
_TARGET_ATTEMPTED_AT = "2026-06-28T03:12:34.851168+00:00"
_TARGET_ERROR_REASON = "phantom_captured_no_parquet_at_canonical_path"

# Cross-reference floor: the live investigation found 99.0% (9,658/9,757) of
# the target population has a real captured sibling at the same (date,
# venue). If a fresh re-check ever comes in materially below this, something
# has changed since the investigation and the run should refuse rather than
# blindly trust the historical number.
_MIN_SIBLING_FRACTION = 0.95


def _orphan_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask for the confirmed-stale blank-data_type orphan set (SSOT predicate)."""
    status = df["capture_status"].fillna("").astype(str)
    dtype = df["data_type"].fillna("").astype(str)
    reason = (
        df["error_reason"].fillna("").astype(str) if "error_reason" in df.columns else pd.Series("", index=df.index)
    )
    attempted_at = df["attempted_at"].astype(str) if "attempted_at" in df.columns else pd.Series("", index=df.index)
    return (
        (status == "attempted_failed")
        & (dtype == "")
        & (reason == _TARGET_ERROR_REASON)
        & (attempted_at == _TARGET_ATTEMPTED_AT)
    )


def _sibling_fraction(df: pd.DataFrame, mask: pd.Series) -> tuple[int, int, float]:
    """Re-derive the (date, venue) cross-reference check live (never trust a stale doc number)."""
    captured_mask = df["capture_status"].fillna("").astype(str) == "captured"
    captured_dv = set(
        zip(df.loc[captured_mask, "date"].astype(str), df.loc[captured_mask, "venue"].astype(str), strict=False)
    )
    orphan_dv = list(zip(df.loc[mask, "date"].astype(str), df.loc[mask, "venue"].astype(str), strict=False))
    have_sibling = sum(1 for dv in orphan_dv if dv in captured_dv)
    total = len(orphan_dv)
    return have_sibling, total, (have_sibling / total if total else 0.0)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="Report only — no writes (default when --apply absent).")
    ap.add_argument("--apply", action="store_true", help="Delete the matched rows and write the trimmed index back.")
    args = ap.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    client: StorageClient = get_storage_client(provider="gcp")

    # Raw canonical-only read (NOT merge_canonical_with_outstanding_shards —
    # see the module docstring's "_legacy_seed.parquet" landmine). Captures
    # the generation for the follow-up CAS write.
    raw, generation = client.download_bytes_with_generation(bucket, _INDEX_BLOB)
    if raw is None:
        logger.error("Canonical index not found at gs://%s/%s — aborting.", bucket, _INDEX_BLOB)
        return 1
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("bucket=%s canonical rows: %d (generation=%d)", bucket, len(df), generation)

    mask = _orphan_mask(df)
    n = int(mask.sum())
    logger.info("orphan predicate matched %d rows (expected 9,757 per phantom_captures_cefi_2026_06_28.md)", n)

    have_sibling, total, frac = _sibling_fraction(df, mask)
    logger.info(
        "cross-reference (re-derived live): %d/%d (%.4f) orphan rows have a real captured "
        "sibling at the same (date, venue); expected ~0.9899 (9658/9757) per the investigation",
        have_sibling,
        total,
        frac,
    )

    if n == 0:
        logger.info("Nothing to delete — orphan set is empty.")
        return 0

    if not args.apply:
        logger.info("DRY-RUN — pass --apply to DELETE the matched rows. No writes.")
        return 0

    if frac < _MIN_SIBLING_FRACTION:
        logger.error(
            "REFUSING DELETE — cross-reference (%.4f) fell below the %.2f floor derived from the "
            "2026-07-15 investigation. Something has changed since that investigation — re-investigate "
            "before deleting.",
            frac,
            _MIN_SIBLING_FRACTION,
        )
        return 1

    # Verified pre-delete backup of the FULL canonical blob (not just the matched
    # rows) — a complete point-in-time snapshot to restore from if anything about
    # this run turns out to be wrong. Mirrors manifest_dedup_2026_07_10.py's
    # quarantine-prefix pattern.
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_dest = f"{_QUARANTINE_PREFIX}availability_index_pre_delete_{ts}.parquet"
    client.upload_bytes(bucket, backup_dest, raw)
    verify_raw = client.download_bytes(bucket, backup_dest)
    if len(verify_raw) != len(raw):
        logger.error("Backup verify failed (size mismatch) — aborting before touching the live manifest.")
        return 1
    logger.info("Backup verified: gs://%s/%s (%d bytes)", bucket, backup_dest, len(raw))

    # Defensive guard: the predicate must never select anything but the
    # confirmed attempted_failed orphan rows.
    deleting_status = df.loc[mask, "capture_status"].fillna("").astype(str)
    bad = int((deleting_status != "attempted_failed").sum())
    if bad:
        raise RuntimeError(
            f"REFUSING DELETE — predicate selected {bad} non-attempted_failed rows (would destroy live data)"
        )

    status_before = df["capture_status"].fillna("").astype(str)
    captured_before = int((status_before == "captured").sum())
    attempted_failed_before = int((status_before == "attempted_failed").sum())
    total_before = len(df)

    kept = df[~mask].copy()
    status_after = kept["capture_status"].fillna("").astype(str)
    captured_after = int((status_after == "captured").sum())
    attempted_failed_after = int((status_after == "attempted_failed").sum())
    if captured_after != captured_before:
        raise RuntimeError(
            f"REFUSING WRITE — captured count changed {captured_before} -> {captured_after} (delete bug)"
        )
    expected_af_after = attempted_failed_before - n
    if attempted_failed_after != expected_af_after:
        raise RuntimeError(
            f"REFUSING WRITE — attempted_failed count changed to an unexpected value "
            f"({attempted_failed_before} -> {attempted_failed_after}, expected {expected_af_after})"
        )

    out = io.BytesIO()
    kept.to_parquet(out, index=False)
    out_bytes = out.getvalue()

    # Atomic CAS write: only succeeds if the canonical's generation is STILL
    # what we read above. A None return means a concurrent writer (e.g. the
    # consolidator) landed a newer canonical in the interim — refuse rather
    # than clobber it; the backup above is untouched either way and the run
    # can simply be re-attempted from a fresh read.
    new_generation = client.conditional_upload_bytes(bucket, _INDEX_BLOB, out_bytes, if_generation_match=generation)
    if new_generation is None:
        logger.error(
            "REFUSING WRITE — canonical generation changed since the read above (a concurrent writer "
            "landed a newer canonical). No write performed. Backup retained at gs://%s/%s — re-run from "
            "a fresh read.",
            bucket,
            backup_dest,
        )
        return 1

    logger.info(
        "DONE — wrote trimmed index (generation %d -> %d): %d -> %d rows (%d deleted; captured=%d "
        "unchanged; attempted_failed %d -> %d). Backup: gs://%s/%s",
        generation,
        new_generation,
        total_before,
        len(kept),
        n,
        captured_after,
        attempted_failed_before,
        attempted_failed_after,
        bucket,
        backup_dest,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
