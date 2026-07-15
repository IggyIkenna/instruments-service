#!/usr/bin/env python3
# Epic: cefi_master
# Lifecycle: oneoff
# Delete-when: after the targeted re-fetch of the affected shards is confirmed complete
"""audit_tardis_concurrency_lease_race_false_failures_2026_07_15.py

Re-audit ``attempted_failed`` manifest rows written by the 3 relaunched
cefi-queue-* VMs during the 2026-07-15T20:2x-20:3xZ TardisConcurrencyLease
intra-process race window, distinguishing bug-caused false failures from
genuine no-data shards.

Root cause (see plans/active/issues/tardis_concurrency_lease_intra_process_race_2026_07_15.md,
todo (1) — the fix, tracked separately as backlog task
``tardis_concurrency_lease_intra_process_race-001``):
``ensure_process_lease_acquired()`` flips its module-global guard BEFORE
``lease.acquire()`` resolves, so up to 15 of 16 concurrently-gathered
Tardis-fetch coroutines skip the lease-wait entirely on a cold process and
fire their HTTP request unguarded, tripping Tardis's single-concurrent-IP
lock (HTTP 403 code=274).

The affected rows carry a STABLE, exact ``error_reason`` string
(``tardis_base_client.py`` ``TardisHTTPError._parse_403_body``, deliberately
kept free of the variable ``retryAfterSeconds`` so it groups cleanly in the
manifest):

    "Tardis HTTP 403 code=274 concurrent-IP-lock"

This script identifies ``attempted_failed`` rows matching that exact string
and reports two scopes:

1. **Session-scoped** (``attempted_at`` within 2026-07-15T20:20-20:39Z) — the
   3 relaunched VMs' race window the parent issue doc's live evidence covers.
2. **Corpus-wide** — EVERY row in the cefi prd manifest carrying this exact
   error_reason, regardless of when it was written. Live-run finding
   (2026-07-15, this session): the corpus-wide count is 21,982 rows with
   ``attempted_at`` spanning 2026-07-13T09:01Z through 2026-07-15T20:49Z —
   i.e. this race has been corrupting the manifest for ~2.5 days across MANY
   cefi backfill VM launches, not just this session's 3 VMs (venues affected
   include BITGET-FUTURES, OKX-SPOT, OKX-FUTURES, LIGHTER-ZKSYNC,
   KRAKEN-FUTURES, COINBASE-SPOT, DERIBIT/DERIBIT-COMBO — well beyond the 3
   session VMs' BINANCE-FUTURES/BITGET-FUTURES/BYBIT scope). This is a BIGGER
   finding than the parent todo anticipated — see the issue doc's Progress Log
   for the corpus-wide-scope addendum and its own follow-up todo.

so a TARGETED re-fetch — not a blind full re-run — can close just the
affected shards, at whichever scope the operator decides to act on.

Flip contract (mirrors retry_transient_cefi_failures_2026_06_28.py)
--------------------------------------------------------------------
Rows must satisfy ALL of:
1. ``capture_status == "attempted_failed"``
2. ``error_reason == "Tardis HTTP 403 code=274 concurrent-IP-lock"`` (exact match —
   this is the ONE stable string this specific race produces; a substring/prefix
   match would over-capture unrelated 403s that are NOT proven race artifacts)

Flip target (only under --apply)
---------------------------------
- ``capture_status``  → ``"expected_unattempted"``
- ``error_reason``    → ``""`` (cleared — next attempt records the real outcome)

HARD GATE — do not --apply until the race fix has shipped
-----------------------------------------------------------
Flipping these rows to ``expected_unattempted`` re-queues them for the next
backfill VM. If the lease race (todo (1) /
``tardis_concurrency_lease_intra_process_race-001``) has NOT shipped yet,
the re-attempt will race and 403 again — the flip would be pure churn.
``--apply`` therefore additionally requires ``--confirm-lease-fix-shipped``
(operator/agent must have verified task -001 is done + its regression test
passed before passing this flag).

Safety gates (ABORT before write if violated)
-----------------------------------------------
1. Per-VM shard isolation: ``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=<unique>``
   must be set when ``--apply`` is given.
2. ``--apply`` additionally requires ``--confirm-lease-fix-shipped`` (see above).
3. Rows at ``capture_status=captured`` or ``expected_unattempted`` are NEVER touched.
4. Count of ``captured`` rows must be unchanged after the flip.

Source: plans/active/issues/tardis_concurrency_lease_intra_process_race_2026_07_15.md todo (2).

Usage::

    # Default: dry-run audit report only (no mutations) — reads the 3 relaunched
    # VMs' per-VM manifest shards directly.
    cd instruments-service
    .venv/bin/python scripts/audit_tardis_concurrency_lease_race_false_failures_2026_07_15.py

    # Apply (only once tardis_concurrency_lease_intra_process_race-001 has shipped):
    MANIFEST_PER_VM_SHARDS=true VM_NAME=cefi-race-refetch-$(date +%s) \\
    .venv/bin/python scripts/audit_tardis_concurrency_lease_race_false_failures_2026_07_15.py \\
        --apply --confirm-lease-fix-shipped
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_DIR = "_index/per_vm/"

# The 3 VMs relaunched in the same wave per the issue doc's live evidence section.
# light-binancefutu is the one that actually showed the race (1928 HTTP 403s);
# the other 2 are included for completeness — the audit must not assume
# a priori which VMs were affected.
RELAUNCHED_VMS: tuple[str, ...] = (
    "cefi-queue-light-binancefutu-x2-20260715-202013",
    "cefi-queue-heavy-binancefutu-x15-20260715-202000",
    "cefi-queue-light-bybit-x4-20260715-202022",
)

# The exact, stable error_reason string this specific race produces
# (tardis_base_client.py TardisHTTPError._parse_403_body — deliberately held
# constant, no variable retryAfterSeconds, so it groups in the manifest).
RACE_ERROR_REASON = "Tardis HTTP 403 code=274 concurrent-IP-lock"

# The parent issue doc's stated live-evidence window for the 3 relaunched VMs
# (session-scoped subset of the corpus-wide race — see module docstring).
SESSION_WINDOW_START = pd.Timestamp("2026-07-15T20:20:00Z")
SESSION_WINDOW_END = pd.Timestamp("2026-07-15T20:39:59Z")


def _get_cefi_prd_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi", deployment_env="prd")


def _load_per_vm_shard(bucket: str, vm_name: str) -> pd.DataFrame | None:
    """Load one VM's per-VM manifest shard. Returns None if the shard doesn't exist."""
    blob = f"{PER_VM_DIR}{vm_name}.parquet"
    client = get_storage_client(provider="gcp")
    if not client.blob_exists(bucket, blob):
        logger.warning("Per-VM shard not found: gs://%s/%s", bucket, blob)
        return None
    raw = client.download_bytes(bucket, blob)
    df = pd.read_parquet(io.BytesIO(raw))
    df["_source_vm"] = vm_name
    logger.info("Loaded per-VM shard: %d rows from gs://%s/%s", len(df), bucket, blob)
    return df


def _load_merged_index(bucket: str) -> pd.DataFrame:
    """Read the merged availability index — the authoritative, consolidated source.

    Per-VM shards (``_index/per_vm/{instance}.parquet``) are transient write-side
    artifacts: the manifest consolidator periodically merges them into
    ``_index/availability_index.parquet`` and removes the shard. Only 1 of the 3
    relaunched VMs' per-VM shards was still present when this audit ran (the other
    2 had already been consolidated away) — so the merged index, not the per-VM
    shards, is the authoritative source for this audit. The merged index has no
    per-row VM/run identifier column (only ``attempted_at`` / ``written_at``), but
    the exact-match ``error_reason`` string is itself bug-specific and sufficient
    to identify affected rows regardless of which VM wrote them.
    """
    client = get_storage_client(provider="gcp")
    raw = client.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    df["_source_vm"] = "merged-index (post-consolidation)"
    logger.info("Loaded merged manifest: %d rows from gs://%s/%s", len(df), bucket, INDEX_BLOB)
    return df


def _load_all_shards(bucket: str, vm_names: tuple[str, ...]) -> pd.DataFrame:
    """Load whichever per-VM shards still exist (informational attribution/cross-check),
    plus the merged index (authoritative — see ``_load_merged_index``).
    """
    shard_frames = [df for vm in vm_names if (df := _load_per_vm_shard(bucket, vm)) is not None]
    merged = _load_merged_index(bucket)
    if shard_frames:
        logger.info(
            "%d of %d relaunched VMs still have a live per-VM shard (rest already "
            "consolidated into the merged index) — merged index remains the audit source.",
            len(shard_frames),
            len(vm_names),
        )
    return merged


def _identify_race_rows(df: pd.DataFrame) -> pd.Index:
    """Return index of attempted_failed rows tagged with the exact race error_reason."""
    if "capture_status" not in df.columns:
        logger.error("Manifest missing 'capture_status' column")
        return df.index[:0]
    if "error_reason" not in df.columns:
        logger.warning("Manifest missing 'error_reason' column — no rows qualify")
        return df.index[:0]

    status = df["capture_status"].fillna("").astype(str)
    failed_mask = status == "attempted_failed"

    error_reason = df["error_reason"].fillna("").astype(str)
    race_mask = error_reason == RACE_ERROR_REASON

    combined = failed_mask & race_mask
    idx = df[combined].index
    logger.info(
        "Race-caused false-failure rows: %d of %d attempted_failed total (%d rows scanned)",
        len(idx),
        int(failed_mask.sum()),
        len(df),
    )
    return idx


def _report_distribution(label: str, sub: pd.DataFrame) -> None:
    logger.info("=== %s: %d rows ===", label, len(sub))
    if len(sub) == 0:
        return
    if "venue" in sub.columns:
        by_venue = sub["venue"].fillna("").value_counts().head(15)
        logger.info("%s — by venue (top 15):\n%s", label, by_venue.to_string())
    if "data_type" in sub.columns:
        by_dt = sub["data_type"].fillna("").value_counts().head(15)
        logger.info("%s — by data_type (top 15):\n%s", label, by_dt.to_string())
    if "date" in sub.columns:
        by_date = sub["date"].fillna("").value_counts().head(15)
        logger.info("%s — by date (top 15):\n%s", label, by_date.to_string())


def _split_session_vs_corpus(df: pd.DataFrame, idx: pd.Index) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split race rows into the session-scoped window vs the full corpus-wide set."""
    sub = df.loc[idx]
    if "attempted_at" not in sub.columns:
        logger.warning("Manifest missing 'attempted_at' — cannot compute session-window scope")
        return sub.iloc[:0], sub
    ts = pd.to_datetime(sub["attempted_at"], utc=True, errors="coerce")
    session_mask = (ts >= SESSION_WINDOW_START) & (ts <= SESSION_WINDOW_END)
    return sub[session_mask], sub


def _validate_apply_env(confirm_lease_fix_shipped: bool) -> bool:
    per_vm = os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower()  # noqa: qg-empty-fallback — unset env => fails below
    vm_name = os.environ.get("VM_NAME", "").strip()  # noqa: qg-empty-fallback — unset env => `not vm_name` fails below
    ok = True
    if per_vm not in ("1", "true", "yes"):
        logger.error("--apply requires MANIFEST_PER_VM_SHARDS=true. Aborting.")
        ok = False
    if not vm_name:
        logger.error("--apply requires VM_NAME=<unique-tag>. Aborting.")
        ok = False
    if not confirm_lease_fix_shipped:
        logger.error(
            "--apply requires --confirm-lease-fix-shipped: flipping these rows to "
            "expected_unattempted re-queues them for re-fetch. Until "
            "tardis_concurrency_lease_intra_process_race-001 (the lease race fix) has "
            "shipped + its regression test passed, re-fetching will just reproduce the "
            "same 403s. Aborting."
        )
        ok = False
    return ok


def _flip_to_expected_unattempted(bucket: str, df: pd.DataFrame, idx: pd.Index) -> None:
    """Mutate race rows to expected_unattempted and write the merged manifest back.

    NOTE: this writes the MERGED index (not per-VM shards) since the flip target
    is the read path every future backfill consults; per-VM shards are append-only
    write-side artifacts, not authoritative for re-queue decisions.
    """
    captured_before = int((df["capture_status"].fillna("").astype(str) == "captured").sum())

    df.loc[idx, "capture_status"] = "expected_unattempted"
    df.loc[idx, "error_reason"] = ""

    captured_after = int((df["capture_status"].fillna("").astype(str) == "captured").sum())
    if captured_after != captured_before:
        logger.error(
            "SAFETY GATE FAILED: captured count changed %d → %d — aborting write",
            captured_before,
            captured_after,
        )
        raise RuntimeError(f"captured count changed {captured_before} → {captured_after} after flip — BUG")

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)

    client = get_storage_client(provider="gcp")
    client.upload_from_file_obj(bucket, INDEX_BLOB, out)
    logger.info(
        "Uploaded manifest: %d rows, %d race rows flipped to expected_unattempted (captured count preserved at %d)",
        len(df),
        len(idx),
        captured_after,
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help=(
            "Flip race-caused attempted_failed rows to expected_unattempted. "
            "Requires MANIFEST_PER_VM_SHARDS=true + VM_NAME=<unique> env vars "
            "AND --confirm-lease-fix-shipped."
        ),
    )
    p.add_argument(
        "--confirm-lease-fix-shipped",
        action="store_true",
        default=False,
        help=(
            "Required alongside --apply: confirms "
            "tardis_concurrency_lease_intra_process_race-001 (the lease race fix) "
            "has shipped and its regression test passed."
        ),
    )
    args = p.parse_args()

    dry_run = not args.apply

    if not dry_run and not _validate_apply_env(args.confirm_lease_fix_shipped):
        return 4

    bucket = _get_cefi_prd_bucket()
    logger.info("CeFi PRD manifest bucket: gs://%s", bucket)

    df = _load_all_shards(bucket, RELAUNCHED_VMS)
    race_idx = _identify_race_rows(df)

    if len(race_idx) == 0:
        logger.info("No race-caused attempted_failed rows found. Nothing to do.")
        return 0

    session_sub, corpus_sub = _split_session_vs_corpus(df, race_idx)
    _report_distribution("SESSION-SCOPED (2026-07-15T20:20-20:39Z, the 3 relaunched VMs)", session_sub)
    _report_distribution("CORPUS-WIDE (all-time, every cefi VM launch)", corpus_sub)
    if len(corpus_sub) > len(session_sub):
        logger.warning(
            "Corpus-wide race rows (%d) exceed the session-scoped window (%d) — this bug has "
            "affected VM launches BEYOND this session's 3 relaunched VMs. See the parent issue doc "
            "for the corpus-wide-scope addendum.",
            len(corpus_sub),
            len(session_sub),
        )

    if dry_run:
        logger.info(
            "DRY-RUN: %d race-caused rows would be flipped attempted_failed→expected_unattempted. "
            "Re-run with --apply --confirm-lease-fix-shipped (+ MANIFEST_PER_VM_SHARDS=true VM_NAME=...) "
            "ONLY after tardis_concurrency_lease_intra_process_race-001 has shipped.",
            len(race_idx),
        )
        return 0

    logger.info("APPLY: flipping %d race-caused rows to expected_unattempted...", len(race_idx))
    _flip_to_expected_unattempted(bucket, df, race_idx)
    logger.info("Done. Next backfill VM will re-attempt these %d shards.", len(race_idx))
    return 0


if __name__ == "__main__":
    sys.exit(main())
