# Epic: sports_master
# Lifecycle: ONE-OFF — one-time reconcile that DELETES the ~362 residual
#   blank-``league_id`` ``LEAGUE_MAP_INCOMPLETE`` (``attempted_failed``)
#   api_football rows from the sports availability index, now that the code
#   no longer mints new ones (instruments-service@a66fc295). Same treatment as
#   scripts/delete_noncanonical_sports_leagues_2026_06_25.py's purge — these
#   are out-of-universe (non-canonical league) fixture artifacts with no
#   league to attribute and an unsupersedable blank-league row_key.
# Delete-when: manifest shows 0 blank-``league_id`` ``LEAGUE_MAP_INCOMPLETE``
#   ``attempted_failed`` rows for source=api_football (run confirms 0 remain).
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""api_football_league_map_incomplete_orphan_purge_2026_07_16.py — one-time
DELETE reconcile of the residual blank-``league_id`` ``LEAGUE_MAP_INCOMPLETE``
``attempted_failed`` rows for source=api_football.

CONTEXT (see the plan's 2026-07-15 Progress Log + the completed sibling todo
``instruments-service@a66fc295``): a completed api_football fixture in a
NON-canonical (out-of-universe) league gets picked up by the per-fixture
enrichment target (``api_football_reference.get_instruments`` calls
``get_fixtures(date=...)`` with NO ``league_ids`` filter → the whole ~1000+
league universe) but can never map to a canonical league via
``af_fid_to_league`` (built only from the curated 94-league set). Before
``a66fc295`` those unmapped per-fixture rows were written as
``record_failed(LEAGUE_MAP_INCOMPLETE)`` with a BLANK ``league_id``. The fix
(``sports_reference_fixtures.py``) now skips them silently (mirroring the
line-578 out-of-universe ``continue``), so NO new ones are minted — but the
historical rows remain.

WHY DELETE (not reclassify-to-empty): these rows carry a blank ``league_id``
and represent leagues that were PURGED from the canonical universe
(``delete_noncanonical_sports_leagues_2026_06_25.py`` removed 1,438
non-canonical leagues / 1,283,171 index rows). There is no canonical league to
attribute them to, they can NEVER be superseded by any current success path
(``record_captured`` / ``record_expected_empty`` always key on a real
canonical ``league_id``), and honest-absence for any GENUINE in-universe gap is
preserved on the FIXTURES shard, not here. The sibling reconcile
``api_football_blank_league_orphan_reconcile_2026_07_15.py`` only retires a
blank orphan to ``empty_confirmed`` when REAL per-league coverage exists for
that (date, data_type) — which never happens for out-of-universe rows — so it
deliberately leaves these untouched. This script closes them the same way the
non-canonical-league purge did: remove the rows from the canonical index.

MECHANISM (consolidator-safe, no whole-corpus GCS walk): the delete predicate
is a cheap boolean mask over the canonical ``_index/availability_index.parquet``
(``source==api_football`` & ``capture_status==attempted_failed`` & blank
``league_id`` & ``error_reason==LEAGUE_MAP_INCOMPLETE``). In --apply mode the
script snapshots the canonical blob, re-reads FRESH via
``merge_canonical_with_outstanding_shards`` immediately before the write-back
(staleness guard — never clobber a per-VM shard written during the read→write
window), re-derives the mask, asserts the deleted set is EXACTLY the predicate
(all attempted_failed / blank-league / LEAGUE_MAP_INCOMPLETE / api_football)
and that ``captured`` + ``empty_confirmed`` counts are UNCHANGED and
``attempted_failed`` drops by exactly the deleted count, then uploads the
trimmed index. Per-VM shard FILES are left as orphans (idempotent — the
predicate no longer matches anything the fixed code writes; a future
``reconcile_manifest`` sweep can clean stragglers). Dry-run by default.

Usage:
  api_football_league_map_incomplete_orphan_purge_2026_07_16.py [--apply]
Writes to REAL prod GCS (instruments-store-sports-prd-<project>) only with
--apply.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("api_football_league_map_incomplete_orphan_purge")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--apply", action="store_true", help="Write the deletion (default: dry-run — scan + report only).")
    return p.parse_args()


ARGS = _args()

import os

os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ.pop("CLOUD_MOCK_MODE", None)

from unified_trading_library import setup_events

setup_events("instruments-service", "local")

from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import (
    StorageClient,
    get_storage_client,
    merge_canonical_with_outstanding_shards,
    resolve_bucket_name,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")
INDEX_BLOB = "_index/availability_index.parquet"  # canonical availability index (SSOT path)
SNAPSHOT_PREFIX = "_index/snapshots"
PER_VM_PREFIX = "_index/per_vm/"
LEGACY_SEED_BLOB = "_index/per_vm/_legacy_seed.parquet"  # frozen seed — never rewritten by this script
SOURCE = "api_football"
ERROR_REASON = "LEAGUE_MAP_INCOMPLETE"
DOCUMENTED_COUNT = 362  # plan's 2026-07-15 measurement — informational, not an assertion


def _is_blank_league(series: pd.Series) -> pd.Series:
    """True where league_id is None/NaN or a blank-ish string sentinel."""
    as_str = series.fillna("").astype(str).str.strip()
    return series.isna() | as_str.isin(["", "none", "None", "nan", "NaN", "<NA>"])


def _orphan_mask(df: pd.DataFrame) -> pd.Series:
    """SSOT predicate for the out-of-universe blank-league LEAGUE_MAP_INCOMPLETE orphans."""
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    status = df.get("capture_status", pd.Series("", index=df.index)).astype("string").fillna("")
    reason = df.get("error_reason", pd.Series("", index=df.index)).astype("string").fillna("")
    is_target_src = src == SOURCE
    is_failed = status == "attempted_failed"
    is_reason = reason == ERROR_REASON
    is_blank = _is_blank_league(df["league_id"]) if "league_id" in df.columns else pd.Series(False, index=df.index)
    return is_target_src & is_failed & is_reason & is_blank


def _drain_outstanding_shards(storage_client: StorageClient) -> int:
    """Rewrite any outstanding per-VM shard that carries orphan rows, minus those rows.

    The consolidator merges outstanding shards INTO the canonical; if a shard
    still holds an orphan, purging only the canonical lets it resurrect on the
    next consolidation. So drain the shards too. The frozen ``_legacy_seed`` is
    never touched. Safe from concurrency: the daily ``sports-fixtures-job`` runs
    T+1 at ~00:21 UTC and is idle the rest of the day — verify the clock before
    a wider re-use. Returns total rows drained across shards.
    """
    try:
        shard_paths = [
            p if isinstance(p, str) else str(p) for p in storage_client.list_blobs(BUCKET, prefix=PER_VM_PREFIX)
        ]
    except (OSError, ValueError) as exc:
        log.warning("Could not list per-VM shards (%s) — skipping shard drain.", exc)
        return 0

    total_drained = 0
    for path in shard_paths:
        blob = path.split(f"{BUCKET}/", 1)[-1] if path.startswith(f"{BUCKET}/") else path
        if not blob.endswith(".parquet") or blob == LEGACY_SEED_BLOB:
            continue
        try:
            data = storage_client.download_bytes(BUCKET, blob)
            sdf = pd.read_parquet(io.BytesIO(data))
        except (OSError, ValueError) as exc:
            log.warning("  shard %s: read failed (%s) — skipping.", blob, exc)
            continue
        if "league_id" not in sdf.columns or "capture_status" not in sdf.columns:
            continue
        smask = _orphan_mask(sdf)
        n = int(smask.sum())
        if n == 0:
            continue
        # Guard — only orphan rows are removed; everything else preserved.
        kept = sdf[~smask].copy()
        if len(kept) != len(sdf) - n:
            raise RuntimeError(f"REFUSING SHARD WRITE {blob} — row math off ({len(sdf)} - {n} != {len(kept)})")
        out = io.BytesIO()
        kept.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
        out.seek(0)
        log.info("  shard %s: draining %d orphan rows (%d -> %d).", blob, n, len(sdf), len(kept))
        storage_client.upload_from_file_obj(BUCKET, blob, out)
        total_drained += n
    return total_drained


def _report(df: pd.DataFrame, mask: pd.Series) -> None:
    orphans = df[mask]
    log.info("Matched %d orphan rows (plan documented ~%d).", len(orphans), DOCUMENTED_COUNT)
    if orphans.empty:
        return
    log.info("  by data_type:\n%s", orphans["data_type"].astype(str).str.upper().value_counts().to_string())
    dmin = str(orphans["date"].min())
    dmax = str(orphans["date"].max())
    log.info("  date span: %s .. %s", dmin, dmax)


def main() -> int:
    storage_client: StorageClient = get_storage_client()

    df = merge_canonical_with_outstanding_shards(storage_client, BUCKET, INDEX_BLOB)
    if df.empty:
        log.error("Canonical index is empty/missing at %s/%s — nothing to do.", BUCKET, INDEX_BLOB)
        return 1
    log.info("Read fresh index: %d rows (canonical + outstanding per-VM shards).", len(df))

    mask = _orphan_mask(df)
    _report(df, mask)
    n_match = int(mask.sum())

    if n_match == 0:
        log.info("Nothing to delete — 0 orphan rows match the predicate. Already clean.")
        return 0

    if not ARGS.apply:
        log.info("DRY RUN — no writes performed. Re-run with --apply to delete these %d rows.", n_match)
        return 0

    # --- APPLY ---
    # 1) snapshot the current canonical before any write (destructive-op safety).
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    snap_blob = f"{SNAPSHOT_PREFIX}/pre_league_map_incomplete_purge_{ts}/availability_index.parquet"
    log.info("Snapshotting canonical %s -> %s", INDEX_BLOB, snap_blob)
    storage_client.copy_blob(BUCKET, INDEX_BLOB, BUCKET, snap_blob)
    log.info("Snapshot done.")

    # 1b) drain outstanding per-VM shards of orphan rows FIRST — else the
    #     consolidator re-merges them into the canonical after this purge.
    n_shard_drained = _drain_outstanding_shards(storage_client)
    log.info("Drained %d orphan rows from outstanding per-VM shards.", n_shard_drained)

    # 2) re-read FRESH immediately before the write-back (staleness guard — do
    #    not trust the earlier read; a per-VM shard may have landed since). Now
    #    reflects the just-drained shards.
    df = merge_canonical_with_outstanding_shards(storage_client, BUCKET, INDEX_BLOB)
    status_all = df["capture_status"].fillna("").astype(str)
    captured_before = int((status_all == "captured").sum())
    empty_before = int((status_all == "empty_confirmed").sum())
    failed_before = int((status_all == "attempted_failed").sum())
    total_before = len(df)

    mask = _orphan_mask(df)
    n_delete = int(mask.sum())
    if n_delete == 0:
        log.info("Nothing to delete after fresh re-check — already clean. No write.")
        return 0

    # 3) defensive guard — every deleted row must satisfy the FULL predicate.
    deleting = df[mask]
    d_status = deleting["capture_status"].fillna("").astype(str)
    d_src = deleting.get("source", pd.Series("", index=deleting.index)).fillna("").astype(str)
    d_reason = deleting.get("error_reason", pd.Series("", index=deleting.index)).fillna("").astype(str)
    bad = int(((d_status != "attempted_failed") | (d_src != SOURCE) | (d_reason != ERROR_REASON)).sum()) + int(
        (~_is_blank_league(deleting["league_id"])).sum()
    )
    if bad:
        raise RuntimeError(
            f"REFUSING DELETE — predicate selected {bad} rows outside the "
            "(api_football / attempted_failed / blank-league / LEAGUE_MAP_INCOMPLETE) set"
        )

    kept = df[~mask].copy()
    status_after = kept["capture_status"].fillna("").astype(str)
    captured_after = int((status_after == "captured").sum())
    empty_after = int((status_after == "empty_confirmed").sum())
    failed_after = int((status_after == "attempted_failed").sum())

    if captured_after != captured_before:
        raise RuntimeError(f"REFUSING WRITE — captured changed {captured_before} -> {captured_after} (delete bug)")
    if empty_after != empty_before:
        raise RuntimeError(f"REFUSING WRITE — empty_confirmed changed {empty_before} -> {empty_after} (delete bug)")
    if failed_after != failed_before - n_delete:
        raise RuntimeError(
            f"REFUSING WRITE — attempted_failed changed {failed_before} -> {failed_after} "
            f"(expected {failed_before - n_delete}; delete bug)"
        )
    if len(kept) != total_before - n_delete:
        raise RuntimeError(
            f"REFUSING WRITE — total rows {total_before} -> {len(kept)} (expected {total_before - n_delete})"
        )

    out = io.BytesIO()
    kept.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    out.seek(0)
    log.info(
        "Writing trimmed index: %d -> %d rows (%d deleted; captured=%d empty_confirmed=%d unchanged; "
        "attempted_failed %d -> %d).",
        total_before,
        len(kept),
        n_delete,
        captured_after,
        empty_after,
        failed_before,
        failed_after,
    )
    storage_client.upload_from_file_obj(BUCKET, INDEX_BLOB, out)
    log.info("Upload done.")

    # 4) post-write verify — re-read fresh and confirm 0 orphans remain.
    verify = merge_canonical_with_outstanding_shards(storage_client, BUCKET, INDEX_BLOB)
    remaining = int(_orphan_mask(verify).sum())
    log.info("=== POST-PURGE VERIFY: %d blank-league LEAGUE_MAP_INCOMPLETE attempted_failed rows remain ===", remaining)
    if remaining != 0:
        log.warning(
            "Non-zero remaining after purge — likely an outstanding per-VM shard re-merged rows; "
            "investigate (do NOT blindly re-run)."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
