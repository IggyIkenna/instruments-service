#!/usr/bin/env python3
# Epic: cefi_master
# Lifecycle: oneoff
# Delete-when: after the CeFi canonical-completeness Phase-1 eu-twin drop is verified
#              live in the cefi -prd main _index (residual-#3 gate GREEN, re-run dry-run
#              shows 0 further drops) and the drain window is closed.
"""D4 SCRIPT 4 — drop stale expected_unattempted twins for native-canonical CeFi venues.

Provenance: plans/active/issues/_cefi_canonical_blueprint_2026_07_17.md § 3 "SCRIPT 4 —
eu-twin drop, native-canonical on-chain lane (instruments-service)". Derivative of
``relabel_cefi_tardis_raw_symbol_to_canonical_2026_07_15.py``'s eu-reconcile pass.

THE DEFECT. For the native-canonical venues (EXTENDED-STARKNET, PACIFICA-SOLANA, and the
handful of others whose captured rows are ALREADY written under the canonical
``instrument_id``), the Layer-1 Tardis enumerator materialised an
``expected_unattempted`` skeleton (``pipeline_mode=batch_tardis``) predicting shards the
native on-chain lane then actually CAPTURED under a different ``pipeline_mode``
(e.g. ``batch_extended``). Because the two rows carry the SAME canonical
``instrument_id`` but a different ``pipeline_mode``, the eu row is a stale twin of a real
``captured`` row — an "expected but not attempted" claim that is contradicted by a
capture that did happen.

THE FIX. Drop every ``expected_unattempted`` row in the MAIN index whose 5-col shard key
``(date, venue, data_type, instrument_type, instrument_id)`` EXACTLY matches a
``captured`` row's key. ``pipeline_mode`` is deliberately EXCLUDED from the join key —
that is the whole point: the eu skeleton and its captured twin differ only by
``pipeline_mode`` (which lane fulfilled the shard). The exact-match join is inherently
SELF-SCOPING to the native-canonical venues: for CEX venues the eu skeleton and captured
rows use different symbol spellings (raw vs canonical), so their keys do not match and
nothing is dropped — measured live 2026-07-17 the only hits are EXTENDED-STARKNET plus a
few dozen exact twins on other venues, never the CEX bulk. This complements SCRIPT 3,
whose retained eu-reconcile handles eu twins of RELABELED (CEX raw->canonical) captured
rows; SCRIPT 4 handles eu twins of NATIVE-canonical (never-relabeled) captured rows.

EXACT-MATCH ONLY — never a fuzzy/prefix join. Under-reconcile is the safe direction: a
genuine gap is only ever left in place, never a real "we didn't attempt this" honestly
dropped.

MAIN index only: the ``expected_unattempted`` skeleton lives only in
``_index/availability_index.parquet`` (per-VM shards carry no eu skeleton).

SNAPSHOT-FIRST: the main index gets a pre-write copy under ``_index/snapshots/pre_d4_<ts>/``
before any write. Default mode is DRY-RUN (read-only); ``--apply`` mutates. Idempotent
(re-run after apply -> 0 drops).

STOP-ON-SURPRISE: total eu-twin drops must land in ``[_EXPECTED_MIN, _EXPECTED_MAX]``
(~8k-15k; measured live 2026-07-17 ~9.8k, dominated by EXTENDED-STARKNET). Halt outside.

Usage::

    cd instruments-service

    # dry-run (default, read-only) — prints per-venue eu-twin drop counts
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/drop_cefi_eu_twins_native_canonical_2026_07_17.py

    # apply (snapshot + drop + verify) — DRAIN-GATED
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV=prod \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/drop_cefi_eu_twins_native_canonical_2026_07_17.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"

# eu-twin join key — 5-col, EXCLUDING pipeline_mode by design (the eu skeleton and its
# captured twin differ only by pipeline_mode). Identical to the fork's SHARD_KEY_COLS.
TWIN_KEY_COLS = ["date", "venue", "data_type", "instrument_type", "instrument_id"]

_CAPTURED = "captured"
_EU = "expected_unattempted"

# STOP-ON-SURPRISE: measured live 2026-07-17 = ~9.8k (EXTENDED-STARKNET dominant). Bound
# generously; halt outside.
_EXPECTED_MIN = 8_000
_EXPECTED_MAX = 15_000


def _load(storage: object, bucket: str, blob: str) -> pd.DataFrame:
    raw = storage.download_bytes(bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
    return pd.read_parquet(io.BytesIO(raw))


def _write(storage: object, bucket: str, blob: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, blob, buf.getvalue())  # pyright: ignore[reportAttributeAccessIssue]


def _snapshot(storage: object, bucket: str, blob: str, run_ts: str) -> str:
    label = blob.rsplit("/", 1)[-1].removesuffix(".parquet")
    snap_blob = f"_index/snapshots/pre_d4_{run_ts}/{label}.parquet"
    logger.info("Snapshotting gs://%s/%s -> gs://%s/%s", bucket, blob, bucket, snap_blob)
    _write(storage, bucket, snap_blob, _load(storage, bucket, blob))
    return snap_blob


def _find_eu_twins(df: pd.DataFrame) -> tuple[list[int], pd.Series]:
    """Return (drop_indices, per_venue_counts) for eu rows exact-matching a captured key."""
    required = {"capture_status", *TWIN_KEY_COLS}
    if not required.issubset(df.columns):
        return [], pd.Series(dtype=int)

    captured_keys = df.loc[df["capture_status"] == _CAPTURED, TWIN_KEY_COLS].drop_duplicates().assign(_hit=True)
    eu = df.loc[df["capture_status"] == _EU, TWIN_KEY_COLS].reset_index().rename(columns={"index": "_orig_index"})
    if eu.empty or captured_keys.empty:
        return [], pd.Series(dtype=int)

    merged = eu.merge(captured_keys, on=TWIN_KEY_COLS, how="left")
    hit = merged["_hit"].astype("boolean").fillna(False)
    drop_idx = merged.loc[hit, "_orig_index"].tolist()
    per_venue = merged.loc[hit, "venue"].value_counts()
    return drop_idx, per_venue


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Drop the eu twins (snapshot first). Default: dry-run.")
    p.add_argument("--bucket", default=None, help="Override the cefi market-data bucket.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    storage = get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    df = _load(storage, bucket, INDEX_BLOB)
    logger.info("Loaded main index gs://%s/%s: %d rows.", bucket, INDEX_BLOB, len(df))

    drop_idx, per_venue = _find_eu_twins(df)
    total = len(drop_idx)

    logger.info("=" * 78)
    logger.info("eu-twin drops (expected_unattempted with an exact captured twin): %d", total)
    if total:
        logger.info("per-venue breakdown:")
        for venue, count in per_venue.items():
            logger.info("  %-22s : %d", str(venue), int(count))
    logger.info("=" * 78)

    if not (_EXPECTED_MIN <= total <= _EXPECTED_MAX):
        logger.error(
            "STOP-ON-SURPRISE: eu-twin drop count %d outside band [%d, %d]. Diagnose before --apply.",
            total,
            _EXPECTED_MIN,
            _EXPECTED_MAX,
        )
        return 1

    if not args.apply:
        logger.info("DRY-RUN (default) — no write. --apply would drop %d eu twins (snapshot-first).", total)
        return 0

    if total == 0:
        logger.info("Nothing to drop.")
        return 0

    _snapshot(storage, bucket, INDEX_BLOB, run_ts)
    out = df.drop(index=drop_idx)
    _write(storage, bucket, INDEX_BLOB, out)
    logger.info("gs://%s/%s: written (%d rows, was %d).", bucket, INDEX_BLOB, len(out), len(df))

    # Verify idempotency: re-scan the written frame -> 0 residual twins.
    residual, _ = _find_eu_twins(out)
    if residual:
        logger.error("GATE FAIL: %d eu twins still present after drop.", len(residual))
        return 1
    logger.info("GATE PASSED: 0 residual eu twins. APPLY COMPLETE: %d rows dropped.", total)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
