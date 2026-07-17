#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: prod prediction `prod/catalog.parquet` carries a populated `question` column
#   on its legacy-covered POLYMARKET rows AND a re-run of this script in --dry-run reports
#   `rows_question_would_change=0` (idempotent no-op — the history half of the forward-only
#   InstrumentRecord.question ship, uac@c1de078a / instruments-service@2257a067, is done).
# pyright: reportAny=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false, reportUnknownParameterType=false, reportArgumentType=false
# pyright: reportAttributeAccessIssue=false
# (one-shot migration over the untyped pandas / StorageClient row surface — same pragma set as
#  the sibling backfill_spot_asset_population_2026_07_16.py / canonicalize_*_2026_07_*.py tools.)
"""backfill_prediction_question_2026_07_17.py — in-place, lossless ``question`` backfill.

Adds the human-readable market ``question`` column to the PREDICTION identity catalogue
(``prod/catalog.parquet`` in bucket ``instruments-store-pred-{env}-{project}``) by re-reading
data ALREADY AT REST on GCS. **No re-capture, no adapter call, no venue API fetch** — the
forward-only code half (``InstrumentRecord.question`` uac@c1de078a, the sourcing adapters
instruments-service@2257a067, the rollup emitting it, deployment-api@2f52991e) is shipped and
new captures carry ``question``; this is the HISTORY half for the ~2.67M rows written before it.

WHY A RE-READ, NOT A RE-CAPTURE (the fatal trap this design avoids): re-fetching from
Polymarket/Kalshi returns LESS than what is stored (the venue prunes resolved markets) and
would overwrite real data with nothing + inject look-ahead bias. The human ``question`` text is
already on disk in the legacy Gamma-payload objects — see docs/PREDICTION_INSTRUMENTS.md
§ "Prediction titles — a REGRESSION, not an eternal gap".

LIVE-VERIFIED DATA MODEL (measured 2026-07-17 against prod; ALWAYS re-verify at run time):
  * ``prod/catalog.parquet``: 2,673,230 rows, 24 columns, NO ``question`` column yet.
    - data_type: trades 1,336,559 / market_lifecycle 1,336,559 / prediction_canonical_question_group 112.
    - venue: POLYMARKET 2,590,757 / KALSHI 82,473.
    - POLYMARKET per-market ``instrument_id`` is 1,235,918 BARE ``0x…64hex`` (the raw condition_id)
      + 59,424 WRAPPED ``POLYMARKET:PREDICTION_MARKET:0x…`` (post-2026-07-09 id-wrap) + 73 other-form.
  * The ``question`` text lives ONLY in the legacy Shape-B ``instruments.parquet`` objects that
    carry the raw ~47-column Gamma payload (columns ``id / condition_id / question / …``). Their
    market key is the RAW ``condition_id`` (bare ``0x…``) — they have NO ``instrument_key`` /
    ``instrument_id`` column, so the catalogue rollup never derived a row FROM them; the catalogue
    rows come from the sibling IS-normalised (30/41-col, ``instrument_key`` = bare condition_id)
    and Shape-A (52-col, WRAPPED instrument_key) objects — NEITHER of which carries ``question``
    (the 36→22 field reduction dropped it), and neither does ``prediction_market_metadata.parquet``.
  * Legacy Shape-B is POLYMARKET-ONLY. There is NO ``question`` source at rest for KALSHI, so Kalshi
    rows stay honestly NULL (never fabricated).

JOIN KEY — VERIFIED AGAINST REAL DATA (not assumed):
  bare ``condition_id`` (from a 47-col legacy object) == catalogue POLYMARKET ``instrument_id``
  for a BARE row, and == the ``0x…`` suffix of a WRAPPED row. On a 13,187-cid live sample, 85.0%
  of legacy condition_ids resolved to a real catalogue POLYMARKET instrument_id; the ~15% that
  do not are markets present only in the raw-Gamma objects and never normalised into the
  catalogue — their questions have no row to attach to and are simply unused (honest, never
  fabricated). Both the ``trades`` and ``market_lifecycle`` rows of a matched market share the
  same ``instrument_id`` and so both gain the question in one left-join.

MOST-RECENT-WINS: where the same condition_id carries a question in several dated objects, the
latest ``day=`` wins (longest non-blank on a tie); a blank NEVER overwrites a non-blank — mirrors
the rollup's own ``_merge_lifecycle`` metadata convention.

ADDITIVE, LOSSLESS PATCH (the acceptance contract): the ``question`` column is ADDED; every other
column and every row is preserved EXACTLY. Row count MUST stay 2,673,230; every pre-existing
column's per-column checksum MUST be byte-identical before/after; NO date column
(``available_from`` / ``available_to`` / ``market_created_at`` / ``settlement_time``) is touched
(the look-ahead-bias hazard). Idempotent: a re-run recomputes an identical ``question`` column.

GATES (refuse to --apply if violated):
  * row count unchanged (== rows_before).
  * every pre-existing column checksum unchanged (no silent mutation).
  * ``question`` non-null count > 0 (the backfill actually did something) AND
    <= rows_with_a_legacy_match (never more than we have evidence for — no fabrication).

Usage (run from the instruments-service repo root)::

    python scripts/backfill_prediction_question_2026_07_17.py                 # dry-run (default)
    python scripts/backfill_prediction_question_2026_07_17.py --workers 48
    python scripts/backfill_prediction_question_2026_07_17.py --apply --confirm
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Mapping

    from unified_trading_library import StorageClient

logger = logging.getLogger(__name__)

_CATALOG_BLOB = "prod/catalog.parquet"
_BY_DATE_PREFIX = "instrument_availability/by_date/"
#: The 2026-07-09 id-wrap prefix on a POLYMARKET per-market instrument_id
#: (build_canonical_instrument_id(PREDICTION, POLYMARKET, PREDICTION_MARKET, condition_id)).
_WRAP_PREFIX = "POLYMARKET:PREDICTION_MARKET:"
_POLYMARKET = "POLYMARKET"

#: Shape-A (cqg-partitioned) objects carry a `canonical_question_group=` path segment and the
#: newer 52-col schema WITHOUT `question`; only the legacy Shape-B objects carry the raw payload.
_CQG_RE = re.compile(r"(?:^|/)canonical_question_group=")
_DAY_RE = re.compile(r"(?:^|/)day=(\d{4}-\d{2}-\d{2})(?:/|$)")

#: Column that holds the market id in the raw Gamma-payload legacy objects, and the text column.
_RAW_ID_COL = "condition_id"
_QUESTION_COL = "question"


def _bucket() -> str:
    from unified_trading_library import resolve_bucket_name

    return resolve_bucket_name(cloud="gcp", kind="instruments-store-prediction")


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def match_key(instrument_id: str, venue: str) -> str | None:
    """Resolve a catalogue row to the bare ``condition_id`` used to key the question map.

    POLYMARKET only: a BARE ``0x…`` id maps to itself; a WRAPPED
    ``POLYMARKET:PREDICTION_MARKET:0x…`` id maps to its ``0x…`` suffix (the same real market).
    Any other venue / any other id shape returns ``None`` (no question source at rest) — a Kalshi
    ticker can never collide with a ``0x…`` condition_id, so this also guards cross-venue leakage.
    """
    if venue != _POLYMARKET:
        return None
    iid = instrument_id.strip()
    if iid.startswith(_WRAP_PREFIX):
        return iid[len(_WRAP_PREFIX) :]
    if iid.startswith("0x"):
        return iid
    return None


def _day_of(path: str) -> str:
    """Parse the ``day=YYYY-MM-DD`` partition from an object path ("" when absent)."""
    m = _DAY_RE.search(path)
    return m.group(1) if m else ""


def list_legacy_objects(storage: StorageClient, bucket: str) -> list[str]:
    """List the KNOWN legacy Shape-B ``instruments.parquet`` objects (bounded, single walk).

    Not a fresh whole-corpus discovery walk over an unbounded prefix: it is the one bounded
    listing of the prediction ``by_date/`` tree the rollup itself walks, filtered to the legacy
    (non-``canonical_question_group=``) ``instruments.parquet`` objects — the only shape that can
    carry the raw ``question`` payload (``prediction_market_metadata.parquet`` and the Shape-A
    52-col objects were live-verified 2026-07-17 to lack the column, so they are skipped).
    """
    names: list[str] = []
    for blob in storage.list_blobs(bucket, prefix=_BY_DATE_PREFIX):
        name = str(getattr(blob, "name", ""))
        if not name.endswith("instruments.parquet"):
            continue
        if _CQG_RE.search(name):
            continue
        names.append(name)
    return names


def _read_object_questions(storage: StorageClient, bucket: str, path: str) -> list[tuple[str, str]]:
    """Read ONE legacy object → ``[(condition_id, question), …]``; ``[]`` when unusable.

    Per-object isolation: any exception is caught and logged, never raised (mirrors the
    shard-level-failure-isolation convention used across this service's writers and the precedent
    migration scripts). An object lacking the raw ``condition_id`` / ``question`` columns (the
    30/41-col IS-normalised family) contributes nothing — it is not the question carrier.
    """
    try:
        raw = storage.download_bytes(bucket, path)
    except Exception:  # broad-except-ok — per-object isolation; a transient read failure is skipped
        logger.warning("legacy object read failed path=%s — skipped", path)
        return []
    try:
        df = pd.read_parquet(io.BytesIO(raw), columns=[_RAW_ID_COL, _QUESTION_COL])
    except (ValueError, KeyError):
        # Not the raw-Gamma family (no condition_id/question columns) — nothing to contribute.
        return []
    except Exception as exc:  # broad-except-ok — per-object isolation
        logger.warning("legacy object parse failed path=%s: %s: %s", path, type(exc).__name__, exc)
        return []
    if df.empty:
        return []
    cid = df[_RAW_ID_COL].astype("string")
    q = df[_QUESTION_COL].astype("string")
    out: list[tuple[str, str]] = []
    for c, text in zip(cid.tolist(), q.tolist(), strict=True):
        if c is None or text is None:
            continue
        c_s = str(c).strip()
        t_s = str(text).strip()
        if not c_s or c_s == "nan" or not t_s or t_s == "nan":
            continue
        out.append((c_s, t_s))
    return out


def build_question_map(
    storage: StorageClient, bucket: str, names: list[str], *, workers: int
) -> tuple[dict[str, str], dict[str, int]]:
    """Fold every legacy object into ``{condition_id -> question}`` (most-recent-wins).

    Latest ``day=`` wins; longest non-blank breaks a tie; a blank never overwrites a non-blank
    (blanks are already dropped in ``_read_object_questions``). Returns the map plus read stats.
    """
    # best[cid] = (day, question) — day is the object-path partition ("" sorts first).
    best: dict[str, tuple[str, str]] = {}
    objects_with_questions = 0
    rows_seen = 0

    def _job(path: str) -> tuple[str, list[tuple[str, str]]]:
        return _day_of(path), _read_object_questions(storage, bucket, path)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {pool.submit(_job, name): name for name in names}
        for done, fut in enumerate(as_completed(futs), start=1):
            day, pairs = fut.result()
            if pairs:
                objects_with_questions += 1
            for cid, question in pairs:
                rows_seen += 1
                cur = best.get(cid)
                if cur is None:
                    best[cid] = (day, question)
                    continue
                cur_day, cur_q = cur
                if day > cur_day or (day == cur_day and len(question) > len(cur_q)):
                    best[cid] = (day, question)
            if done % 2000 == 0:
                logger.info("  legacy objects read: %d/%d (distinct cids so far: %d)", done, len(names), len(best))

    qmap = {cid: q for cid, (_d, q) in best.items()}
    stats = {
        "legacy_objects_total": len(names),
        "legacy_objects_with_questions": objects_with_questions,
        "legacy_rows_seen": rows_seen,
        "distinct_condition_ids": len(qmap),
    }
    return qmap, stats


def _column_checksums(df: pd.DataFrame, columns: list[str]) -> dict[str, str]:
    """Per-column content checksum (order-sensitive) — the byte-identity gate for the patch."""
    out: dict[str, str] = {}
    for col in columns:
        h = pd.util.hash_pandas_object(df[col], index=False).to_numpy().tobytes()
        out[col] = hashlib.sha256(h).hexdigest()
    return out


def patch_catalog(cat: pd.DataFrame, question_map: Mapping[str, str]) -> tuple[pd.DataFrame, dict[str, object]]:
    """Left-join ``question_map`` onto the catalogue as an ADDITIVE column. Never mutates ``cat``.

    Preserves every pre-existing row and column exactly (checksum-gated by the caller). The new
    ``question`` column is inserted right after ``base_asset`` to match the rollup's own
    ``CATALOG_COLUMNS`` order; rows with no legacy match keep ``None`` (honest absence).
    """
    pre_existing = [c for c in cat.columns if c != _QUESTION_COL]

    venue = cat["venue"].astype("string").fillna("").astype(str)
    iid = cat["instrument_id"].astype("string").fillna("").astype(str)
    keys = [match_key(i, v) for i, v in zip(iid.tolist(), venue.tolist(), strict=True)]
    questions: list[str | None] = [question_map.get(k) if k is not None else None for k in keys]

    out = cat.copy()
    if _QUESTION_COL in out.columns:
        out = out.drop(columns=[_QUESTION_COL])  # idempotent re-run: recompute cleanly
    q_series = pd.Series(questions, index=out.index, dtype="object")
    out[_QUESTION_COL] = q_series

    # Column order: existing order with `question` slotted right after `base_asset`.
    ordered = [c for c in cat.columns if c != _QUESTION_COL]
    if "base_asset" in ordered:
        ordered.insert(ordered.index("base_asset") + 1, _QUESTION_COL)
    else:
        ordered.append(_QUESTION_COL)
    out = out[ordered]

    n_matched_rows = sum(1 for k in keys if k is not None and k in question_map)
    n_question_nonnull = int(q_series.notna().sum())
    matched_poly_rows = out[(out["venue"] == _POLYMARKET) & (out[_QUESTION_COL].notna())]
    distinct_markets = int(matched_poly_rows["instrument_id"].nunique())

    stats: dict[str, object] = {
        "rows_before": len(cat),
        "rows_after": len(out),
        "pre_existing_columns": len(pre_existing),
        "question_nonnull": n_question_nonnull,
        "rows_with_legacy_match": n_matched_rows,
        "distinct_markets_gained": distinct_markets,
        "rows_left_null": len(out) - n_question_nonnull,
    }
    return out, stats


def gate(
    before_ck: Mapping[str, str], after_ck: Mapping[str, str], stats: Mapping[str, object]
) -> tuple[bool, list[str]]:
    """Refuse to --apply unless the patch is provably additive + evidence-bounded."""
    failures: list[str] = []
    if int(stats["rows_after"]) != int(stats["rows_before"]):
        failures.append(f"row count changed {stats['rows_before']} -> {stats['rows_after']}")
    changed = [c for c, h in before_ck.items() if after_ck.get(c) != h]
    if changed:
        failures.append(f"pre-existing column(s) mutated: {changed}")
    if int(stats["question_nonnull"]) <= 0:
        failures.append("question non-null count is 0 (backfill would be a no-op)")
    if int(stats["question_nonnull"]) > int(stats["rows_with_legacy_match"]):
        failures.append(
            f"question non-null ({stats['question_nonnull']}) exceeds legacy-matched rows "
            f"({stats['rows_with_legacy_match']}) — fabrication guard"
        )
    return (not failures), failures


def run(*, apply: bool, workers: int) -> int:
    from unified_trading_library import get_storage_client

    bucket = _bucket()
    storage = get_storage_client()
    logger.info(
        "prediction question backfill: bucket=%s blob=%s apply=%s workers=%d", bucket, _CATALOG_BLOB, apply, workers
    )

    raw_bytes = storage.download_bytes(bucket, _CATALOG_BLOB)
    before = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info(
        "BEFORE: catalogue rows=%d cols=%d has_question=%s",
        len(before),
        len(before.columns),
        _QUESTION_COL in before.columns,
    )
    pre_existing = [c for c in before.columns if c != _QUESTION_COL]
    before_ck = _column_checksums(before, pre_existing)

    names = list_legacy_objects(storage, bucket)
    logger.info("legacy Shape-B instruments.parquet objects to read: %d", len(names))
    qmap, read_stats = build_question_map(storage, bucket, names, workers=workers)
    logger.info("=== legacy read stats === %s", read_stats)

    out, stats = patch_catalog(before, qmap)
    after_ck = _column_checksums(out, pre_existing)
    logger.info("=== patch stats === %s", stats)

    gate_ok, failures = gate(before_ck, after_ck, stats)
    logger.info(
        "GATE (row-count fixed, pre-existing cols byte-identical, evidence-bounded fill): %s",
        "OK" if gate_ok else "VIOLATION",
    )
    if not gate_ok:
        for f in failures:
            logger.error("GATE FAILURE: %s", f)
        logger.error("aborting before write.")
        return 2

    # A few sample (instrument_id -> question) pairs for the record.
    sample = out[(out["venue"] == _POLYMARKET) & (out[_QUESTION_COL].notna()) & (out["data_type"] == "trades")]
    for iid, q in zip(sample["instrument_id"].head(5).tolist(), sample[_QUESTION_COL].head(5).tolist(), strict=False):
        logger.info("  SAMPLE %s -> %r", iid, str(q)[:80])
    pct = 100.0 * int(stats["question_nonnull"]) / max(1, int(stats["rows_after"]))
    logger.info(
        "SUMMARY: %d/%d rows gained a question (%.2f%%), %d rows honest-null, %d distinct POLYMARKET markets covered.",
        stats["question_nonnull"],
        stats["rows_after"],
        pct,
        stats["rows_left_null"],
        stats["distinct_markets_gained"],
    )

    if not apply:
        logger.info("DRY RUN — live catalogue NOT modified. Re-run with --apply --confirm to write.")
        return 0

    stamp = _utc_stamp()
    snap_blob = f"prod/catalog.{stamp}.questionbackfill.pred.bak.parquet"
    storage.upload_bytes(bucket, snap_blob, raw_bytes)
    logger.info("snapshot written: gs://%s/%s", bucket, snap_blob)

    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    storage.upload_bytes(bucket, _CATALOG_BLOB, buf.getvalue())
    logger.info("APPLIED — live catalogue written: gs://%s/%s (%d rows, +question)", bucket, _CATALOG_BLOB, len(out))

    # Post-verify by RE-READING the written object.
    verify_raw = storage.download_bytes(bucket, _CATALOG_BLOB)
    verify = pd.read_parquet(io.BytesIO(verify_raw))
    ok = (
        len(verify) == int(stats["rows_after"])
        and _QUESTION_COL in verify.columns
        and int(verify[_QUESTION_COL].notna().sum()) == int(stats["question_nonnull"])
    )
    logger.info(
        "POST-VERIFY (re-read): rows=%d has_question=%s question_nonnull=%d — %s",
        len(verify),
        _QUESTION_COL in verify.columns,
        int(verify[_QUESTION_COL].notna().sum()),
        "OK" if ok else "MISMATCH",
    )
    return 0 if ok else 3


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0] if __doc__ else "")
    p.add_argument("--apply", action="store_true", default=False, help="Write back (default: dry-run)")
    p.add_argument("--confirm", action="store_true", default=False, help="Required alongside --apply")
    p.add_argument("--workers", type=int, default=32, help="Parallel per-object GCS reads (default: 32)")
    args = p.parse_args(argv)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm (refusing to write the live catalogue without it).")
        return 2
    return run(apply=args.apply, workers=args.workers)


if __name__ == "__main__":
    sys.exit(main())
