#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: the instruments-store-tradfi + instruments-store-sports _index canonicalisation
#              has been --apply'd in prod (N2/F5/N4 resolved + re-audited clean) and a GCS
#              orphan-sweep shows 0 stale v4 shadow rows.
"""canonicalize_instruments_store_index.py — de-dup 2x-per-cell + classify blank capture_status.

Fixes the instruments-store ``_index/availability_index.parquet`` data-quality defects
surfaced by the 2026-06-17 instruments<->MTDS subset+consistency audit (findings N2 / F5 /
N4). The defect is a v8/v9 re-emit that **appended** new rows per cell instead of replacing
the stale legacy ``schema_version=4`` row → every cell carries TWO rows (one captured v8/v9
+ one blank-status v4 shadow). Plus a tail of v4 rows that are the ONLY representation of
their cell (the v9 walk never re-classified them) and, in sports, a set of malformed
enumerator rows (blank ``data_type`` + blank ``league_id`` = no valid shard key).

Three transforms (all honest-absence-preserving — never a silent placeholder, never a
fabricated count):

  1. **De-dup 2x-per-cell** — group by the cell key ``(date, venue, data_type)`` and keep
     ONE canonical row per cell: highest ``capture_status`` priority
     (captured > empty_confirmed > attempted_failed > expected_unattempted > blank), tie-break
     by highest ``schema_version`` then most-recent ``written_at``. The stale v4 blank-status
     shadow loses to its captured v8/v9 sibling and is dropped. (N2 — "INST index rows
     duplicated 2x/cell".)

  2. **Classify the orphan blank-status rows** (v4 rows that are the only representation of
     their cell) to a TRUE 4-state status:
       * ``instrument_count > 0``  → ``captured`` (a real instrument listing — incl. the CME
         weekend carry-forward snapshot, which IS a populated listing, NOT empty).
       * ``instrument_count == 0`` → ``empty_confirmed`` with a typed reason from the venue
         trading calendar (``EXPECTED_WEEKEND`` / ``EXPECTED_HOLIDAY``); a 0-listing trading
         day (calendar returns None) gets ``SOURCE_RETURNED_ZERO`` (a genuine in-coverage
         empty). (N2 — honest weekend classification; the live index already classifies CME
         Saturdays as ``EXPECTED_WEEKEND`` so this transform PRESERVES that, never reintroduces
         ``SOURCE_RETURNED_ZERO`` for a populated weekend snapshot.)

  3. **Drop malformed sports rows** — a blank-``data_type`` + blank-``league_id`` row has no
     valid shard identity (it can map to no real cell), so it is a write artifact, not a real
     captured/empty cell. Dropping it hides no gap (there is no cell to be absent for). The
     by-design ``date='all'`` TEAMS/VENUES reference rows are date-agnostic entities and are
     PRESERVED. (F5 — sports INSTR index hygiene.)

N4 (sports ``instrument_count==0`` on per-league companion rows) is CONFIRMED a count-
attribution display artifact, NOT a defect: the per-league rows ARE legitimately ``captured``
(the league is listed that day) — the global count lands on the blank-league companion row.
The cells are correctly captured; we do not fabricate per-league counts and do not flip their
status. The script reports the N4 population for the audit but makes no change to it.

Output: a v9 PROJECTED ``_index`` written to a THROWAWAY audit sandbox
(``_index/audit/projected_index_{ag}_v2.parquet`` — never the live baseline) via
:func:`beta_manifest_writer.write_projected_index`. ``--apply`` writes the canonicalised
index back to the live blob (operator-dispatched; gated by the plan's pre-apply checklist).

Idempotent — re-running over an already-canonicalised index is a no-op (every cell has 1 row,
no blank-status rows remain).

Usage::

    # dry-run + write the v2 audit projection (the default audit path):
    python scripts/canonicalize_instruments_store_index.py --asset-group tradfi --dry-run
    python scripts/canonicalize_instruments_store_index.py --asset-group sports --dry-run

    # apply to the live _index (operator-dispatched):
    python scripts/canonicalize_instruments_store_index.py --asset-group tradfi

Plan ref: ``plans/active/instruments_mtds_subset_consistency_remediation_2026_06_17.md``
Step 4 (Phase-B F5 + Phase-D N2/N4). Audit:
``plans/audit/results/instruments_mtds_subset_and_consistency_audit_2026_06_17.md``.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys

import pandas as pd
from unified_api_contracts import non_trading_day_reason
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_MANIFEST_BLOB = "_index/availability_index.parquet"

# The cell (shard-identity) key. The shard grain MUST include EVERY populated identity
# dimension — collapsing on too coarse a key silently destroys real per-shard rows (e.g.
# sports is per-LEAGUE, cefi/defi per-instrument). The base axes are always present; the
# finer-grain axes (league_id / instrument_id / underlying / chain) are added to the key
# only when the column exists, so a cell is uniquely a single logical shard.
_BASE_CELL_KEY: tuple[str, str, str] = ("date", "venue", "data_type")
_FINE_GRAIN_AXES: tuple[str, ...] = ("league_id", "instrument_id", "underlying", "chain")


def cell_key_columns(df: pd.DataFrame) -> list[str]:
    """Return the shard-identity key columns present in ``df``.

    Always the base ``(date, venue, data_type)`` plus every fine-grain axis column that
    exists (``league_id`` / ``instrument_id`` / ``underlying`` / ``chain``). This keeps the
    dedup grain identical to the writer's shard atom — collapsing on the base key alone would
    merge distinct per-league / per-instrument shards into one (a silent correctness bug).
    """
    return list(_BASE_CELL_KEY) + [c for c in _FINE_GRAIN_AXES if c in df.columns]


# Canonical-row pick priority — highest wins. Blank/None maps to 0 (always loses to a real
# status), so a stale v4 blank shadow can never beat its captured v8/v9 sibling.
_STATUS_PRIORITY: dict[str, int] = {
    "captured": 4,
    "empty_confirmed": 3,
    "attempted_failed": 2,
    "expected_unattempted": 1,
}

# The honest-absence reason for an in-coverage 0-listing trading day (calendar says it IS a
# trading day yet 0 instruments listed). A populated day (count>0) or a calendar non-trading
# day never reaches this.
_SOURCE_RETURNED_ZERO = "SOURCE_RETURNED_ZERO"

# Note: the by-design sports reference entities carry ``date='all'`` (TEAMS / VENUES,
# date-agnostic). They are preserved automatically — they carry a real ``data_type`` so they
# never match the malformed-row filter (blank data_type AND blank league_id).


def _bucket_for(asset_group: str) -> str:
    """Resolve the instruments-store bucket for an asset_group via the bucket-name SSOT."""
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=asset_group)


def _is_blank(series: pd.Series) -> pd.Series:
    """Boolean mask of blank/None/empty-string values in a (possibly object) column."""
    return series.isna() | (series.astype(str).str.strip() == "")


def classify_orphan_blank_row(venue: str, iso_date: str, instrument_count: float) -> tuple[str, str]:
    """Classify a single orphan blank-status row to a TRUE 4-state status + reason.

    The orphan is the ONLY representation of its cell (no captured sibling), so it must be
    honestly classified, never left blank.

    Returns ``(capture_status, error_reason)``:
      * ``count > 0``  → ``("captured", "")`` — a real instrument listing (incl. the CME
        weekend carry-forward snapshot, which IS populated).
      * ``count == 0`` on a calendar non-trading day → ``("empty_confirmed", "EXPECTED_WEEKEND")``
        / ``("empty_confirmed", "EXPECTED_HOLIDAY")``.
      * ``count == 0`` on a calendar trading day (or a non-calendar venue) → a genuine
        in-coverage empty → ``("empty_confirmed", "SOURCE_RETURNED_ZERO")``.
    """
    try:
        count = float(instrument_count)
    except (TypeError, ValueError):
        count = 0.0
    if count > 0:
        return ("captured", "")
    reason = non_trading_day_reason(str(venue), str(iso_date))
    if reason is not None:
        return ("empty_confirmed", reason)
    return ("empty_confirmed", _SOURCE_RETURNED_ZERO)


def drop_malformed_sports_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop sports rows with no valid shard identity (blank data_type AND blank league_id).

    Such a row maps to no real cell, so it is a write artifact — dropping it hides no gap.
    The date-agnostic ``date='all'`` TEAMS/VENUES reference rows are PRESERVED (they carry a
    real data_type, so they are never matched here). Returns ``(kept_df, n_dropped)``.
    """
    if "league_id" not in df.columns:
        return df, 0
    no_data_type = _is_blank(df["data_type"])
    no_league = _is_blank(df["league_id"])
    malformed = no_data_type & no_league
    n_dropped = int(malformed.sum())
    return df[~malformed].copy(), n_dropped


def classify_blank_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Classify every remaining blank-status row to a TRUE 4-state status (in place on a copy).

    Applies :func:`classify_orphan_blank_row` row-wise. Rows that still carry a non-blank
    status are untouched. Returns ``(df, n_classified)``.
    """
    out = df.copy()
    blank_mask = _is_blank(out["capture_status"])
    n_classified = int(blank_mask.sum())
    if n_classified == 0:
        return out, 0
    classified = out.loc[blank_mask].apply(
        lambda r: classify_orphan_blank_row(r.get("venue", ""), r.get("date", ""), r.get("instrument_count", 0)),
        axis=1,
        result_type="expand",
    )
    out.loc[blank_mask, "capture_status"] = classified[0].to_numpy()
    out.loc[blank_mask, "error_reason"] = classified[1].to_numpy()
    return out, n_classified


def dedup_cells(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Keep ONE canonical row per ``(date, venue, data_type)`` cell.

    Canonical = highest ``_STATUS_PRIORITY`` (blank→0), tie-broken by highest
    ``schema_version`` then most-recent ``written_at``. So a captured v8/v9 row always beats
    its stale v4 blank shadow. Vectorised (sort-once + drop_duplicates). Returns
    ``(deduped_df, n_dropped)``.
    """
    work = df.copy()
    work["_status_rank"] = work["capture_status"].map(_STATUS_PRIORITY).fillna(0).astype(int)
    sort_cols = ["_status_rank"]
    sort_asc = [False]
    if "schema_version" in work.columns:
        sort_cols.append("schema_version")
        sort_asc.append(False)
    if "written_at" in work.columns:
        sort_cols.append("written_at")
        sort_asc.append(False)
    work = work.sort_values(sort_cols, ascending=sort_asc, kind="stable")
    key_cols = cell_key_columns(work)
    # Normalize blank/None to a single sentinel in EVERY key column so a stale v4 row carrying
    # ``instrument_id=None`` collapses with its v8 sibling carrying ``instrument_id=""`` (both
    # mean "no finer grain"). Without this, None != "" splits a true dup into two rows.
    norm_cols: list[str] = []
    for col in key_cols:
        norm = f"_keynorm_{col}"
        work[norm] = work[col].where(~_is_blank(work[col]), other="")
        norm_cols.append(norm)
    deduped = work.drop_duplicates(subset=norm_cols, keep="first")
    n_dropped = len(df) - len(deduped)
    return deduped.drop(columns=["_status_rank", *norm_cols]).reset_index(drop=True), n_dropped


def count_multi_row_cells(df: pd.DataFrame) -> int:
    """Count logical cells carrying >1 row, under the blank-normalised shard key.

    Uses the same blank/None normalisation as :func:`dedup_cells` so the post-dedup
    invariant (every cell has exactly 1 row) is measured against the same key — a cell that
    differs only by ``None`` vs ``""`` in a key column counts as ONE cell, not two.
    """
    key_cols = cell_key_columns(df)
    normed = pd.DataFrame({col: df[col].where(~_is_blank(df[col]), other="") for col in key_cols})
    return int((normed.groupby(key_cols, dropna=False).size() > 1).sum())


def count_n4_companion_rows(df: pd.DataFrame) -> int:
    """N4 report-only: count captured rows carrying ``instrument_count == 0``.

    These are the per-league companion rows whose count attribution is a display artifact
    (the global count lands on the blank-league companion). They are correctly ``captured`` —
    this function does NOT change them; it only reports the population for the audit.
    """
    if "instrument_count" not in df.columns:
        return 0
    cap = df["capture_status"] == "captured"
    return int((cap & (df["instrument_count"] == 0)).sum())


def canonicalize(df: pd.DataFrame, asset_group: str) -> tuple[pd.DataFrame, dict[str, int]]:
    """Run the full canonicalisation: drop-malformed (sports) → classify-blanks → dedup-cells.

    Returns ``(canonical_df, stats)``. ``stats`` carries before/after counts for the audit.
    """
    stats: dict[str, int] = {"rows_in": len(df)}
    stats["blank_status_in"] = int(_is_blank(df["capture_status"]).sum())
    stats["n4_companion_captured_count0"] = count_n4_companion_rows(df)

    work = df
    if asset_group == "sports":
        work, n_malformed = drop_malformed_sports_rows(work)
        stats["malformed_sports_dropped"] = n_malformed
    else:
        stats["malformed_sports_dropped"] = 0

    work, n_classified = classify_blank_rows(work)
    stats["blank_status_classified"] = n_classified

    work, n_dedup_dropped = dedup_cells(work)
    stats["dedup_rows_dropped"] = n_dedup_dropped

    stats["rows_out"] = len(work)
    stats["blank_status_out"] = int(_is_blank(work["capture_status"]).sum())
    stats["cells_with_multiple_rows_out"] = count_multi_row_cells(work)
    return work, stats


def _read_index(storage: object, bucket: str) -> pd.DataFrame:
    raw = storage.download_bytes(bucket, _MANIFEST_BLOB)  # type: ignore[attr-defined]
    return pd.read_parquet(io.BytesIO(raw))


def _write_live_index(storage: object, bucket: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    storage.upload_from_file_obj(bucket, _MANIFEST_BLOB, buf)  # type: ignore[attr-defined]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--asset-group",
        required=True,
        choices=("tradfi", "sports", "cefi", "defi", "prediction"),
        help="instruments-store asset_group whose _index to canonicalise.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute the canonical index + write the v2 audit projection; do NOT touch the live _index.",
    )
    p.add_argument(
        "--projection-uri",
        default="",
        help=(
            "Override the v2 audit projection target gs:// URI. Defaults to "
            "gs://<bucket>/_index/audit/projected_index_{ag}_v2.parquet (an _index/audit/ sandbox)."
        ),
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)
    asset_group = args.asset_group
    bucket = _bucket_for(asset_group)
    storage = get_storage_client()

    logger.info("Reading instruments-store %s _index from gs://%s/%s", asset_group, bucket, _MANIFEST_BLOB)
    df = _read_index(storage, bucket)
    canonical, stats = canonicalize(df, asset_group)

    logger.info("Canonicalisation stats for %s:", asset_group)
    for key, value in stats.items():
        logger.info("  %s = %d", key, value)

    projection_uri = args.projection_uri or (f"gs://{bucket}/_index/audit/projected_index_{asset_group}_v2.parquet")
    from beta_manifest_writer import write_projected_index

    n_written = write_projected_index(canonical, projection_uri)
    logger.info("Wrote v2 audit projection (%d rows) → %s", n_written, projection_uri)

    if args.dry_run:
        logger.info("DRY RUN — live _index NOT modified. Audit projection written to %s", projection_uri)
        return 0

    logger.info("APPLY — writing canonical _index back to gs://%s/%s", bucket, _MANIFEST_BLOB)
    _write_live_index(storage, bucket, canonical)
    logger.info("Done. Live _index now %d rows (was %d).", stats["rows_out"], stats["rows_in"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
