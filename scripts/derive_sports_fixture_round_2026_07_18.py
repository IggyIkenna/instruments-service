#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the sports `round` gap is closed (derive + residual fetch) and
#   competition_phase is verified non-UNKNOWN in the catalogue — this is a one-shot
#   corpus filler, not a pipeline component.
"""Derive sports fixture ``round`` from same-matchday siblings — ZERO api-football calls.

WHY (measured 2026-07-18, ``sports_features_layer_findings_sweep_2026_07_18`` §§ O/P):

* ``round`` is ~50% populated in RAW (not ~0% — that figure was the stale CATALOGUE).
* Of populated values **69% are ``Regular Season - N``**; the rest are cup/qualifying rounds.
* **97.0%** of ``(af_league_id, day)`` groups carry EXACTLY ONE round number (225/232) —
  i.e. "all fixtures for a league on a matchday share one round" is nearly always true.
* **92%** of blank fixtures sit in leagues that ALREADY have round data (1,539/1,672), so
  the populated rows are valid ground truth for exactly the leagues we need to fill.

=> ~89% of blanks are derivable with NO API calls. The residual is bounded by DISTINCT
(league, season) pairs, not fixture count — one ``GET /fixtures?league&season`` returns a
whole season (measured: 242 fixtures in one call).

THE RULE (deliberately conservative — unanimity, not inference):

    For each (af_league_id, day) group:
        known = {non-blank round values}
        if len(known) == 1 and blanks exist:  fill blanks with that value
        else:                                 leave blank (residual -> API fetch)

This SELF-REFUSES the 3% of matchdays that span multiple rounds (rescheduling), so we never
guess. ``round`` is NOT chronological position — a postponed ``Regular Season - 12`` can be
played after round 15 — so date-ORDERING is never used to invent a number. We only propagate
a value that same-day siblings unanimously assert.

PROVENANCE (workspace rule: never silent placeholders): a derived value MUST NOT be
indistinguishable from a captured one. ``--apply`` writes ``round_provenance='derived'`` on
filled rows (captured rows are stamped ``'captured'``), so downstream can always tell.

SAFETY: ``--dry-run`` (default) measures and writes NOTHING. ``--apply`` snapshots each
parquet to ``<path>.pre_round_derive.bak`` before overwriting. Idempotent — a second run
finds nothing to fill. SINGLE-WALK: one listing of the corpus, grouped in memory.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from collections import defaultdict

import pandas as pd

logger = logging.getLogger("derive_sports_fixture_round")


def _storage():
    from unified_trading_library import get_storage_client  # noqa: qg-inside-import

    return get_storage_client()


_BUCKET = "instruments-store-sports-prd-central-element-323112"
_PREFIX = "sports_reference/by_date/"
_ENTITY = "/entity=fixtures_schedule/"  # the LIVE entity (legacy `entity=fixtures/` is stale)
_BACKUP_SUFFIX = ".pre_round_derive.bak"
_PROVENANCE_COL = "round_provenance"


def _day_of(blob_name: str) -> str:
    for seg in blob_name.split("/"):
        if seg.startswith("day="):
            return seg[4:]
    return ""


def _corpus_blobs(bucket: str) -> list[str]:
    """SINGLE walk of the fixtures_schedule corpus."""
    client = _storage()
    names: list[str] = []
    for blob in client.list_blobs(bucket, prefix=_PREFIX):
        name = str(blob.name)
        if _ENTITY in name and name.endswith(".parquet"):
            names.append(name)
    return names


def _load(bucket: str, name: str) -> pd.DataFrame | None:
    client = _storage()
    try:
        raw = client.download_bytes(bucket=bucket, blob_path=name)
        return pd.read_parquet(io.BytesIO(raw))
    except Exception as exc:
        logger.warning("unreadable %s: %s", name, exc)
        return None


def _day_round_lookup(frames: list[pd.DataFrame]) -> dict[object, set[str]]:
    """Known round values per league, pooled across ALL of a day's parquets.

    A single day carries BOTH a bare multi-league `entity=fixtures_schedule/fixtures_schedule.parquet`
    AND per-league `entity=fixtures_schedule/league=<L>/fixtures_schedule.parquet` files. A league's
    populated rows may sit in one while its blanks sit in the other, so grouping PER PARQUET makes
    siblings invisible and mis-reports them as "no sibling". Pool the day first, then fill.
    """
    known: dict[object, set[str]] = {}
    for df in frames:
        if "round" not in df.columns or "af_league_id" not in df.columns:
            continue
        r = df["round"].astype(str).str.strip()
        for league, idx in df.groupby("af_league_id").groups.items():
            vals = {v for v in r.loc[idx] if v}
            if vals:
                known.setdefault(league, set()).update(vals)
    return known


def _fill_frame(df: pd.DataFrame, known: dict[object, set[str]]) -> tuple[pd.DataFrame, int, int, int]:
    """Fill blanks whose league has exactly ONE known round for the day. Returns (df, filled, ambiguous, no_sibling)."""
    if "round" not in df.columns or "af_league_id" not in df.columns:
        return df, 0, 0, 0
    rounds = df["round"].astype(str).str.strip()
    blank_mask = rounds.eq("")
    if not bool(blank_mask.any()):
        return df, 0, 0, 0

    if _PROVENANCE_COL not in df.columns:
        df[_PROVENANCE_COL] = ""
    df.loc[~blank_mask & df[_PROVENANCE_COL].astype(str).eq(""), _PROVENANCE_COL] = "captured"

    filled = ambiguous = no_sibling = 0
    for league, idx in df.groupby("af_league_id").groups.items():
        blanks = [i for i in idx if not rounds.loc[i]]
        if not blanks:
            continue
        vals = known.get(league, set())
        if len(vals) == 1:
            df.loc[blanks, "round"] = next(iter(vals))
            df.loc[blanks, _PROVENANCE_COL] = "derived"
            filled += len(blanks)
        elif len(vals) > 1:
            ambiguous += len(blanks)  # multi-round matchday (rescheduling) — never guess
        else:
            no_sibling += len(blanks)  # no populated sibling anywhere that day — API residual
    return df, filled, ambiguous, no_sibling


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write (default: dry-run). Snapshots each parquet first.")
    ap.add_argument("--bucket", default=_BUCKET)
    ap.add_argument("--max-days", type=int, default=None, help="Cap days scanned (pilot).")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info("[%s] single-walk of %s%s ...", mode, args.bucket, _PREFIX)

    by_day: dict[str, list[str]] = defaultdict(list)
    for name in _corpus_blobs(args.bucket):
        by_day[_day_of(name)].append(name)
    days = sorted(d for d in by_day if d)
    if args.max_days:
        days = days[: args.max_days]
    logger.info("[%s] %d day(s) with fixtures_schedule parquets", mode, len(days))

    tot_rows = tot_blank = tot_filled = tot_amb = tot_nosib = 0
    residual_days: set[str] = set()
    client = _storage()

    for n, day in enumerate(days, 1):
        loaded: list[tuple[str, pd.DataFrame]] = []
        for name in by_day[day]:
            df = _load(args.bucket, name)
            if df is not None and not df.empty:
                loaded.append((name, df))
        if not loaded:
            continue
        known = _day_round_lookup([d for _, d in loaded])  # PASS 1: pool the whole day
        for name, df in loaded:  # PASS 2: fill per file
            blanks_before = int(df["round"].astype(str).str.strip().eq("").sum()) if "round" in df.columns else 0
            tot_rows += len(df)
            tot_blank += blanks_before
            df, filled, amb, nosib = _fill_frame(df, known)
            tot_filled += filled
            tot_amb += amb
            tot_nosib += nosib
            if amb or nosib:
                residual_days.add(day)
            if filled and args.apply:
                from unified_trading_library import gcs_copy_object  # noqa: qg-inside-import

                gcs_copy_object(f"gs://{args.bucket}/{name}", f"gs://{args.bucket}/{name}{_BACKUP_SUFFIX}")
                buf = io.BytesIO()
                df.to_parquet(buf, index=False)
                client.upload_bytes(bucket=args.bucket, blob_path=name, data=buf.getvalue())
        if n % 200 == 0:
            logger.info(
                "[%s] %d/%d days — filled=%d ambiguous=%d no-sibling=%d",
                mode,
                n,
                len(days),
                tot_filled,
                tot_amb,
                tot_nosib,
            )

    pct = (100.0 * tot_filled / tot_blank) if tot_blank else 0.0
    logger.info("[%s] DONE", mode)
    logger.info("  rows scanned      : %d", tot_rows)
    logger.info("  blank round       : %d", tot_blank)
    logger.info("  DERIVED (filled)  : %d  (%.1f%% of blanks) — zero api-football calls", tot_filled, pct)
    logger.info("  ambiguous (multi-round matchday, refused): %d", tot_amb)
    logger.info("  no populated sibling (API residual)      : %d across %d day(s)", tot_nosib, len(residual_days))
    if not args.apply:
        logger.info("  Re-run with --apply to write (snapshots each parquet to *%s).", _BACKUP_SUFFIX)
    return 0


if __name__ == "__main__":
    sys.exit(main())
