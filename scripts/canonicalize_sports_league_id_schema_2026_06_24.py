#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the sports _index is single-schema (numeric+suffixed in-universe rows re-keyed/deduped to canonical) AND the out-of-universe decision in plans/active/issues/sports_league_id_out_of_universe_overcapture_2026_06_24.md is executed, and the campaign plan archived
"""Canonicalize the SPORTS availability `_index` league_id schema to ONE canonical form.

The live sports `_index` carries league_id in 5 shapes (measured 2026-06-24):
  numeric (api_football id, e.g. "39"), provider-suffixed ("EPL_39"), canonical
  slug ("EPL"), 16-hex hash (SFI_LEAGUES), and blank. This script collapses the
  IN-UNIVERSE numeric + suffixed rows onto their canonical key (resolved via UAC
  ``get_league_by_api_football_id`` then ``canonicalize_league_id`` suffix-strip,
  membership-tested against the 101-league ``LEAGUE_REGISTRY``), then DEDUPES
  against the canonical-keyed twin for the same
  ``(date, data_type, league_id, <other dedup dims>)`` — keeping the BEST status
  (captured > empty_confirmed > attempted_failed > expected_unattempted; on a tie,
  the most-recent ``written_at``).

OUT-OF-UNIVERSE rows (leagues outside our 94/101-league canonical set) are LEFT
UNTOUCHED by default — dropping them is a coverage-model decision gated on the
operator (see ``plans/active/issues/sports_league_id_out_of_universe_overcapture_2026_06_24.md``).
Pass ``--drop-out-of-universe`` to also remove them (requires operator sign-off).

This is a WHOLE-``_index`` rewrite (NOT a per-VM-shard append): re-keying changes
the consolidator dedup key, so a numeric-keyed row cannot be overwritten by a
canonical-keyed append — the stale numeric row must be physically dropped from the
consolidated index. The consolidator's per-VM shards are NOT touched; this rewrites
only the consolidated ``_index/availability_index.parquet`` after snapshotting it.

Safety: dry-run by default (prints before/after counts + numeric→0 proof); writes a
timestamped snapshot to ``_index/snapshots/`` before any ``--apply`` write; resolves
the bucket via ``resolve_bucket_name`` (env-short ``-prd-``), never an inline name.

Idempotent (a second run finds 0 in-universe numeric/suffixed rows).
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import tempfile
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts.sports import (
    LEAGUE_REGISTRY,
    canonicalize_league_id,
    get_league_by_api_football_id,
)
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEX_BLOB = "_index/availability_index.parquet"

# Status preference for dedup (higher wins).
_STATUS_RANK = {
    "captured": 3,
    "empty_confirmed": 2,
    "attempted_failed": 1,
    "expected_unattempted": 0,
}

_NUMERIC_RE = re.compile(r"^\d+$")
_SUFFIX_RE = re.compile(r".+_\d+$")


def _registry_ids() -> set[str]:
    reg = LEAGUE_REGISTRY
    try:
        return {v.league_id for v in reg.values()}  # type: ignore[union-attr]
    except AttributeError:
        return {getattr(v, "league_id", str(v)) for v in reg}


def _canonicalize(raw: str) -> str:
    """Replicate the write-path ``_canonical_league_id`` two-pass logic."""
    s = raw.strip()
    if s and s.isdigit():
        league = get_league_by_api_football_id(int(s))
        s = league.league_id if league is not None else s
    return canonicalize_league_id(s)


def _resolve_bucket() -> str:
    os.environ.setdefault("DEPLOYMENT_ENV", "prod")
    # resolve_bucket_name needs the project env for the {GCP_PROJECT_ID} template fragment.
    if not os.environ.get("GCP_PROJECT_ID") and os.environ.get("PROJECT_ID"):
        os.environ["GCP_PROJECT_ID"] = os.environ["PROJECT_ID"]
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the rewritten index (default: dry-run)")
    parser.add_argument(
        "--drop-out-of-universe",
        action="store_true",
        help="ALSO drop rows for leagues outside the canonical registry (operator sign-off required)",
    )
    parser.add_argument("--bucket", default=None, help="Override resolved bucket (debug only)")
    args = parser.parse_args()

    bucket = args.bucket or _resolve_bucket()
    logger.info("Sports instruments bucket: %s", bucket)

    import gcsfs  # local import — script-only dep

    fs = gcsfs.GCSFileSystem()
    blob = f"{bucket}/{INDEX_BLOB}"
    tmp_in = f"{tempfile.gettempdir()}/sports_index_in.parquet"
    fs.get(blob, tmp_in)
    df = pd.read_parquet(tmp_in)
    n_total = len(df)
    logger.info("Loaded _index: %d rows", n_total)

    lid = df["league_id"].fillna("").astype(str)
    reg = _registry_ids()
    logger.info("Canonical LEAGUE_REGISTRY size: %d", len(reg))

    is_numeric = lid.str.match(_NUMERIC_RE) & (lid != "")
    is_suffixed = lid.str.match(_SUFFIX_RE) & (~is_numeric)
    is_rekey_candidate = is_numeric | is_suffixed

    # Canonicalize the candidate keys (cache by distinct raw value).
    distinct = sorted(lid[is_rekey_candidate].unique())
    canon_map: dict[str, str] = {v: _canonicalize(v) for v in distinct}
    df["_orig_lid"] = lid
    df["_canon_lid"] = lid.map(lambda v: canon_map.get(v, v))
    df["_canon_in_reg"] = df["_canon_lid"].isin(reg)

    rekey_in_universe = is_rekey_candidate & df["_canon_in_reg"]
    rekey_out_universe = is_rekey_candidate & (~df["_canon_in_reg"])
    logger.info(
        "Re-key candidates — numeric=%d suffixed=%d | in-universe(canon in registry)=%d | out-of-universe=%d",
        int(is_numeric.sum()),
        int(is_suffixed.sum()),
        int(rekey_in_universe.sum()),
        int(rekey_out_universe.sum()),
    )

    # Apply the canonical key ONLY to in-universe candidates. Out-of-universe rows
    # keep their original key (operator-gated) unless --drop-out-of-universe.
    df.loc[rekey_in_universe, "league_id"] = df.loc[rekey_in_universe, "_canon_lid"]

    if args.drop_out_of_universe:
        out_mask = (~df["_canon_in_reg"]) & (df["_orig_lid"] != "")
        # blank league_id rows are not league-axis dedupable here; leave them.
        n_drop = int(out_mask.sum())
        logger.info("--drop-out-of-universe: removing %d out-of-universe rows", n_drop)
        df = df[~out_mask].copy()

    # DEDUP: collapse rows that now share the canonical dedup key. The consolidator
    # dedups on (service_name, date, data_type, league_id, + optional dims); we
    # reproduce a conservative key (the dims present) and keep the best-status row.
    dedup_dims = [
        c
        for c in ("service_name", "date", "data_type", "league_id", "timeframe", "pipeline_mode", "source")
        if c in df.columns
    ]
    df["_rank"] = df["capture_status"].astype(str).map(lambda s: _STATUS_RANK.get(s, -1))
    df["_wat"] = pd.to_datetime(df.get("written_at"), errors="coerce", utc=True)
    before_dedup = len(df)
    df = (
        df.sort_values(["_rank", "_wat"], ascending=[False, False], na_position="last")
        .drop_duplicates(subset=dedup_dims, keep="first")
        .copy()
    )
    n_deduped = before_dedup - len(df)
    logger.info("Dedup on %s: %d rows collapsed", dedup_dims, n_deduped)

    # Verification.
    new_lid = df["league_id"].fillna("").astype(str)
    new_in_universe_numeric = int(((new_lid.str.match(_NUMERIC_RE)) & (new_lid != "") & df["_canon_in_reg"]).sum())
    residual_numeric_total = int(((new_lid.str.match(_NUMERIC_RE)) & (new_lid != "")).sum())
    logger.info(
        "AFTER: rows=%d (was %d) | in-universe numeric residual=%d (must be 0) | total numeric residual=%d "
        "(out-of-universe, expected nonzero unless --drop-out-of-universe)",
        len(df),
        n_total,
        new_in_universe_numeric,
        residual_numeric_total,
    )

    df = df.drop(columns=[c for c in ("_orig_lid", "_canon_lid", "_canon_in_reg", "_rank", "_wat") if c in df.columns])

    if not args.apply:
        logger.info("DRY RUN — no write. Re-run with --apply to snapshot + rewrite the _index.")
        return 0

    # Snapshot first.
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    snap = f"{bucket}/_index/snapshots/pre_league_id_canonicalize_{ts}.parquet"
    fs.copy(blob, snap)
    logger.info("Snapshot written: gs://%s", snap)

    tmp_out = f"{tempfile.gettempdir()}/sports_index_out.parquet"
    df.to_parquet(tmp_out, index=False)
    fs.put(tmp_out, blob)
    logger.info("Rewrote _index: gs://%s (%d rows)", blob, len(df))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
