#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after sports_g1_noise_population_mismatch_and_scope_bug_2026_07_27.md todo 2 closes and its
#   §U-subset finding is folded into sports_closeout_track_s2_foldin_2026_07_25.md (that issue doc's todo 3).
"""Reconcile §U's approved 489-pair/10,869-row FIXTURES_SCHEDULE non-registry population
(`sports_features_layer_findings_sweep_2026_07_18_part3_2026_07_26.md` § U) against the G1 NOISE-wipe
manifest-index census that `delete_noncanonical_sports_leagues_2026_06_25.py` measures.

Context (`sports_g1_noise_population_mismatch_and_scope_bug_2026_07_27.md` todo 2): does §U's approved
purge population sit INSIDE the residual the G1 delete script would touch, so the two can be treated as
the same population? This script answers it two ways:

1. STRUCTURAL check (no I/O): `delete_noncanonical_sports_leagues_2026_06_25.py`'s own `_FOOTBALL_DATA_TYPES`
   frozenset — the exact scope the G1 census/delete script applies — does NOT include ``FIXTURES_SCHEDULE``
   or ``FIXTURES_OUTCOMES``. §U's ENTIRE population is drawn from ``FIXTURES_SCHEDULE`` raw content. So the
   G1 manifest-index cut structurally contains ZERO ``FIXTURES_SCHEDULE`` rows — the two populations are
   disjoint by construction, not merely "different sized".
2. MEASURED check (single, SCOPED walk): a fresh read of the raw ``fixtures_schedule`` corpus, restricted
   to the non-registry league_ids the consolidated manifest index already flags for
   ``data_type=FIXTURES_SCHEDULE`` (one small metadata read narrows the target-league set — never a new
   whole-corpus GCS walk), counts blank-``round`` rows today the same way §U originally did, so the doc can
   record what today's equivalent of the 489/10,869 figure actually is.

Read-only: no ``--apply``, no writes, no deletes, no GCS object mutation of any kind.
"""

from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from threading import Lock

import pandas as pd
from unified_api_contracts.sports import get_expected_leagues_for_source
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reconcile_sports_g1_fixtures_schedule_population")

_ENTITY_PREFIX = "sports_reference/by_date/"
_ENTITY_MARKER = "/entity=fixtures_schedule/"
_LEAGUE_MARKER = "/league="

# The exact scope `delete_noncanonical_sports_leagues_2026_06_25.py` uses for the G1 census/delete —
# duplicated here (not imported) because that module is a standalone script, not a package, and this
# constant is the one fact this reconciliation depends on staying in sync; a divergence would be caught by
# `test_reconcile_sports_g1_fixtures_schedule_population_2026_08_03.py`'s cross-check against the sibling
# script's own frozenset.
_G1_CENSUS_DATA_TYPES = frozenset(
    {
        "FIXTURES",
        "FIXTURE_EVENTS",
        "FIXTURE_LINEUPS",
        "FIXTURE_PLAYER_STATS",
        "FIXTURE_STATS",
        "INJURIES",
        "LEAGUES",
        "MATCHES",
        "ODDS",
        "ODDS_HORIZON_BUCKET",
        "ODDS_MOVEMENT",
        "ODDS_SNAPSHOT",
        "PLAYER_STATS",
        "PLAYERS",
        "PLAYER_VALUES",
        "PREDICTIONS",
        "RESULTS",
        "STANDINGS",
        "TEAMS",
        "TRANSFER_RECORDS",
        "VENUES",
        "XG",
        "XG_SHOTS",
    }
)


def _blank(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.isin(["", "none", "None", "nan", "NaN", "<NA>"])


def league_from_blob_path(path: str) -> str | None:
    """Extract the ``league=<L>`` partition value from a fixtures_schedule blob path.

    Returns None for the "bare" multi-league day file (no `/league=` segment) — the live reader never
    reads it (`derive_sports_fixture_round_2026_07_18.py`'s own note), so it is not part of this
    population either.
    """
    for seg in path.split("/"):
        if seg.startswith("league="):
            return seg[len("league=") :]
    return None


def tally_frame(df: pd.DataFrame) -> tuple[int, int]:
    """Return (total_rows, blank_round_rows) for one fixtures_schedule parquet's content."""
    total = len(df)
    if "round" not in df.columns:
        return total, 0
    blank = int(_blank(df["round"]).sum())
    return total, blank


def is_g1_census_disjoint_from_fixtures_schedule() -> bool:
    """True iff the G1 census's own data_type scope structurally excludes FIXTURES_SCHEDULE/OUTCOMES —
    i.e. the two populations can never overlap regardless of league_id content."""
    return not ({"FIXTURES_SCHEDULE", "FIXTURES_OUTCOMES"} & _G1_CENSUS_DATA_TYPES)


@dataclass
class CensusResult:
    blobs_read: int = 0
    total_rows: int = 0
    blank_rows: int = 0
    non_registry_blank_rows: int = 0
    non_registry_leagues_with_blanks: set[str] = field(default_factory=set)


def _storage():
    from unified_trading_library import get_storage_client  # noqa: qg-inside-import

    return get_storage_client()


def _candidate_non_registry_leagues(bucket: str, canonical_slugs: frozenset[str]) -> frozenset[str]:
    """Scope the raw walk: read the ALREADY-consolidated manifest index (one small file, not a new
    whole-corpus GCS walk) and take the FIXTURES_SCHEDULE league_ids it already knows are non-registry.
    This is the "scoped" half of the single-walk instruction — it narrows which league partitions the raw
    content walk below actually needs to visit."""
    client = _storage()
    raw = client.download_bytes(bucket=bucket, blob_path="_index/availability_index.parquet")
    import io

    idx = pd.read_parquet(io.BytesIO(raw), columns=["data_type", "league_id"])
    fx = idx[idx["data_type"] == "FIXTURES_SCHEDULE"]
    lid = fx["league_id"].fillna("").astype(str).str.strip()
    non_registry = lid[~lid.isin(canonical_slugs) & (lid != "")]
    return frozenset(non_registry.unique())


def run_census(*, max_workers: int = 24) -> CensusResult:
    """SINGLE walk (one `list_blobs` pass) of the raw fixtures_schedule corpus, SCOPED to the league
    partitions the manifest index already flags as non-registry. Read-only."""
    canonical_slugs = frozenset(lg.league_id for lg in get_expected_leagues_for_source("api_football"))
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    target_leagues = _candidate_non_registry_leagues(bucket, canonical_slugs)
    logger.info(
        "canonical registry: %d leagues; scoping raw walk to %d non-registry FIXTURES_SCHEDULE leagues",
        len(canonical_slugs),
        len(target_leagues),
    )

    client = _storage()
    blob_names = [
        str(blob.name)
        for blob in client.list_blobs(bucket, prefix=_ENTITY_PREFIX)
        if _ENTITY_MARKER in str(blob.name) and str(blob.name).endswith(".parquet")
    ]
    scoped = [name for name in blob_names if (lg := league_from_blob_path(name)) is not None and lg in target_leagues]
    logger.info(
        "single walk found %d fixtures_schedule blobs; %d scoped to target leagues", len(blob_names), len(scoped)
    )

    result = CensusResult()
    lock = Lock()

    def _one(name: str) -> tuple[str, int, int] | None:
        import io

        raw = client.download_bytes(bucket=bucket, blob_path=name)
        df = pd.read_parquet(io.BytesIO(raw), columns=["af_league_id", "round"])
        total, blank = tally_frame(df)
        return name, total, blank

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_one, name): name for name in scoped}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                _name, total, blank = fut.result()
            except Exception as exc:
                logger.warning("unreadable %s: %s", name, exc)
                continue
            league = league_from_blob_path(name)
            with lock:
                result.blobs_read += 1
                result.total_rows += total
                result.blank_rows += blank
                result.non_registry_blank_rows += blank
                if blank and league is not None:
                    result.non_registry_leagues_with_blanks.add(league)

    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-workers", type=int, default=24)
    args = parser.parse_args()

    disjoint = is_g1_census_disjoint_from_fixtures_schedule()
    logger.info(
        "STRUCTURAL check: G1 census data_type scope %s FIXTURES_SCHEDULE/FIXTURES_OUTCOMES",
        "EXCLUDES" if disjoint else "includes",
    )

    result = run_census(max_workers=args.max_workers)
    logger.info(
        "MEASURED: %d blobs read, %d total rows, %d non-registry blank-round rows across %d non-registry leagues",
        result.blobs_read,
        result.total_rows,
        result.non_registry_blank_rows,
        len(result.non_registry_leagues_with_blanks),
    )
    logger.info(
        "ANSWER: §U's population is%s a subset of the G1 manifest-index cut (%s)",
        "" if not disjoint else " NOT",
        "populations are disjoint by data_type scope"
        if disjoint
        else "same data_type scope — league_id overlap would determine subset",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
