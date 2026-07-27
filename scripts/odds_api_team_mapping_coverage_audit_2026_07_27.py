# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after this has run against production at least once and the
#   resulting gap report + any --apply mapping update has landed (this is a
#   point-in-time coverage census, not a recurring job — a future re-audit
#   would be a fresh dated script, not a re-run of this one).
"""Coverage census + gap-fill for `sports_reference/mappings/odds_api_team_mapping.parquet`
against the distinct home/away team-name strings actually present in MDPS's bucketed-odds
shards (`pipeline_mode=batch_mdps_odds_horizon_bucket`).

Plan: ``unified-trading-pm/plans/active/sports_satellite_ao_dispatch_batch3_2026_07_25.md``
(todo "Audit instruments-service's `odds_api_team_mapping.parquet` ... coverage").

Confirmed gap as of 2026-07-14: ``Burgos CF`` (league_id=SEGUNDA_DIVISION) is absent from the
mapping table. Source: ``plans/archive/issues/
sports_derived_features_per_league_layout_unread_by_ml_loader_2026_07_14.md``.

What this does
---------------
1. CENSUS (read-only): sample MDPS bucketed-odds shards
   (``processed/by_date/day={D}/pipeline_mode=batch_mdps_odds_horizon_bucket/asset_group=sports/
   data_type=odds_horizon_bucket/league_id={L}/timeframe={T}/bucketed.parquet``) across the full
   2020-06-06..today data-floor range at a bounded stride (default every 11th day — a prime step so
   the sampled weekday rotates, avoiding a single-weekday blind spot; NOT a full-corpus walk). For
   each sampled (day, league) pair, downloads exactly ONE timeframe file (tries a short fixed list
   until one exists) and extracts distinct ``home_team``/``away_team`` values plus the GCS
   ``league_id`` they were observed under. This is genuinely a SAMPLE, not exhaustive enumeration —
   the stride and day range actually used are printed/reported so nothing is silently claimed as
   complete.
2. GAP: diffs the sampled team-name set against the mapping table's existing ``od_team_name`` keys.
3. RESOLUTION (no guessing): for each gap name, resolves via the SAME alias resolver the live
   pipeline already uses (``unified_api_contracts.external.api_football.team_mappings.
   validate_team_resolution``, provider="odds_api") -> canonical_team_id -> looked up in
   ``team_mapping_v2.parquet``'s ``canonical_team_id`` -> ``api_football_id`` column. The GCS
   ``af_league_id`` for the new row is taken by majority vote from the ALREADY-mapped teams observed
   in the same (day, league_id) sample (i.e. reusing real existing table data, never invented) --
   emitted only when that vote is unambiguous. Any name that raises ``TeamResolutionError``, or whose
   canonical id isn't in team_mapping_v2, or whose league has zero already-mapped teams to vote from,
   is reported as a RESIDUAL UNMAPPABLE gap and is deliberately left out of the table (matches the
   plan's Done-when: "any residual honestly-unmappable names are left dropping at ml-service merge
   time as already documented, not fabricated").

Usage
-----
::

  # Dry-run (default) -- runs the census + resolution, writes a JSON report, no GCS write:
  python scripts/odds_api_team_mapping_coverage_audit_2026_07_27.py --report /tmp/odds_gap_report.json

  # Apply -- also uploads the extended mapping parquet (new rows appended) back to GCS:
  python scripts/odds_api_team_mapping_coverage_audit_2026_07_27.py --apply --report /tmp/odds_gap_report.json

  # Narrower/faster run while iterating:
  python scripts/odds_api_team_mapping_coverage_audit_2026_07_27.py --stride 61 --report /tmp/x.json
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta

import pandas as pd
from unified_api_contracts.external.api_football.team_mappings import (
    TeamResolutionError,
    validate_team_resolution,
)
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

DATA_FLOOR = date(2020, 6, 6)
MAPPING_BLOB = "sports_reference/mappings/odds_api_team_mapping.parquet"
TEAM_MAPPING_V2_BLOB = "sports_reference/mappings/team_mapping_v2.parquet"
ODDS_PIPELINE_MODE = "batch_mdps_odds_horizon_bucket"
CANDIDATE_TIMEFRAMES = ("T-1h", "T-2h", "T-4h", "T-6h", "T-12h", "T-24h", "T-10m")


def _instruments_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _market_data_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="sports")


def _sample_days(stride: int) -> list[str]:
    today = datetime.now(UTC).date()
    days = []
    d = DATA_FLOOR
    while d <= today:
        days.append(d.isoformat())
        d += timedelta(days=stride)
    return days


def _load_mapping_table(client, bucket: str) -> pd.DataFrame:
    data = client.download_bytes(bucket, MAPPING_BLOB)
    return pd.read_parquet(io.BytesIO(data))


def _load_team_mapping_v2(client, bucket: str) -> pd.DataFrame:
    data = client.download_bytes(bucket, TEAM_MAPPING_V2_BLOB)
    return pd.read_parquet(io.BytesIO(data))


_LEAGUE_TIMEFRAME_RE = re.compile(r"league_id=([^/]+)/timeframe=([^/]+)/bucketed\.parquet$")

# One (day) list_blobs call, no delimiter (the GCP StorageClient wrapper only yields real
# blob items -- it never surfaces delimiter "prefixes" -- see gcp.py list_blobs: it iterates
# the raw HTTPIterator directly rather than reading its post-exhaustion `.prefixes` property,
# so a delimiter-based directory-only listing always silently returns zero). A single day's
# odds_horizon_bucket population is small (~leagues x 7 timeframes, observed 15 x 7 = 105 on a
# sample day) so one bounded listing (cap below) is cheap and NOT a whole-corpus walk.
_DAY_LISTING_CAP = 4000


def _day_shard_index(client, md_bucket: str, day: str) -> dict[str, str]:
    """Returns {league_id: one chosen blob_path} for a single sampled day."""
    prefix = (
        f"processed/by_date/day={day}/pipeline_mode={ODDS_PIPELINE_MODE}/"
        f"asset_group=sports/data_type=odds_horizon_bucket/"
    )
    by_league: dict[str, dict[str, str]] = defaultdict(dict)
    for blob in client.list_blobs(md_bucket, prefix=prefix, max_results=_DAY_LISTING_CAP):
        name = blob if isinstance(blob, str) else getattr(blob, "name", "")
        m = _LEAGUE_TIMEFRAME_RE.search(name or "")
        if not m:
            continue
        league_id, timeframe = m.group(1), m.group(2)
        by_league[league_id][timeframe] = name

    chosen: dict[str, str] = {}
    for league_id, tf_to_blob in by_league.items():
        for tf in CANDIDATE_TIMEFRAMES:
            if tf in tf_to_blob:
                chosen[league_id] = tf_to_blob[tf]
                break
        else:
            # none of the preferred timeframes present -- take whatever exists
            chosen[league_id] = next(iter(tf_to_blob.values()))
    return chosen


def _read_shard(client, md_bucket: str, blob_path: str) -> pd.DataFrame | None:
    try:
        data = client.download_bytes(md_bucket, blob_path)
        return pd.read_parquet(io.BytesIO(data))
    except Exception:
        logger.warning("failed to read %s/%s", md_bucket, blob_path, exc_info=True)
        return None


def run_census(client, md_bucket: str, stride: int) -> tuple[dict[str, set[str]], list[str], int, int]:
    """Returns (league_id -> set of observed team names, sampled_days, days_with_data, shard_reads)."""
    sampled_days = _sample_days(stride)
    league_to_names: dict[str, set[str]] = defaultdict(set)
    days_with_data = 0
    shard_reads = 0
    for day in sampled_days:
        shard_by_league = _day_shard_index(client, md_bucket, day)
        if shard_by_league:
            days_with_data += 1
        for league_id, blob_path in shard_by_league.items():
            df = _read_shard(client, md_bucket, blob_path)
            if df is None or df.empty:
                continue
            shard_reads += 1
            for col in ("home_team", "away_team"):
                if col in df.columns:
                    league_to_names[league_id].update(v for v in df[col].dropna().unique().tolist() if v)
    return league_to_names, sampled_days, days_with_data, shard_reads


def resolve_gaps(
    league_to_names: dict[str, set[str]],
    existing_names: set[str],
    existing_af_league_by_name: dict[str, int],
    v2_canonical_to_af_id: dict[str, int],
) -> tuple[list[dict], list[dict]]:
    """Returns (resolved_rows, residual_unmappable)."""
    resolved: list[dict] = []
    residual: list[dict] = []
    for league_id, names in league_to_names.items():
        gap_names = sorted(n for n in names if n not in existing_names)
        if not gap_names:
            continue
        # Majority-vote af_league_id from the ALREADY-mapped teams observed in this same
        # GCS league_id -- never invented, always sourced from real existing table rows.
        already_mapped_in_league = [existing_af_league_by_name[n] for n in names if n in existing_af_league_by_name]
        af_league_id = None
        if already_mapped_in_league:
            vote = Counter(already_mapped_in_league).most_common(1)[0]
            # only trust the vote if it's unambiguous (every already-mapped team agrees)
            if vote[1] == len(already_mapped_in_league):
                af_league_id = vote[0]

        for name in gap_names:
            try:
                canonical_id = validate_team_resolution(name, provider="odds_api")
            except TeamResolutionError as exc:
                residual.append(
                    {
                        "od_team_name": name,
                        "league_id": league_id,
                        "reason": f"TeamResolutionError: {exc}",
                    }
                )
                continue
            af_team_id = v2_canonical_to_af_id.get(canonical_id)
            if af_team_id is None:
                residual.append(
                    {
                        "od_team_name": name,
                        "league_id": league_id,
                        "reason": (
                            f"resolved to canonical_team_id={canonical_id!r} but that id is not "
                            "in team_mapping_v2.parquet's canonical_team_id -> api_football_id map"
                        ),
                    }
                )
                continue
            if af_league_id is None:
                residual.append(
                    {
                        "od_team_name": name,
                        "league_id": league_id,
                        "reason": (
                            "af_team_id resolved but af_league_id could not be confirmed "
                            "(no unambiguous already-mapped team in this GCS league_id to vote from)"
                        ),
                    }
                )
                continue
            resolved.append(
                {
                    "af_league_id": af_league_id,
                    "af_team_id": af_team_id,
                    "af_team_name": name,
                    "od_team_name": name,
                    "gcs_league_id": league_id,
                    "canonical_team_id": canonical_id,
                }
            )
    return resolved, residual


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stride",
        type=int,
        default=11,
        help="sample every Nth day from the 2020-06-06 data floor to today (default 11)",
    )
    parser.add_argument("--report", type=str, default="", help="path to write the JSON report")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="upload the extended mapping parquet with resolved gap rows appended",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = get_storage_client(provider="gcp")
    is_bucket = _instruments_bucket()
    md_bucket = _market_data_bucket()

    mapping_df = _load_mapping_table(client, is_bucket)
    existing_names = set(mapping_df["od_team_name"].dropna().unique().tolist())
    existing_af_league_by_name = dict(zip(mapping_df["od_team_name"], mapping_df["af_league_id"], strict=False))

    v2_df = _load_team_mapping_v2(client, is_bucket)
    v2_canonical_to_af_id = dict(zip(v2_df["canonical_team_id"], v2_df["api_football_id"], strict=False))
    v2_canonical_to_af_id = {k: int(v) for k, v in v2_canonical_to_af_id.items() if k and pd.notna(v)}

    logger.info(
        "loaded mapping table: %d existing rows, %d distinct od_team_name",
        len(mapping_df),
        len(existing_names),
    )

    league_to_names, sampled_days, days_with_data, shard_reads = run_census(client, md_bucket, args.stride)
    total_observed = {n for names in league_to_names.values() for n in names}
    gap_names = sorted(total_observed - existing_names)

    logger.info(
        "census: %d sampled days (%d had data), %d shard reads, %d distinct team names observed, "
        "%d gap names vs the %d-row mapping table",
        len(sampled_days),
        days_with_data,
        shard_reads,
        len(total_observed),
        len(gap_names),
        len(mapping_df),
    )

    resolved, residual = resolve_gaps(
        league_to_names, existing_names, existing_af_league_by_name, v2_canonical_to_af_id
    )

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "sampling": {
            "data_floor": DATA_FLOOR.isoformat(),
            "stride_days": args.stride,
            "sampled_day_count": len(sampled_days),
            "sampled_days_with_data": days_with_data,
            "shard_reads": shard_reads,
            "note": (
                "This is a STRIDE SAMPLE across the full data-floor range, not an exhaustive "
                "day-by-day corpus walk (single-walk discipline) -- team rosters recur weekly "
                "within a season so a prime-step sample cycles through weekdays and captures "
                "the great majority of names, but is not guaranteed 100% exhaustive."
            ),
        },
        "existing_mapping_rows": len(mapping_df),
        "distinct_team_names_observed": len(total_observed),
        "gap_count": len(gap_names),
        "gap_names": gap_names,
        "resolved_count": len(resolved),
        "resolved_rows": resolved,
        "residual_unmappable_count": len(residual),
        "residual_unmappable": residual,
    }

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        logger.info("wrote report to %s", args.report)
    else:
        print(json.dumps(report, indent=2, default=str))

    if args.apply and resolved:
        new_rows = pd.DataFrame(
            [
                {
                    "af_league_id": r["af_league_id"],
                    "af_team_id": r["af_team_id"],
                    "af_team_name": r["af_team_name"],
                    "od_team_name": r["od_team_name"],
                    "data_available_at": datetime.now(UTC).isoformat(),
                }
                for r in resolved
            ]
        )
        extended = pd.concat([mapping_df, new_rows], ignore_index=True)
        extended = extended.drop_duplicates(subset=["od_team_name"], keep="first")
        buf = io.BytesIO()
        extended.to_parquet(buf, index=False)
        client.upload_bytes(is_bucket, MAPPING_BLOB, buf.getvalue(), content_type="application/octet-stream")
        logger.info(
            "APPLIED: uploaded extended mapping table (%d -> %d rows) to gs://%s/%s",
            len(mapping_df),
            len(extended),
            is_bucket,
            MAPPING_BLOB,
        )
    elif args.apply:
        logger.info("APPLY requested but zero resolved rows -- nothing to upload")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
