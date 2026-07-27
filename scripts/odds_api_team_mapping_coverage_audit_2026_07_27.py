#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: a re-run of this script reports 0 genuinely-resolvable gaps
#   remaining against odds_api_team_mapping.parquet.
"""odds_api_team_mapping_coverage_audit_2026_07_27.py -- audit instruments-service's
sports_reference/mappings/odds_api_team_mapping.parquet (od_team_name -> af_team_id
crosswalk) coverage against the distinct od team names actually present in MDPS's
bucketed-odds shards, and extend the table with every genuinely-resolvable gap.

Source: plans/active/sports_satellite_ao_dispatch_batch3_2026_07_25.md todo "Audit
instruments-service's odds_api_team_mapping.parquet coverage" (confirmed gap as of
2026-07-14: Burgos CF, SEGUNDA_DIVISION, unmapped).

METHOD (single-walk discipline -- bounded by the manifest, never a blind date range
or a fresh full-bucket walk):
  1. Read the canonical sports manifest (_index/availability_index.parquet in
     instruments-store-sports-{env}) ONCE, filter to
     source=mdps_odds_horizon_bucket & capture_status=captured, and take the
     distinct `date` values -- the manifest already materializes which days have
     real captured bucketed-odds data, so this IS the bounded day-list (no blind
     2020-06..today iteration).
  2. For EACH captured day (bounded, from step 1), list blobs under BOTH the
     canonical (`pipeline_mode=batch_mdps_odds_horizon_bucket`) and legacy
     (`data_type=odds_horizon_bucket`) day-prefixes in the market-data bucket --
     a single bounded prefix list per day, mirroring the pattern established by
     ml-service's `SportsFeatureLoaderMixin._load_odds_event_teams` (this crosswalk
     is IS-owned reference data, so the read is reimplemented here rather than
     importing ml-service) -- and read ONLY the `home_team`/`away_team` columns of
     each `bucketed.parquet` shard (columnar projection, not a full-frame load) to
     build the distinct od_team_name vocabulary.
  3. Diff that vocabulary against the existing od_team_name keys in
     odds_api_team_mapping.parquet -> the coverage gap.
  4. Resolve each gap name via an EXACT (accent/case/whitespace-normalized) match
     against team_mapping_v2.parquet's `odds_api_name` column (the maintained,
     5-provider team crosswalk instruments-service already owns) -- a confirmed
     identity, not a guess; ambiguous (duplicate-normalized) odds_api_name rows are
     never used for resolution. `af_league_id` is intentionally left null for
     newly-resolved rows: team_mapping_v2's own `league` field uses a different
     naming convention than league_mapping.parquet's `canonical_league_id`
     (confirmed by direct probe -- e.g. 'LA_LIGA' vs 'SPAIN_LA_LIGA', 'EPL' vs
     'ENGLAND_PREMIER_LEAGUE') with no reliable translation, and af_league_id is not
     read by the one live consumer (ml-service's `_load_odds_api_team_map` only
     uses `od_team_name`/`af_team_id`) -- an honest null beats a fabricated league
     id.
  5. Any remaining unresolved names are reported, NOT fabricated -- they keep
     dropping at ml-service merge time exactly as already documented (honest
     absence).

Usage::

    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/odds_api_team_mapping_coverage_audit_2026_07_27.py [--apply] [--workers 40]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

import gcsfs
import pandas as pd
import pyarrow.parquet as pq
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("odds_api_team_mapping_coverage_audit")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_MAPPING_BLOB = "sports_reference/mappings/odds_api_team_mapping.parquet"
_TEAM_MAPPING_V2_BLOB = "sports_reference/mappings/team_mapping_v2.parquet"
_ODDS_BUCKETED_PREFIXES: tuple[str, str] = (
    "processed/by_date/day={date}/pipeline_mode=batch_mdps_odds_horizon_bucket/",
    "processed/by_date/day={date}/data_type=odds_horizon_bucket/",
)


def _normalize(name: str) -> str:
    """Casefold + strip accents/whitespace for tolerant-but-exact matching."""
    stripped = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return " ".join(stripped.strip().casefold().split())


def _list_bucketed_paths(client: object, bucket: str, date: str) -> list[str]:
    paths: set[str] = set()
    for tpl in _ODDS_BUCKETED_PREFIXES:
        prefix = tpl.format(date=date)
        try:
            blobs = client.list_blobs(bucket, prefix=prefix)  # pyright: ignore[reportAttributeAccessIssue]
        except (ValueError, OSError):
            continue
        for b in blobs:  # pyright: ignore[reportUnknownVariableType]
            name: object = b if isinstance(b, str) else getattr(b, "name", None)
            if isinstance(name, str) and name.endswith("/bucketed.parquet"):
                paths.add(name)
    return sorted(paths)


def _read_team_names(client: object, bucket: str, path: str) -> set[str]:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            raw = client.download_bytes(bucket, path)  # pyright: ignore[reportAttributeAccessIssue]
            table = pq.read_table(io.BytesIO(raw), columns=["home_team", "away_team"])
            names: set[str] = set()
            for col in ("home_team", "away_team"):
                if col in table.column_names:
                    names.update(v for v in table.column(col).to_pylist() if v)
            return names
        except (ValueError, OSError) as exc:
            last_exc = exc
            time.sleep(0.2 * (attempt + 1))
    logger.warning("giving up on %s: %s", path, last_exc)
    return set()


def census_day(client: object, bucket: str, date: str) -> set[str]:
    names: set[str] = set()
    for path in _list_bucketed_paths(client, bucket, date):
        names |= _read_team_names(client, bucket, path)
    return names


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="Write the extended mapping back to GCS.")
    ap.add_argument("--workers", type=int, default=40)
    ap.add_argument("--limit-days", type=int, default=None, help="Cap the number of captured days censused (pilot).")
    args = ap.parse_args()

    env_short = os.environ.get("DEPLOYMENT_ENV_SHORT")
    if not env_short:
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing.")
        return 1

    is_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    md_bucket = resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="sports")
    if not is_bucket.startswith(f"instruments-store-sports-{env_short}"):
        logger.error("Resolved instruments-store bucket %s unexpected shape. Refusing.", is_bucket)
        return 1
    if not md_bucket.startswith(f"market-data-tick-sports-{env_short}"):
        logger.error("Resolved market-data bucket %s unexpected shape. Refusing.", md_bucket)
        return 1

    fs = gcsfs.GCSFileSystem()
    logger.info("Reading canonical sports manifest gs://%s/%s", is_bucket, _MANIFEST_BLOB)
    with fs.open(f"{is_bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        manifest = pd.read_parquet(fh, columns=["date", "source", "capture_status"])
    captured = manifest[
        (manifest["source"] == "mdps_odds_horizon_bucket") & (manifest["capture_status"] == "captured")
    ]
    days = sorted(captured["date"].dropna().unique().tolist())
    if args.limit_days:
        days = days[: args.limit_days]
    logger.info("Captured odds-bucket days to census: %d (bounded by manifest, not a blind date range)", len(days))

    client = get_storage_client()
    census: set[str] = set()
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(census_day, client, md_bucket, d): d for d in days}
        for fut in as_completed(futures):
            done += 1
            try:
                census |= fut.result()
            except (ValueError, OSError) as exc:
                logger.warning("day %s failed: %s", futures[fut], exc)
            if done % 100 == 0 or done == len(days):
                logger.info("censused %d/%d days, distinct od_team_name so far=%d", done, len(days), len(census))

    logger.info("Final census: %d distinct od_team_name values across %d captured days", len(census), len(days))

    logger.info("Reading existing mapping gs://%s/%s", is_bucket, _MAPPING_BLOB)
    with fs.open(f"{is_bucket}/{_MAPPING_BLOB}", "rb") as fh:
        mapping = pd.read_parquet(fh)
    existing_keys = set(mapping["od_team_name"].dropna().unique())

    gap = sorted(n for n in census if n not in existing_keys)
    logger.info(
        "Coverage gap: %d od_team_name values present in MDPS shards but absent from the mapping table", len(gap)
    )

    logger.info("Reading team_mapping_v2 crosswalk gs://%s/%s for resolution", is_bucket, _TEAM_MAPPING_V2_BLOB)
    with fs.open(f"{is_bucket}/{_TEAM_MAPPING_V2_BLOB}", "rb") as fh:
        tmv2 = pd.read_parquet(fh, columns=["api_football_id", "api_football_name", "odds_api_name"])
    tmv2 = tmv2[tmv2["odds_api_name"].notna()].copy()
    tmv2["_norm"] = tmv2["odds_api_name"].map(_normalize)
    # Guard against ambiguous (duplicate-normalized) odds_api_name rows -- never
    # resolve off an ambiguous match.
    dup_norms = set(tmv2["_norm"][tmv2["_norm"].duplicated(keep=False)])
    lookup: dict[str, tuple[object, object]] = {
        str(row["_norm"]): (row["api_football_id"], row["api_football_name"])
        for _, row in tmv2.iterrows()
        if row["_norm"] not in dup_norms
    }

    resolved_rows: list[dict[str, object]] = []
    unresolved: list[str] = []
    for name in gap:
        key = _normalize(name)
        hit = lookup.get(key)
        if hit is None:
            unresolved.append(name)
            continue
        af_team_id, af_team_name = hit
        resolved_rows.append(
            {
                "af_league_id": pd.NA,
                "af_team_id": float(af_team_id),  # pyright: ignore[reportArgumentType]
                "af_team_name": af_team_name,
                "od_team_name": name,
                "data_available_at": None,
            }
        )

    logger.info(
        "Resolved %d/%d gap names via team_mapping_v2 odds_api_name crosswalk; %d remain honestly unmappable",
        len(resolved_rows),
        len(gap),
        len(unresolved),
    )
    if unresolved:
        logger.info("Unresolved names (left dropping at merge time, not fabricated): %s", unresolved)

    if not args.apply:
        logger.info("DRY RUN -- mapping table untouched. Re-run with --apply to write %d new rows.", len(resolved_rows))
        return 0

    if not resolved_rows:
        logger.info("Nothing to apply -- 0 genuinely-resolvable rows found.")
        return 0

    new_df = pd.DataFrame(resolved_rows)
    merged = pd.concat([mapping, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["od_team_name"], keep="first")
    buf = io.BytesIO()
    merged.to_parquet(buf, index=False)
    buf.seek(0)
    with fs.open(f"{is_bucket}/{_MAPPING_BLOB}", "wb") as fh:
        fh.write(buf.getvalue())
    logger.info(
        "APPLIED. Mapping table rewritten: rows_in=%d rows_added=%d rows_out=%d.",
        len(mapping),
        len(new_df),
        len(merged),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
