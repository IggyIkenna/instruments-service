#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after all per-source out-of-scope findings are resolved and verified
"""Out-of-scope sports league audit — all sources, all data_types.

Compares each vendor source's captured/empty_confirmed manifest rows against its
current UAC league scope (``get_expected_leagues_for_source``), looking for
footystats-China/Russia-style residue: rows that exist in the manifest for league_ids
no longer (or never) in that source's subscription scope.

Sources audited (manifest source column → UAC scope lookup):
  api_football, footystats, transfermarkt, understat, odds_api, open_meteo,
  soccer_football_info

Sources skipped (no UAC league-level scope definition):
  instruments_service, mdps_odds_horizon_bucket

Findings are classified as:
  HIGH RISK   — captured rows for out-of-scope league_ids (real data that needs purging)
  LOWER RISK  — empty_confirmed rows only (bookkeeping residue, safe to drop per
                delete-safety-protocol §3a if reversibility qualifies)

Read-only — produces a report only. No manifest modifications.
Fix path: per-source purge scripts modelled on
  ``scripts/footystats_purge_out_of_scope_leagues_2026_08_07.py``.

Usage (run via run-bounded-analysis.sh to cap memory on the shared planning VM):
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    bash scripts/dev/run-bounded-analysis.sh \\
    python scripts/audit_out_of_scope_sports_leagues_2026_08_07.py
"""

from __future__ import annotations

import io
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts.canonical.domain.sports.league_data import get_expected_leagues_for_source
from unified_trading_library import get_storage_client, resolve_bucket_name

DATA_FLOOR = "2020-06-06"  # SSOT: codex/02-data/sports-2020-06-data-floor.md
INDEX_BLOB = "_index/availability_index.parquet"

# manifest source column value → UAC source_key for get_expected_leagues_for_source
SOURCES_TO_CHECK: dict[str, str] = {
    "api_football": "api_football",
    "footystats": "footystats",
    "transfermarkt": "transfermarkt",
    "understat": "understat",
    "odds_api": "odds_api",
    "open_meteo": "open_meteo",
    "soccer_football_info": "soccer_football_info",
}

# Sources that have no per-league UAC scope — listed here so unknown-source detection
# does not false-positive on them.
SOURCES_NO_LEAGUE_SCOPE: frozenset[str] = frozenset({"instruments_service", "mdps_odds_horizon_bucket"})

# Maximum number of out-of-scope league_ids to enumerate per source in the output;
# remaining count is logged as "... and N more".
MAX_LEAGUES_SHOWN = 30


def main() -> int:
    run_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%MZ")
    today = datetime.now(UTC).date().isoformat()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    print(f"Sports bucket: {bucket}", file=sys.stderr)
    client = get_storage_client()
    raw = client.download_bytes(bucket, INDEX_BLOB)

    # 5-column read to minimise in-memory footprint (~10M rows).
    manifest = pd.read_parquet(
        io.BytesIO(raw),
        columns=["date", "source", "league_id", "data_type", "capture_status"],
    )
    manifest = manifest[(manifest["date"] >= DATA_FLOOR) & (manifest["date"] <= today)]
    print(f"manifest rows (post-floor, ≤today): {len(manifest):,}", file=sys.stderr)

    # Resolve UAC league scope for every audited source up-front (fast, no I/O).
    expected: dict[str, frozenset[str]] = {
        src: frozenset(lg.league_id for lg in get_expected_leagues_for_source(uac_key))
        for src, uac_key in SOURCES_TO_CHECK.items()
    }

    print(f"\n=== Out-of-scope sports league audit ({run_ts}) ===")
    print("Compares manifest captured/empty_confirmed rows against current UAC scope.")
    print()

    any_findings = False

    for manifest_src, _uac_src in sorted(SOURCES_TO_CHECK.items()):
        scope = expected[manifest_src]
        src_df = manifest[manifest["source"].astype(str) == manifest_src]

        if src_df.empty:
            print(
                f"[{manifest_src}] ⚠  NO ROWS found — verify source column value "
                f"(expected '{manifest_src}' in manifest source column)"
            )
            continue

        captured = src_df[src_df["capture_status"] == "captured"]
        ec = src_df[src_df["capture_status"] == "empty_confirmed"]

        cap_oos = captured[~captured["league_id"].astype(str).isin(scope)]
        ec_oos = ec[~ec["league_id"].astype(str).isin(scope)]

        if cap_oos.empty and ec_oos.empty:
            all_league_ids = set(captured["league_id"].astype(str).unique()) | set(ec["league_id"].astype(str).unique())
            print(
                f"[{manifest_src}] ✓ CLEAN — UAC scope={len(scope)} leagues, "
                f"manifest has data for {len(all_league_ids)} leagues, 0 out-of-scope"
            )
            continue

        any_findings = True
        print(f"[{manifest_src}] *** OUT-OF-SCOPE RESIDUE (UAC scope={len(scope)} leagues) ***")

        if not cap_oos.empty:
            oos_leagues = sorted(cap_oos["league_id"].astype(str).unique())
            print(f"  HIGH RISK: {len(cap_oos):,} captured rows across {len(oos_leagues)} out-of-scope league_id(s):")
            for lg in oos_leagues[:MAX_LEAGUES_SHOWN]:
                lg_df = cap_oos[cap_oos["league_id"].astype(str) == lg]
                dt_counts = lg_df["data_type"].astype(str).value_counts()
                dts = ", ".join(f"{dt}={cnt}" for dt, cnt in dt_counts.items())
                print(f"    {lg}: {dts}")
            if len(oos_leagues) > MAX_LEAGUES_SHOWN:
                print(f"    ... and {len(oos_leagues) - MAX_LEAGUES_SHOWN} more leagues")

        if not ec_oos.empty:
            oos_leagues_ec = sorted(ec_oos["league_id"].astype(str).unique())
            print(
                f"  LOWER RISK: {len(ec_oos):,} empty_confirmed rows across "
                f"{len(oos_leagues_ec)} out-of-scope league_id(s):"
            )
            for lg in oos_leagues_ec[:MAX_LEAGUES_SHOWN]:
                lg_df = ec_oos[ec_oos["league_id"].astype(str) == lg]
                dt_counts = lg_df["data_type"].astype(str).value_counts()
                dts = ", ".join(f"{dt}={cnt}" for dt, cnt in dt_counts.items())
                print(f"    {lg}: {dts}")
            if len(oos_leagues_ec) > MAX_LEAGUES_SHOWN:
                print(f"    ... and {len(oos_leagues_ec) - MAX_LEAGUES_SHOWN} more leagues")

        print()

    # Detect any source in the manifest not covered by either set.
    all_known = set(SOURCES_TO_CHECK.keys()) | SOURCES_NO_LEAGUE_SCOPE
    other = manifest[~manifest["source"].astype(str).isin(all_known)]
    if not other.empty:
        unknown_srcs = sorted(other["source"].astype(str).unique())
        any_findings = True
        print(f"⚠  UNKNOWN sources in manifest (not in SOURCES_TO_CHECK or SOURCES_NO_LEAGUE_SCOPE): {unknown_srcs}")

    # Summary for skipped sources.
    skipped = manifest[manifest["source"].astype(str).isin(SOURCES_NO_LEAGUE_SCOPE)]
    if not skipped.empty:
        print(
            "[SKIPPED] Sources with no UAC per-league scope — not audited here; "
            "check separately if out-of-scope residue is suspected:"
        )
        for src in sorted(SOURCES_NO_LEAGUE_SCOPE):
            src_df = skipped[skipped["source"].astype(str) == src]
            if src_df.empty:
                continue
            cap_n = int((src_df["capture_status"] == "captured").sum())
            ec_n = int((src_df["capture_status"] == "empty_confirmed").sum())
            print(f"  {src}: {len(src_df):,} total (captured={cap_n:,}, empty_confirmed={ec_n:,})")

    print()
    if any_findings:
        print("=== VERDICT: OUT-OF-SCOPE RESIDUE FOUND — see per-source breakdown above ===")
        return 1
    print("=== VERDICT: CLEAN — no out-of-scope captured/empty_confirmed rows found ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
