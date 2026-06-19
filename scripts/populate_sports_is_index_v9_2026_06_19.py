#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: sports instruments-store _index v9-column population (schema_version=9 +
#   asset_group + source) applied to live + verified; fleet-wide IS v9 populate tracked
#   under data_source_provenance_all_asset_groups_2026_06_01.md.
"""Populate v9 columns on the SPORTS instruments-store ``_index``.

AUDIT (2026-06-19): the live ``instruments-store-sports-prd`` ``_index`` had the
blank-status/dedup canonicalisation applied (blank_status=0, dup_cells=0) but the v9
*column* population was NEVER run — ``schema_version`` was a v4/v5/v6/v8 mix (only 75k of
2.6M at v9), ``source`` was 0%-populated, ``asset_group`` only 13k/2.6M. This is a FLEET-WIDE
instruments-store gap (cefi/tradfi/defi `_index` are in the same state — the plan's
"instruments-store _index v9-canonical for ALL 5 AGs — DONE" referred only to blank/dedup,
not the v9 columns). This tool closes it for SPORTS; the other AGs + the live-writer
auto-stamp are tracked under ``data_source_provenance_all_asset_groups_2026_06_01.md``.

ROW-PRESERVING (never adds/drops a row — only fills 3 columns), so captured is provably
preserved:
  * ``schema_version`` → 9 (canonical).
  * ``asset_group`` → ``"sports"`` (constant — one AG per bucket).
  * ``source`` → the canonical vendor from the UAC SSOT
    ``unified_api_contracts.sports.get_source_for_data_type(data_type)``. When the SSOT does
    not map a data_type (the retired/catalog references LEAGUES/VENUES/SFI_LEAGUES/
    SFI_STANDINGS/TRANSFERMARKT_LEAGUES), the existing manifest ``venue`` is used as the
    source IF it is already a known lowercase vendor token (api_football/footystats/odds_api/
    …); otherwise left blank (honest — the SSOT defines what is source-attributable; never a
    fabricated source). ``pipeline_mode`` is left untouched (instruments-store is reference
    data — no batch/live mode; no pipeline_mode twin model, per the plan).

DRY-RUN by default (writes a projection to ``_index/audit/`` + before/after). ``--apply``
snapshots the live ``_index`` then writes it back.
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import gcsfs
import pandas as pd
from unified_api_contracts.sports import get_source_for_data_type
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("populate_sports_is_v9")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_KNOWN_VENDORS = frozenset(
    {
        "api_football",
        "footystats",
        "understat",
        "transfermarkt",
        "soccer_football_info",
        "open_meteo",
        "odds_api",
        "mdps_odds_horizon_bucket",
    }
)


def _resolve_source(data_type: str, venue: str) -> str:
    """Canonical vendor source for a (data_type, venue), or "" when not attributable."""
    if data_type:
        mapped = get_source_for_data_type(data_type) or get_source_for_data_type(data_type.upper())
        if mapped:
            return mapped
    v = (venue or "").strip().lower()
    return v if v in _KNOWN_VENDORS else ""


def populate(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    out = df.copy()
    n = len(out)
    if "asset_group" not in out.columns:
        out["asset_group"] = ""
    if "source" not in out.columns:
        out["source"] = ""

    dt = out["data_type"].astype("string").fillna("")
    venue = out["venue"].astype("string").fillna("")
    out["source"] = [_resolve_source(d, v) for d, v in zip(dt.tolist(), venue.tolist(), strict=True)]
    out["asset_group"] = "sports"
    out["schema_version"] = 9

    src = out["source"].astype("string")
    stats = {
        "rows": n,
        "schema_v9": int((out["schema_version"] == 9).sum()),
        "asset_group_sports": int((out["asset_group"].astype("string") == "sports").sum()),
        "source_populated": int((src.notna() & (src.str.len() > 0)).sum()),
        "source_blank_unmapped": int((src.isna() | (src.str.len() == 0)).sum()),
    }
    return out, stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write the populated index back to live (snapshots first).")
    args = p.parse_args(argv)

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    fs = gcsfs.GCSFileSystem()
    logger.info("Reading live _index gs://%s/%s", bucket, _MANIFEST_BLOB)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)

    before_sv = df["schema_version"].value_counts(dropna=False).to_dict() if "schema_version" in df else {}
    out, stats = populate(df)
    logger.info("Before schema_version: %s", before_sv)
    for k, v in stats.items():
        logger.info("  %s = %d", k, v)
    # source distribution preview
    logger.info("source distribution: %s", out["source"].astype("string").value_counts(dropna=False).head(12).to_dict())

    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    buf.seek(0)
    proj = f"{bucket}/_index/audit/projected_sports_is_v9_2026_06_19.parquet"
    with fs.open(proj, "wb") as fh:
        fh.write(buf.getvalue())
    logger.info("Wrote projection → gs://%s", proj)

    if not args.apply:
        logger.info("DRY RUN — live _index untouched.")
        return 0

    stamp = datetime.now(UTC).strftime("%Y%m%d")
    snap = f"{bucket}/_index/snapshots/pre_sports_is_v9_{stamp}.parquet"
    sbuf = io.BytesIO()
    df.to_parquet(sbuf, index=False)
    sbuf.seek(0)
    with fs.open(snap, "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info("Snapshot → gs://%s", snap)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "wb") as fh:
        fh.write(buf.getvalue())
    logger.info("APPLIED. Live _index v9-columns populated (%d rows).", stats["rows"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
