#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: the cefi/defi/tradfi/prediction instruments-store _index objects carry
#   schema_version=9 + populated source/asset_group/pipeline_mode (verified live), and the
#   IS writer emits v9 on every new instrument capture so no new write regresses; tracked
#   under data_source_provenance_all_asset_groups_2026_06_01.md +
#   instruments_mtds_subset_consistency_remediation_2026_06_17.md.
"""Populate v9 columns on the cefi/defi/tradfi/prediction instruments-store ``_index``.

AUDIT (2026-06-19): the live ``instruments-store-{ag}-prd`` ``_index`` for cefi/defi/tradfi
had the blank-status/dedup canonicalisation applied but the v9 *column* population was NEVER
run — ``schema_version`` was a v4/v8/v9 mix (cefi 21.8% v9, defi 3.2%, tradfi 8.0%, pred
37.7%), ``source`` 0%-populated (cefi/defi/tradfi), the ``asset_group`` column ABSENT, and
``pipeline_mode`` mostly blank. The plan's "instruments-store _index v9-canonical for ALL 5
AGs — DONE" referred only to blank/dedup, NOT the v9 columns. The sports AG was closed by
``populate_sports_is_index_v9_2026_06_19.py``; this tool closes cefi/defi/tradfi/prediction.

ROW-PRESERVING for every AG EXCEPT the defi spelling-dedup (captured-preferring; collapses
legacy↔canonical venue-spelling twins of the SAME cell — never drops a captured cell that
lacks a captured twin), so captured is provably preserved modulo legitimate spelling-dedup.

Column rules (mirror the IS WRITER ``engine/orchestrator/writers.py`` lines 185-205 — the
C-#6 source⇔pipeline_mode producer contract):
  * ``pipeline_mode`` — keep an existing concrete value; fill blank/None with
    ``BATCH_INSTRUMENTS_SERVICE`` (instrument-definition rows are PRODUCER-emitted; instruments
    are always IS-produced, batch=live, single producer mode).
  * ``source`` — DERIVED PER CELL from that cell's ``pipeline_mode`` via UAC
    ``source_string_for(PipelineMode(pipeline_mode))`` (NOT a SOURCE_PRIORITY default and NOT
    the vendor — for a BATCH_INSTRUMENTS_SERVICE producer row the contract source is
    ``"instruments_service"``; prediction's gamma rows resolve to ``polymarket_gamma_api``).
  * ``asset_group`` — the constant AG (one AG per bucket).
  * ``schema_version`` → 9.

DeFi ALSO: canonicalise the ``venue`` column to the single ``PROTOCOL-CHAIN`` SSOT identity
(``ALL_DEFI_VENUES`` is 150/159 protocol-chain) via the SAME resolver the readers use
(``canonicalize_defi_manifest_venue_2026_06_14.canonicalise_venue_column``), then
captured-preferring dedup of the resulting cell-key collisions (legacy ``AAVEV3-ARBITRUM`` /
bare ``AAVE_V3``+chain → canonical ``AAVE_V3-ETHEREUM``). This is the "DeFi venue-naming
drift" fix folded into the SAME single defi ``_index`` walk to avoid two whole-corpus rewrites.

DRY-RUN by default (writes an audit projection + before/after). ``--apply`` snapshots the live
``_index`` to ``_index/snapshots/pre_is_v9_{ag}_2026_06_19.parquet`` then writes it back.
Apply order: pred → tradfi → cefi → defi (safest first; defi last — the only mutating AG).

SSOT: unified-trading-pm/plans/active/instruments_mtds_subset_consistency_remediation_2026_06_17.md
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from unified_api_contracts.canonical.crosscutting.pipeline_mode import PipelineMode, source_string_for
from unified_trading_library import get_storage_client, resolve_bucket_name

sys.path.insert(0, str(Path(__file__).resolve().parent))
from canonicalize_defi_manifest_venue_2026_06_14 import canonicalise_venue_column

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("populate_is_v9")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_PRODUCER_MODE = "batch_instruments_service"  # PipelineMode.BATCH_INSTRUMENTS_SERVICE.value
# cell-key columns (the shard atom — identical to the manifest dedup key cols when present)
_KEY_CANDIDATES = ("date", "venue", "chain", "data_type", "instrument_type", "instrument_id", "underlying", "source")


def _resolve_bucket(ag: str) -> str:
    if ag == "prediction":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store-prediction")
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group=ag)


def _source_for_pm(pm: str) -> str:
    """Canonical source for a pipeline_mode (C-#6 producer contract). Blank pm → blank."""
    pm = (pm or "").strip()
    if not pm:
        return ""
    try:
        return source_string_for(PipelineMode(pm)) or ""
    except (ValueError, KeyError):
        return ""


def populate(df: pd.DataFrame, ag: str) -> tuple[pd.DataFrame, dict[str, object]]:
    out = df.copy()
    n_before = len(out)
    cap_before = int((out["capture_status"].astype(str) == "captured").sum())

    venue_changes = 0
    dedup_dropped = 0
    if ag == "defi":
        out, venue_changes = canonicalise_venue_column(out)
        # captured-preferring dedup of the cell-key collisions the venue-canon created
        keys = [c for c in _KEY_CANDIDATES if c in out.columns]
        out["_cap"] = (out["capture_status"].astype(str) == "captured").astype(int)
        # keep=last with captured sorted last → a captured twin always wins over a non-captured
        out = out.sort_values("_cap").drop_duplicates(subset=keys, keep="last").drop(columns="_cap")
        dedup_dropped = n_before - len(out)

    # v9 columns
    if "asset_group" not in out.columns:
        out["asset_group"] = ""
    if "source" not in out.columns:
        out["source"] = ""
    if "pipeline_mode" not in out.columns:
        out["pipeline_mode"] = ""

    pm = out["pipeline_mode"].astype("string").fillna("").str.strip()
    pm = pm.where(pm.str.len() > 0, _PRODUCER_MODE)  # fill blank/None with the producer mode
    out["pipeline_mode"] = pm
    out["source"] = [_source_for_pm(p) for p in pm.tolist()]
    out["asset_group"] = ag
    out["schema_version"] = 9

    cap_after = int((out["capture_status"].astype(str) == "captured").sum())
    src = out["source"].astype("string")
    pmc = out["pipeline_mode"].astype("string")
    agc = out["asset_group"].astype("string")
    stats: dict[str, object] = {
        "rows_before": n_before,
        "rows_after": len(out),
        "venue_changes": venue_changes,
        "dedup_dropped": dedup_dropped,
        "captured_before": cap_before,
        "captured_after": cap_after,
        "schema_v9_pct": round(100 * (out["schema_version"] == 9).mean(), 2),
        "pipeline_mode_pct": round(100 * (pmc.str.len() > 0).mean(), 2),
        "source_pct": round(100 * (src.str.len() > 0).mean(), 2),
        "asset_group_pct": round(100 * (agc == ag).mean(), 2),
        "source_dist": src.value_counts(dropna=False).head(8).to_dict(),
    }
    return out, stats


def run_ag(ag: str, *, apply: bool) -> int:
    bucket = _resolve_bucket(ag)
    storage = get_storage_client()
    logger.info("── %s  gs://%s/%s ──", ag, bucket, _MANIFEST_BLOB)
    df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, _MANIFEST_BLOB)))
    before_sv = df["schema_version"].value_counts(dropna=False).to_dict() if "schema_version" in df else {}
    out, stats = populate(df, ag)
    logger.info("  before schema_version: %s", before_sv)
    for k, v in stats.items():
        logger.info("    %s = %s", k, v)

    if stats["captured_after"] < stats["captured_before"] - stats["dedup_dropped"]:
        logger.error("  ABORT: captured dropped beyond spelling-dedup — refusing to write")
        return 1

    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    proj = f"_index/audit/projected_is_v9_{ag}_2026_06_19.parquet"
    storage.upload_bytes(bucket, proj, buf.getvalue())
    logger.info("  ✎ projection → gs://%s/%s", bucket, proj)

    if not apply:
        logger.info("  DRY RUN — live _index untouched")
        return 0

    snap = f"_index/snapshots/pre_is_v9_{ag}_2026_06_19.parquet"
    sbuf = io.BytesIO()
    df.to_parquet(sbuf, index=False)
    storage.upload_bytes(bucket, snap, sbuf.getvalue())
    logger.info("  ✎ snapshot → gs://%s/%s", bucket, snap)
    storage.upload_bytes(bucket, _MANIFEST_BLOB, buf.getvalue())
    logger.info("  ✅ APPLIED %s (%d rows)", ag, stats["rows_after"])
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--asset-group",
        action="append",
        choices=["cefi", "defi", "tradfi", "prediction"],
        help="AG(s) to process (repeatable). Default: all four, in safest-first order.",
    )
    p.add_argument("--apply", action="store_true", help="Write back (snapshots first). Default: dry-run.")
    args = p.parse_args(argv)
    ags = args.asset_group or ["prediction", "tradfi", "cefi", "defi"]
    rc = 0
    for ag in ags:
        rc |= run_ag(ag, apply=args.apply)
    logger.info("done at %s", datetime.now(UTC).isoformat())
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
