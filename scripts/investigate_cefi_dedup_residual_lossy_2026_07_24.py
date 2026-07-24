#!/usr/bin/env python
# Epic: cefi_master
# Lifecycle: oneoff (re-runnable diagnostic)
# Delete-when: Surface C v2 manifest apply is DONE + verified and the
#              _CHAIN_LOSSY_TOLERANCE_MAX residual in complete_cefi_manifest_canonical_dedup_v2_2026_07_20.py
#              is either closed or re-baselined against a fresh measurement (this script is how you
#              get that fresh measurement — do not delete while that tolerance constant still cites
#              2026-07-24 numbers).
"""Drill-down into the residual "lossy" PIN_ATOM/effective-key groups
`complete_cefi_manifest_canonical_dedup_v2_2026_07_20.py`'s `_chain_merge_safety` flags — i.e. groups
holding >1 CAPTURED row with a DIFFERING non-zero `row_count` that a manifest de-dup would otherwise
silently discard one of. Read-only: reruns the SAME v1 canonicalize + v2 transforms + eu-reconcile
pipeline as that script's `main()` (dry-run column projection), then instead of just counting the
residual, prints venue/instrument_type/underlying/data_type/chain breakdowns AND writes the full
row-level detail to a CSV so the population can be understood before deciding whether it is safe to
`--apply` past.

WHY THIS EXISTS: found 2026-07-24 while root-causing the "3304 lossy PIN_ATOM groups" Surface-C
apply blocker (`plans/active/issues/cefi_chain_drop_root_cause_and_heavy_io_vm_rule_2026_07_24.md`
Findings 2 + 5). The DERIBIT chain-BUNDLE `underlying`-key gap fix alone reduced it to 92; this
script is what identified those 92 as 64 ASTER (a `chain`-tagging transition, fixed by folding
`chain` into the key too) + 28 BITFINEX-SPOT/BYBIT-SPOT (a genuinely irreducible market-wide
blank-id/underlying/chain aggregate-shard near-duplicate, accepted as a small explicit tolerance —
see `_CHAIN_LOSSY_TOLERANCE_MAX` in the v2 script). Re-run this whenever that tolerance needs
re-verifying against the live corpus (e.g. after further backfill/consolidator activity, or before
raising/lowering the tolerance band) — the numbers in this docstring and the tolerance constant both
have a date on them, not a permanent guarantee.

TRAP HIT while building this: the emitted `Bash run_in_background` timeout parameter (up to 600s)
turned out to enforce a hard kill of the backgrounded process on this shared host — NOT just the
foreground-wait cap the tool docs describe — so a >10min run silently died with no useful signal
beyond exit 143. Omitting `timeout` (letting the harness auto-convert to a persistent background
task after its own default cap) let the SAME command run to natural completion. Separately, this
exact shared host runs `earlyoom -m 10 -s 10` (SIGTERM-first): a multi-GB-RSS pandas run competing
with other concurrent agent sessions' memory use can get killed by that daemon regardless of the
`Bash` tool's own timeout handling — signal 143 is the same in both cases, so don't assume a kill is
your own timeout without checking `earlyoom` is even running (`systemctl status earlyoom`) and
`free -h`/`uptime` at the time.

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 CLOUD_PROVIDER=gcp DEPLOYMENT_ENV=prod \\
      UNIFIED_ENVIRONMENT=prod CLOUD_MOCK_MODE=false \\
      .venv/bin/python scripts/investigate_cefi_dedup_residual_lossy_2026_07_24.py [--csv-out PATH]
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_BASE = os.path.dirname(os.path.abspath(__file__))
_V1_PATH = os.path.join(_BASE, "complete_cefi_manifest_canonical_dedup_2026_07_17.py")
_V2_PATH = os.path.join(_BASE, "complete_cefi_manifest_canonical_dedup_v2_2026_07_20.py")


def _load_module(name: str, path: str) -> object:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - import guard
        raise RuntimeError(f"cannot load module at {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


v1 = _load_module("v1_dedup", _V1_PATH)
v2 = _load_module("v2_dedup", _V2_PATH)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--csv-out",
        default=None,
        help="Where to write the full row-level detail CSV (default: a tempfile.gettempdir() path).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    csv_out = args.csv_out or str(Path(tempfile.gettempdir()) / "cefi_dedup_residual_lossy_2026_07_24.csv")

    bucket = v1.resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    inst_bucket = v1.resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    storage = v1.get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]

    cat = v1._load_catalog(storage, inst_bucket)
    wire_map = v1._build_wire_map(cat)
    marker_base = v1._build_marker_base_map(cat)
    itype_infer = v1._build_itype_infer_map(cat)
    base_quote_map = v1._build_base_quote_map(cat)
    base_quote_date_map = v1._build_base_quote_date_map(cat)
    catalog_ids = v1._catalog_id_set(cat)
    catalog_wires = v1._catalog_wire_set(cat)

    per_vm_names = [
        meta.name
        for meta in storage.list_blobs(bucket, prefix=v1.PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
        if meta.name.endswith(".parquet") and "/snapshots/" not in meta.name
    ]
    blobs = [v1.INDEX_BLOB, *per_vm_names]

    dfs: dict[str, pd.DataFrame] = {}
    all_captured_keys: list[pd.DataFrame] = []
    for blob in blobs:
        try:
            df = v1._load(storage, bucket, blob, columns=v1._DRYRUN_COLS)
        except Exception as exc:
            if blob == v1.INDEX_BLOB:
                raise
            logger.warning("skip %s: %s", blob, exc)
            continue
        df, _added = v1._ensure_cols(df)
        df1, _keys, _stats = v1._canonicalize_blob(
            df, wire_map, marker_base, itype_infer, base_quote_map, base_quote_date_map, catalog_ids, catalog_wires
        )
        df2, _v2stats = v2._apply_v2_transforms(df1, v2._OKX_OPTION_VENUE_DEFAULT, "purge")
        dfs[blob] = df2
        if not df2.empty and set(v1.SHARD_KEY_COLS).issubset(df2.columns) and "capture_status" in df2.columns:
            cap_out = df2["capture_status"].astype(str) == "captured"
            if bool(cap_out.any()):
                all_captured_keys.append(df2.loc[cap_out, v1.SHARD_KEY_COLS].drop_duplicates().reset_index(drop=True))
        logger.info("loaded+canonicalised %s (%d rows)", blob, len(df2))

    combined = (
        pd.concat(all_captured_keys, ignore_index=True).drop_duplicates() if all_captured_keys else v1._empty_keys()
    )
    for blob in dfs:
        dfs[blob], n = v1._reconcile_eu_duplicates(dfs[blob], combined)
        logger.info("eu-reconciled %s (dropped=%d)", blob, n)

    # Drill into the residual lossy groups using the SAME effective key `_dedup_blob` collapses on.
    rows_out = []
    for blob, df in dfs.items():
        if df.empty or "chain" not in df.columns or not set(v1.PIN_ATOM).issubset(df.columns):
            continue
        key = v1._effective_dedup_key(df)
        rc = pd.to_numeric(df["row_count"], errors="coerce").fillna(0)
        capd = (df["capture_status"].astype(str) == "captured") & (rc > 0)
        sub = df.loc[capd].copy()
        sub["_key"] = key[capd]
        sub["_rc"] = rc[capd]
        ndist = sub.groupby("_key")["_rc"].nunique()
        lossy_keys = ndist[ndist > 1].index
        hit = sub[sub["_key"].isin(lossy_keys)]
        if not hit.empty:
            hit = hit.assign(_blob=blob)
            rows_out.append(hit)

    if not rows_out:
        logger.info("NO residual lossy rows found (0 groups) — the corpus is fully clean of this invariant.")
        return 0

    allhit = pd.concat(rows_out, ignore_index=True)
    allhit.to_csv(csv_out, index=False)
    logger.info("Wrote FULL detail (%d rows) to %s BEFORE any further logging", len(allhit), csv_out)
    logger.info("Residual lossy rows: %d across %d distinct effective-keys", len(allhit), allhit["_key"].nunique())

    logger.info("=== venue breakdown ===")
    logger.info("\n%s", allhit["venue"].value_counts().to_string())
    logger.info("=== instrument_type breakdown ===")
    logger.info("\n%s", allhit["instrument_type"].value_counts().to_string())
    logger.info("=== data_type breakdown ===")
    logger.info("\n%s", allhit["data_type"].value_counts().to_string())
    iid_blank = allhit["instrument_id"].fillna("").astype(str).str.strip() == ""
    logger.info("instrument_id blank=%d non_blank=%d", int(iid_blank.sum()), int((~iid_blank).sum()))
    und_blank = allhit["underlying"].fillna("").astype(str).str.strip() == ""
    logger.info("underlying blank=%d non_blank=%d", int(und_blank.sum()), int((~und_blank).sum()))

    cols = [
        "_blob",
        "venue",
        "instrument_type",
        "data_type",
        "instrument_id",
        "underlying",
        "chain",
        "pipeline_mode",
        "date",
        "_rc",
    ]
    cols = [c for c in cols if c in allhit.columns]
    logger.info("=== sample rows (up to 60) ===\n%s", allhit.sort_values(["venue", "_key"]).head(60)[cols].to_string())

    logger.info("=== first 15 groups, full detail ===")
    for i, k in enumerate(sorted(allhit["_key"].unique())):
        if i >= 15:
            break
        grp = allhit[allhit["_key"] == k]
        logger.info("--- key=%r (n=%d) ---\n%s", k, len(grp), grp[cols].to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
