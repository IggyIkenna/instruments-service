#!/usr/bin/env python3
# Epic: manifest_master
# Lifecycle: oneoff
# Delete-when: after this migration has run in prod and been verified (see
#   distinct_values_noncanonical_audit_2026_07_20.md Progress Log — RESTAKING InstrumentType)
"""Re-stamp the four liquid-restaking-token (LRT) catalogue rows from LST to RESTAKING.

Operator decision 2026-07-20/22 (``distinct_values_noncanonical_audit_2026_07_20.md``):
``InstrumentType.RESTAKING`` (``uac@bb42d8ee``) is the canonical classification for liquid
restaking tokens — ezETH (Renzo), rsETH (KelpDAO), pufETH (Puffer), weETH (ether.fi) — because
they carry EigenLayer AVS slashing risk STACKED on top of the base ETH staking slashing a plain
LST carries. The reference-data adapters (``renzo.py``/``kelpdao.py``/``puffer.py``/
``etherfi.py``) were hardcoding ``instrument_type=InstrumentType.LST`` until this same change
lands their going-forward fix; this script re-stamps the ALREADY-CATALOGUED rows so the
existing catalogue matches the going-forward writer.

Scope, deliberately narrow:
  * Only the 4 already-known LRT ``instrument_id``s below (measured against the live
    ``prod/catalog.parquet``, 2026-07-22: exactly 5 rows — RENZO has both an ETHEREUM and an
    ARBITRUM ezETH row).
  * ``instrument_type`` COLUMN value only. The ``instrument_id``/``canonical_instrument_id``
    strings keep their legacy ``:LST:`` segment UNCHANGED — this is a values-only
    reclassification, not an id/GCS-partition-path rename (mirrors the EQUITY_PERP/
    TOKENIZED_EQUITY precedent: old id segments outlive a reclassification so persisted rows +
    external string consumers stay parseable). See the adapters' module docstrings.
  * eETH (the base, unwrapped ether.fi receipt token) has NO row in this catalogue — only the
    wrapped weETH is discovered by ``etherfi.py`` — so there is nothing to re-stamp for it.
  * Does NOT touch cbETH/wBETH/sanctum/solblaze/rocket_pool/lido/stETH/wstETH — those stay
    plain LST (no EigenLayer restaking exposure).

Row count is unchanged (values only), so the catalogue monotonic guard is unaffected.
Backup-then-write, idempotent, ``--dry-run``/``--apply``.

Usage:
  python scripts/canonicalize_restaking_lrt_catalog_2026_07_22.py --dry-run
  python scripts/canonicalize_restaking_lrt_catalog_2026_07_22.py --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logger = logging.getLogger(__name__)

_CATALOG_BLOB = "prod/catalog.parquet"

#: The 4 LRT instrument_ids known to be stamped LST as of 2026-07-22 (RENZO has 2 rows —
#: ETHEREUM + ARBITRUM ezETH). Exact match on instrument_id, not a venue/symbol heuristic —
#: deliberately narrow so this script can never touch a row it wasn't measured against.
RESTAKING_LRT_INSTRUMENT_IDS: frozenset[str] = frozenset(
    {
        "ETHERFI-ETHEREUM:LST:WEETH",
        "KELPDAO-ETHEREUM:LST:RSETH",
        "PUFFER-ETHEREUM:LST:PUFETH",
        "RENZO-ARBITRUM:LST:EZETH",
        "RENZO-ETHEREUM:LST:EZETH",
    }
)


def _catalogue_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")


def _backup_path(blob: str, run_ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{run_ts}.restakinglrt.bak.parquet"
    return f"{blob}.{run_ts}.restakinglrt.bak"


def restamp_frame(
    df: pd.DataFrame, instrument_ids: frozenset[str] = RESTAKING_LRT_INSTRUMENT_IDS
) -> tuple[pd.DataFrame, int]:
    """Re-stamp ``instrument_type`` LST → RESTAKING for the named LRT rows. Pure + idempotent."""
    if df.empty or "instrument_id" not in df.columns or "instrument_type" not in df.columns:
        return df, 0
    hit = df["instrument_id"].isin(instrument_ids) & (df["instrument_type"] == "LST")
    n = int(hit.sum())
    if n == 0:
        return df, 0
    work = df.copy()
    work.loc[hit, "instrument_type"] = "RESTAKING"
    return work, n


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    apply_write = bool(args.apply)

    bucket = _catalogue_bucket()
    storage = get_storage_client()
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    raw = storage.download_bytes(bucket, _CATALOG_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    before_missing = sorted(RESTAKING_LRT_INSTRUMENT_IDS - set(df["instrument_id"]))
    out, n = restamp_frame(df)
    logger.info(
        "defi catalogue %d rows: re-stamped %d/%d target LRT rows LST -> RESTAKING (missing from catalogue: %s) mode=%s",
        len(df),
        n,
        len(RESTAKING_LRT_INSTRUMENT_IDS),
        before_missing or "none",
        "APPLY" if apply_write else "DRY-RUN",
    )
    if n == 0:
        logger.info("nothing to re-stamp — already clean (idempotent) or targets absent")
        return 0
    if not apply_write:
        for iid in sorted(RESTAKING_LRT_INSTRUMENT_IDS):
            row = out[out["instrument_id"] == iid]
            if not row.empty:
                logger.info("[dry-run] %s -> instrument_type=%s", iid, row.iloc[0]["instrument_type"])
        logger.info("[dry-run] would rewrite gs://%s/%s (row count unchanged: %d)", bucket, _CATALOG_BLOB, len(out))
        return 0
    backup = _backup_path(_CATALOG_BLOB, run_ts)
    storage.upload_bytes(bucket, backup, raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    storage.upload_bytes(bucket, _CATALOG_BLOB, buf.read())
    logger.info("backup -> gs://%s/%s ; rewrote %d rows (re-stamped %d LRT rows)", bucket, backup, len(out), n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
