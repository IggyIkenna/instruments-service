#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the defi prod/catalog.parquet has 0 AAVEV3-OPTIMISM ghost instrument_ids and
#   0 3-colon MORPHO LENDING_MARKET instrument_ids (both verified 0 by this script's own report),
#   AND unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md
#   findings 5+6 are marked resolved.
"""One-off backfill — relabel two real ``instrument_id`` string bugs in the live DeFi catalog
(``prod/catalog.parquet``) in place, per the operator's migration-mechanics decision
(``instrument_id_format_canonicalization_2026_07_08.md``: "rewrite/relabel already-downloaded
data in place ... not a re-download from the source venue").

Both bugs are STRING-level drift inside the ``instrument_id`` column specifically — the
``venue``/``chain`` COLUMNS were already normalised by
``collapse_defi_drift_to_canonical_2026_06_25.py`` (2026-06-25), which rewrites ``venue``/``chain``
but never touches the ``instrument_id`` string itself. That gap is exactly what left these two
bugs live in the real catalog as of 2026-07-08 (verified directly against
``gs://instruments-store-defi-prd-central-element-323112/prod/catalog.parquet``):

Bug 1 — AAVE_V3-OPTIMISM misspelled venue-token duplicate (finding 5): 4 real rows carry
``instrument_id`` prefixed ``AAVEV3-OPTIMISM:`` (missing underscore — a stale write from before
the current ``aave_v3.py`` adapter code, which already builds the correct
``f"{self._venue_prefix}-{self._chain}"`` = ``AAVE_V3-OPTIMISM`` for every row it writes today; no
adapter/registry code produces the ghost spelling any more, confirmed by reading
``AaveV3ReferenceDataAdapter`` + every caller). Target: relabel ``AAVEV3-OPTIMISM:`` →
``AAVE_V3-OPTIMISM:`` — no merge needed, the 4 affected symbols (AAVE/LINK/LUSD/SUSD reserves,
since retired from the curated static-reserve list) don't collide with any of the 12 correctly
spelled rows.

Bug 2 — MORPHO's 3rd-colon market-address disambiguator (finding 6): all 466 real MORPHO rows
carry a 3rd colon inside the symbol segment (``MORPHO-BASE:LENDING_MARKET:USDC-EURC:0x305dd1``) —
ambiguous to any naive ``split(":")`` parser since colon is the reserved top-level
``VENUE:TYPE:SYMBOL`` delimiter. Target: dash-separate instead (matching ``morpho.py``'s
go-forward fix, shipped alongside this script) — replace the LAST colon with a dash
(``MORPHO-BASE:LENDING_MARKET:USDC-EURC-0x305dd1``). Verified zero collisions: joining on a dash
instead of a colon does not create any duplicate ``instrument_id`` across the 466 rows.

Both transforms are pure string ops on already-captured data — no re-fetch from Aave/Morpho.

Safety: writes a timestamped ``.venuefix.bak.parquet`` backup before overwriting; refuses to write
if the row count changes or a new duplicate ``instrument_id`` is introduced. Dry-run by default;
``--apply --confirm`` mutates the live blob. Idempotent — a catalog with 0 remaining ghost/3-colon
rows reports 0 changes and is a no-op on re-run.

Usage::

    cd instruments-service
    .venv/bin/python scripts/instrument_id_venue_spelling_backfill_2026_07_08.py            # dry-run
    .venv/bin/python scripts/instrument_id_venue_spelling_backfill_2026_07_08.py --apply --confirm

SSOT: ``unified-trading-pm/plans/active/issues/instrument_id_format_canonicalization_2026_07_08.md``
findings 5 + 6; ``instruments-service/docs/DEFI_INSTRUMENTS.md`` "Systemic venue-token
duplicate-spelling pattern".
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOGUE_BLOB = "prod/catalog.parquet"

_AAVE_GHOST_PREFIX = "AAVEV3-OPTIMISM:"
_AAVE_CANONICAL_PREFIX = "AAVE_V3-OPTIMISM:"


def _fix_aave_v3_optimism(instrument_id: str) -> str:
    """Relabel the misspelled AAVEV3-OPTIMISM prefix → the canonical AAVE_V3-OPTIMISM."""
    if instrument_id.startswith(_AAVE_GHOST_PREFIX):
        return _AAVE_CANONICAL_PREFIX + instrument_id[len(_AAVE_GHOST_PREFIX) :]
    return instrument_id


def _fix_morpho_third_colon(instrument_id: str, venue: str) -> str:
    """Dash-separate MORPHO's 3rd-colon market-key disambiguator: last ':' -> '-'.

    Only touches MORPHO rows whose instrument_id has exactly 3 colons (the
    VENUE:LENDING_MARKET:SYMBOL:MARKETKEY shape) — a no-op for every other row.
    """
    if venue != "MORPHO" or instrument_id.count(":") != 3:
        return instrument_id
    idx = instrument_id.rfind(":")
    return instrument_id[:idx] + "-" + instrument_id[idx + 1 :]


def backfill_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Apply both relabels; pure + idempotent. Returns (new_df, stats)."""
    stats = {"rows_in": len(df), "rows_out": len(df), "aave_fixed": 0, "morpho_fixed": 0}
    if df.empty or "instrument_id" not in df.columns:
        return df, stats

    orig_ids = df["instrument_id"].astype(str)
    venues = df["venue"].astype(str) if "venue" in df.columns else pd.Series([""] * len(df), index=df.index)

    new_ids = [_fix_morpho_third_colon(_fix_aave_v3_optimism(iid), v) for iid, v in zip(orig_ids, venues, strict=True)]

    stats["aave_fixed"] = int(orig_ids.str.startswith(_AAVE_GHOST_PREFIX).sum())
    stats["morpho_fixed"] = int(((venues == "MORPHO") & (orig_ids.str.count(":") == 3)).sum())

    work = df.copy()
    work["instrument_id"] = new_ids
    stats["rows_out"] = len(work)
    return work, stats


def _bak(blob: str, ts: str) -> str:
    if blob.endswith(".parquet"):
        return f"{blob[:-8]}.{ts}.venuefix.bak.parquet"
    return f"{blob}.{ts}.venuefix.bak"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write the fix back to GCS (default: dry-run report only).")
    ap.add_argument("--confirm", action="store_true", help="Required alongside --apply to actually mutate the blob.")
    args = ap.parse_args(argv)
    apply = bool(args.apply and args.confirm)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as well — refusing to mutate without both flags.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi", deployment_env="prd")
    st = get_storage_client(project_id=None)
    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    logger.info("Mode=%s bucket=%s blob=%s", "APPLY" if apply else "DRY-RUN", bucket, CATALOGUE_BLOB)

    raw = st.download_bytes(bucket, CATALOGUE_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))

    dup_before = int(df["instrument_id"].astype(str).duplicated().sum())
    out, stats = backfill_frame(df)
    dup_after = int(out["instrument_id"].astype(str).duplicated().sum())

    logger.info(
        "rows %d->%d | aave_v3_optimism_relabeled=%d morpho_dash_relabeled=%d | dup_instrument_id %d->%d",
        stats["rows_in"],
        stats["rows_out"],
        stats["aave_fixed"],
        stats["morpho_fixed"],
        dup_before,
        dup_after,
    )

    if stats["rows_out"] != stats["rows_in"]:
        logger.error("ABORT: row count changed (%d -> %d) — refusing to write", stats["rows_in"], stats["rows_out"])
        return 1
    if dup_after > dup_before:
        logger.error(
            "ABORT: relabel introduced %d new duplicate instrument_id value(s) — refusing to write",
            dup_after - dup_before,
        )
        return 1

    if not (stats["aave_fixed"] or stats["morpho_fixed"]):
        logger.info("Nothing to fix — catalog already clean (idempotent no-op).")
        return 0

    if apply:
        st.upload_bytes(bucket, _bak(CATALOGUE_BLOB, ts), raw)
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        st.upload_bytes(bucket, CATALOGUE_BLOB, buf.read())
        logger.info("wrote %s (backup -> %s)", CATALOGUE_BLOB, _bak(CATALOGUE_BLOB, ts))
    else:
        logger.info("Dry-run only — re-run with --apply --confirm to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
