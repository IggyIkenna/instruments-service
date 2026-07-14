#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after BITGET-FUTURES's 16 real dated-quarterly FUTURE rows are confirmed present
#   in prod/catalog.parquet (verified by this script's own dry-run report showing 0 new/16
#   already_present), AND
#   unified-trading-pm/plans/active/issues/cefi_layer1_denominator_gaps_2026_07_03.md's G4
#   BITGET-FUTURES finding (2026-07-14, slot-2) is marked resolved.
"""One-off backfill — append BITGET-FUTURES's 16 real dated-quarterly FUTURE instruments
directly to ``prod/catalog.parquet``.

Root cause (``cefi_layer1_denominator_gaps_2026_07_03.md``, finding 2026-07-14): BITGET-FUTURES's
16 real dated-quarterly FUTURE symbols (e.g. ``BTCUSDH25``) only became resolvable this session
(``instruments-service@cd902fb1`` fixed base/quote parsing, ``@75bdf02d`` fixed the coin-margined
``margin_type``) — but ``prod/catalog.parquet`` is a pure roll-up of the
``instrument_availability/by_date/**`` snapshot history (see ``build_instrument_catalogue.py``'s
own docstring): an instrument's ``available_from``/``available_to`` window is derived from the
first/last day it APPEARS in a by_date snapshot. Every one of these 16 symbols expired (Tardis's
own ``availableTo``) before today (2026-07-14) — verified live below — so a symbol that expired
before its first correctly-parsing fetch can never enter the catalogue via the normal daily
refresh, no matter how many times that refresh re-runs today or in the future (same bug class as
the 2026-07-09 Bybit/Kraken-Futures precedent, ``canonicalize_bybit_kraken_futures_catalog_
2026_07_09.py``'s docstring).

Fix (this script): re-fetch BITGET-FUTURES's full instrument universe via the now-fixed
``TardisReferenceDataAdapter.get_instruments()`` — cheap, single REST call, no whole-corpus GCS
walk needed since Tardis's own ``availableSince``/``availableTo`` metadata already IS the lifecycle
window — filter to the 16 real FUTURE rows, and APPEND them directly to ``prod/catalog.parquet``
(rather than relying on ``build_instrument_catalogue.py``'s by_date roll-up, which would also
require backfilling 16 x ~2-year-span ``instrument_availability/by_date/**`` snapshot files — the
more expensive, whole-corpus-walk-adjacent path).

Safety (matches the established ``canonicalize_*_catalog_2026_07_*.py`` pattern): writes a
timestamped backup blob before overwriting; refuses to write if the append introduces a new
duplicate ``instrument_id`` or the resulting row count doesn't match input + appended exactly;
idempotent — rows already present in the catalogue (by ``instrument_id``) are skipped, so a
re-run after a partial/duplicate apply is a safe no-op. Dry-run by default; ``--apply --confirm``
mutates.

Usage::

    cd instruments-service
    GCP_PROJECT_ID=central-element-323112 .venv/bin/python \\
        scripts/recapture_bitget_futures_dated_futures_2026_07_14.py

    GCP_PROJECT_ID=central-element-323112 .venv/bin/python \\
        scripts/recapture_bitget_futures_dated_futures_2026_07_14.py --apply --confirm

SSOT: ``unified-trading-pm/plans/active/issues/cefi_layer1_denominator_gaps_2026_07_03.md``
(2026-07-14 finding, G4 BITGET-FUTURES).
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import io
import logging
import os

import pandas as pd
from build_instrument_catalogue import CATALOG_COLUMNS
from unified_api_contracts import is_in_mvp_capture_universe
from unified_api_contracts.internal import InstrumentRecord, InstrumentType
from unified_trading_library import get_storage_client, resolve_bucket_name

from instruments_service.reference_data.adapters.cefi.tardis.adapter import TardisReferenceDataAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOGUE_BLOB = "prod/catalog.parquet"
VENUE = "BITGET-FUTURES"
_EXCHANGE = "bitget-futures"
_EXPECTED_FUTURE_COUNT = 16


async def _fetch_future_records() -> list[InstrumentRecord]:
    """Re-fetch BITGET-FUTURES's full universe live and filter to real FUTURE rows.

    Single REST call (Tardis's free, no-auth ``/v1/exchanges/{exchange}`` metadata endpoint —
    same call the daily catalogue refresh already makes); no whole-corpus GCS walk.
    """
    adapter = TardisReferenceDataAdapter(exchanges=[_EXCHANGE], canonical_venue_override=VENUE)
    records = await adapter.get_instruments()
    return [r for r in records if r.instrument_type == InstrumentType.FUTURE]


def _row_from_record(record: InstrumentRecord) -> dict[str, object]:
    """Build one ``CATALOG_COLUMNS``-shaped row from a live-fetched FUTURE ``InstrumentRecord``.

    Mirrors ``build_instrument_catalogue.py``'s own per-date-row -> catalogue-row field mapping
    (``available_from``/``available_to`` as ISO date strings, ``margin_type``/``instrument_type``
    as their enum ``.value``, ``canonical_instrument_id`` mirroring the already-canonical
    ``instrument_key`` the adapter constructs) so this appended row is indistinguishable from one
    the normal daily roll-up would have produced, had the parsing fixes landed before these
    contracts expired.
    """
    base = (record.base_asset or "").strip().upper()
    margin_type = record.margin_type.value if record.margin_type is not None else ""
    available_from = record.available_from_datetime.date().isoformat() if record.available_from_datetime else None
    # available_to for a dated FUTURE is its real contract expiry (venue truth) — matches
    # build_instrument_catalogue.py's `agg.expiry is not None -> available_to = expiry.isoformat()`.
    available_to = record.expiry.date().isoformat() if record.expiry is not None else available_from
    mvp = is_in_mvp_capture_universe(VENUE, base, "FUTURE", has_perp_for_base=True)
    return {
        "instrument_id": record.instrument_key,
        "instrument_type": record.instrument_type.value,
        "venue": VENUE,
        "chain": "",
        "league_id": "",
        "available_from": available_from,
        "available_to": available_to,
        "market_created_at": None,
        "settlement_time": None,
        "data_type": None,
        "underlying": record.underlying or base,
        "raw_symbol": record.raw_symbol,
        "base_asset": base,
        "canonical_instrument_id": record.canonical_instrument_id or record.instrument_key,
        "mvp": mvp,
        "margin_type": margin_type,
        "glued_pair_id": "",
        "pool_address": "",
    }


def run(apply: bool) -> int:
    records = asyncio.run(_fetch_future_records())
    logger.info("Fetched %d BITGET-FUTURES FUTURE record(s) from Tardis", len(records))
    if len(records) != _EXPECTED_FUTURE_COUNT:
        logger.warning(
            "Expected %d real dated-quarterly FUTURE symbols per the 2026-07-14 finding, got %d — "
            "the real universe may have changed since; proceeding with what Tardis returned now.",
            _EXPECTED_FUTURE_COUNT,
            len(records),
        )
    if not records:
        logger.error("ABORT: 0 FUTURE records fetched — refusing to run.")
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi", deployment_env="prd")
    st = get_storage_client(project_id=None)
    raw = st.download_bytes(bucket, CATALOGUE_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    rows_before = len(df)
    existing_ids = set(df["instrument_id"].astype(str))

    new_rows: list[dict[str, object]] = []
    skipped_existing = 0
    for record in records:
        row = _row_from_record(record)
        if row["instrument_id"] in existing_ids:
            skipped_existing += 1
            continue
        new_rows.append(row)

    logger.info("new=%d already_present=%d (of %d fetched)", len(new_rows), skipped_existing, len(records))
    if not new_rows:
        logger.info("Nothing to append — idempotent no-op (all fetched rows already present).")
        return 0

    # Stop-on-surprise: TWO OR MORE genuinely distinct real raw_symbols can resolve to the
    # SAME canonical instrument_id when Tardis's own `availableTo` (the source `expiry`
    # falls back to) happens to coincide — live-verified 2026-07-14 via
    # api.tardis.dev/v1/exchanges/bitget-futures: BTCUSDM26 (Jun-2026 contract) and
    # BTCUSDU26 (Sep-2026 contract) BOTH report availableTo=2026-04-28 (same for their ETH
    # siblings). This is a pre-existing collision risk in the SHARED
    # `_build_canonical_future_key` (every CeFi venue's dated-future capture routes through
    # it, parsing.py) — fixing it means changing shared instrument_key construction other
    # venues also depend on, out of scope for this one-off append. Never silently pick one
    # of two distinct real contracts to keep (data loss) — skip the WHOLE collision group,
    # append only the clean rows, and surface the skipped group for a follow-up fix.
    id_to_raw: dict[str, set[str]] = {}
    for row in new_rows:
        id_to_raw.setdefault(str(row["instrument_id"]), set()).add(str(row["raw_symbol"]))
    colliding_ids = {iid for iid, raws in id_to_raw.items() if len(raws) > 1}
    if colliding_ids:
        for iid in sorted(colliding_ids):
            logger.warning(
                "SKIP collision group %s: %d distinct real raw_symbols map to this one canonical id "
                "(%s) — needs a shared-code disambiguation fix, not silently resolved here",
                iid,
                len(id_to_raw[iid]),
                sorted(id_to_raw[iid]),
            )
        new_rows = [r for r in new_rows if str(r["instrument_id"]) not in colliding_ids]
    if not new_rows:
        logger.info("Nothing left to append after collision skip — no-op.")
        return 0

    new_df = pd.DataFrame(new_rows, columns=list(CATALOG_COLUMNS))
    out = pd.concat([df, new_df], ignore_index=True)

    dup_before = int(df["instrument_id"].astype(str).duplicated().sum())
    dup_after = int(out["instrument_id"].astype(str).duplicated().sum())
    if dup_after > dup_before:
        logger.error(
            "ABORT: append introduced %d new duplicate instrument_id value(s) — refusing to write",
            dup_after - dup_before,
        )
        return 1
    if len(out) != rows_before + len(new_rows):
        logger.error(
            "ABORT: row count math mismatch (%d -> %d, expected +%d) — refusing to write",
            rows_before,
            len(out),
            len(new_rows),
        )
        return 1

    logger.info("rows %d -> %d (+%d)", rows_before, len(out), len(new_rows))
    for row in new_rows:
        logger.info(
            "  %s (available_from=%s available_to=%s mvp=%s)",
            row["instrument_id"],
            row["available_from"],
            row["available_to"],
            row["mvp"],
        )

    if not apply:
        logger.info("DRY-RUN — pass --apply --confirm to write.")
        return 0

    ts = _dt.datetime.now(_dt.UTC).strftime("%Y%m%d-%H%M%S")
    backup_blob = f"{CATALOGUE_BLOB[:-8]}.{ts}.bitgetfuturesfix.bak.parquet"
    st.upload_bytes(bucket, backup_blob, raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    st.upload_bytes(bucket, CATALOGUE_BLOB, buf.read())
    logger.info("wrote %s (backup -> %s)", CATALOGUE_BLOB, backup_blob)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--apply", action="store_true", help="Write the new rows back to GCS (default: dry-run report only)."
    )
    ap.add_argument("--confirm", action="store_true", help="Required alongside --apply to actually mutate the blob.")
    args = ap.parse_args(argv)
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm as well — refusing to mutate without both flags.")
        return 1
    # resolve_bucket_name needs the project env for the {GCP_PROJECT_ID} template fragment
    # (same pattern as canonicalize_sports_league_id_schema_2026_06_24.py).
    if not os.environ.get("GCP_PROJECT_ID"):
        os.environ["GCP_PROJECT_ID"] = "central-element-323112"
    return run(apply=bool(args.apply and args.confirm))


if __name__ == "__main__":
    raise SystemExit(main())
