#!/usr/bin/env python3
# Epic: cefi_master
# Lifecycle: permanent
# Delete-when: NA
"""Measure the CURRENT CeFi catalogue-enumeration-gap count (re-runnable SSOT).

Provenance: 2026-07-23 investigation into the historical ~422 catalogue-
enumeration-gap figure (OKX-SPOT fiat-quote, COINBASE-SPOT crypto-quote,
BITGET-FUTURES CME-letter-month gaps). This script mechanises that
investigation so the next person does not have to re-derive it by hand.

WHAT "GAP" MEANS: a manifest row (real Tardis capture evidence, `captured` /
`empty_confirmed` / `attempted_failed`) whose instrument has NO backing row in
the rolled-up ``prod/catalog.parquet`` reference-data catalogue -- i.e. the
instrument was captured/attempted but a future catalogue enumeration run would
never list it. Two independent mechanisms produce this gap today:

  Case class "spot-quote-gap" (live code gate, NOT yet fixed):
    ``instruments_service/reference_data/adapters/cefi/tardis/parsing.py``'s
    ``_passes_asset_filter()`` calls UAC's ``accepted_quotes_for_venue(venue)``
    (``unified_api_contracts/registry/cefi_instrument_universe.py``), which
    returns only the fleet-default {USD, USDC, USDT} unless the venue has a
    ``_CEFI_VENUE_QUOTE_EXTENSIONS`` entry (today: only UPBIT->KRW and
    BITFINEX-FUTURES->BTC). Any SPOT_PAIR quote outside that set is silently
    dropped from every future reference-data enumeration for that venue.
    Measured live 2026-07-23: fires for OKX-SPOT (BTC/TRY/ETH quotes) and
    COINBASE-SPOT (ETH quote). Generalized here across EVERY venue, not
    hand-scoped to those two -- any venue that later hits the same gate is
    picked up automatically.

  Case class "cme-letter-month-gap" (stale catalogue ROLLUP, parser already
    fixed 2026-07-14):
    CME-letter-month dated-future wire ids (``BTCUSDH26``, ``ETHUSDZ24``, ...
    -- Bybit's own no-dash coin-margined-quarterly shape, ``_BYBIT_MONTH_CODE_RE``
    in parsing.py, reused for ``exchange in ("bybit", "bitget-futures")`` since
    2026-07-14) now parse correctly and pass ``_passes_asset_filter`` (quote
    resolves to USD, which is fleet-default-accepted). But
    ``scripts/build_instrument_catalogue.py``'s ``catalog.parquet`` is a
    cumulative ROLL-UP over ``instrument_availability/by_date/...`` snapshots;
    snapshots captured BEFORE the 2026-07-14 fix don't carry these rows, and no
    snapshot since has re-included them into the rollup. Measured live
    2026-07-23: fires for BITGET-FUTURES (33 rows, all ``capture_status=captured``
    -- real tick data with zero catalogue backing). BYBIT's own instances of
    this exact wire shape already have catalogue rows (Bybit is the shape's
    origin venue), so they never appear here -- confirms the fix is
    catalogue-staleness, not a parser regression.

READS (bounded only -- SINGLE-WALK DISCIPLINE, no fresh whole-corpus GCS walk):
  1. ``prod/catalog.parquet`` from the cefi instruments-store bucket -- ONE
     bounded object download (mirrors
     ``complete_cefi_manifest_canonical_dedup_2026_07_17.py``'s own
     ``_load_catalog``). Used to build a per-venue set of catalogue-enumerated
     wire tokens (``raw_symbol``, upper-cased).
  2. ``_index/availability_index.parquet`` (the main manifest index) + every
     blob under ``_index/per_vm/`` from the cefi market-data bucket. The
     per-VM prefix is discovered via ONE bounded ``list_blobs(prefix=...)``
     call against a known small index directory (today: 1 object,
     ``_legacy_seed.parquet``) -- this is a listing of a KNOWN INDEX PREFIX,
     not a corpus walk of the raw tick data (which holds tens of millions of
     objects). If a future per-VM fan-out grows this prefix to hundreds of
     shards, this remains bounded (index shards only, never raw ticks).
  3. ``unified_api_contracts.registry.cefi_instrument_universe.accepted_quotes_for_venue``
     -- imported LIVE (never copied) so this script tracks the UAC SSOT as it
     evolves, rather than a hardcoded snapshot that silently drifts stale.

Read-only by construction: this measures a gap, it does not fix it and never
writes to GCS (no snapshot, no report artifact, no manifest mutation). Any fix
(a UAC quote-extension operator decision, or a re-run of
``build_instrument_catalogue.py`` after a fresh capture) is separate, later
work that consumes this script's output.

STOP-ON-SURPRISE: the total gap-row count is checked against
``_TOTAL_GAP_MIN``/``_TOTAL_GAP_MAX`` (see below) -- a future run outside that
band logs a loud warning (exit code 2) instead of being trusted blindly.

Usage:
    python scripts/measure_cefi_catalogue_enumeration_gap_2026_07_23.py
    python scripts/measure_cefi_catalogue_enumeration_gap_2026_07_23.py --json-out /tmp/gap_detail.csv
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
from dataclasses import dataclass

import pandas as pd
from unified_api_contracts.registry.cefi_instrument_universe import accepted_quotes_for_venue
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CATALOG_BLOB = "prod/catalog.parquet"
INDEX_BLOB = "_index/availability_index.parquet"
PER_VM_PREFIX = "_index/per_vm/"

# instrument_type carries known case/alias drift across the manifest corpus
# (codex 2026-07-20 ruling: compare case-insensitively, do NOT flag, do NOT
# refuse -- e.g. live-measured 2026-07-23 OKX-SPOT gap rows split
# {"SPOT_PAIR": 170, "spot": 4}). Both spellings are in scope for each case
# class.
_SPOT_ITYPES = frozenset({"SPOT_PAIR", "SPOT"})
_FUTURE_ITYPES = frozenset({"FUTURE", "FUTURES"})

# Bybit's own no-dash coin-margined-quarterly shape -- the live SSOT is
# ``_BYBIT_MONTH_CODE_RE`` in
# instruments_service/reference_data/adapters/cefi/tardis/parsing.py; mirrored
# here (not imported) because that module is adapter-internal and this script
# only needs the shape, not the parsing behaviour.
_CME_MONTH_RE = re.compile(r"^([A-Z0-9]+)USD([FGHJKMNQUVXZ])(\d{2})$")

# STOP-ON-SURPRISE band around the TOTAL gap-row count. The 2026-07-23
# investigation measured 211 (down from a historical ~422 snapshot, moving
# target as the manifest/catalogue corpus keeps changing during the ongoing
# filename-canonicalization migration). A future run outside this band pages
# loudly: ballooning means a new venue/quote hit the same gate; vanishing
# toward 0 could be a genuine fix landing OR a comparison-logic bug -- either
# way it needs a human look, not a silent pass.
_TOTAL_GAP_MIN = 20
_TOTAL_GAP_MAX = 5000

_GAP_CASE_SPOT_QUOTE = "spot-quote-gap"
_GAP_CASE_CME_MONTH = "cme-letter-month-gap"


def extract_base_quote(instrument_id: str) -> str | None:
    """Extract the wire-ish BASE-QUOTE (or bare-wire) token from a manifest ``instrument_id``.

    Handles both the canonical ``VENUE:TYPE:BASE-QUOTE[@MARKER]`` form and the
    raw/bare-wire form a row keeps when it never resolved into the catalogue
    (e.g. ``MATIC-BTC``, ``USDC-TRY`` -- exactly the rows this script exists to
    find; live-verified 2026-07-23 that these bare forms are how the OKX-SPOT /
    COINBASE-SPOT gap rows actually appear in the manifest, NOT as canonical
    ``OKX-SPOT:SPOT_PAIR:...`` ids). Returns ``None`` for an empty id or a
    canonical id with fewer than 3 colon-segments (defensive; not expected for
    real cefi rows).
    """
    if not instrument_id:
        return None
    if ":" in instrument_id:
        parts = instrument_id.split(":")
        if len(parts) < 3:
            return None
        base_quote = parts[2]
    else:
        base_quote = instrument_id
    return base_quote.split("@", 1)[0]


def extract_quote(base_quote: str | None) -> str | None:
    """Return the quote token (segment after the LAST dash) of a BASE-QUOTE wire.

    Dash-only by design -- mirrors the 2026-07-23 investigation's own
    extraction, live-verified to reproduce its 174 (OKX-SPOT) / 4
    (COINBASE-SPOT) row counts exactly. A concatenated no-dash wire (e.g.
    ``WEETHUSDT``) is deliberately NOT decomposed here: that shape is a
    different, unrelated failure mode (base-asset / alias resolution), not the
    accepted-quote gate this script targets, and decomposing it would inflate
    the count past the reproduced historical baseline.
    """
    if not base_quote or "-" not in base_quote:
        return None
    return base_quote.rsplit("-", 1)[-1]


def is_cme_month_shape(wire_upper: str) -> bool:
    """True when ``wire_upper`` matches Bybit's no-dash CME-letter-month shape (e.g. ``BTCUSDH26``)."""
    return bool(_CME_MONTH_RE.match(wire_upper))


def build_catalog_wire_sets(catalog: pd.DataFrame) -> dict[str, set[str]]:
    """Build ``{venue: {raw_symbol.upper(), ...}}`` from the rolled-up catalogue.

    One set per venue across ALL instrument types on that venue -- both case
    classes only ask "does this exact wire exist under this venue at all",
    never itype-scoped (a row moving itype on re-canonicalization should still
    count as catalogue-backed).
    """
    wire_sets: dict[str, set[str]] = {}
    cols = catalog[["venue", "raw_symbol"]].fillna("").astype(str)
    for venue, raw_symbol in zip(cols["venue"], cols["raw_symbol"], strict=True):
        if not venue or not raw_symbol:
            continue
        wire_sets.setdefault(venue, set()).add(raw_symbol.upper())
    return wire_sets


def find_spot_quote_gaps(manifest: pd.DataFrame, catalog_wires: dict[str, set[str]]) -> pd.DataFrame:
    """Case class 'spot-quote-gap' -- generalized across every venue, not hand-scoped.

    A SPOT_PAIR-typed manifest row is flagged when its quote token is outside
    ``accepted_quotes_for_venue(venue)`` (the live UAC SSOT) AND its
    (venue, BASE-QUOTE) wire is not already present in the rolled-up
    catalogue. Fires for OKX-SPOT + COINBASE-SPOT today (2026-07-23) but any
    venue hitting the same ungapped-quote gate is picked up automatically --
    no further code change needed.
    """
    itype_upper = manifest["instrument_type"].fillna("").astype(str).str.upper()
    spot_rows = manifest.loc[itype_upper.isin(_SPOT_ITYPES)].copy()
    if spot_rows.empty:
        return spot_rows.assign(gap_case=pd.Series(dtype=str), gap_token=pd.Series(dtype=str))

    spot_rows["base_quote"] = spot_rows["instrument_id"].fillna("").map(extract_base_quote)
    spot_rows["gap_token"] = spot_rows["base_quote"].map(extract_quote)
    spot_rows = spot_rows[spot_rows["gap_token"].notna()]
    if spot_rows.empty:
        spot_rows = spot_rows.assign(gap_case=pd.Series(dtype=str))
        return spot_rows

    # Precompute accepted-quote sets once per distinct venue (not once per row) --
    # accepted_quotes_for_venue is a plain dict lookup but there is no reason to
    # re-call it hundreds of thousands of times for a handful of venues.
    venue_accepted = {v: accepted_quotes_for_venue(v) for v in spot_rows["venue"].unique()}
    quote_accepted = [q in venue_accepted[v] for v, q in zip(spot_rows["venue"], spot_rows["gap_token"], strict=True)]
    already_catalogued = [
        (bq or "").upper() in catalog_wires.get(v, set())
        for v, bq in zip(spot_rows["venue"], spot_rows["base_quote"], strict=True)
    ]
    is_gap = [
        (not accepted) and (not catalogued)
        for accepted, catalogued in zip(quote_accepted, already_catalogued, strict=True)
    ]
    out = spot_rows.loc[is_gap].copy()
    out["gap_case"] = _GAP_CASE_SPOT_QUOTE
    return out


def find_cme_month_gaps(manifest: pd.DataFrame, catalog_wires: dict[str, set[str]]) -> pd.DataFrame:
    """Case class 'cme-letter-month-gap' -- generalized across every venue.

    A FUTURE-typed manifest row whose wire matches ``_CME_MONTH_RE`` is
    flagged when (venue, wire) is not in the rolled-up catalogue. Fires for
    BITGET-FUTURES today (parser fix landed 2026-07-14, rollup not yet
    refreshed); BYBIT's own instances of this shape already have catalogue
    rows (Bybit is the shape's origin venue) so they never appear here.
    """
    itype_upper = manifest["instrument_type"].fillna("").astype(str).str.upper()
    future_rows = manifest.loc[itype_upper.isin(_FUTURE_ITYPES)].copy()
    if future_rows.empty:
        return future_rows.assign(gap_case=pd.Series(dtype=str), gap_token=pd.Series(dtype=str))

    future_rows["gap_token"] = future_rows["instrument_id"].fillna("").astype(str).str.upper()
    cme_shape = future_rows["gap_token"].map(is_cme_month_shape)
    future_rows = future_rows.loc[cme_shape].copy()
    if future_rows.empty:
        future_rows = future_rows.assign(gap_case=pd.Series(dtype=str))
        return future_rows

    already_catalogued = [
        wire in catalog_wires.get(v, set())
        for v, wire in zip(future_rows["venue"], future_rows["gap_token"], strict=True)
    ]
    out = future_rows.loc[[not c for c in already_catalogued]].copy()
    out["gap_case"] = _GAP_CASE_CME_MONTH
    return out


@dataclass(frozen=True)
class GapSummaryRow:
    venue: str
    gap_case: str
    row_count: int
    captured_count: int
    distinct_tokens: str


def summarize(gap_df: pd.DataFrame) -> list[GapSummaryRow]:
    """Group flagged rows into one summary row per (venue, gap_case)."""
    if gap_df.empty:
        return []
    rows: list[GapSummaryRow] = []
    for (venue, gap_case), group in gap_df.groupby(["venue", "gap_case"], sort=True):
        captured = int((group["capture_status"] == "captured").sum()) if "capture_status" in group.columns else 0
        tokens = sorted(group["gap_token"].dropna().astype(str).unique())
        token_str = ", ".join(tokens[:10]) + (f", +{len(tokens) - 10} more" if len(tokens) > 10 else "")
        rows.append(
            GapSummaryRow(
                venue=str(venue),
                gap_case=str(gap_case),
                row_count=len(group),
                captured_count=captured,
                distinct_tokens=token_str,
            )
        )
    return rows


def _load_manifest_blobs(storage: object, bucket: str) -> pd.DataFrame:
    """Bounded read: the main index + every blob under the per-VM index prefix.

    ``list_blobs(prefix=PER_VM_PREFIX)`` is a listing of a KNOWN INDEX
    DIRECTORY (today: 1 object) -- not a walk of the raw tick corpus (which
    holds tens of millions of objects under a completely different prefix).
    This mirrors ``complete_cefi_manifest_canonical_dedup_2026_07_17.py``'s
    own discovery pattern exactly.
    """
    columns = ["date", "venue", "instrument_type", "instrument_id", "capture_status", "data_type"]
    blob_names = [INDEX_BLOB]
    per_vm_names = sorted(
        meta.name  # pyright: ignore[reportAttributeAccessIssue]
        for meta in storage.list_blobs(bucket, prefix=PER_VM_PREFIX)  # pyright: ignore[reportAttributeAccessIssue]
    )
    blob_names.extend(per_vm_names)

    frames: list[pd.DataFrame] = []
    for blob in blob_names:
        try:
            raw = storage.download_bytes(bucket, blob)  # pyright: ignore[reportAttributeAccessIssue]
        except Exception as exc:  # broad-except-ok — per-shard isolation for a vanished/transient blob
            logger.warning("Skipping manifest blob gs://%s/%s (unreadable): %s", bucket, blob, exc)
            continue
        schema_cols = set(pd.read_parquet(io.BytesIO(raw)).columns)
        proj = [c for c in columns if c in schema_cols]
        frames.append(pd.read_parquet(io.BytesIO(raw), columns=proj))
        logger.info("Loaded gs://%s/%s (%d rows)", bucket, blob, len(frames[-1]))

    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def _load_catalog(storage: object, bucket: str) -> pd.DataFrame:
    """Bounded read: the rolled-up cefi ``prod/catalog.parquet`` (ONE object)."""
    raw = storage.download_bytes(bucket, CATALOG_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    return pd.read_parquet(io.BytesIO(raw), columns=["venue", "instrument_type", "raw_symbol", "instrument_id"])


def _print_report(summary_rows: list[GapSummaryRow], total_rows: int, total_captured: int) -> None:
    print("\n=== CeFi catalogue-enumeration-gap measurement ===")
    if not summary_rows:
        print("No gap rows found.")
    else:
        header = f"{'venue':<18} {'gap_case':<22} {'row_count':>9} {'captured':>9}  distinct_tokens"
        print(header)
        print("-" * len(header))
        for row in summary_rows:
            print(
                f"{row.venue:<18} {row.gap_case:<22} {row.row_count:>9} {row.captured_count:>9}  {row.distinct_tokens}"
            )
    print("-" * 60)
    noise = total_rows - total_captured
    print(f"TOTAL gap rows: {total_rows}  (captured: {total_captured}, non-captured/noise: {noise})")
    print("===================================================\n")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", default=None, help="Override the cefi market-data bucket.")
    p.add_argument("--instruments-bucket", default=None, help="Override the cefi instruments-store bucket.")
    p.add_argument(
        "--json-out",
        default=None,
        help="Optional LOCAL file path to dump the per-row gap detail as CSV (never written to GCS).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    mkt_bucket = args.bucket or resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    inst_bucket = args.instruments_bucket or resolve_bucket_name(
        cloud="gcp", kind="instruments-store", asset_group="cefi"
    )
    storage = get_storage_client()  # pyright: ignore[reportAttributeAccessIssue]

    logger.info("Loading catalogue gs://%s/%s ...", inst_bucket, CATALOG_BLOB)
    catalog = _load_catalog(storage, inst_bucket)
    catalog_wires = build_catalog_wire_sets(catalog)

    logger.info("Loading manifest (main index + per-VM shards) from gs://%s ...", mkt_bucket)
    manifest = _load_manifest_blobs(storage, mkt_bucket)
    logger.info("Loaded %d total manifest rows across all blobs.", len(manifest))

    spot_gaps = find_spot_quote_gaps(manifest, catalog_wires)
    cme_gaps = find_cme_month_gaps(manifest, catalog_wires)
    all_gaps = pd.concat([spot_gaps, cme_gaps], ignore_index=True)

    summary_rows = summarize(all_gaps)
    total_rows = len(all_gaps)
    has_status = total_rows and "capture_status" in all_gaps.columns
    total_captured = int((all_gaps["capture_status"] == "captured").sum()) if has_status else 0

    _print_report(summary_rows, total_rows, total_captured)

    if args.json_out:
        all_gaps.to_csv(args.json_out, index=False)
        logger.info("Wrote local gap detail to %s (%d rows).", args.json_out, total_rows)

    if not (_TOTAL_GAP_MIN <= total_rows <= _TOTAL_GAP_MAX):
        logger.warning(
            "STOP-ON-SURPRISE: total gap count %d is OUTSIDE the expected band [%d, %d] "
            "(2026-07-23 baseline: 211). Diagnose before trusting this number.",
            total_rows,
            _TOTAL_GAP_MIN,
            _TOTAL_GAP_MAX,
        )
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
