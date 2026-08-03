#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after this migration has run in prod (defi + cefi) and been verified
#   (see data_status_page_ux_and_canonicalisation_2026_07_16.md P4-B Progress Log)
"""Backfill SPOT_ASSET catalogue rows for every distinct DeFi + spot-CeFi token leg.

``SPOT_ASSET`` is an already-canonical ``InstrumentType`` (``_instrument_enums.py``,
mapped to ``LedgerAssetClass.SPOT_TOKEN``) but no adapter has ever emitted it, so
``catalog.parquet`` carries zero SPOT_ASSET rows even though every on-chain DeFi
row already resolves the addresses of the token(s) it touches. The P4-B enabler
(instruments-service@77f0fdaa) projected those addresses
(``base_asset_contract_address`` / ``quote_asset_contract_address`` /
``atoken_address`` / ``debt_token_address``) into ``CATALOG_COLUMNS`` — this
script is the one-off historical backfill that derives + appends the missing
SPOT_ASSET rows from what the catalogue ALREADY has (no re-fetch).

DeFi derivation (``derive_defi_spot_assets``)
----------------------------------------------
Walks every ``SPOT_PAIR`` / ``POOL`` / ``LST`` / ``A_TOKEN`` / ``DEBT_TOKEN`` row
and collects every non-blank on-chain address column relevant to that row's type:

* ``POOL`` / ``SPOT_PAIR`` — both legs: ``base_asset_contract_address`` (the base
  token) AND ``quote_asset_contract_address`` (the quote token) — two distinct
  real tokens.
* ``LST`` — its own receipt-token address (``base_asset_contract_address`` — the
  adapters store the LST's OWN address there, e.g. ezETH/weETH; see
  ``renzo.py``/``etherfi.py``).
* ``A_TOKEN`` — the underlying (``base_asset_contract_address``) AND, when the
  adapter populated it, the Aave receipt aToken itself (``atoken_address``) — two
  distinct real tokens (most non-Aave adapters, e.g. ``solend.py``, only ever
  populate ``base_asset_contract_address``, so this degrades to one leg there).
* ``DEBT_TOKEN`` — same pattern via ``debt_token_address`` (currently unpopulated
  by every live adapter — reserved for a future Aave debt-token query expansion;
  degrades to the ``base_asset_contract_address`` leg only, same as A_TOKEN).

Every unique ``(chain, address)`` pair collapses to ONE SPOT_ASSET row (the same
physical token referenced from many pools/markets must not duplicate). The base
symbol label comes straight from the row's own ``base_asset`` column; the quote
leg's symbol is NOT a standalone ``CATALOG_COLUMNS`` field (see
``build_instrument_catalogue.py::CATALOG_COLUMNS`` — there is no ``quote_asset``
column), so it is parsed, best-effort, from the DEX-pool ``glued_pair_id``
projection (``VENUE:POOL:BASE-QUOTE[-FEE_BPS]``, populated for POOL rows only) —
honest-absence when that projection is blank or not POOL-shaped: the row is still
emitted (the address is real and known) with a blank ``base_asset`` label rather
than a guessed one.

CeFi derivation (``derive_cefi_spot_assets``)
----------------------------------------------
A CeFi spot/perp leg has no venue contract address of its own — cefi
``catalog.parquet`` rows carry a real, exchange-native ``base_asset`` symbol
(BTC / ETH / USDT / ...) with no on-chain identifier at all. Per the plan spec
("ETH -> WETH/native on ethereum"), each distinct cefi ``base_asset`` is mapped
through the Ethereum-mainnet ``DEFI_MAJOR_ASSET_ADDRESSES`` registry (native
ETH/BTC redirected to their canonical wrapped form first — WETH/WBTC are what
the registry actually keys, matching ``token_wrapping.py``'s wrap-direction
convention). ``DEFI_MAJOR_ASSET_ADDRESSES`` carries no chain dimension (unlike
``TokenWrappingRule.chain`` — checked, and deliberately NOT extended here: the
registry is documented as Ethereum-mainnet-derived and this backfill treats it
as exactly that), so a symbol that only exists off-Ethereum (a Solana-native
token with no Ethereum listing, e.g. a raw SPL asset) has no resolvable address
here and is honestly skipped (logged, never fabricated).

KNOWN GAP (documented, not silently dropped): ``catalog.parquet`` has no
``quote_asset`` column for cefi rows either (see CATALOG_COLUMNS) — this
backfill covers cefi BASE legs only. Covering cefi quote legs (almost always a
handful of stablecoins/majors that mostly already appear as SOME row's own
``base_asset`` elsewhere in the universe) is a follow-up requiring either a
catalogue projection change or fragile ``raw_symbol`` suffix-stripping this
script deliberately does not attempt (honest-absence over a guessed match).

Idempotent + real-infra
------------------------
Re-running is a no-op for every already-present ``instrument_id`` (dedup against
the CURRENT catalogue before writing, mirroring
``canonicalize_defi_lending_atoken_debttoken_catalog_2026_07_13.py``'s pattern).
``--dry-run`` computes + logs the summary only; ``--apply`` backs up the current
catalogue object, then writes the merged frame back to the same
``prod/catalog.parquet`` blob.

Usage:
  python scripts/backfill_spot_asset_population_2026_07_16.py --asset-group defi --dry-run
  python scripts/backfill_spot_asset_population_2026_07_16.py --asset-group defi --apply
  python scripts/backfill_spot_asset_population_2026_07_16.py --asset-group cefi --apply
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import is_mvp
from unified_api_contracts.registry.defi_major_assets import DEFI_MAJOR_ASSET_ADDRESSES
from unified_trading_library import get_storage_client, resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_CATALOG_BLOB = "prod/catalog.parquet"

#: DeFi instrument_types whose rows carry an on-chain address column worth
#: mining for SPOT_ASSET legs (mirrors DEFI_ONCHAIN_INSTRUMENT_TYPES minus
#: POOL/YIELD_BEARING/STAKING/PERPETUAL — plan scope: "base+quote token leg of
#: every SPOT_PAIR/POOL + LST/A_TOKEN/DEBT_TOKEN underlyings").
_DEFI_ONCHAIN_TYPES = frozenset({"SPOT_PAIR", "POOL", "LST", "A_TOKEN", "DEBT_TOKEN"})

#: CeFi-native symbol -> its canonical WRAPPED on-chain form. Native ETH/BTC have
#: no contract address of their own; DEFI_MAJOR_ASSET_ADDRESSES keys the WRAPPED
#: forms (WETH/WBTC), matching token_wrapping.py's wrap-direction convention.
_CEFI_NATIVE_TO_WRAPPED: dict[str, str] = {"ETH": "WETH", "BTC": "WBTC"}

#: DEFI_MAJOR_ASSET_ADDRESSES is Ethereum-mainnet-derived only (checked against
#: token_wrapping.py's per-entry `chain` field — that registry DOES model a chain
#: dimension; this one deliberately does not, so it is treated as Ethereum-only).
_CEFI_ONCHAIN_CHAIN = "ETHEREUM"


def _catalogue_bucket(asset_group: str) -> str:
    """Resolve the instruments-store bucket for a defi/cefi backfill run.

    Mirrors build_instrument_catalogue.py's `_instruments_store_bucket_for` —
    explicit per-value branches (rather than passing `asset_group` straight
    through) since `resolve_bucket_name`'s `asset_group` kwarg is a `Literal`.
    """
    if asset_group == "defi":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")
    if asset_group == "cefi":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    raise ValueError(f"Unsupported asset_group for SPOT_ASSET backfill: {asset_group!r}")


def _backup_path(asset_group: str, run_ts: str) -> str:
    return f"prod/catalog.{run_ts}.spotasset.{asset_group}.bak.parquet"


def _nonblank(value: object) -> str:
    """Return a stripped string, or "" for None/NaN/blank (never raises)."""
    if value is None:
        return ""
    try:
        if pd.isna(value):  # pyright: ignore[reportArgumentType, reportCallIssue]
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _spot_asset_instrument_id(chain: str, address: str) -> str:
    """Canonical SPOT_ASSET catalogue id — mirrors the POOL ``pool_address.lower()``
    convention (build_instrument_catalogue.py's ``_pool_address_of`` /
    ``_defi_pool_dual_form``) so a fresh capture and this backfill would agree on
    the identical shape for the same physical token."""
    return f"spot_asset:{chain.lower()}:{address.lower()}"


def _quote_symbol_from_glued_pair_id(glued_pair_id: str) -> str:
    """Best-effort quote-leg symbol from a POOL's ``glued_pair_id``.

    Format: ``VENUE:POOL:BASE-QUOTE`` or ``VENUE:POOL:BASE-QUOTE-FEE_BPS`` (the
    human-readable glued-pair grammar dash-joins an optional trailing fee —
    ``build_pool_identity`` / ``_fee_from_instrument_key``). Strips a trailing
    numeric fee segment before splitting, so "SOL-WETH-5" -> "WETH" (not the
    fee-corrupted "WETH-5"). Returns "" (honest-absence, never guessed) when the
    id is blank, not POOL-shaped, or the pair segment doesn't split into exactly
    two symbols.
    """
    if not glued_pair_id:
        return ""
    parts = glued_pair_id.split(":")
    if len(parts) < 3:
        return ""
    segments = parts[2].split("-")
    if len(segments) >= 3 and segments[-1].isdigit():
        segments = segments[:-1]
    if len(segments) != 2:
        return ""
    return segments[1]


class _Aggregate:
    """Accumulated SPOT_ASSET row state for one unique (chain, address)."""

    __slots__ = ("address", "available_from", "available_to", "base_asset", "chain", "still_active", "venue")

    def __init__(self, chain: str, address: str, base_asset: str, venue: str) -> None:
        self.chain = chain
        self.address = address
        self.base_asset = base_asset
        self.venue = venue
        self.available_from: str = ""
        self.available_to: str = ""
        self.still_active = False

    def fold(self, available_from: str, available_to: str, base_asset: str = "") -> None:
        """Fold one more contributing source row's lifecycle window into this aggregate.

        Adopts ``base_asset`` when the aggregate doesn't have one yet (the
        symbol-provenance row and the earliest-lifecycle row need not be the
        same contributor — e.g. a POOL row with a blank ``glued_pair_id`` may
        be sorted before one that resolves the quote symbol; never overwrites
        an already-resolved label).
        """
        if not self.base_asset and base_asset:
            self.base_asset = base_asset
        if available_from and (not self.available_from or available_from < self.available_from):
            self.available_from = available_from
        if not available_to:
            self.still_active = True
        elif not self.still_active and (not self.available_to or available_to > self.available_to):
            self.available_to = available_to


def derive_defi_spot_assets(df: pd.DataFrame) -> pd.DataFrame:
    """Derive one SPOT_ASSET row per unique (chain, address) DeFi token leg.

    Rows are walked in ``instrument_id`` order so the "first-observed venue"
    provenance a synthesized row inherits is stable / idempotent across re-runs.
    """
    onchain = df[df["instrument_type"].astype(str).isin(_DEFI_ONCHAIN_TYPES)].sort_values("instrument_id")
    # dict rows (not pd.Series) — mirrors build_instrument_catalogue.py's own
    # by_date row-walk convention (`frame.to_dict("records")`).
    records: list[dict[str, object]] = onchain.to_dict("records")  # pyright: ignore[reportAssignmentType]
    aggregates: dict[tuple[str, str], _Aggregate] = {}

    def _consider(chain_raw: str, address_raw: str, symbol: str, row: dict[str, object]) -> None:
        chain = chain_raw.strip().upper()
        address = address_raw.strip()
        if not chain or not address:
            return
        key = (chain, address.lower())
        agg = aggregates.get(key)
        if agg is None:
            agg = _Aggregate(chain=chain, address=address, base_asset=symbol, venue=_nonblank(row.get("venue")))
            aggregates[key] = agg
        agg.fold(_nonblank(row.get("available_from")), _nonblank(row.get("available_to")), base_asset=symbol)

    for row in records:
        itype = _nonblank(row.get("instrument_type")).upper()
        chain = _nonblank(row.get("chain"))
        base_addr = _nonblank(row.get("base_asset_contract_address"))
        base_sym = _nonblank(row.get("base_asset"))
        quote_addr = _nonblank(row.get("quote_asset_contract_address"))
        quote_sym = _quote_symbol_from_glued_pair_id(_nonblank(row.get("glued_pair_id")))

        if base_addr:
            _consider(chain, base_addr, base_sym, row)
        if itype in ("POOL", "SPOT_PAIR") and quote_addr:
            _consider(chain, quote_addr, quote_sym, row)
        if itype == "A_TOKEN":
            atoken_addr = _nonblank(row.get("atoken_address"))
            if atoken_addr:
                _consider(chain, atoken_addr, base_sym, row)
        if itype == "DEBT_TOKEN":
            debt_addr = _nonblank(row.get("debt_token_address"))
            if debt_addr:
                _consider(chain, debt_addr, base_sym, row)

    return _aggregates_to_dataframe(aggregates, asset_group="defi")


def derive_cefi_spot_assets(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Derive one SPOT_ASSET row per resolvable CeFi ``base_asset`` symbol.

    Returns ``(new_rows, unresolved_symbols)`` — ``unresolved_symbols`` is every
    distinct base_asset with NO Ethereum-mainnet registry entry (honest-absence;
    the caller logs these, nothing is fabricated for them).
    """
    base_assets = sorted({_nonblank(v).upper() for v in df["base_asset"]} - {""})
    aggregates: dict[tuple[str, str], _Aggregate] = {}
    unresolved: list[str] = []
    for symbol in base_assets:
        onchain_symbol = _CEFI_NATIVE_TO_WRAPPED.get(symbol, symbol)
        address = DEFI_MAJOR_ASSET_ADDRESSES.get(onchain_symbol)
        if not address:
            unresolved.append(symbol)
            continue
        key = (_CEFI_ONCHAIN_CHAIN, address.lower())
        agg = _Aggregate(
            chain=_CEFI_ONCHAIN_CHAIN,
            address=address,
            base_asset=symbol,
            venue=f"SPOT_ASSET-{_CEFI_ONCHAIN_CHAIN}",
        )
        agg.still_active = True
        aggregates[key] = agg

    return _aggregates_to_dataframe(aggregates, asset_group="cefi"), unresolved


def _aggregates_to_dataframe(aggregates: dict[tuple[str, str], _Aggregate], *, asset_group: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    # Iterate values only — the dict KEY's address component is lowercased for
    # dedup; the ORIGINAL-case address (real, copyable on-chain address) lives on
    # the aggregate itself (`agg.address`/`agg.chain`), never the key.
    for agg in aggregates.values():
        chain = agg.chain
        address = agg.address
        instrument_id = _spot_asset_instrument_id(chain, address)
        venue = agg.venue or f"SPOT_ASSET-{chain}"
        base_asset = agg.base_asset
        try:
            mvp = bool(is_mvp(asset_group, venue, "SPOT_ASSET", None, base_ccy=base_asset or None))
        except Exception:
            logger.debug("is_mvp() raised for venue=%s base_asset=%s — defaulting mvp=False", venue, base_asset)
            mvp = False
        rows.append(
            {
                "instrument_id": instrument_id,
                "instrument_type": "SPOT_ASSET",
                "venue": venue,
                "chain": chain,
                "league_id": "",
                "available_from": agg.available_from,
                "available_to": "" if agg.still_active else agg.available_to,
                "market_created_at": "",
                "settlement_time": "",
                "data_type": "",
                "underlying": base_asset,
                "raw_symbol": address,
                "base_asset": base_asset,
                "canonical_instrument_id": instrument_id,
                "mvp": mvp,
                "tracks_equity": "",
                "is_equity_perp": False,
                "margin_type": "",
                "glued_pair_id": "",
                "pool_address": "",
                "base_asset_contract_address": address,
                "quote_asset_contract_address": "",
                "atoken_address": "",
                "debt_token_address": "",
            }
        )
    return pd.DataFrame(rows)


def migrate(df: pd.DataFrame, *, asset_group: str) -> tuple[pd.DataFrame, dict[str, int], list[str]]:
    """Return (merged_catalogue, summary_counts, unresolved_cefi_symbols). Idempotent."""
    if asset_group == "defi":
        new_rows = derive_defi_spot_assets(df)
        unresolved: list[str] = []
    elif asset_group == "cefi":
        new_rows, unresolved = derive_cefi_spot_assets(df)
    else:
        raise ValueError(f"Unsupported asset_group for SPOT_ASSET backfill: {asset_group!r}")

    existing_ids = set(df["instrument_id"].astype(str))
    if not new_rows.empty:
        new_rows = new_rows[~new_rows["instrument_id"].isin(existing_ids)]

    if not new_rows.empty:
        # Column-order-align to the existing schema (extra/missing keys handled)
        # so the parquet write stays a clean superset-consistent frame.
        new_rows = new_rows.reindex(columns=list(df.columns), fill_value="")
        merged = pd.concat([df, new_rows], ignore_index=True)
    else:
        merged = df.copy()

    summary = {
        "rows_before": len(df),
        "new_spot_asset_rows": len(new_rows),
        "rows_after": len(merged),
        "unresolved_cefi_symbols": len(unresolved),
    }
    return merged, summary, unresolved


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asset-group", required=True, choices=["defi", "cefi"])
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)

    asset_group: str = args.asset_group
    bucket = _catalogue_bucket(asset_group)
    client = get_storage_client(project_id=None)
    logger.info("Reading gs://%s/%s", bucket, _CATALOG_BLOB)
    raw = client.download_bytes(bucket, _CATALOG_BLOB)  # pyright: ignore[reportAttributeAccessIssue]
    df = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded %d catalogue rows for asset_group=%s", len(df), asset_group)

    merged, summary, unresolved = migrate(df, asset_group=asset_group)
    for k, v in summary.items():
        logger.info("  %s = %d", k, v)
    if unresolved:
        logger.info(
            "  unresolved cefi base_asset symbols (no Ethereum-mainnet registry entry, honest-absence): %s",
            ", ".join(unresolved),
        )

    if args.dry_run:
        logger.info(
            "[dry-run] would write %d rows (was %d, +%d SPOT_ASSET) — no cloud writes performed",
            len(merged),
            len(df),
            summary["new_spot_asset_rows"],
        )
        return 0

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_blob = _backup_path(asset_group, run_ts)
    logger.info("Backing up current catalogue to gs://%s/%s", bucket, backup_blob)
    client.upload_bytes(bucket, backup_blob, raw)  # pyright: ignore[reportAttributeAccessIssue]

    buf = io.BytesIO()
    merged.to_parquet(buf, index=False)
    buf.seek(0)
    logger.info(
        "Writing migrated catalogue (%d rows, +%d SPOT_ASSET) to gs://%s/%s",
        len(merged),
        summary["new_spot_asset_rows"],
        bucket,
        _CATALOG_BLOB,
    )
    client.upload_bytes(bucket, _CATALOG_BLOB, buf.read())  # pyright: ignore[reportAttributeAccessIssue]
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
