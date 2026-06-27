#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""Roll-up producer — derive the lifecycle instrument catalogue from the per-date definitions.

The v2 expected-universe enumerator (``enumerate_expected_universe.py --enumerator-version v2``)
consumes a ``catalog.parquet`` of :class:`InstrumentCatalogEntry` rows — ONE row per instrument,
each carrying an ``available_from`` / ``available_to`` lifecycle window — so it can emit
``EXPECTED_INSTRUMENT_NOT_LISTED`` (``date < available_from``) and ``EXPECTED_INSTRUMENT_DELISTED``
(``date > available_to``). That file is a *cumulative, all-instruments-ever* lifecycle catalogue,
NOT a current snapshot.

Until now nothing produced it on a recurring basis (slot-3 finding, 2026-06-04): it was an
operator-supplied static snapshot, stale two ways at once — missing newly-listed instruments AND
wrong about what is still alive (a since-delisted instrument shown alive → its cells stay
``expected_unattempted`` forever instead of ``DELISTED``).

This script makes the catalogue a **derivative of the maintained per-date definitions**. The
instruments-service already writes
``instrument_availability/by_date/day={date}/venue={venue}/instruments.parquet`` daily (the
point-in-time, reproducible-batch "what existed on date t" slice). For each instrument:

* ``available_from`` = the first day it appears across the ``by_date/`` snapshots;
* ``available_to``  = the last day it appears, OR ``None`` when it is present on the latest
  snapshot day (still active / no upper bound).

So the data already exists and the catalogue is a roll-up of it — correct + self-refreshing, with
no separate artifact to drift.

Output path (operator decision 2026-06-04, the path the launcher + enumerator already expect):
``gs://{get_write_bucket_name("instruments", ag)}/{DEPLOYMENT_ENV}/catalog.parquet`` — i.e.
``instruments-store-{ag_short}-{env_short}-{pid}/{env}/catalog.parquet``.

Monotonic-guard promotion (operator decision 2026-06-04): regenerate → write to a TEMP object →
assert the new row count is ``>=`` the current catalogue (instrument rows grow monotonically: new
listings add rows; delisted rows persist with ``available_to`` set) → on pass **promote** (copy
temp over the canonical object + delete temp); on a ``<`` regression KEEP the previous good
catalogue + alert, do NOT overwrite. ``--allow-catalogue-shrink`` overrides the ratchet for a
legitimate corrective shrink.

v9, NOT v10 — this is part of the v9 canonicalisation; no schema-version bump is introduced.

Plan: proper_instrument_catalogue_lifecycle_rollup_2026_06_04.md (Phase 1 — [CODE] P0).
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TypeVar

import pandas as pd
from unified_api_contracts import build_pool_identity, is_in_mvp_capture_universe, is_mvp
from unified_trading_library import (
    StorageClient,
    get_config,
    get_storage_client,
    resolve_bucket_name,
)


def _instruments_store_bucket_for(asset_group: str) -> str:
    """Resolve the instruments-store bucket via the bucket-name SSOT.

    Prediction is a dedicated FLAT kind ``instruments-store-prediction``
    (→ ``instruments-store-pred-{env}-{pid}``); the SSOT (cloud-providers.yaml)
    omits a ``PREDICTION`` entry from the per-asset_group ``instruments-store``
    dict, so the prior ``get_write_bucket_name("instruments", "prediction")``
    (→ ``resolve_bucket_name(kind="instruments-store", asset_group="prediction")``)
    raised ``BucketNamingError`` and the prediction catalogue roll-up crashed at
    bucket resolution before reaching the roll-up. Mirrors the IS engine SSOT
    ``resolve_instruments_store_kind`` + ``enumerate_expected_universe._default_bucket_for``.
    Identical to the prior ``get_write_bucket_name`` for every OTHER AG (both resolve
    the per-AG ``instruments-store`` dict). ``resolve_bucket_name``'s ``asset_group``
    is a ``Literal`` — narrow by equality so the call is type-clean.
    """
    if asset_group == "prediction":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store-prediction")
    if asset_group == "sports":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if asset_group == "cefi":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    if asset_group == "defi":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="defi")
    if asset_group == "tradfi":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="tradfi")
    raise ValueError(f"Unknown asset_group for instruments catalogue: {asset_group!r}")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

#: Generic item/result types for the memory-bounded parallel loader below.
_LoadItemT = TypeVar("_LoadItemT")
_LoadResultT = TypeVar("_LoadResultT")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: GCS prefix the per-date instrument definitions live under, for cefi / defi /
#: tradfi / prediction. (Sports fixtures use ``sports_reference/by_date`` with an
#: ``entity=`` key — see :data:`SPORTS_BY_DATE_PREFIX`; overridable via
#: ``--by-date-prefix``.)
DEFAULT_BY_DATE_PREFIX = "instrument_availability/by_date"

#: GCS prefix the per-date SPORTS reference definitions live under. The sports
#: writer partitions ``sports_reference/by_date/day={date}/entity={entity}/`` with
#: one parquet per entity (leagues / fixtures / teams / standings / …). The
#: could-exist catalogue rolls up the ``entity=leagues`` slice ONLY — the sports
#: captured manifest atom is per-(league_id, data_type, date) (slot-4 finding
#: 2026-06-07: league_id populated 97.6% / venue ~blank / instrument_id blank on
#: the canonical _index), so the could-exist grain is per-LEAGUE, NOT per-fixture.
SPORTS_BY_DATE_PREFIX = "sports_reference/by_date"

#: The ``entity=leagues`` slice the sports league-grain roll-up consumes.
SPORTS_LEAGUE_ENTITY = "leagues"

#: The ``instrument_type`` stamped on every sports league catalogue row.
SPORTS_LEAGUE_INSTRUMENT_TYPE = "league"

#: The canonical catalogue filename the launcher + v2 enumerator read.
CATALOG_FILENAME = "catalog.parquet"

#: Columns the enumerator's ``_catalog_from_dataframe`` consumes. ``instrument_id``
#: is written as the canonical column (the helper also accepts ``instrument_key``).
#: ``data_type`` is the OPTIONAL grain-binding (prediction multi-grain catalogue) —
#: empty/None for the single-grain AGs (the enumerator then iterates all data_types).
CATALOG_COLUMNS: tuple[str, ...] = (
    "instrument_id",
    "instrument_type",
    "venue",
    "chain",
    "league_id",
    "available_from",
    "available_to",
    "market_created_at",
    "settlement_time",
    "data_type",
    # G1-ENUM bundle-grain roll-up key — base asset for derivatives (Deribit
    # options/futures). Propagated from the instruments-store ``underlying``
    # column so enumerate's roll-up keys options_chain candidates per underlying.
    "underlying",
    # Exchange-native symbol + base asset (CeFi/defi lifecycle cross-ref keys).
    # The UTL ``instruments_catalog_reader`` matches a manifest row's bare symbol
    # via ``venue+raw_symbol`` (proven UNIQUE per instrument — 0 collisions across
    # the full 2019→2026 history) and falls back to ``venue+base_asset`` (lossy —
    # base_asset alone maps to many instruments, so it is the last resort). Carried
    # from the instruments-store by_date source so the reader's existing strategies
    # match. Blank for prediction/sports (no exchange-native symbol there).
    "raw_symbol",
    "base_asset",
    # MVP-scope tag (mvp_scope_catalogue_tagging_2026_06_08): per-entry boolean
    # computed via the UAC ``is_mvp(...)`` predicate over the rolled-up catalogue.
    # Read by deployment-api's ``scope=mvp`` coverage denominator + the data-status
    # MVP toggle. On-the-fly at roll-up time — never a baked rule (UAC owns the rule).
    "mvp",
    # Margin type: "linear" (USDT/USDC-margined) or "inverse" (coin-margined/delivery).
    # Propagated from the per-date instruments parquet ``margin_type`` column
    # (populated by the IS cefi Tardis adapter). Empty for non-derivative instruments
    # (spot pairs, DeFi pools, etc.). cefi_universe_capture_rule 2026-06-24.
    "margin_type",
    # DUAL-FORM DeFi pool ids (defi_instrument_catalogue_and_capture_pipeline_2026_06_23,
    # operator Refinement 1 — keep BOTH forms per pool): the manifest-canonical
    # ``instrument_id`` above is ``pool_address.lower()`` (for DeFi POOL rows), while
    # ``glued_pair_id`` carries the HUMAN-READABLE UI id
    # ``UNISWAPV3-ARBITRUM:POOL:AAVE-USDC:100`` (venue-chain glued + POOL + token0-token1
    # PAIR + FEE). Built via the UAC SSOT converter ``build_pool_identity`` so the two
    # forms are reversible. Blank for non-DeFi-pool rows (cefi/tradfi/sports/prediction).
    "glued_pair_id",
    # On-chain pool contract address (DeFi POOL rows) — the canonical-id source +
    # the address the bidirectional converter needs to re-resolve from a glued-pair.
    # Blank for non-pool rows.
    "pool_address",
)

#: Per-date parquet columns holding the instrument identifier (first match wins).
_ID_COLUMNS: tuple[str, ...] = ("instrument_key", "instrument_id")

#: Concurrency for the by_date download (I/O-bound — MAX_WORKERS=16 per the coding standards).
MAX_DOWNLOAD_WORKERS = 16

#: Extract the ISO ``day=`` partition from a ``by_date`` blob path.
_DAY_RE = re.compile(r"(?:^|/)day=(\d{4}-\d{2}-\d{2})(?:/|$)")

#: Extract the ``entity=`` partition from a sports ``by_date`` blob path.
_ENTITY_RE = re.compile(r"(?:^|/)entity=([^/]+)(?:/|$)")

#: Extract the ``venue=`` / ``canonical_question_group=`` partitions (prediction).
#: The prediction writer (instruments-service ``orchestrator.py``) partitions
#: ``instrument_availability/by_date/day=/venue=/canonical_question_group=/instruments.parquet``
#: and DROPS the ``_canonical_group`` column before writing — so the cqg lives in
#: the PATH, not a column. The prediction roll-up parses it from the path.
_VENUE_RE = re.compile(r"(?:^|/)venue=([^/]+)(?:/|$)")
_CQG_RE = re.compile(r"(?:^|/)canonical_question_group=([^/]+)(?:/|$)")

#: Prediction data_types whose captured manifest atom is per-conditionId grain.
_PREDICTION_CID_DATA_TYPES: tuple[str, ...] = ("trades", "market_lifecycle")
#: Prediction bundle data_type whose captured atom is per-canonical_question_group grain.
_PREDICTION_CQG_DATA_TYPE = "prediction_canonical_question_group"


def _emit_event(event: str, /, **details: object) -> None:
    """Best-effort structured event log (mirrors the enumerator's ENUMERATOR_* shape)."""
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


# ---------------------------------------------------------------------------
# Pure roll-up math (no I/O — unit-tested directly)
# ---------------------------------------------------------------------------


@dataclass
class _InstrumentAggregate:
    """Lifecycle accumulator for a single instrument across the per-date snapshots."""

    first_day: date
    last_day: date
    meta_day: date
    meta: dict[str, str | None]
    # BUG #4 (B): the earliest DECLARED listing date the writer stamped on the row
    # (``available_from_datetime`` — e.g. the IS cefi adapters' per-instrument
    # genesis-funding probe). The catalogue ``available_from`` is the MIN of the
    # observed first snapshot day and this declared date, so a perp only observed on
    # one recent snapshot still carries its true historical listing date (else the
    # catalogue-driven backfill would never attempt its history). None = no declared
    # date (legacy rows) → observed-day only, unchanged.
    declared_from: date | None = None
    # §7.3 venue-truth lifecycle close — the exchange-declared expiry (dated
    # FUTURE/OPTION/COMBO) and explicit delisting date, taken from the
    # most-recent snapshot row (``meta_day``). When either is present the catalogue
    # ``available_to`` is set from venue truth instead of last-seen, so a thin
    # latest snapshot day cannot false-delist a live perp/spot. None = no
    # venue-truth close → fall back to per-venue last-trading-day liveness.
    expiry: date | None = None
    delisted_at: date | None = None


def _row_id(row: dict[str, object]) -> str | None:
    """Return the instrument identifier for a per-date row, or None when absent/blank."""
    for col in _ID_COLUMNS:
        raw = row.get(col)
        if raw is None:
            continue
        try:
            if pd.isna(raw):  # pyright: ignore[reportArgumentType]
                continue
        except (TypeError, ValueError):
            pass
        text = str(raw).strip()
        if text:
            return text
    return None


def _opt_field(row: dict[str, object], col: str) -> str | None:
    """Return a string field from a per-date row, or None for missing/NaN."""
    raw = row.get(col)
    if raw is None:
        return None
    try:
        if pd.isna(raw):  # pyright: ignore[reportArgumentType]
            return None
    except (TypeError, ValueError):
        pass
    return str(raw)


def _str_field(row: dict[str, object], col: str) -> str:
    """Return a string field from a per-date row, or "" for missing/NaN."""
    return _opt_field(row, col) or ""


def _declared_from(row: dict[str, object]) -> date | None:
    """Parse the writer-declared listing date (``available_from_datetime`` /
    ``available_from``) from a per-date row → ``date``, or None when absent/unparseable.

    BUG #4 (B): lets the rollup honour a per-instrument genesis date the writer
    stamped (the IS cefi adapters' earliest-funding probe) as the ``available_from``
    lower bound, independent of which snapshot day the instrument was observed on.
    """
    for col in ("available_from_datetime", "available_from"):
        raw = _opt_field(row, col)
        if not raw:
            continue
        try:
            return pd.Timestamp(raw).date()
        except (ValueError, TypeError):
            continue
    return None


def _parse_truth_date(raw: str | None) -> date | None:
    """Parse a venue-truth lifecycle date (``expiry`` / ``delisted_at``) → ``date``.

    §7.3: these come from the exchange-declared per-date instrument fields, so a
    dated FUTURE/OPTION/COMBO's catalogue ``available_to`` is its real contract
    expiry (and an explicitly delisted perp/spot its real removal day) rather than
    the last snapshot day it happened to appear in our (possibly thin) capture.
    Returns None for blank/unparseable values.
    """
    if not raw:
        return None
    try:
        return pd.Timestamp(raw).date()
    except (ValueError, TypeError):
        return None


#: A venue's latest day counts as a FULL trading day (rather than a thin/partial
#: capture) when its instrument count is at least this fraction of the venue's
#: recent median. A genuinely down day (e.g. mass expiry roll) still exceeds this;
#: a half-written/partial capture (BINANCE-FUTURES 678 → 47 = ~7%) falls below it
#: and is skipped so it cannot false-delist the venue's live universe (§7.3).
_THIN_DAY_FRACTION = 0.5

#: How many of a venue's most-recent days form the median baseline for thin-day
#: detection (a short, recent window — robust to slow universe growth/shrink).
_VENUE_RECENT_WINDOW = 14


def _venue_last_full_day(day_counts: dict[date, int]) -> date | None:
    """Return a venue's most-recent NON-THIN capture day (§7.3 liveness anchor).

    Perp/spot ``available_to`` is ``None`` (active) iff the instrument is present on
    its venue's last FULL day. A thin/partial latest snapshot (a half-completed
    capture) would otherwise mass-false-delist the venue, so we walk back from the
    latest day and return the first day whose instrument count is not a thin outlier
    vs the venue's recent median. Falls back to the absolute latest day when every
    recent day is thin (no better anchor exists) or the venue has a single day.
    """
    if not day_counts:
        return None
    days = sorted(day_counts)
    if len(days) == 1:
        return days[0]
    # Recent-window median as the "full day" baseline (resistant to a thin tail).
    recent = days[-_VENUE_RECENT_WINDOW:]
    counts = sorted(day_counts[d] for d in recent)
    mid = len(counts) // 2
    median = counts[mid] if len(counts) % 2 else (counts[mid - 1] + counts[mid]) / 2
    threshold = median * _THIN_DAY_FRACTION
    for d in reversed(days):
        if day_counts[d] >= threshold:
            return d
    # Every day thinner than threshold (degenerate) → the latest day is the anchor.
    return days[-1]


#: DeFi pool instrument_type values (lowercased) the dual-form id applies to.
#: A pool's canonical manifest atom is ``pool_address.lower()``; the glued-pair
#: id is the human-readable UI form. Other DeFi instrument_types (lending / lst /
#: spot_asset / perpetual) key on a contract address too but are not pools.
_DEFI_POOL_ITYPES: frozenset[str] = frozenset({"pool"})


def _fee_from_instrument_key(instrument_key: str) -> str:
    """Extract the fee token from a legacy glued ``…:POOL:PAIR:FEE`` instrument_key.

    The by_date ``pool_fee_tier`` column is in BPS (Uniswap feeTier / 100, e.g.
    ``5.0``) but the human-readable glued id the operator specified uses the RAW
    fee amount (``:500`` / ``:3000`` / ``:100``) the adapter stamped into
    ``instrument_key``. So prefer the instrument_key's trailing fee segment for a
    faithful UI id; return "" when the key is not a 4-part POOL key.
    """
    parts = instrument_key.split(":")
    if len(parts) >= 4 and parts[1] == "POOL":
        return parts[-1]
    return ""


def _pool_address_of(meta: dict[str, str | None]) -> str:
    """Return the canonical pool address for a row's meta, or "" when not a pool.

    Prefers the explicit ``pool_address`` column; falls back to ``raw_symbol`` when
    it is a 0x address (the IS adapter sets ``raw_symbol = str(pool_id)``).
    """
    pool_address = (meta.get("pool_address") or "").strip()
    if not pool_address:
        raw_sym = (meta.get("raw_symbol") or "").strip()
        if raw_sym.lower().startswith("0x"):
            pool_address = raw_sym
    return pool_address


def _canonical_instrument_id(instrument_id: str) -> str:
    """Normalise the venue-prefix portion of a DeFi non-pool ``instrument_id``.

    Non-pool DeFi rows (lending / lst / staking) use instrument_keys of the form
    ``VENUE-CHAIN:TYPE:SYMBOL`` (e.g. ``AAVEV3-ARBITRUM:A_TOKEN:USDC``).  When
    the IS adapter switched from the no-underscore ghost spelling (``AAVEV3-``) to
    the canonical underscore form (``AAVE_V3-``) around 2026-05-08, the SAME
    logical market appeared under TWO distinct ``instrument_key`` strings → TWO
    catalogue rows, both marked active (the "+171 AAVE_V3 / +26 COMPOUND_V3
    dual-key ghosts" triad discrepancy).

    Fix: for any instrument_id that contains a ``:``, split on the first ``:``,
    run ``canonicalize_defi_venue_combined`` on the prefix (which is the
    PROTOCOL-CHAIN combined form), and rejoin.  The canonicaliser is a no-op for
    already-canonical or non-DeFi prefixes, so this is safe for ALL rows.

    Pure + idempotent.  Only called from ``_aggregate_key`` (non-pool branch).
    """
    colon_idx = instrument_id.find(":")
    if colon_idx < 0:
        # No colon — not a structured DeFi key; return as-is.
        return instrument_id
    from unified_api_contracts.registry.capability_declarations._defi import canonicalize_defi_venue_combined

    prefix = instrument_id[:colon_idx]
    canonical_prefix = canonicalize_defi_venue_combined(prefix)
    if canonical_prefix == prefix:
        return instrument_id
    return canonical_prefix + instrument_id[colon_idx:]


def _aggregate_key(instrument_id: str, row: dict[str, object]) -> str:
    """Lifecycle-aggregation key for one per-date row.

    DeFi POOL rows key on the CANONICAL pool identity (``pool::<chain>::<pool_address>``)
    so spelling-variant ``instrument_key``s of the SAME physical pool
    (``UNISWAPV3-ARBITRUM`` vs ``UNISWAP_V3-ARBITRUM``) collapse into ONE continuous
    lifecycle — the Phase 2 premature-delisting fix.

    Non-pool DeFi rows (lending / lst / staking) whose ``instrument_id`` prefix is
    a ghost venue form (``AAVEV3-ARBITRUM``, ``COMPOUNDV3-BASE``) are normalised via
    ``_canonical_instrument_id`` so they collapse onto the canonical key
    (``AAVE_V3-ARBITRUM:…``, ``COMPOUND_V3-BASE:…``) — the dual-key-ghost collapse
    fix (+171 AAVE_V3 / +26 COMPOUND_V3 triad discrepancy 2026-06-27).

    All other rows key on the ``instrument_id`` (= instrument_key) as before, so
    non-DeFi behaviour is unchanged.
    """
    itype = str(row.get("instrument_type") or "").strip().lower()
    if itype in _DEFI_POOL_ITYPES:
        pool_address = _pool_address_of(_extract_meta(row))
        if pool_address:
            chain = str(row.get("chain") or "").strip().upper()
            if not chain:
                # venue carries the chain in the glued PROTOCOL-CHAIN form.
                venue = str(row.get("venue") or "")
                if "-" in venue:
                    chain = venue.rsplit("-", 1)[1].upper()
            return f"pool::{chain}::{pool_address.lower()}"
    # Non-pool: normalise the venue-prefix portion of structured DeFi instrument_ids
    # so ghost-spelling variants (AAVEV3-ARBITRUM:…) collapse onto the canonical key
    # (AAVE_V3-ARBITRUM:…).  No-op for non-DeFi / already-canonical keys.
    return _canonical_instrument_id(instrument_id)


#: Known chain suffixes for the glued PROTOCOL-CHAIN venue split (catalogue read-side
#: canonicalisation). Mirrors the UAC ``KNOWN_CHAINS`` set the manifest writer uses.
_CATALOGUE_KNOWN_CHAINS = frozenset(
    {
        "ETHEREUM",
        "ARBITRUM",
        "BASE",
        "OPTIMISM",
        "POLYGON",
        "BSC",
        "AVALANCHE",
        "SOLANA",
        "ZKSYNC",
        "SCROLL",
        "LINEA",
        "HYPERLIQUID",
        "STARKNET",
        "PLASMA",
    }
)


def _canonical_bare_venue_chain(venue: str, chain: str) -> tuple[str, str]:
    """Map any DeFi venue drift form ``(venue, chain)`` → canonical ``(bare_protocol, chain)``.

    1. ghost-normalise the full venue (``AAVEV3-ARBITRUM`` → ``AAVE_V3-ARBITRUM``) via the
       UAC authority ``canonicalize_defi_venue_combined`` (no-op for bare/non-DeFi venues).
    2. if the result is glued (ends ``-<KNOWN_CHAIN>``), split → bare protocol + that chain.
    3. else keep the venue; preserve the existing chain column (upper).
    Pure + idempotent. A non-DeFi or already-bare canonical venue passes through unchanged.
    """
    from unified_api_contracts.registry.capability_declarations._defi import canonicalize_defi_venue_combined

    v = str(venue).strip()
    c = str(chain).strip().upper()
    if not v:
        return v, c
    normed = canonicalize_defi_venue_combined(v)
    if "-" in normed:
        for ch in _CATALOGUE_KNOWN_CHAINS:
            if normed.endswith("-" + ch):
                return normed[: -(len(ch) + 1)], ch
    return normed, c


def _defi_pool_dual_form(
    meta: dict[str, str | None],
) -> tuple[str, str, str, str, str]:
    """Derive the dual-form pool ids + canonicalised venue/chain for a POOL row.

    Returns ``(canonical_instrument_id, glued_pair_id, bare_venue, chain, pool_address)``.

    For a DeFi POOL row the canonical manifest ``instrument_id`` is
    ``pool_address.lower()`` (matching the MTDS writer + ``_canonical_defi_id``),
    the ``venue`` is split to the bare protocol (``UNISWAP_V3``), the ``chain`` is
    populated (``ARBITRUM``), and ``glued_pair_id`` is the human-readable UI form
    ``UNISWAPV3-ARBITRUM:POOL:AAVE-USDC:100`` — all via the UAC SSOT converter
    ``build_pool_identity`` so the two forms stay reversible. For a non-pool /
    non-DeFi row this returns the instrument_key unchanged (instrument_id, "",
    venue, chain, "") so the catalogue row is untouched.
    """
    # Non-pool pass-through uses the row's RESOLVED id (``_row_id`` = instrument_key
    # OR instrument_id), not just instrument_key — a catalogue keyed only on
    # instrument_id (no instrument_key column) must keep that id.
    resolved_id = meta.get("_resolved_id") or meta.get("instrument_key") or ""
    itype = (meta.get("instrument_type") or "").strip().lower()
    pool_address = _pool_address_of(meta)
    if itype not in _DEFI_POOL_ITYPES or not pool_address:
        # Non-pool / non-DeFi fallthrough. For a non-pool DeFi row (lending / lst /
        # staking / perpetual) the venue may still arrive glued ``PROTOCOL-CHAIN`` or
        # no-underscore ghost (``AAVEV3-ARBITRUM``) from the by_date snapshot — split
        # it to the SINGLE canonical bare ``venue=PROTOCOL`` + a populated ``chain``
        # so a fresh catalogue regen NEVER re-introduces glued/ghost (the treadmill).
        # ``_canonical_bare_venue_chain`` is a no-op for an already-bare canonical
        # venue + for non-DeFi venues (no known chain suffix). SSOT:
        # codex/02-data/defi-canonical-naming-ssot.md.
        bare_v, bare_c = _canonical_bare_venue_chain(meta.get("venue") or "", meta.get("chain") or "")
        return resolved_id, "", bare_v, bare_c, ""

    venue_raw = meta.get("venue") or ""
    chain_raw = meta.get("chain") or ""
    # The legacy glued ``instrument_key`` carries the faithful raw fee token; the
    # by_date ``pool_fee_tier`` is bps. Prefer the key's fee for the UI id, else bps.
    fee = _fee_from_instrument_key(meta.get("instrument_key") or "") or (meta.get("pool_fee_tier") or "")
    identity = build_pool_identity(
        venue=venue_raw,
        chain=chain_raw,
        pool_address=pool_address,
        base_asset=meta.get("base_asset") or "",
        quote_asset=meta.get("quote_asset") or "",
        fee=fee or None,
    )
    return (
        identity.canonical_instrument_id,
        identity.glued_pair_id,
        identity.venue,
        identity.chain,
        identity.pool_address,
    )


def build_catalogue_dataframe(snapshots: Iterable[tuple[date, pd.DataFrame]]) -> pd.DataFrame:
    """Roll the per-date instrument definitions up into one lifecycle catalogue.

    Args:
        snapshots: iterable of ``(day, frame)`` pairs — one per
            ``by_date/day={day}/.../instruments.parquet`` slice. Each frame holds
            that day's instrument definitions (one row per instrument).

    Returns:
        A DataFrame with :data:`CATALOG_COLUMNS`, one row per distinct instrument:
        ``available_from`` = first day present (ISO ``YYYY-MM-DD``); ``available_to``
        (§7.3 venue-truth) = the venue-declared ``delisted_at`` / dated-contract
        ``expiry`` when present, else ``None`` (active) iff the instrument is present
        on its OWN venue's last FULL trading day (a thin/partial latest capture day is
        skipped so it cannot mass-false-delist live perps/spot), else the last day
        present (a genuine delisting). Metadata columns (instrument_type / venue /
        chain / league_id / market_created_at / settlement_time) are taken from the
        instrument's most-recent snapshot row. Rows are sorted by ``instrument_id``
        for deterministic output.
    """
    aggregates: dict[str, _InstrumentAggregate] = {}
    all_days: set[date] = set()
    # §7.3 per-venue, thin-day-aware liveness: per (venue, day) the instrument
    # count we observed, so a venue's last FULL capture day (not a thin/partial
    # latest day) governs perp/spot delisting.
    # Keyed on the CANONICAL venue form (ghost normalised via
    # ``canonicalize_defi_venue_combined``) so that old-key-scheme ghost venues
    # (``PANCAKESWAPV3-BSC``, ``AAVEV3-ARBITRUM``) are merged into their canonical
    # counterpart's liveness window.  Without this, a ghost venue whose last snapshot
    # was the May-8 switchover day generates its own tiny liveness window that marks
    # every pool that was last seen on that day as "present on its venue's last full
    # day" → ``available_to=None`` (false-active) — the +73 PANCAKESWAP_V3-BSC
    # old-format discrepancy (2026-06-27).  Normalisation is a no-op for already-
    # canonical or non-DeFi venues.
    venue_day_counts: dict[str, dict[date, int]] = {}
    # Memoise canonicalization per unique raw venue string (the by_date walk hits
    # the same venue thousands of times per run — avoid repeated UAC import calls).
    _canon_venue_cache: dict[str, str] = {}

    def _canonical_venue_key(raw: str) -> str:
        cached = _canon_venue_cache.get(raw)
        if cached is not None:
            return cached
        from unified_api_contracts.registry.capability_declarations._defi import canonicalize_defi_venue_combined

        canonical = canonicalize_defi_venue_combined(raw)
        _canon_venue_cache[raw] = canonical
        return canonical

    for day, frame in snapshots:
        all_days.add(day)
        if frame.empty:
            continue
        records: list[dict[str, object]] = frame.to_dict("records")  # pyright: ignore[reportAssignmentType]
        for row in records:
            iid = _row_id(row)
            if iid is None:
                continue
            _venue = str(row.get("venue") or "").strip()
            if _venue:
                _canonical_v = _canonical_venue_key(_venue)
                vc = venue_day_counts.setdefault(_canonical_v, {})
                vc[day] = vc.get(day, 0) + 1
            # DUAL-FORM lifecycle key (operator Refinement 1 + Phase 2 premature-delisting fix):
            # a DeFi POOL must accumulate ONE lifecycle keyed by its canonical pool
            # identity (chain + pool_address), NOT by the spelling-variant
            # ``instrument_key`` — the IS adapter switched the venue spelling
            # (``UNISWAPV3`` → ``UNISWAP_V3``) on ~2026-05-08, so the SAME physical
            # pool appears under two ``instrument_key``s; keyed by instrument_key the
            # OLD-spelling lifecycle CLOSES at the switchover (``available_to``=05-08)
            # → the canonical pool wrongly reads DELISTED on every later date (the
            # 2,311-pool 05-08 cliff; 2,199 pool_addresses present BOTH closed+open).
            # Keying the aggregate by the canonical pool identity collapses the
            # spelling variants into ONE continuous lifecycle → ``available_to``=None.
            agg_key = _aggregate_key(iid, row)
            declared = _declared_from(row)
            _meta = _extract_meta(row)
            _expiry = _parse_truth_date(_meta.get("expiry"))
            _delisted = _parse_truth_date(_meta.get("delisted_at"))
            existing = aggregates.get(agg_key)
            if existing is None:
                aggregates[agg_key] = _InstrumentAggregate(
                    first_day=day,
                    last_day=day,
                    meta_day=day,
                    meta=_meta,
                    declared_from=declared,
                    expiry=_expiry,
                    delisted_at=_delisted,
                )
                continue
            if day < existing.first_day:
                existing.first_day = day
            if day > existing.last_day:
                existing.last_day = day
            # BUG #4 (B): keep the EARLIEST declared listing date seen across snapshots.
            if declared is not None and (existing.declared_from is None or declared < existing.declared_from):
                existing.declared_from = declared
            # Metadata follows the most-recent definition of the instrument.
            if day >= existing.meta_day:
                existing.meta_day = day
                existing.meta = _meta
                # §7.3: venue-truth lifecycle dates follow the most-recent snapshot
                # (the freshest exchange-declared expiry / delisting for this id).
                existing.expiry = _expiry
                existing.delisted_at = _delisted

    if not all_days:
        return pd.DataFrame(columns=list(CATALOG_COLUMNS))

    # §7.3 fix — replace the global last-seen ``available_to`` (which mass-false-
    # delists every venue off a single thin/partial latest capture day) with a
    # PER-VENUE, thin-day-aware last-full-trading-day. ``_venue_last_full_day``
    # returns, per venue, the most-recent day whose instrument count is NOT a thin
    # outlier vs that venue's own recent history — so a partial capture of the
    # latest day never delists a venue's live universe.
    venue_last_full: dict[str, date] = {
        venue: _venue_last_full_day(day_counts) for venue, day_counts in venue_day_counts.items()
    }

    rows: list[dict[str, str | None]] = []
    for agg_key in sorted(aggregates):
        agg = aggregates[agg_key]
        # §7.3 ``available_to`` priority (venue truth first, last-seen only as a
        # labelled fallback for perps/spot with no venue-truth lifecycle field):
        #   1. explicit ``delisted_at`` (the venue reported removal) — venue truth.
        #   2. dated FUTURE/OPTION/COMBO ``expiry`` (the contract expiry) — venue truth.
        #   3. else perp/spot: ACTIVE (None) iff present on its OWN venue's last FULL
        #      trading day; else last-seen (a genuine delisting, not a thin-day artefact).
        _raw_venue = str(agg.meta.get("venue") or "").strip()
        # venue_last_full is keyed on CANONICAL venue names (ghost-normalised);
        # the aggregate's meta venue may still be the old ghost form if the
        # instrument's last snapshot used the pre-switchover adapter — canonicalise
        # before lookup so the liveness window resolves to the merged canonical entry.
        _canonical_meta_venue = _canonical_venue_key(_raw_venue) if _raw_venue else _raw_venue
        _venue_full_day = venue_last_full.get(_canonical_meta_venue)
        if agg.delisted_at is not None:
            available_to = agg.delisted_at.isoformat()
        elif agg.expiry is not None:
            available_to = agg.expiry.isoformat()
        elif _venue_full_day is not None and agg.last_day >= _venue_full_day:
            available_to = None
        else:
            available_to = agg.last_day.isoformat()
        # BUG #4 (B): available_from = MIN(observed first snapshot day, declared
        # listing date). A perp only observed on one recent snapshot still carries
        # its true historical listing date so the catalogue-driven backfill attempts
        # its full history. Legacy rows (no declared date) keep observed-day behaviour.
        available_from = agg.first_day
        if agg.declared_from is not None and agg.declared_from < available_from:
            available_from = agg.declared_from
        # DUAL-FORM (operator Refinement 1): for a DeFi POOL row re-key the
        # catalogue ``instrument_id`` to the canonical ``pool_address.lower()``
        # (matching the MTDS writer + the seeder), split ``venue`` to the bare
        # protocol + populate ``chain``, and carry the human-readable
        # ``glued_pair_id`` alongside. Non-pool / non-DeFi rows pass through
        # unchanged (canonical_id == the original instrument_key, glued_pair_id "").
        canonical_id, glued_pair_id, bare_venue, chain, pool_address = _defi_pool_dual_form(agg.meta)
        rows.append(
            {
                "instrument_id": canonical_id,
                "instrument_type": agg.meta["instrument_type"] or "",
                "venue": bare_venue,
                "chain": chain,
                "league_id": agg.meta["league_id"] or "",
                "available_from": available_from.isoformat(),
                "available_to": available_to,
                "market_created_at": agg.meta["market_created_at"],
                "settlement_time": agg.meta["settlement_time"],
                # Single-grain AGs leave data_type empty → the enumerator iterates
                # the full DATA_TYPES_BY_ASSET_GROUP list (legacy behaviour).
                "data_type": None,
                "underlying": agg.meta.get("underlying") or "",
                "raw_symbol": agg.meta.get("raw_symbol") or "",
                "base_asset": agg.meta.get("base_asset") or "",
                # Margin type: propagated from the per-date instruments parquet.
                # "" for non-derivative instruments (spot pairs, DeFi pools).
                "margin_type": agg.meta.get("margin_type") or "",
                "glued_pair_id": glued_pair_id,
                "pool_address": pool_address,
            }
        )

    return pd.DataFrame(rows, columns=list(CATALOG_COLUMNS))


def _extract_meta(row: dict[str, object]) -> dict[str, str | None]:
    """Pull the catalogue metadata fields out of a per-date instrument row."""
    return {
        "instrument_type": _str_field(row, "instrument_type"),
        "venue": _str_field(row, "venue"),
        "chain": _str_field(row, "chain"),
        "league_id": _str_field(row, "league_id"),
        "market_created_at": _opt_field(row, "market_created_at"),
        "settlement_time": _opt_field(row, "settlement_time"),
        # §7.3 venue-truth lifecycle close: the per-date instruments parquet carries
        # the exchange-declared ``expiry`` (dated FUTURE/OPTION/COMBO) and an explicit
        # ``delisted_at`` (when the venue reports a removal). ``available_to`` is keyed
        # off these (venue truth) — NOT last-seen — so a thin/partial latest capture
        # day cannot mass-false-delist live perps/spot. Blank for non-dated rows.
        "expiry": _opt_field(row, "expiry"),
        "delisted_at": _opt_field(row, "delisted_at"),
        "underlying": _str_field(row, "underlying"),
        "raw_symbol": _str_field(row, "raw_symbol"),
        "base_asset": _str_field(row, "base_asset"),
        # Margin type: "linear" | "inverse" | "" (empty for non-derivatives).
        # Propagated from the per-date instruments parquet margin_type column
        # (IS cefi Tardis adapter populates it for PERPETUAL/FUTURE rows).
        "margin_type": _str_field(row, "margin_type"),
        # DeFi dual-form pool-id source fields (operator Refinement 1) — used by
        # ``_defi_pool_dual_form`` to derive the canonical instrument_id +
        # glued_pair_id. Blank for non-DeFi rows.
        "quote_asset": _str_field(row, "quote_asset"),
        "pool_address": _str_field(row, "pool_address"),
        "pool_fee_tier": _opt_field(row, "pool_fee_tier"),
        # The original glued ``instrument_key`` — carried so ``_defi_pool_dual_form``
        # can recover the faithful raw fee token for the human-readable glued_pair_id
        # even though the lifecycle is keyed by the canonical pool identity.
        "instrument_key": _str_field(row, "instrument_key"),
        # The row's RESOLVED id (instrument_key OR instrument_id) — the non-pool
        # pass-through canonical id (a catalogue keyed only on instrument_id keeps it).
        "_resolved_id": _row_id(row) or "",
    }


# ---------------------------------------------------------------------------
# Prediction MULTI-GRAIN roll-up (cqg bundle + per-conditionId)
# ---------------------------------------------------------------------------


@dataclass
class _PredLifecycle:
    """Lifecycle accumulator for one prediction entity (a cqg OR a conditionId)."""

    first_day: date
    last_day: date
    venue: str
    instrument_type: str
    #: min ``start_date`` / max ``end_date_iso`` / min ``available_from_datetime``
    #: across per-date rows — when the snapshot carries them (more precise than day-presence).
    created: str | None = None
    #: ISO date string of the market's settlement day (max ``end_date_iso`` /
    #: ``available_to_datetime`` across rows).  ``None`` when the snapshot provides no
    #: settlement date.  Used as ``available_to`` in the catalogue (settlement-date
    #: convention — prediction settlement / availability semantics SSOT:
    #: ``codex/02-data/prediction-settlement-availability-convention.md``).
    settled: str | None = None


def _merge_lifecycle(
    acc: dict[tuple[str, str], _PredLifecycle],
    key: tuple[str, str],
    day: date,
    venue: str,
    instrument_type: str,
    created: str | None,
    settled: str | None,
) -> None:
    """Fold one (entity, day) observation into the lifecycle accumulator."""
    cur = acc.get(key)
    if cur is None:
        acc[key] = _PredLifecycle(
            first_day=day,
            last_day=day,
            venue=venue,
            instrument_type=instrument_type,
            created=created,
            settled=settled,
        )
        return
    if day < cur.first_day:
        cur.first_day = day
    if day > cur.last_day:
        cur.last_day = day
        # Metadata (instrument_type) follows the most-recent observation.
        if instrument_type:
            cur.instrument_type = instrument_type
    if created and (cur.created is None or created < cur.created):
        cur.created = created
    if settled and (cur.settled is None or settled > cur.settled):
        cur.settled = settled


def build_prediction_catalogue_dataframe(
    snapshots: Iterable[tuple[date, str, str, pd.DataFrame]],
) -> pd.DataFrame:
    """Roll the prediction per-date definitions up into a MULTI-GRAIN catalogue.

    Prediction has two captured grains (``DATA_TYPES_BY_ASSET_GROUP["prediction"]``
    is mixed-grain): the ``prediction_canonical_question_group`` bundle is per
    canonical-question-group, while ``trades`` / ``market_lifecycle`` are per
    conditionId. A single-grain roll-up (the generic ``build_catalogue_dataframe``,
    one row per ``instrument_key`` = conditionId) would seed the cqg denominator
    at conditionId grain → inflated by the cqg→conditionId fan-out. So this
    producer emits ONE catalogue row per grain, each carrying its own
    ``data_type`` (the v2 enumerator then seeds each at the right grain — see
    ``enumerate_expected_universe.InstrumentCatalogEntry.data_type``).

    Args:
        snapshots: iterable of ``(day, venue, cqg, frame)`` — one per
            ``instrument_availability/by_date/day=/venue=/canonical_question_group=/instruments.parquet``
            blob. ``venue`` + ``cqg`` come from the PATH (the writer drops the
            ``_canonical_group`` column); ``frame`` holds that day's per-market
            InstrumentRecords (``instrument_key`` = conditionId).

    Returns:
        A DataFrame with :data:`CATALOG_COLUMNS`:
          * one row per ``(venue, cqg)`` with ``data_type=prediction_canonical_question_group``,
            ``instrument_id=cqg``;
          * one row per ``(venue, conditionId)`` for EACH of
            :data:`_PREDICTION_CID_DATA_TYPES`, ``instrument_id=conditionId``.
        ``available_from`` = first day present; ``available_to`` = settlement date
        (from ``end_date_iso`` / ``available_to_datetime``) when the snapshot carries
        one, else last snapshot day (``None`` = open-ended when last_day >= latest_day).
        Settlement-date convention: a market is considered active on day D iff
        ``available_from <= D <= available_to``, which equals the set of days the
        market is capturable — see SSOT
        ``codex/02-data/prediction-settlement-availability-convention.md``.
    """
    cqg_acc: dict[tuple[str, str], _PredLifecycle] = {}
    cid_acc: dict[tuple[str, str], _PredLifecycle] = {}
    all_days: set[date] = set()

    for day, venue, cqg, frame in snapshots:
        all_days.add(day)
        venue_str = venue.strip()
        cqg_str = cqg.strip()
        # 249-a: the conditionId grain (instrument_key) accumulates from EVERY
        # non-empty frame — it does NOT require a canonical_question_group. The
        # cqg grain (gated below on cqg_str) is materialised only when the writer
        # emits a cqg, which it does not in the current venue=/market= layout
        # (that's 249-b, gated on operator decision 338). Skipping a frame on
        # `not cqg_str` (the pre-fix behaviour) dropped BOTH grains → 0-row
        # catalogue. Skip only genuinely-empty frames.
        if frame.empty:
            continue
        records: list[dict[str, object]] = frame.to_dict("records")  # pyright: ignore[reportAssignmentType]
        # cqg-grain lifecycle: the cqg is present on this day if ANY member is.
        cqg_itype = ""
        cqg_created: str | None = None
        cqg_settled: str | None = None
        saw_member = False
        for row in records:
            cid = _row_id(row)
            if cid is None:
                continue
            saw_member = True
            itype = _str_field(row, "instrument_type")
            created = (
                _opt_field(row, "start_date")
                or _opt_field(row, "market_created_at")
                or _opt_field(row, "available_from_datetime")
            )
            # Collect the settlement date from whichever field the venue's snapshot
            # format carries: raw Polymarket → ``end_date_iso``; IS-normalised KALSHI
            # → ``available_to_datetime``; explicit settlement field → ``settlement_time``.
            # This populates ``lc.settled`` so that ``_emit`` can set ``available_to``
            # from the venue-declared settlement date (not from last-snapshot-day).
            settled = (
                _opt_field(row, "end_date_iso")
                or _opt_field(row, "settlement_time")
                or _opt_field(row, "available_to_datetime")
            )
            cqg_itype = cqg_itype or itype
            if created and (cqg_created is None or created < cqg_created):
                cqg_created = created
            if settled and (cqg_settled is None or settled > cqg_settled):
                cqg_settled = settled
            _merge_lifecycle(cid_acc, (venue_str, cid), day, venue_str, itype, created, settled)
        # cqg grain only when the writer emits a cqg (249-b, gated on decision
        # 338). Currently always empty → no cqg rows, conditionId grain only.
        if saw_member and cqg_str:
            _merge_lifecycle(cqg_acc, (venue_str, cqg_str), day, venue_str, cqg_itype, cqg_created, cqg_settled)

    if not all_days:
        return pd.DataFrame(columns=list(CATALOG_COLUMNS))
    latest_day = max(all_days)

    rows: list[dict[str, str | None]] = []

    def _emit(entity_id: str, data_type: str, lc: _PredLifecycle) -> None:
        # Settlement-date convention (SSOT: codex/02-data/prediction-settlement-availability-convention.md):
        # ``available_to`` = the market's settlement DATE (inclusive last day), so
        # ``available_from <= D <= available_to`` iff the market was live/capturable on D.
        # Priority: (1) venue-declared settlement date in ``lc.settled``; (2) last
        # snapshot day (open-ended ``None`` when last_day >= latest_day — still active).
        settled_date = _parse_truth_date(lc.settled)
        if settled_date is not None:
            # Venue-declared settlement: use that as the inclusive upper bound.
            # Cap at latest_day so genuinely future dates don't create a false "delisted"
            # signal in the enumerator when the market is still live (settlement in future
            # but already in a snapshot → mark open-ended until that day arrives).
            available_to = None if settled_date >= latest_day else settled_date.isoformat()
        elif lc.last_day >= latest_day:
            available_to = None  # still active (open-ended)
        else:
            available_to = lc.last_day.isoformat()
        rows.append(
            {
                "instrument_id": entity_id,
                "instrument_type": lc.instrument_type or "",
                "venue": lc.venue,
                "chain": "",
                "league_id": "",
                "available_from": lc.first_day.isoformat(),
                "available_to": available_to,
                "market_created_at": lc.created,
                "settlement_time": lc.settled,
                "data_type": data_type,
                # Non-DeFi grain → no dual-form pool ids.
                "glued_pair_id": "",
                "pool_address": "",
            }
        )

    for _venue, cqg_id in sorted(cqg_acc):
        _emit(cqg_id, _PREDICTION_CQG_DATA_TYPE, cqg_acc[(_venue, cqg_id)])
    for _venue, cid in sorted(cid_acc):
        for dt in _PREDICTION_CID_DATA_TYPES:
            _emit(cid, dt, cid_acc[(_venue, cid)])

    return pd.DataFrame(rows, columns=list(CATALOG_COLUMNS))


# ---------------------------------------------------------------------------
# Sports LEAGUE-GRAIN roll-up (entity=leagues)
# ---------------------------------------------------------------------------


def _league_id_of(row: dict[str, object]) -> str | None:
    """Return the league identifier for an ``entity=leagues`` row, or None.

    The sports ``leagues`` parquet carries a raw provider ``league_id`` column
    (api-football numeric id, as a string) — NOT the ``instrument_key`` /
    ``instrument_id`` the generic :func:`build_catalogue_dataframe` requires
    (which is why a plain ``--asset-group sports`` run yields a 0-row catalogue).
    """
    raw = row.get("league_id")
    if raw is None:
        return None
    try:
        if pd.isna(raw):  # pyright: ignore[reportArgumentType]
            return None
    except (TypeError, ValueError):
        pass
    text = str(raw).strip()
    return text or None


def build_sports_catalogue_dataframe(snapshots: Iterable[tuple[date, pd.DataFrame]]) -> pd.DataFrame:
    """Roll the per-date ``entity=leagues`` definitions up into a LEAGUE-grain catalogue.

    The sports captured manifest atom is per-``(league_id, data_type, date)``
    (slot-4 finding 2026-06-07 on the canonical ``instruments-store-sports-prd``
    ``_index``: ``league_id`` populated 97.6% / ``venue`` blank 97.7% /
    ``instrument_id`` blank ~100%), so the could-exist universe is per-LEAGUE,
    not per-fixture. A fixture-grain catalogue would never match the league-grain
    manifest present-set → every cell would seed ``expected_unattempted`` →
    massively inflated coverage denominator.

    So this producer emits ONE catalogue row per league. The data_type axis is
    NOT bound here (``data_type=None``): the v2 sports enumerator iterates the
    captured sports data_types itself and applies each source's
    ``SOURCE_COVERAGE_START`` / ``DATA_TYPE_COVERAGE_START`` window
    (``enumerate_expected_universe._enumerate_v2_sports``).

    ``venue`` / ``instrument_id`` / ``instrument_type`` are deliberately written
    so the seeded ``expected_unattempted`` atom matches the captured atom:
    ``venue=""`` (captured venue is blank), ``instrument_id=league_id`` (a stable
    per-row identity for the monotonic guard; the enumerator blanks it on the
    yielded row so it does NOT participate in the league-grain present-set match),
    ``instrument_type="league"``, ``league_id=<league_id>``.

    Args:
        snapshots: iterable of ``(day, frame)`` pairs — one per
            ``sports_reference/by_date/day={day}/entity=leagues/leagues.parquet``
            slice. Each frame holds that day's league definitions (``league_id``
            / ``name`` / ``country`` / …; one row per league).

    Returns:
        A DataFrame with :data:`CATALOG_COLUMNS`, one row per distinct league:
        ``available_from`` = first day the league appears; ``available_to`` =
        last day present, or ``None`` when present on the latest snapshot day
        (still active). Rows are sorted by ``league_id`` for deterministic output.
    """
    first_day: dict[str, date] = {}
    last_day: dict[str, date] = {}
    all_days: set[date] = set()

    for day, frame in snapshots:
        all_days.add(day)
        if frame.empty:
            continue
        records: list[dict[str, object]] = frame.to_dict("records")  # pyright: ignore[reportAssignmentType]
        for row in records:
            lid = _league_id_of(row)
            if lid is None:
                continue
            if lid not in first_day or day < first_day[lid]:
                first_day[lid] = day
            if lid not in last_day or day > last_day[lid]:
                last_day[lid] = day

    if not all_days:
        return pd.DataFrame(columns=list(CATALOG_COLUMNS))

    latest_day = max(all_days)

    rows: list[dict[str, str | None]] = []
    for lid in sorted(first_day):
        available_to = None if last_day[lid] >= latest_day else last_day[lid].isoformat()
        rows.append(
            {
                "instrument_id": lid,
                "instrument_type": SPORTS_LEAGUE_INSTRUMENT_TYPE,
                # Blank venue → matches the venue-blank captured manifest atom.
                "venue": "",
                "chain": "",
                "league_id": lid,
                "available_from": first_day[lid].isoformat(),
                "available_to": available_to,
                "market_created_at": None,
                "settlement_time": None,
                # Single-grain-per-league: the enumerator iterates the sports
                # data_types (source-coverage-aware), so leave data_type unbound.
                "data_type": None,
                # Non-DeFi grain → no dual-form pool ids.
                "glued_pair_id": "",
                "pool_address": "",
            }
        )

    return pd.DataFrame(rows, columns=list(CATALOG_COLUMNS))


# ---------------------------------------------------------------------------
# Sports LEAGUE-GRAIN roll-up — FROM THE MANIFEST (the namespace-correct universe)
# ---------------------------------------------------------------------------


def build_sports_catalogue_from_manifest(manifest_df: pd.DataFrame) -> pd.DataFrame:
    """Build the sports LEAGUE-grain could-exist catalogue from the MANIFEST.

    Supersedes :func:`build_sports_catalogue_dataframe` (the ``entity=leagues``
    roll-up) as the sports could-exist source. **Why** (slot-4 re-diagnosis
    2026-06-07, measured on real prod): the ``entity=leagues`` slice carries RAW
    NUMERIC api-football ``league_id``s (``"4"`` / ``"21"`` / …), but the captured
    manifest atom uses the **canonical** sports league namespace, and
    ``canonicalize_league_id()`` is a NO-OP on the numeric ids — so the
    entity=leagues roll-up covered only **131 / 606** distinct manifest
    current-data_type leagues and an ``--apply-write`` would MASSIVELY OVER-seed
    false ``expected_unattempted`` for numeric leagues that match no manifest row.

    The reliable, namespace-correct superset is the MANIFEST itself: every league
    that was captured for a **current** data_type provably could-exist. So this
    producer emits one catalogue row per distinct manifest ``league_id`` (scoped to
    the current ``SPORTS_DATA_TYPE_TO_SOURCE`` data_types — the retired
    ``LEAGUES`` / ``TRANSFERMARKT_LEAGUES`` / ``SFI_LEAGUES`` numeric/hex namespaces
    are excluded), with ``available_from`` = first captured date and
    ``available_to`` = ``None`` (active — the v2 enumerator applies each source's
    coverage-start window + per-entity league coverage). This guarantees the
    catalogue league set ⊇ the manifest league set by construction.

    Honest residual delta (a follow-up, NOT a silent drop): api-football leagues
    that are LISTED (``entity=leagues``) but were never captured for any current
    data_type are not added here — adding them needs a numeric→canonical
    ``api_football_id`` map (``canonicalize_league_id`` does not provide it) and is
    gated on the IS instrument backfill regardless.

    Args:
        manifest_df: the canonical sports ``_index`` (needs ``league_id`` /
            ``data_type`` / ``date`` columns). Read via
            :func:`_read_sports_manifest_index`.

    Returns:
        A DataFrame with :data:`CATALOG_COLUMNS`, one row per distinct current
        canonical league, sorted by ``league_id`` for deterministic output.
    """
    cols = list(CATALOG_COLUMNS)
    needed = {"league_id", "data_type", "date"}
    if manifest_df.empty or not needed.issubset(manifest_df.columns):
        return pd.DataFrame(columns=cols)

    from unified_api_contracts.sports import SPORTS_DATA_TYPE_TO_SOURCE

    current = frozenset(SPORTS_DATA_TYPE_TO_SOURCE)
    df = manifest_df.loc[:, ["league_id", "data_type", "date"]].copy()
    df["league_id"] = df["league_id"].fillna("").astype(str)
    df["data_type"] = df["data_type"].fillna("").astype(str)
    df = df[(df["league_id"] != "") & (df["data_type"].isin(current))]
    if df.empty:
        return pd.DataFrame(columns=cols)
    df["date"] = df["date"].astype(str)
    first_seen = df.groupby("league_id")["date"].min()

    rows: list[dict[str, str | None]] = [
        {
            "instrument_id": lid,
            "instrument_type": SPORTS_LEAGUE_INSTRUMENT_TYPE,
            "venue": "",
            "chain": "",
            "league_id": lid,
            "available_from": first_seen[lid],
            "available_to": None,
            "market_created_at": None,
            "settlement_time": None,
            "data_type": None,
            "glued_pair_id": "",
            "pool_address": "",
        }
        for lid in sorted(first_seen.index)
    ]
    return pd.DataFrame(rows, columns=cols)


def _read_sports_manifest_index(storage: StorageClient, bucket: str) -> pd.DataFrame:
    """Read the canonical sports ``_index`` for the manifest-derived league universe.

    Reads the consolidated ``_index/availability_index.parquet`` DIRECTLY (NOT
    ``read_availability_index`` — its per-VM-consolidated view has been observed
    to return 0 rows mid-rewrite; slot-4 2026-06-07). Returns an empty frame when
    the index is absent (→ empty catalogue, never a silent crash).
    """
    blob_path = "_index/availability_index.parquet"
    if not storage.blob_exists(bucket, blob_path):
        logger.warning("Sports manifest _index absent at gs://%s/%s — empty catalogue", bucket, blob_path)
        return pd.DataFrame()
    payload = storage.download_bytes(bucket, blob_path)
    return pd.read_parquet(io.BytesIO(payload), columns=["league_id", "data_type", "date"])


# ---------------------------------------------------------------------------
# Monotonic-guard decision (pure — unit-tested directly)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GuardDecision:
    """Outcome of the monotonic row-count guard."""

    accept: bool
    reason: str


def evaluate_monotonic_guard(
    new_count: int,
    current_count: int | None,
    *,
    allow_shrink: bool,
) -> GuardDecision:
    """Decide whether a freshly-rolled catalogue may replace the current one.

    Instrument rows grow monotonically — new listings add rows and delisted rows
    persist (with ``available_to`` stamped) — so a strictly smaller row count
    signals an incomplete / buggy regeneration, not a real change.

    Args:
        new_count: row count of the freshly-rolled catalogue.
        current_count: row count of the current canonical catalogue, or None when
            no catalogue exists yet (first run).
        allow_shrink: when True, a legitimate corrective shrink is permitted.

    Returns:
        A :class:`GuardDecision`; ``accept=False`` means KEEP the previous
        catalogue and do NOT overwrite.
    """
    if current_count is None:
        return GuardDecision(accept=True, reason="no_prior_catalogue")
    if new_count >= current_count:
        return GuardDecision(accept=True, reason="monotonic_ok")
    if allow_shrink:
        return GuardDecision(accept=True, reason="shrink_overridden")
    return GuardDecision(accept=False, reason="shrink_blocked")


# ---------------------------------------------------------------------------
# GCS I/O
# ---------------------------------------------------------------------------


def _tune_download_pool(storage: StorageClient, size: int) -> None:
    """Best-effort: enlarge the GCS client's HTTP connection pool to ``size``.

    The native ``google.cloud.storage`` client defaults to a small (~8-10) urllib3
    connection pool, which throttles the concurrent by_date download below the
    ``max_workers`` count (the "Connection pool is full, discarding connection"
    warning) — the full-corpus walk then runs at ~half the intended concurrency.
    Mounting a larger ``HTTPAdapter`` on the client's session lifts that cap.

    No-op on a non-GCS client or when the native client does not expose a mountable
    session (guarded via ``getattr``/``hasattr`` — degrades to the default pool,
    never raises).
    """
    if getattr(storage, "provider_name", "") != "gcp":
        return
    native = getattr(storage, "_client", None)
    http = getattr(native, "_http", None)
    if http is None or not hasattr(http, "mount"):
        return
    from requests.adapters import HTTPAdapter

    adapter = HTTPAdapter(pool_connections=size, pool_maxsize=size)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    logger.info("Tuned GCS HTTP connection pool to %d (matches download workers)", size)


def _bounded_parallel_load(
    items: list[_LoadItemT],
    load: Callable[[_LoadItemT], _LoadResultT],
    *,
    max_workers: int,
) -> Iterator[_LoadResultT]:
    """Stream ``load(item)`` over ``items`` with a memory-bounded sliding window.

    **Why this exists (OOM root-fix, 2026-06-23).** The previous
    ``yield from pool.map(load, items)`` submitted ALL ~11.6k by_date download
    tasks at once. ``ThreadPoolExecutor.map`` yields results in SUBMISSION order,
    so if blob #0 is slow EVERY later completed frame buffers in RAM waiting its
    turn — peak memory is O(len(items)) decoded parquet frames (the whole corpus
    in memory at once → the catalogue-regen Cloud Run job hit "configured memory
    limit was reached" and OOM-died even at 32Gi, leaving ``catalog.parquet`` stale).

    This keeps at most ``max_workers`` futures in flight (a sliding window): submit
    a window, then for each completed future yield its result and immediately drop
    the reference + top the window back up with the next item. Peak in-flight
    decoded frames is O(max_workers), NOT O(len(items)). Results are yielded in
    COMPLETION order — order does NOT matter to the lifecycle roll-up
    (:func:`build_catalogue_dataframe` accumulates per-instrument min/max across all
    snapshots regardless of arrival order; ``all_days`` is a set), so completion
    order is correct and strictly more memory-efficient than submission order.

    A per-item ``load`` exception propagates (the run fails loud rather than
    silently producing an under-counted catalogue the monotonic guard rejects).
    """
    if not items:
        return
    window = max(1, max_workers)
    with ThreadPoolExecutor(max_workers=window) as pool:
        pending: set[Future[_LoadResultT]] = set()
        next_idx = 0
        # Prime the window.
        while next_idx < len(items) and len(pending) < window:
            pending.add(pool.submit(load, items[next_idx]))
            next_idx += 1
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for fut in done:
                yield fut.result()  # propagates the first worker exception
                # Top the window back up as each slot frees.
                if next_idx < len(items):
                    pending.add(pool.submit(load, items[next_idx]))
                    next_idx += 1


def _iter_by_date_snapshots(
    storage: StorageClient,
    bucket: str,
    prefix: str,
    *,
    max_blobs: int | None = None,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
) -> Iterator[tuple[date, pd.DataFrame]]:
    """Yield ``(day, frame)`` for every ``by_date`` instruments parquet under ``prefix``.

    Downloads run concurrently (I/O-bound, ``max_workers`` threads) — the full
    ``by_date`` history is thousands of parquets and single-threaded download does
    not complete in a reasonable window. A per-blob download/read error propagates
    (the run fails loud rather than silently producing an under-counted catalogue
    the monotonic guard would then reject anyway).

    ``max_blobs`` truncates the walk to the first N parquets (path-sorted, for
    determinism) — DIAGNOSTIC ONLY: a truncated walk yields an INCOMPLETE catalogue
    with wrong ``available_from`` / ``available_to``, so the caller forces dry-run
    when it is set (never promotable).
    """
    walk_prefix = prefix.rstrip("/") + "/"
    targets: list[tuple[date, str]] = []
    for blob in storage.list_blobs(bucket, prefix=walk_prefix):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        match = _DAY_RE.search(name)
        if match is None:
            logger.warning("Skipping blob with no day= partition: %s", name)
            continue
        targets.append((date.fromisoformat(match.group(1)), name))

    targets.sort(key=lambda item: item[1])
    if max_blobs is not None:
        targets = targets[:max_blobs]
    logger.info("Found %d by_date parquet(s) to roll up (workers=%d)", len(targets), max_workers)

    def _load(item: tuple[date, str]) -> tuple[date, pd.DataFrame]:
        day, name = item
        payload = storage.download_bytes(bucket, name)
        return day, pd.read_parquet(io.BytesIO(payload))

    # Memory-bounded sliding window (peak O(max_workers) frames, NOT O(len(targets)))
    # — see _bounded_parallel_load: the full-corpus pool.map() OOM-killed the
    # catalogue-regen Cloud Run job. Completion-order yield is correct here (the
    # lifecycle roll-up is order-independent).
    yield from _bounded_parallel_load(targets, _load, max_workers=max_workers)


def _iter_prediction_by_date_snapshots(
    storage: StorageClient,
    bucket: str,
    prefix: str,
    *,
    max_blobs: int | None = None,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
) -> Iterator[tuple[date, str, str, pd.DataFrame]]:
    """Yield ``(day, venue, cqg, frame)`` for every prediction ``by_date`` blob.

    **249-a (2026-06-16): conditionId/market grain reads the ACTUAL writer
    layout.** The prediction writer partitions ``by_date/day=/venue=<V>/[market=<M>/]
    instruments.parquet`` — it does NOT emit a ``canonical_question_group=`` path
    segment (the prior code required one and so skipped EVERY blob → 0-row
    catalogue). The conditionId (``instrument_key``) is read from the FRAME, so
    the cqg is no longer needed for the conditionId grain; ``cqg`` is yielded as
    ``""`` (the cqg grain is 249-b, gated on operator decision 338, and the
    rollup only materialises it when cqg is non-empty). Both the venue-level
    ``instruments.parquet`` (full conditionId universe) and the per-market
    ``market=<M>/instruments.parquet`` blobs are read; the rollup dedups by
    ``(venue, conditionId)`` via ``_merge_lifecycle``. The metadata sibling
    ``prediction_market_metadata.parquet`` is excluded (it is not an instruments
    frame). Blobs with no ``day=``/``venue=`` partition are skipped with a warning.

    Downloads run concurrently (``max_workers`` threads), mirroring
    :func:`_iter_by_date_snapshots` — the prediction ``by_date`` history is also
    thousands of parquets. ``max_blobs`` truncates the path-sorted walk for a
    DIAGNOSTIC smoke-test (forces dry-run upstream — a truncated walk is never
    promotable).
    """
    walk_prefix = prefix.rstrip("/") + "/"
    targets: list[tuple[date, str, str, str]] = []
    for blob in storage.list_blobs(bucket, prefix=walk_prefix):
        name = blob.name
        # Only the instruments frames — never the prediction_market_metadata.parquet sibling.
        if not name.endswith("instruments.parquet"):
            continue
        day_m = _DAY_RE.search(name)
        venue_m = _VENUE_RE.search(name)
        if day_m is None or venue_m is None:
            logger.warning("Skipping prediction blob missing day=/venue=: %s", name)
            continue
        cqg_m = _CQG_RE.search(name)
        cqg = cqg_m.group(1) if cqg_m else ""
        targets.append((date.fromisoformat(day_m.group(1)), venue_m.group(1), cqg, name))

    targets.sort(key=lambda item: item[3])
    if max_blobs is not None:
        targets = targets[:max_blobs]
    logger.info("Found %d prediction by_date parquet(s) to roll up (workers=%d)", len(targets), max_workers)

    def _load(item: tuple[date, str, str, str]) -> tuple[date, str, str, pd.DataFrame]:
        day, venue, cqg, name = item
        payload = storage.download_bytes(bucket, name)
        return day, venue, cqg, pd.read_parquet(io.BytesIO(payload))

    # Memory-bounded sliding window (peak O(max_workers) frames, NOT O(len(targets)))
    # — see _bounded_parallel_load: the full-corpus pool.map() OOM-killed the
    # catalogue-regen Cloud Run job. Completion-order yield is correct here (the
    # lifecycle roll-up is order-independent).
    yield from _bounded_parallel_load(targets, _load, max_workers=max_workers)


def _iter_sports_by_date_snapshots(
    storage: StorageClient,
    bucket: str,
    prefix: str,
    *,
    max_blobs: int | None = None,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
) -> Iterator[tuple[date, pd.DataFrame]]:
    """Yield ``(day, frame)`` for every ``entity=leagues`` parquet under ``prefix``.

    The sports ``by_date`` tree carries ~17 entities per day (leagues / fixtures
    / teams / standings / …); the league-grain catalogue rolls up the
    ``entity=leagues`` slice ONLY. Non-leagues entities + parquets with no
    ``day=`` partition are skipped (the latter with a warning). Downloads run
    concurrently (``max_workers`` threads), mirroring :func:`_iter_by_date_snapshots`.

    ``max_blobs`` truncates the path-sorted walk for a DIAGNOSTIC smoke-test
    (forces dry-run upstream — a truncated walk is never promotable).
    """
    walk_prefix = prefix.rstrip("/") + "/"
    targets: list[tuple[date, str]] = []
    for blob in storage.list_blobs(bucket, prefix=walk_prefix):
        name = blob.name
        if not name.endswith(".parquet"):
            continue
        entity_m = _ENTITY_RE.search(name)
        if entity_m is None or entity_m.group(1) != SPORTS_LEAGUE_ENTITY:
            continue  # not the leagues slice — skip silently (other entities)
        day_m = _DAY_RE.search(name)
        if day_m is None:
            logger.warning("Skipping sports leagues blob with no day= partition: %s", name)
            continue
        targets.append((date.fromisoformat(day_m.group(1)), name))

    targets.sort(key=lambda item: item[1])
    if max_blobs is not None:
        targets = targets[:max_blobs]
    logger.info("Found %d sports leagues by_date parquet(s) to roll up (workers=%d)", len(targets), max_workers)

    def _load(item: tuple[date, str]) -> tuple[date, pd.DataFrame]:
        day, name = item
        payload = storage.download_bytes(bucket, name)
        return day, pd.read_parquet(io.BytesIO(payload))

    # Memory-bounded sliding window (peak O(max_workers) frames, NOT O(len(targets)))
    # — see _bounded_parallel_load: the full-corpus pool.map() OOM-killed the
    # catalogue-regen Cloud Run job. Completion-order yield is correct here (the
    # lifecycle roll-up is order-independent).
    yield from _bounded_parallel_load(targets, _load, max_workers=max_workers)


def _read_current_row_count(storage: StorageClient, bucket: str, blob_path: str) -> int | None:
    """Return the row count of the current canonical catalogue, or None when absent."""
    if not storage.blob_exists(bucket, blob_path):
        return None
    payload = storage.download_bytes(bucket, blob_path)
    current = pd.read_parquet(io.BytesIO(payload))
    return len(current)


def _catalogue_object_paths(env: str) -> tuple[str, str]:
    """Return ``(canonical_blob, temp_blob)`` for the env tier."""
    canonical = f"{env}/{CATALOG_FILENAME}"
    temp = f"{env}/_catalogue_staging/{CATALOG_FILENAME}"
    return canonical, temp


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise a catalogue DataFrame to parquet bytes."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def promote_catalogue(
    storage: StorageClient,
    bucket: str,
    env: str,
    df: pd.DataFrame,
    *,
    allow_shrink: bool,
    dry_run: bool,
) -> int:
    """Apply the monotonic-guard promotion. Returns a process exit code (0 = ok)."""
    canonical_blob, temp_blob = _catalogue_object_paths(env)
    new_count = len(df)
    current_count = _read_current_row_count(storage, bucket, canonical_blob)
    decision = evaluate_monotonic_guard(new_count, current_count, allow_shrink=allow_shrink)

    logger.info(
        "Monotonic guard: new=%d current=%s decision=%s (%s)",
        new_count,
        current_count,
        "ACCEPT" if decision.accept else "REJECT",
        decision.reason,
    )

    if not decision.accept:
        _emit_event(
            "CATALOGUE_SHRINK_BLOCKED",
            bucket=bucket,
            env=env,
            new_count=new_count,
            current_count=current_count,
            hint="re-run a complete regeneration, or pass --allow-catalogue-shrink for a corrective shrink",
        )
        logger.error(
            "CATALOGUE_SHRINK_BLOCKED: new=%d < current=%d — keeping previous good catalogue at gs://%s/%s "
            "(pass --allow-catalogue-shrink to override for a legitimate corrective shrink)",
            new_count,
            current_count,
            bucket,
            canonical_blob,
        )
        return 1

    if dry_run:
        logger.info(
            "[dry-run] would promote %d-row catalogue to gs://%s/%s",
            new_count,
            bucket,
            canonical_blob,
        )
        return 0

    payload = _to_parquet_bytes(df)
    # Temp-first so a crash mid-write never corrupts the canonical object.
    storage.upload_bytes(bucket, temp_blob, payload, content_type="application/octet-stream")
    storage.copy_blob(bucket, temp_blob, bucket, canonical_blob)
    storage.delete_blob(bucket, temp_blob)
    _emit_event(
        "CATALOGUE_PROMOTED",
        bucket=bucket,
        env=env,
        rows=new_count,
        path=f"gs://{bucket}/{canonical_blob}",
        guard_reason=decision.reason,
    )
    logger.info("Promoted %d-row catalogue to gs://%s/%s", new_count, bucket, canonical_blob)
    return 0


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _add_mvp_column(df: pd.DataFrame, asset_group: str) -> pd.DataFrame:
    """Tag each catalogue row with ``mvp: bool`` via the UAC ``is_mvp`` predicate.

    The predicate is the UAC SSOT for "what is MVP scope"; it is applied on-the-fly
    over the rolled-up catalogue (real expiries) — never baked into a rule here.
    Empty-catalogue frames keep the typed ``mvp`` column so the schema is stable.

    The catalogue carries ``data_type=None`` for single-grain asset groups (the
    instrument exists across ALL the AG's data_types — data_type is not a catalogue
    axis). ``is_mvp`` now honours the **unbound-data_type convention** (a blank
    ``data_type`` == "any MVP data_type", mvp_instrument_universe_gap_audit P2 #2),
    so a single-grain row simply passes its (absent) data_type through — no local
    "representative data_type" workaround needed. A row that DOES carry a data_type
    (prediction multi-grain) uses its own. The base asset comes from ``base_asset``
    (spot/perp legs) with ``underlying`` as the derivative/option fallback — the axis
    the cefi/tradfi MVP rules gate on.
    """
    if df.empty:
        out = df.copy()
        out["mvp"] = pd.Series([], dtype="bool")
        return out

    def _cell(row: pd.Series[object], col: str) -> str:
        """Return a row's string cell, treating NaN/None/empty as "".

        ``str(np.nan)`` is ``"nan"`` and ``np.nan or x`` keeps the NaN (NaN is
        truthy) — so a naive ``str(row.get(col) or "")`` turns a missing
        ``base_asset`` into the literal ``"nan"`` (the all-False MVP tag bug,
        mvp_instrument_universe_gap_audit 2026-06-17). Guard with ``pd.isna``.
        """
        raw = row.get(col)
        if raw is None:
            return ""
        try:
            if pd.isna(raw):  # pyright: ignore[reportArgumentType]
                return ""
        except (TypeError, ValueError):
            pass
        return str(raw)

    # DeFi tag-all default (defi_mvp_tag_all_2026_06_26): the UAC ``is_mvp``
    # predicate returns False for most DeFi catalogue rows because the DeFi MVP
    # rule in UAC is scoped to a small set of specific EVM+Solana venues (Uniswap
    # V3, Curve, Orca, etc.) and instrument types (POOL, DEX_POOL, LST, LENDING),
    # but the production catalogue contains 7 416 rows spanning a much wider set
    # of venues and instrument types. Until a real per-instrument MVP screen exists
    # in UAC, the operator has instructed that ALL DeFi catalogue rows are MVP.
    # DEFI-ONLY GUARD: this shortcut applies exclusively to ``asset_group == "defi"``
    # (every other asset group continues to use the UAC ``is_mvp`` predicate below).
    # Remove this block once the UAC DeFi MVP rule is expanded to cover the full
    # production catalogue (defi_instrument_catalogue_and_capture_pipeline_2026_06_23).
    if asset_group == "defi":
        out = df.copy()
        out["mvp"] = True
        return out

    # CeFi capture universe is PERP-GATED (cefi_universe_capture_rule_2026_06_23):
    # a SPOT / dated-FUTURE cell is mvp ONLY IF the venue also lists a PERP for the
    # same base. The rollup sees ALL instruments per venue/day, so it can compute
    # the per-(venue, base) ``has_perp_for_base`` flag the shared UAC predicate
    # ``is_in_mvp_capture_universe`` needs. We compute it ONCE over the frame (the
    # catalogue is the full could-exist universe; the gate is a venue/base property,
    # not per-expiry) and pass it per row. Non-cefi asset_groups keep the plain
    # ``is_mvp`` rule (no perp-gate concept).
    perp_bases: set[tuple[str, str]] = set()
    if asset_group == "cefi" and not df.empty and "instrument_type" in df.columns:
        for _, _prow in df.iterrows():
            _itype = _cell(_prow, "instrument_type").strip().upper()
            if _itype in ("PERPETUAL", "EQUITY_PERP"):
                _v = _cell(_prow, "venue")
                _b = _cell(_prow, "base_asset") or _cell(_prow, "underlying")
                if _v and _b:
                    # Key on the base-exchange token — the perp-gate is per EXCHANGE,
                    # not per sub-venue (BINANCE-SPOT spot ↔ BINANCE-FUTURES perp).
                    perp_bases.add((_v.strip().upper().split("-", 1)[0], _b.strip().upper()))

    def _row_is_mvp(row: pd.Series[object]) -> bool:
        league = _cell(row, "league_id") or None
        # Unbound (single-grain) catalogue rows carry no data_type → pass "" straight
        # through; ``is_mvp`` treats a blank data_type as "any MVP data_type" so the
        # venue/instrument_type/base axes (not a missing data_type) decide membership.
        # A multi-grain row (prediction) carries its own data_type and is gated on it.
        data_type = _cell(row, "data_type") or None
        # Base asset: ``base_asset`` is populated for spot/perp legs (the cefi MVP
        # base_ccy axis); ``underlying`` carries it for derivatives/options. "" → None
        # so a non-derivative row is not over-constrained.
        base = _cell(row, "base_asset") or _cell(row, "underlying") or None

        if asset_group == "cefi":
            # Perp-gated CeFi capture predicate — the shared UAC SSOT. ``base`` ""
            # (no base) → not in universe; pass through as "" so the predicate's
            # base-membership check fails cleanly.
            venue = _cell(row, "venue")
            base_norm = (base or "").strip().upper()
            has_perp = (venue.strip().upper().split("-", 1)[0], base_norm) in perp_bases
            return is_in_mvp_capture_universe(
                venue,
                base_norm,
                _cell(row, "instrument_type"),
                has_perp_for_base=has_perp,
            )

        return is_mvp(
            asset_group,
            venue=_cell(row, "venue"),
            instrument_type=_cell(row, "instrument_type"),
            data_type=data_type,
            base_ccy=base,
            league=league,
        )

    out = df.copy()
    out["mvp"] = out.apply(_row_is_mvp, axis=1).astype("bool")
    return out


def run_rollup(
    asset_group: str,
    *,
    allow_shrink: bool,
    dry_run: bool,
    by_date_prefix: str = DEFAULT_BY_DATE_PREFIX,
    max_blobs: int | None = None,
    storage: StorageClient | None = None,
) -> int:
    """Roll up the per-date definitions for ``asset_group`` and promote the catalogue."""
    run_id = f"catalogue-rollup-{asset_group}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"

    # Phase A: storage-client init
    # Cloud Run Logging truncates multi-line tracebacks — bisection markers below
    # (print+flush=True) surface exactly which phase dies before the logger's
    # structured output reaches Cloud Logging. See plan Folded-in I-1 §"Make the cloud
    # lifecycle-catalogue-regen job log, then fix the real error".
    print(f"[BISECT-A] storage-client init asset_group={asset_group}", flush=True)
    storage = storage or get_storage_client()
    _tune_download_pool(storage, MAX_DOWNLOAD_WORKERS)

    # Phase B: bucket resolve
    print(f"[BISECT-B] bucket resolve asset_group={asset_group}", flush=True)
    bucket = _instruments_store_bucket_for(asset_group)
    env = get_config("DEPLOYMENT_ENV", "prod")

    # Sports per-date definitions live under a different prefix (the leagues
    # entity slice), so default to it when the caller did not override.
    if asset_group == "sports" and by_date_prefix == DEFAULT_BY_DATE_PREFIX:
        by_date_prefix = SPORTS_BY_DATE_PREFIX

    if max_blobs is not None and not dry_run:
        # A truncated walk yields an incomplete catalogue → never promotable.
        logger.warning("--max-blobs is diagnostic-only — forcing --dry-run (truncated walk is not promotable)")
        dry_run = True
    _emit_event(
        "CATALOGUE_ROLLUP_STARTED",
        run_id=run_id,
        asset_group=asset_group,
        bucket=bucket,
        env=env,
        by_date_prefix=by_date_prefix,
    )
    logger.info(
        "Rolling up %s catalogue from gs://%s/%s/ (env=%s)",
        asset_group,
        bucket,
        by_date_prefix.rstrip("/"),
        env,
    )

    # Phase C: by_date listing + download-pool
    print(f"[BISECT-C] by_date listing / download-pool asset_group={asset_group} bucket={bucket}", flush=True)
    if asset_group == "prediction":
        # Multi-grain roll-up: the cqg bundle is per-canonical_question_group while
        # trades/market_lifecycle are per-conditionId. Parse the cqg from the path.
        df = build_prediction_catalogue_dataframe(
            _iter_prediction_by_date_snapshots(storage, bucket, by_date_prefix, max_blobs=max_blobs)
        )
    elif asset_group == "sports":
        # League-grain could-exist universe — derived from the canonical MANIFEST
        # (the namespace-correct superset), NOT the entity=leagues slice (whose RAW
        # NUMERIC api-football league_ids do not match the manifest's canonical
        # namespace → 131/606 coverage + numeric over-seed; slot-4 2026-06-07).
        # The captured manifest atom is per-(league_id, data_type, date).
        df = build_sports_catalogue_from_manifest(_read_sports_manifest_index(storage, bucket))
    else:
        df = build_catalogue_dataframe(_iter_by_date_snapshots(storage, bucket, by_date_prefix, max_blobs=max_blobs))

    # Phase D: dedup / row-count
    print(f"[BISECT-D] dedup complete rows={len(df)} asset_group={asset_group}", flush=True)
    logger.info("Rolled up %d catalogue rows", len(df))

    # MVP-scope tag (mvp_scope_catalogue_tagging_2026_06_08): per-entry boolean via
    # the UAC is_mvp predicate, so deployment-api / data-status can scope coverage.
    df = _add_mvp_column(df, asset_group)
    _mvp_count = int(df["mvp"].sum()) if not df.empty else 0
    logger.info("MVP-tagged catalogue: %d / %d rows in MVP scope", _mvp_count, len(df))

    # Phase E: monotonic-guard + promote-write
    print(f"[BISECT-E] monotonic-guard + promote-write asset_group={asset_group} rows={len(df)}", flush=True)
    code = promote_catalogue(
        storage,
        bucket,
        env,
        df,
        allow_shrink=allow_shrink,
        dry_run=dry_run,
    )
    _emit_event(
        "CATALOGUE_ROLLUP_COMPLETED" if code == 0 else "CATALOGUE_ROLLUP_FAILED",
        run_id=run_id,
        asset_group=asset_group,
        rows=len(df),
        exit_code=code,
    )
    print(f"[BISECT-DONE] exit_code={code} asset_group={asset_group}", flush=True)
    return code


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Roll the per-date instrument definitions up into the lifecycle catalogue.",
    )
    parser.add_argument(
        "--asset-group",
        required=True,
        choices=["cefi", "defi", "tradfi", "sports", "prediction"],
        help="Asset group to roll up (lowercase).",
    )
    parser.add_argument(
        "--by-date-prefix",
        default=DEFAULT_BY_DATE_PREFIX,
        help=(
            "GCS prefix the per-date instrument definitions live under "
            f"(default: {DEFAULT_BY_DATE_PREFIX!r}; sports fixtures use sports_reference/by_date)."
        ),
    )
    parser.add_argument(
        "--allow-catalogue-shrink",
        action="store_true",
        help="Override the monotonic guard for a legitimate corrective shrink (removing a bad instrument row).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Roll up + evaluate the guard, but do NOT write the catalogue.",
    )
    parser.add_argument(
        "--max-blobs",
        type=int,
        default=None,
        help=(
            "DIAGNOSTIC ONLY — truncate the by_date walk to the first N parquets (path-sorted). "
            "Produces an INCOMPLETE catalogue (wrong lifecycle windows), so it forces --dry-run. "
            "Use to smoke-test the walk without reading the full history."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = _parse_args(argv)
    asset_group: str = args.asset_group
    allow_shrink: bool = args.allow_catalogue_shrink
    dry_run: bool = args.dry_run
    by_date_prefix: str = args.by_date_prefix
    max_blobs: int | None = args.max_blobs
    return run_rollup(
        asset_group,
        allow_shrink=allow_shrink,
        dry_run=dry_run,
        by_date_prefix=by_date_prefix,
        max_blobs=max_blobs,
    )


if __name__ == "__main__":
    # Cloud Run / Cloud Logging splits the default multi-line stderr traceback
    # into per-line entries and drops the tail — every failed lifecycle-catalogue-regen
    # run truncated at the ``run_rollup(`` call frame, hiding the real exception
    # (R6, plan proper_instrument_catalogue_lifecycle_rollup §R6). Re-log any uncaught
    # error via ``logger.exception`` so it lands as ONE structured record (full
    # traceback in a single field) + flush, surfacing the actual cause.
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except BaseException:
        logger.exception("build_instrument_catalogue FAILED — full traceback follows")
        sys.stdout.flush()
        sys.stderr.flush()
        raise
