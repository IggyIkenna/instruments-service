#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: permanent
# Delete-when: NA
"""enumerate_expected_universe.py — Phase 3.D.4 backward-fill (writegate honest-coverage).

Enumerates the expected universe per asset_group, finds (shard_key, day) tuples
with NO manifest row, writes ``record_expected_empty(reason=EXPECTED_*)`` rows
via per-VM shard isolation.

Closes the rollup-vs-drilldown denominator divergence per
`unified-trading-pm/codex/02-data/availability-manifest-and-data-status.md`
§ "Rollup-vs-drilldown denominator divergence (codified 2026-05-07)" by
ensuring every expected (shard_key, day) tuple has a manifest row.

Sister script to ``reconcile_expected_absence_reasons.py`` (which handles
legacy null-reason rows ALREADY in the manifest). This script handles the
complementary case: tuples that have NO manifest row at all.

Default scan-only (CSV report). ``--apply-write`` requires
``MANIFEST_PER_VM_SHARDS=true`` + ``VM_NAME=...`` per the per-VM shard
isolation rule. ``--max-writes-per-run`` default 100k halt safety.

Per-asset-group implementation status (2026-05-07):

* TradFi: FULL — calendar pre-skip via UAC ``non_trading_day_reason``.
* DeFi:   FULL — chain pre-genesis + protocol pre-launch via UAC
  ``CHAIN_GENESIS_DATES`` + ``PROTOCOL_LAUNCH_DATES``.
* Sports: FULL (v2) — per-LEAGUE could-exist enumeration from the
  ``build_instrument_catalogue.py`` league-grain roll-up
  (``build_sports_catalogue_dataframe``). The captured atom is
  ``(league_id, data_type, date)``; the v2 enumerator iterates the captured
  sports data_types (``SPORTS_DATA_TYPE_TO_SOURCE``) per league, applying each
  source's ``SOURCE_COVERAGE_START`` / ``DATA_TYPE_COVERAGE_START`` window
  (pre-coverage dates stay owned by the v1 ``_enumerate_sports`` rows).
  v1 still emits the per-source pre-coverage slice.
* CeFi:   FULL (v2) — per-instrument lifecycle from the
  ``build_instrument_catalogue.py`` roll-up (``available_from`` /
  ``available_to``); the G1-ENUM shape-aware producer (2026-06-07) filters
  each instrument to its valid ``(instrument_type x data_type)`` cells via the
  UAC validity matrix + bundle-grain roll-up. The v1 ``_enumerate_cefi`` below
  retains only the pre-venue-launch slice. (CF-16: the former STUB is closed —
  the v2 ``_enumerate_v2_cefi`` is the live path.)
* Prediction: FULL (v2) — per-market lifecycle (``market_created_at`` /
  ``settlement_time``) with per-row data_type grain-binding (cqg bundle vs
  per-conditionId trades) from the prediction catalogue. (CF-16: the former
  ``PREDICTION_GROUPS`` STUB is closed — the v2 ``_enumerate_v2_prediction`` is
  the live path; the catalogue carries the grain.)

Example::

    # Scan-only (TradFi)
    python scripts/enumerate_expected_universe.py --asset-group tradfi

    # Apply-write (DeFi)
    MANIFEST_PER_VM_SHARDS=true VM_NAME=enum-universe-defi-$(date +%s) \\
    python scripts/enumerate_expected_universe.py \\
        --asset-group defi --apply-write --max-writes-per-run 50000
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import logging
import os
import sys
import tempfile
import time
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple

import pandas as pd
from google.cloud import storage
from unified_api_contracts import (
    DATA_TYPES_BY_ASSET_GROUP,
    GRAIN_BUNDLE_BY_UNDERLYING,
    VENUES_BY_ASSET_GROUP,
    Mode,
    bundle_instrument_type_for_leaf,
    default_transport_for_source,
    external_sources_for,
    grain_for_instrument_type,
    has_source_priority,
    is_in_mvp_capture_universe,
    pipeline_mode_for_source,
    valid_data_types_for_venue_instrument_type,
)
from unified_api_contracts.registry.chain_env import (
    CHAIN_GENESIS_DATES,
    GAS_FEE_CHAIN_START_DATES,
    MAINNET_CHAIN_IDS,
    PROTOCOL_LAUNCH_DATES,
)
from unified_api_contracts.registry.venue_launch_dates import (
    CEFI_VENUE_LAUNCH_DATES,
    PREDICTION_VENUE_LAUNCH_DATES,
)
from unified_api_contracts.registry.venue_mapping import VenueMapping
from unified_api_contracts.registry.venue_trading_calendar import (
    is_non_trading_day,
    non_trading_day_reason,
)
from unified_trading_library import MANIFEST_SCHEMA_VERSION, resolve_bucket_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ID = "central-element-323112"

# Sports could-exist denominator is per-LEAGUE, not per-fixture/instrument
# (slot-4 finding 2026-06-07: the canonical instruments-store-sports-prd _index
# captured atom is (league_id, data_type, date) — league_id populated 97.6% but
# venue ~blank / instrument_id ~blank / instrument_type ~blank). So the
# present-set match + the seeded expected_unattempted atom are LEAGUE-grain:
# (data_type, league_id, date) ONLY — venue / instrument_id / instrument_type are
# blank-tolerant (excluded from the key) or a fixture-grain catalogue would never
# match the league-grain manifest → every cell would inflate the denominator.
_SPORTS_PRESENT_COLS: list[str] = ["data_type", "league_id", "date"]

# Sports data_types retired as of 2026-05-05 — were provider-catalog mappings (now
# live in UAC as static ID tables) rather than captured GCS data.  They are NOT in
# ``SPORTS_DATA_TYPE_TO_SOURCE``, so ``_sports_data_types()`` never returns them;
# this set is a defensive guard in case a caller passes a legacy data_types list.
_RETIRED_SPORTS_DATA_TYPES: frozenset[str] = frozenset(
    {
        "TM_LEAGUES",
        "TRANSFERMARKT_LEAGUES",
        "TRANSFERMARKT_VALUES",
        "SFI_LEAGUES",
        "SFI_STANDINGS",
    }
)

# Full per-instrument present-set columns for cefi / defi / prediction.
_DEFAULT_PRESENT_COLS: list[str] = [
    "venue",
    "chain",
    "data_type",
    "instrument_type",
    "instrument_id",
    "league_id",
    "date",
]

# TradFi present-set columns = the default per-instrument grain PLUS ``underlying``
# (axis-3, 2026-06-22). The MTDS writer records per-underlying BUNDLE captures
# (futures_chain / combo / options_chain) at ``instrument_id=""`` + ``underlying=<U>``
# (venue_fetch.py:318-320 → manifest_finalize.py base_row_key), so the present-set
# match + the seeded ``expected_unattempted`` atom MUST key on ``underlying`` — keying
# on the (blank) instrument_id alone would collapse every underlying of a
# (venue, bundle_type) into ONE tuple and the seed would never reconcile against the
# real capture. Leaf rows carry ``underlying=""`` on BOTH sides, so the extra column is
# a no-op for non-bundle tradfi cells. ``_present_cols_for`` intersects this with the
# manifest's actual columns, so a manifest WITHOUT an ``underlying`` column drops it
# (backward-safe). Scoped to tradfi to leave the cefi / defi / prediction grain — and
# their per-AG enumerators, which do not yet collapse bundle ``instrument_id`` —
# untouched.
_TRADFI_PRESENT_COLS: list[str] = [
    "venue",
    "chain",
    "data_type",
    "instrument_type",
    "instrument_id",
    "underlying",
    "league_id",
    "date",
]


def _present_cols_for(asset_group: str, available_in_df: list[str]) -> list[str]:
    """Return the manifest present-set column grain for ``asset_group``.

    Sports is LEAGUE-grain (``_SPORTS_PRESENT_COLS``); tradfi is the per-instrument
    grain PLUS ``underlying`` (``_TRADFI_PRESENT_COLS`` — so per-underlying bundle
    captures with a blank instrument_id reconcile on the underlying); every other group
    is the full per-instrument grain (``_DEFAULT_PRESENT_COLS``). Intersected with the
    columns actually present in the manifest so the present-set tuples and the
    enumerator row-keys line up.
    """
    if asset_group == "sports":
        base = _SPORTS_PRESENT_COLS
    elif asset_group == "tradfi":
        base = _TRADFI_PRESENT_COLS
    else:
        base = _DEFAULT_PRESENT_COLS
    return [c for c in base if c in available_in_df]


def _sports_data_types() -> list[str]:
    """Return the captured sports manifest data_types (source-coverage-bearing).

    The could-exist sports denominator iterates the data_types that actually
    appear in the captured manifest AND carry a UAC source-coverage window —
    i.e. the keys of ``SPORTS_DATA_TYPE_TO_SOURCE`` (FIXTURES / STANDINGS / XG /
    …). This is a DIFFERENT axis from ``DATA_TYPES_BY_ASSET_GROUP["sports"]``
    (the MTDS market-data odds types) — the reference-data league manifest is
    keyed by these provider data_types, verified on the canonical _index
    (slot-4 2026-06-07). Sorted for deterministic output.
    """
    from unified_api_contracts.sports import SPORTS_DATA_TYPE_TO_SOURCE

    return sorted(SPORTS_DATA_TYPE_TO_SOURCE.keys())


# Asset groups this enumerator supports. The canonical MANIFEST bucket per group is
# resolved at run-time via ``resolve_bucket_name`` (the bucket-name SSOT,
# deployment-service/configs/cloud-providers.yaml) — see ``_default_bucket_for``.
SUPPORTED_ASSET_GROUPS: tuple[str, ...] = ("cefi", "defi", "tradfi", "sports", "prediction")


def _default_bucket_for(asset_group: str) -> str:
    """Resolve the canonical manifest bucket for ``asset_group`` via the bucket-name SSOT.

    Replaces the prior hardcoded ``market-data-tick-{ag}-{PROJECT_ID}`` literals which were
    ALL missing the ``-{DEPLOYMENT_ENV_SHORT}-`` env tier (e.g. ``market-data-tick-cefi-{pid}``
    instead of the canonical ``market-data-tick-cefi-prd-{pid}``; prediction was the legacy
    long-form ``market-data-tick-prediction-{pid}`` slated for L6 delete instead of
    ``market-data-tick-pred-prd-{pid}``) — so a no-``--bucket`` run targeted a NON-EXISTENT
    bucket for EVERY asset_group → an empty manifest read → wrong/no expected_unattempted seed.

    Routing mirrors the MTDS reader / MDPS consolidator gate:
    - sports' manifest lives in the ``instruments-store`` bucket;
    - prediction uses the dedicated flat kind ``market-data-tick-prediction`` (→ ``*-pred-prd-``);
    - cefi/defi/tradfi use the per-asset_group ``market-data`` kind.
    """
    # ``resolve_bucket_name``'s ``asset_group`` is a Literal["cefi","defi","tradfi",
    # "sports","prediction"] — pass the explicit literal (equality-narrowed) so the call
    # is type-clean (the param is NOT the UAC AssetGroup enum).
    if asset_group == "sports":
        return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    if asset_group == "prediction":
        return resolve_bucket_name(cloud="gcp", kind="market-data-tick-prediction")
    if asset_group == "cefi":
        return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="cefi")
    if asset_group == "defi":
        return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="defi")
    if asset_group == "tradfi":
        return resolve_bucket_name(cloud="gcp", kind="market-data", asset_group="tradfi")
    raise ValueError(f"_default_bucket_for: unsupported asset_group={asset_group!r}")


MANIFEST_BLOB = "_index/availability_index.parquet"
DEFAULT_START_DATE = "2018-01-01"

#: Prediction bundle data_type whose captured manifest atom is per-canonical_question_group
#: grain (mirrors build_instrument_catalogue._PREDICTION_CQG_DATA_TYPE). The v2 prediction
#: enumerator seeds expected_unattempted at THIS grain ONLY (decision 338) — never the
#: per-conditionId trades/market_lifecycle grain (the >50M-row false-EU blow-up).
_PREDICTION_CQG_DATA_TYPE = "prediction_canonical_question_group"

#: Full-history range-encoded expected_unattempted companion artifact. Keeps the main
#: _index at per-day grain (recent bounded window) while a range-encoded companion carries
#: the FULL 2018→today could-exist universe as one row per contiguous (shard-key, reason)
#: date-span — ~100x smaller than the naive per-day full-history (~190M rows fleet-wide).
EXPECTED_UNIVERSE_RANGES_BLOB = "_index/expected_universe_ranges.parquet"


@dataclass(frozen=True)
class ExpectedRow:
    """One row in the expected universe — either present in the manifest
    already (in which case the enumerator skips it) or missing (in which
    case the enumerator writes the appropriate manifest status)."""

    asset_group: str
    venue: str
    chain: str
    data_type: str
    instrument_type: str
    instrument_id: str
    league_id: str
    date: str
    reason: str  # one of EMPTY_CONFIRMED_REASONS (empty string when capture_status=expected_unattempted)
    capture_status: str = "empty_confirmed"  # "empty_confirmed" | "expected_unattempted"
    # Underlying asset for per-underlying BUNDLE cells (futures_chain / combo /
    # options_chain). The MTDS writer records bundle captures at
    # ``instrument_id=""`` + ``underlying=<U>`` (venue_fetch.py:318-320 →
    # manifest_finalize.py base_row_key); a leaf cell carries ``instrument_id=<id>``
    # + ``underlying=""``. So a bundle seed MUST mirror that shape (blank
    # instrument_id, populated underlying) or the seed's shard-atom never matches
    # the capture and the cell stays permanently ``expected_unattempted``. "" for
    # leaf / non-derivative rows.
    underlying: str = ""


def _emit_event(event: str, /, **details: object) -> None:
    """Best-effort structured event log (mirrors RECONCILER_* shape)."""
    payload = {"event": event, "ts": datetime.now(UTC).isoformat(), **details}
    logger.info("EVENT %s", payload)


def _derive_pm_source_transport(asset_group: str, data_type: str) -> tuple[str, str, str]:
    """Return ``(pipeline_mode, source, transport)`` for a seeded expected row.

    #4 (``pipeline_mode_source_batch_live_replay_standardisation_2026_06_05`` +
    catalogue C-#2/C-TRANSPORT): the ``expected_unattempted`` /
    ``empty_confirmed`` seeds the enumerator materialises MUST carry the same
    ``pipeline_mode`` + ``source`` (+ ``transport``) as the real rows they will
    be reconciled against — else the denominator-seed rows diverge from real
    rows (CF-3 reads blank corpus-wide). Derived from the cell's primary EXTERNAL
    source in UAC ``SOURCE_PRIORITY``:

    * ``source`` = the top external source for ``(asset_group, data_type)``.
    * ``pipeline_mode`` = ``pipeline_mode_for_source(source, Mode.BATCH)`` — the
      seed denominator is a BATCH expectation (the T+1 floor we owe).
    * ``transport`` = ``default_transport_for_source(source)`` (the column SSOT).

    Computed/service-only + unregistered cells (no external source) get
    ``("", "", "")`` — they are exempt (no external vendor to owe data from).
    Sports data_types are registered upper-case in ``SOURCE_PRIORITY`` while the
    enumerator carries them lower-case, so both are tried.
    """
    ag = asset_group.lower() if asset_group else ""
    if not ag or not data_type:
        return "", "", ""
    external: list[str] = []
    for dt in (data_type, data_type.upper(), data_type.lower()):
        if has_source_priority(ag, dt):
            external = external_sources_for(ag, dt)
            if external:
                break
    if not external:
        return "", "", ""
    source = external[0]
    try:
        pipeline_mode = pipeline_mode_for_source(source, Mode.BATCH).value
    except ValueError:
        return "", "", ""
    return pipeline_mode, source, default_transport_for_source(source)


# ---------------------------------------------------------------------------
# Per-asset-group enumerators
# ---------------------------------------------------------------------------


def _enumerate_tradfi(start: str, end: str) -> Iterator[ExpectedRow]:
    """Calendar pre-skip days x (venue, data_type) cross-product.

    For each TradFi venue, for each calendar non-trading day in window,
    yield one row per data_type with reason = EXPECTED_HOLIDAY / WEEKEND.
    """
    venues = VENUES_BY_ASSET_GROUP.get("tradfi", [])
    data_types = DATA_TYPES_BY_ASSET_GROUP.get("tradfi", [])
    if not venues or not data_types:
        logger.warning("TradFi venues/data_types empty — nothing to enumerate")
        return

    days = pd.date_range(start, end, freq="D")
    for venue in venues:
        venue_str = str(venue)
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            if not is_non_trading_day(venue_str, iso):
                continue
            reason = non_trading_day_reason(venue_str, iso) or "EXPECTED_HOLIDAY"
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="tradfi",
                    venue=venue_str,
                    chain="",
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason=reason,
                )

    # Per-instrument pre-genesis slice for the Yahoo-sourced index universe
    # (VIX/DXY/US-treasuries). The venue-level holiday loop above can't express
    # per-instrument genesis (CBOE carries VIX@1990 + treasuries@2000), so a
    # dedicated instrument-grain pass marks each index's pre-genesis days.
    yield from _enumerate_tradfi_indices(start, end)


def _enumerate_tradfi_indices(start: str, end: str) -> Iterator[ExpectedRow]:
    """Per-Yahoo-index pre-genesis days → EXPECTED_INSTRUMENT_NOT_LISTED.

    Each ``YahooIndexDef`` carries its empirically-confirmed
    ``first_available_date`` (VIX 1990-01-02 / DXY 2019-01-02 / treasuries
    2000-01-03). For days before an index's genesis, yield an instrument-grain
    row under its canonical ``-USD`` key for each data_type that has a
    registered source resolver — so the could-exist denominator counts these
    instruments instead of leaving pre-genesis cells as silent gaps.
    """
    from unified_api_contracts.registry import YAHOO_INDICES
    from unified_api_contracts.registry.data_source_continuity import (
        data_types_for_instrument,
    )

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    for idx in YAHOO_INDICES:
        canonical_key = f"{idx.venue}:INDEX:{idx.base_asset}-USD"
        data_types = data_types_for_instrument(canonical_key)
        if not data_types:
            logger.warning("Yahoo index %s has no source resolver — skipping enumeration", canonical_key)
            continue
        genesis_ts = pd.Timestamp(idx.first_available_date)
        if start_ts >= genesis_ts:
            continue  # whole window is post-genesis — nothing to pre-list
        last_day = min(end_ts, genesis_ts - pd.Timedelta(days=1))
        for day in pd.date_range(start, last_day, freq="D"):
            iso = day.strftime("%Y-%m-%d")
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="tradfi",
                    venue=idx.venue,
                    chain="",
                    data_type=str(dt),
                    instrument_type="INDEX",
                    instrument_id=canonical_key,
                    league_id="",
                    date=iso,
                    reason="EXPECTED_INSTRUMENT_NOT_LISTED",
                )


# Chain-/source-level DeFi data_types — fetched ONCE PER CHAIN (or per
# relay) at a synthetic infrastructure venue, NEVER per real protocol.
# Declared in UAC ``PROTOCOL_CAPABILITIES`` only by synthetic INFRASTRUCTURE
# pseudo-protocols (``ALCHEMY-ONCHAIN`` / ``FLASHBOTS``), not by the DEX /
# lending / LST protocols in ``PROTOCOL_LAUNCH_DATES``. Iterating these for
# every ``(chain, protocol)`` in ``PROTOCOL_LAUNCH_DATES`` produces FALSE
# ``empty_confirmed[EXPECTED_INSTRUMENT_NOT_LISTED]`` cells keyed to a
# protocol venue (e.g. ``venue=AAVE_V3, data_type=gas_fees``) for pre-protocol-
# launch dates — but gas/transfers/MEV exist from CHAIN genesis regardless of
# when any DEX launched, and the real capture is keyed ``venue=ALCHEMY`` /
# ``venue=FLASHBOTS`` + ``chain=X``. So these must NOT ride the per-protocol
# loop. ``gas_fees`` chain-level pre-genesis cells are seeded by
# ``_enumerate_defi_gas_fees`` below; ``token_transfers`` / ``mev_events``
# post-genesis absence is the handler/backfill's concern, not the enumerator's.
#
# NOTE — ``oracle_prices`` is DELIBERATELY NOT here: it IS genuinely
# per-protocol. ~15 LST/yield/staking/perp protocols (LIDO, ETHERFI, ETHENA,
# EIGENLAYER, HYPERLIQUID, DRIFT, MARINADE, JITO, YEARN_V3, PENDLE, SYMBIOTIC,
# KARAK, RENZO, KELPDAO, PUFFER, …) emit ``oracle_prices`` as their protocol
# exchange rate at ``venue=<PROTOCOL>`` (verified captured at AAVE_V3/ETHENA/
# LIDO/ETHERFI venues in the live ``market-data-tick-defi-prd`` manifest); the
# CHAINLINK/PYTH chain-level feed is a SEPARATE source. A pre-protocol-launch
# ``oracle_prices`` empty at ``venue=<PROTOCOL>`` is therefore CORRECT, not a
# phantom — leave it on the per-protocol loop.
_DEFI_CHAIN_LEVEL_DATA_TYPES: frozenset[str] = frozenset({"gas_fees", "token_transfers", "mev_events"})

# Synthetic chain-level gas venue (mirrors the MTDS gas_fee_handler's
# ``_GAS_FEE_VENUE``). Gas is collected once per chain at this venue, never
# per protocol.
_GAS_FEE_VENUE = "ALCHEMY"


def _gas_fee_chain_names() -> dict[str, str]:
    """Chain NAME -> genesis-date for chains the gas pipeline covers.

    Derived purely from UAC (no MTDS import): a chain has gas coverage when its
    ``MAINNET_CHAIN_IDS`` id is in ``GAS_FEE_CHAIN_START_DATES`` (the Alchemy
    archival-coverage set) — plus SOLANA. The pre-genesis clip uses the chain's
    mainnet ``CHAIN_GENESIS_DATES``, not the (later) gas-coverage start.
    """
    names: dict[str, str] = {}
    for name, chain_id in MAINNET_CHAIN_IDS.items():
        if chain_id in GAS_FEE_CHAIN_START_DATES:
            genesis = CHAIN_GENESIS_DATES.get(name.upper())
            if genesis is not None:
                names[name.upper()] = genesis
    sol_genesis = CHAIN_GENESIS_DATES.get("SOLANA")
    if sol_genesis is not None:
        names["SOLANA"] = sol_genesis
    return names


def _enumerate_defi_gas_fees(start: str, end: str) -> Iterator[ExpectedRow]:
    """Chain-level ``gas_fees`` pre-CHAIN-genesis days at ``venue=ALCHEMY``.

    Gas exists since CHAIN genesis (independent of any DEX/protocol launch), so
    the only honest enumerator concern is the pre-genesis window: one series per
    gas-covered chain, ``venue=ALCHEMY`` + ``chain=X``, for days before that
    chain's mainnet genesis -> ``EXPECTED_PRE_GENESIS_CHAIN``. Post-genesis gas
    absence is the gas_fee_handler / backfill's concern (it captures at the same
    ``venue=ALCHEMY`` shard key), NOT the enumerator's.
    """
    end_ts = pd.Timestamp(end)
    start_ts = pd.Timestamp(start)
    for chain_upper, chain_genesis in _gas_fee_chain_names().items():
        genesis_ts = pd.Timestamp(chain_genesis)
        if start_ts >= genesis_ts:
            continue  # whole window is post-genesis — nothing pre-genesis to seed
        last_day = min(end_ts, genesis_ts - pd.Timedelta(days=1))
        for day in pd.date_range(start, last_day, freq="D"):
            yield ExpectedRow(
                asset_group="defi",
                venue=_GAS_FEE_VENUE,
                chain=chain_upper,
                data_type="gas_fees",
                instrument_type="",
                instrument_id="",
                league_id="",
                date=day.strftime("%Y-%m-%d"),
                reason="EXPECTED_PRE_GENESIS_CHAIN",
            )


def _enumerate_defi(start: str, end: str) -> Iterator[ExpectedRow]:
    """Chain pre-genesis + protocol pre-launch days x data_types.

    For each (chain, protocol) in PROTOCOL_LAUNCH_DATES:
      * effective_start = max(chain_genesis, protocol_launch)
      * for each day in [start, effective_start - 1]:
          - day < chain_genesis  -> EXPECTED_PRE_GENESIS_CHAIN
          - day < protocol_launch -> EXPECTED_INSTRUMENT_NOT_LISTED

    Chain-/source-level data_types (``_DEFI_CHAIN_LEVEL_DATA_TYPES``:
    gas_fees / token_transfers / mev_events) are EXCLUDED from the per-protocol
    loop — they're fetched once per chain at a synthetic venue, not per
    protocol; enumerating them per (chain, protocol) produced ~142k false
    ``venue=<PROTOCOL>`` empties (phantom wrong-key duplicates). Chain-level
    ``gas_fees`` pre-genesis cells come from ``_enumerate_defi_gas_fees``.
    """
    data_types = [dt for dt in DATA_TYPES_BY_ASSET_GROUP.get("defi", []) if dt not in _DEFI_CHAIN_LEVEL_DATA_TYPES]
    # Chain-level gas_fees pre-genesis cells (venue=ALCHEMY), independent of
    # the per-protocol loop below.
    yield from _enumerate_defi_gas_fees(start, end)

    if not data_types:
        logger.warning("DeFi data_types empty — nothing to enumerate")
        return

    end_ts = pd.Timestamp(end)
    for (chain, protocol), launch_date_str in PROTOCOL_LAUNCH_DATES.items():
        chain_upper = chain.upper()
        chain_genesis = CHAIN_GENESIS_DATES.get(chain_upper)
        if chain_genesis is None:
            logger.warning("Skipping (%s, %s): no chain genesis date in UAC", chain, protocol)
            continue
        # Effective start = max(chain_genesis, protocol_launch).
        effective_start = max(chain_genesis, launch_date_str)
        eff_ts = pd.Timestamp(effective_start)
        if pd.Timestamp(start) >= eff_ts:
            continue  # all days in window are post-launch — nothing to backfill
        # Yield rows for [start, min(end, effective_start - 1day)].
        last_day = min(end_ts, eff_ts - pd.Timedelta(days=1))
        days = pd.date_range(start, last_day, freq="D")
        venue_label = protocol.upper()  # canonical: venue=PROTOCOL only; chain= carries chain separately
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            reason = "EXPECTED_PRE_GENESIS_CHAIN" if iso < chain_genesis else "EXPECTED_INSTRUMENT_NOT_LISTED"
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="defi",
                    venue=venue_label,
                    chain=chain_upper,
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason=reason,
                )


def _enumerate_sports(start: str, end: str) -> Iterator[ExpectedRow]:
    """Pre-source-coverage-start days x data_types (per source).

    Per-league enumeration is deferred (v2 — needs sports leagues catalog).
    For now enumerates per-source pre-coverage dates which is the
    largest absent slice.
    """
    from unified_api_contracts.sports import SOURCE_COVERAGE_START

    data_types = DATA_TYPES_BY_ASSET_GROUP.get("sports", [])
    if not data_types:
        logger.warning("Sports data_types empty — nothing to enumerate")
        return

    end_ts = pd.Timestamp(end)
    for source_key, coverage_start in SOURCE_COVERAGE_START.items():
        if coverage_start is None:
            continue
        if pd.Timestamp(start) >= pd.Timestamp(coverage_start):
            continue  # source covers entire window — nothing pre-coverage
        last_day = min(end_ts, pd.Timestamp(coverage_start) - pd.Timedelta(days=1))
        days = pd.date_range(start, last_day, freq="D")
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="sports",
                    venue=str(source_key),  # in sports the "venue" axis is the source key
                    chain="",
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason="EXPECTED_PRE_SOURCE_COVERAGE_START",
                )


def _enumerate_cefi(start: str, end: str) -> Iterator[ExpectedRow]:
    """Pre-venue-launch days x data_types per CeFi venue.

    For each CeFi venue with a launch date in UAC ``CEFI_VENUE_LAUNCH_DATES``
    that is after the window start, yield rows for every
    ``(venue, data_type, day)`` tuple where ``day < launch_date``. Reason:
    ``EXPECTED_PRE_VENUE_LAUNCH``. Sister of the DeFi pre-genesis-chain branch
    above (chain genesis vs venue launch — same shape, different SSOT).

    **What this DOES NOT cover (deferred to v2 with a per-instrument catalog
    read):** ``EXPECTED_INSTRUMENT_NOT_LISTED`` / ``EXPECTED_INSTRUMENT_DELISTED``
    per-(venue, instrument_id, day) rows. Per-instrument lifecycle requires
    a ``gs://instruments-store-cefi-…`` catalog walk that's not wired here.
    Tracked as a P1 follow-up in writegate plan Phase 3.D.4 CeFi sub-task.

    The shard-key matrix declares CeFi spot/perp shards as
    ``(asset_group, venue, data_type, instrument_type, instrument_id, day)``.
    For pre-venue-launch dates ALL instruments are absent (the venue did not
    exist), so we use sentinel values ``instrument_type=""`` +
    ``instrument_id=""`` — the ``(venue, data_type, day)`` tuple alone is the
    correct atom for "no instruments existed yet" semantics. The reader-side
    classifier treats these venue-level rows as covering all per-instrument
    rows for that ``(venue, data_type, day)``.
    """
    venues = VENUES_BY_ASSET_GROUP.get("cefi", [])
    data_types = DATA_TYPES_BY_ASSET_GROUP.get("cefi", [])
    if not venues or not data_types:
        logger.warning("CeFi venues/data_types empty — nothing to enumerate")
        return

    end_ts = pd.Timestamp(end)
    start_ts = pd.Timestamp(start)
    for venue in venues:
        venue_str = str(venue)
        launch_str = CEFI_VENUE_LAUNCH_DATES.get(venue_str)
        if launch_str is None:
            logger.info(
                "CeFi venue %s: no launch date in UAC CEFI_VENUE_LAUNCH_DATES; skipping pre-launch enumeration",
                venue_str,
            )
            continue
        launch_ts = pd.Timestamp(launch_str)
        if start_ts >= launch_ts:
            continue  # entire window is post-launch — nothing to backfill
        last_day = min(end_ts, launch_ts - pd.Timedelta(days=1))
        days = pd.date_range(start_ts, last_day, freq="D")
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="cefi",
                    venue=venue_str,
                    chain="",
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason="EXPECTED_PRE_VENUE_LAUNCH",
                )


def _enumerate_prediction(start: str, end: str) -> Iterator[ExpectedRow]:
    """Pre-venue-launch days x data_types per Prediction venue.

    Same shape as the CeFi enumerator — for each Prediction venue with a
    launch date in UAC ``PREDICTION_VENUE_LAUNCH_DATES`` after the window
    start, yield rows for every ``(venue, data_type, day)`` tuple where
    ``day < launch_date``. Reason: ``EXPECTED_PRE_VENUE_LAUNCH``.

    **What this DOES NOT cover (deferred to v2 once UAC ``PREDICTION_GROUPS``
    canonical_question_group registry lands per
    ``predictions_master.md``):** per-canonical-group market
    lifecycle bounds (``market_created_at`` / ``settlement_time``) which would
    yield ``EXPECTED_INSTRUMENT_NOT_LISTED`` / ``EXPECTED_INSTRUMENT_DELISTED``
    rows for individual canonical question groups. The pre-venue-launch slice
    is the largest absent universe by date count and is independently useful;
    per-canonical-group enumeration adds finer detail on top.
    """
    venues = VENUES_BY_ASSET_GROUP.get("prediction", [])
    data_types = DATA_TYPES_BY_ASSET_GROUP.get("prediction", [])
    if not venues or not data_types:
        logger.warning("Prediction venues/data_types empty — nothing to enumerate")
        return

    end_ts = pd.Timestamp(end)
    start_ts = pd.Timestamp(start)
    for venue in venues:
        venue_str = str(venue)
        launch_str = PREDICTION_VENUE_LAUNCH_DATES.get(venue_str)
        if launch_str is None:
            logger.info(
                "Prediction venue %s: no launch date in UAC PREDICTION_VENUE_LAUNCH_DATES; "
                "skipping pre-launch enumeration",
                venue_str,
            )
            continue
        launch_ts = pd.Timestamp(launch_str)
        if start_ts >= launch_ts:
            continue
        last_day = min(end_ts, launch_ts - pd.Timedelta(days=1))
        days = pd.date_range(start_ts, last_day, freq="D")
        for day in days:
            iso = day.strftime("%Y-%m-%d")
            for dt in data_types:
                yield ExpectedRow(
                    asset_group="prediction",
                    venue=venue_str,
                    chain="",
                    data_type=str(dt),
                    instrument_type="",
                    instrument_id="",
                    league_id="",
                    date=iso,
                    reason="EXPECTED_PRE_VENUE_LAUNCH",
                )


_ENUMERATORS: dict[str, object] = {
    "tradfi": _enumerate_tradfi,
    "defi": _enumerate_defi,
    "sports": _enumerate_sports,
    "cefi": _enumerate_cefi,
    "prediction": _enumerate_prediction,
}


# ---------------------------------------------------------------------------
# v2 enumerator — per-instrument grain (Phase 1.A, expected_universe_v2_design)
# ---------------------------------------------------------------------------


class InstrumentCatalogEntry(NamedTuple):
    """Minimal per-instrument lifecycle record for v2 enumeration.

    Fields are consumed from the instruments-service catalog parquets.
    All date fields are ISO ``YYYY-MM-DD`` strings or ``None``.

    - ``instrument_id``: canonical instrument identifier.
    - ``instrument_type``: e.g. ``SPOT``, ``PERP``, ``FUTURE``, ``OPTION``.
    - ``venue``: CeFi/TradFi venue name; for DeFi use the protocol label.
    - ``chain``: DeFi chain key (e.g. ``ARBITRUM``); empty for CeFi/TradFi.
    - ``league_id``: Sports league identifier; empty for non-sports groups.
    - ``available_from``: First day the instrument was listed / tradable.
      ``None`` = unknown → treat as no lower bound (emit rows from start).
    - ``available_to``: Last day the instrument was listed / tradable.
      ``None`` = still active → no upper bound (emit rows to end).
    - ``market_created_at``: Prediction market creation ISO date (prediction only).
    - ``settlement_time``: Prediction market settlement ISO date (prediction only).
    - ``data_type``: OPTIONAL grain-binding for multi-grain asset groups
      (prediction). When set, the v2 enumerator emits ``expected_unattempted``
      rows for THIS data_type ONLY (at this row's ``instrument_id`` grain),
      instead of cross-producting the row against the full ``data_types`` list.
      ``None`` (the default, all other AGs + legacy prediction catalogues) →
      legacy behaviour (iterate every passed data_type). This is how a single
      prediction catalogue can carry both the per-cqg bundle row
      (``data_type=prediction_canonical_question_group``, ``instrument_id=cqg``)
      and per-conditionId rows (``data_type=trades`` / ``market_lifecycle``)
      without inflating the denominator by the cqg→conditionId fan-out.
    """

    instrument_id: str
    instrument_type: str
    venue: str
    chain: str
    league_id: str
    available_from: str | None
    available_to: str | None
    market_created_at: str | None
    settlement_time: str | None
    data_type: str | None = None
    # Underlying asset for derivatives (G1-ENUM bundle-grain roll-up key). Read
    # from the instruments-store ``underlying`` column; "" for non-derivatives.
    underlying: str = ""
    # Base asset for spot/perp legs (the cefi MVP base_ccy axis). Read from the
    # instruments-store ``base_asset`` column; "" when absent.
    base_asset: str = ""
    # MVP capture-universe tag from the catalogue ``mvp`` column (the rollup tags
    # it with the shared UAC ``is_in_mvp_capture_universe`` SSOT). ``None`` = column
    # absent → the cefi enumerator computes the predicate itself (same SSOT) so the
    # denominator stays the MVP universe even on a pre-mvp-tag catalogue.
    mvp: bool | None = None


def _row_data_types(
    asset_group: str,
    instr: InstrumentCatalogEntry,
    data_types: list[str],
) -> list[str]:
    """Return the data_types to emit for a single catalogue entry.

    Resolution order:
    1. If ``instr.data_type`` is set (prediction per-row grain binding) → emit
       ONLY that one data_type (already validated at catalogue-build time).
    2. Otherwise call the UAC validity matrix for ``(asset_group, venue, instrument_type)``.
       For DeFi this narrows validity to the SPECIFIC protocol named by
       ``instr.venue`` (e.g. ``UNISWAP_V3-ETHEREUM`` → only the Uniswap-V3
       data_types), so a hybrid protocol's data_type (e.g. GMX's
       ``perp_funding``) no longer leaks to every pool of that instrument_type.
       For every NON-DeFi asset_group, or an unmapped DeFi protocol, the helper
       delegates to the instrument_type-grain matrix (unchanged behaviour).
       - ``None`` returned (unmapped instrument type) → log a warning and fall
         back to ALL ``data_types`` (legacy behaviour; the row is never silently
         dropped for unknown types — only known-invalid cross-products are filtered).
       - ``frozenset`` returned → keep each data_type where EITHER it is in the
         valid set OR it is not in the asset group's canonical data types at all
         (the latter preserves test/non-standard data_types that are not part of
         the G1-ENUM matrix — only real cross-products are filtered out).
         May return empty list for bundle-only instrument types (e.g. cefi OPTION
         → frozenset()) where ALL canonical data_types are excluded.
    """
    if instr.data_type is not None:
        return [instr.data_type]

    valid = valid_data_types_for_venue_instrument_type(asset_group, instr.venue, instr.instrument_type)
    if valid is None:
        logger.warning(
            "G1-ENUM: unmapped instrument_type=%r for asset_group=%r (instrument=%r) "
            "— falling back to all data_types. Add a matrix entry to "
            "unified_api_contracts.registry.market_data_categories to suppress.",
            instr.instrument_type,
            asset_group,
            instr.instrument_id,
        )
        return list(data_types)

    # The canonical data_types for this asset group — used to distinguish
    # real cross-products (known AG data_type NOT in valid) from
    # test/non-standard data_types (not in ANY valid set → pass through).
    known_ag_dts = frozenset(DATA_TYPES_BY_ASSET_GROUP.get(asset_group.lower(), []))

    return [dt for dt in data_types if dt in valid or dt not in known_ag_dts]


# ---------------------------------------------------------------------------
# CeFi MVP capture-universe gate (cefi_universe_capture_rule_2026_06_23)
# Shared helpers so the bundle roll-up + the cefi enumerator apply the SAME
# perp-gated MVP predicate (the UAC ``is_in_mvp_capture_universe`` SSOT).
# ---------------------------------------------------------------------------


def _mvp_capture_itype(instrument_type: str) -> str:
    """Normalise an option/futures BUNDLE (or COMBO) instrument_type to the
    leaf-equivalent the MVP predicate recognises: options_chain/combo → OPTION
    (the Deribit BTC/ETH options carve-out), futures_chain → FUTURE.
    """
    norm = instrument_type.strip().upper()
    if norm in ("OPTIONS_CHAIN", "COMBO"):
        return "OPTION"
    if norm == "FUTURES_CHAIN":
        return "FUTURE"
    return instrument_type


def _base_exchange(venue: str) -> str:
    """Base-exchange token of a canonical venue (``BINANCE-SPOT`` → ``BINANCE``).

    The perp-gate is per EXCHANGE, not per sub-venue: a ``BINANCE-SPOT`` spot is
    gated by a ``BINANCE-FUTURES`` perp.
    """
    return venue.strip().upper().split("-", 1)[0]


def _cefi_perp_bases(catalog: list[InstrumentCatalogEntry]) -> set[tuple[str, str]]:
    """``(base_exchange, base_upper)`` set of exchanges that list a PERPETUAL/EQUITY_PERP."""
    return {
        (_base_exchange(e.venue), (e.base_asset or e.underlying).strip().upper())
        for e in catalog
        if e.instrument_type.strip().upper() in ("PERPETUAL", "EQUITY_PERP")
        and e.venue
        and (e.base_asset or e.underlying)
    }


def _cefi_entry_in_mvp_universe(
    instr: InstrumentCatalogEntry,
    perp_bases: set[tuple[str, str]],
) -> bool:
    """Whether a cefi catalogue entry is in the MVP capture universe.

    Prefers the catalogue's pre-tagged ``mvp`` column (the rollup tags it with the
    SAME shared predicate); falls back to computing the shared UAC predicate when
    the column is absent (``mvp is None``) — bundle instrument_types are normalised
    via :func:`_mvp_capture_itype`.
    """
    if instr.mvp is not None:
        return instr.mvp
    base = (instr.base_asset or instr.underlying).strip().upper()
    return is_in_mvp_capture_universe(
        instr.venue,
        base,
        _mvp_capture_itype(instr.instrument_type),
        has_perp_for_base=(_base_exchange(instr.venue), base) in perp_bases,
    )


def _enumerate_v2_cefi(
    catalog: list[InstrumentCatalogEntry],
    date_axis: list[date],
    data_types: list[str],
    *,
    present_set: set[tuple[str, ...]] | None = None,
    present_cols: list[str] | None = None,
) -> Iterator[ExpectedRow]:
    """Per-instrument cefi v2 enumerator.

    For each instrument in the catalog:
      * date < available_from  → EXPECTED_INSTRUMENT_NOT_LISTED (empty_confirmed)
      * date > available_to    → EXPECTED_INSTRUMENT_DELISTED (empty_confirmed)
      * alive AND no manifest row (present_set provided) → expected_unattempted
      * alive AND present_set not provided → skip (legacy mode)

    Also respects venue launch dates (EXPECTED_PRE_VENUE_LAUNCH) for dates
    before the venue launched — same logic as v1's _enumerate_cefi.
    """
    _pcols = present_cols or ["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"]
    # Pre-compute window bounds once for overlap filter below.
    window_start_ts = pd.Timestamp(date_axis[0]) if date_axis else None
    window_end_ts = pd.Timestamp(date_axis[-1]) if date_axis else None
    # MVP capture-universe denominator gate (cefi_universe_capture_rule_2026_06_23):
    # the expected_unattempted denominator is the perp-gated MVP universe — NOT the
    # full IS catalogue. Out-of-MVP cells are NOT seeded (excluded from the
    # denominator entirely). ``_cefi_perp_bases`` is the venue/base property the
    # shared UAC predicate needs; computed once over the catalog list.
    _perp_bases = _cefi_perp_bases(catalog)
    for instr in catalog:
        # Skip cells outside the MVP capture universe (prefer the pre-tagged ``mvp``
        # column; fall back to the shared predicate — same SSOT).
        if not _cefi_entry_in_mvp_universe(instr, _perp_bases):
            continue
        af_raw = pd.Timestamp(instr.available_from) if instr.available_from else None
        at_raw = pd.Timestamp(instr.available_to) if instr.available_to else None
        # Normalize to tz-naive date-only for comparison with the date axis
        af_ts = af_raw.tz_localize(None) if (af_raw is not None and af_raw.tzinfo is not None) else af_raw
        at_ts = at_raw.tz_localize(None) if (at_raw is not None and at_raw.tzinfo is not None) else at_raw
        # Skip instruments with no lifecycle overlap with the window — avoids
        # generating millions of EXPECTED_INSTRUMENT_DELISTED rows for options
        # that expired before the window started (179K+ in a 12-month cefi run).
        if at_ts is not None and window_start_ts is not None and at_ts < window_start_ts:
            continue  # fully delisted before window started
        if af_ts is not None and window_end_ts is not None and af_ts > window_end_ts:
            continue  # not yet listed when window ended
        venue_launch_str = CEFI_VENUE_LAUNCH_DATES.get(instr.venue)
        venue_launch_ts = pd.Timestamp(venue_launch_str) if venue_launch_str else None
        # G1-ENUM: filter data_types to those valid for this instrument's shape.
        row_dts = _row_data_types("cefi", instr, data_types)
        if not row_dts:
            continue  # e.g. cefi OPTION leaf → frozenset() → skip entirely
        for d in date_axis:
            d_ts = pd.Timestamp(d)
            iso = d.isoformat()
            # venue pre-launch beats instrument lifecycle
            if venue_launch_ts is not None and d_ts < venue_launch_ts:
                reason = "EXPECTED_PRE_VENUE_LAUNCH"
            elif af_ts is not None and d_ts < af_ts:
                reason = "EXPECTED_INSTRUMENT_NOT_LISTED"
            elif at_ts is not None and d_ts > at_ts:
                reason = "EXPECTED_INSTRUMENT_DELISTED"
            else:
                if present_set is None:
                    continue  # legacy mode: alive on this day — skip
                # alive + manifest-aware: yield expected_unattempted for missing rows
                for dt in row_dts:
                    row_key = tuple(
                        {
                            "venue": instr.venue,
                            "chain": instr.chain,
                            "data_type": dt,
                            "instrument_type": instr.instrument_type,
                            "instrument_id": instr.instrument_id,
                            "league_id": "",
                            "date": iso,
                        }.get(c, "")
                        for c in _pcols
                    )
                    if row_key not in present_set:
                        yield ExpectedRow(
                            asset_group="cefi",
                            venue=instr.venue,
                            chain=instr.chain,
                            data_type=dt,
                            instrument_type=instr.instrument_type,
                            instrument_id=instr.instrument_id,
                            league_id="",
                            date=iso,
                            reason="",
                            capture_status="expected_unattempted",
                        )
                continue
            for dt in row_dts:
                yield ExpectedRow(
                    asset_group="cefi",
                    venue=instr.venue,
                    chain=instr.chain,
                    data_type=dt,
                    instrument_type=instr.instrument_type,
                    instrument_id=instr.instrument_id,
                    league_id="",
                    date=iso,
                    reason=reason,
                )


def _enumerate_v2_defi(
    catalog: list[InstrumentCatalogEntry],
    date_axis: list[date],
    data_types: list[str],
    *,
    present_set: set[tuple[str, ...]] | None = None,
    present_cols: list[str] | None = None,
) -> Iterator[ExpectedRow]:
    """Per-instrument defi v2 enumerator.

    Respects both chain genesis dates and protocol launch dates.
    For instruments with available_from/available_to bounds also applies
    per-instrument lifecycle rules.

    * date < chain_genesis     → EXPECTED_PRE_GENESIS_CHAIN (empty_confirmed)
    * date < available_from    → EXPECTED_INSTRUMENT_NOT_LISTED (empty_confirmed)
    * date > available_to      → EXPECTED_INSTRUMENT_DELISTED (empty_confirmed)
    * alive AND no manifest row (present_set provided) → expected_unattempted
    * alive AND present_set not provided → skip (legacy mode)
    """
    _pcols = present_cols or ["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"]
    window_start_ts = pd.Timestamp(date_axis[0]) if date_axis else None
    window_end_ts = pd.Timestamp(date_axis[-1]) if date_axis else None
    for instr in catalog:
        af_raw = pd.Timestamp(instr.available_from) if instr.available_from else None
        at_raw = pd.Timestamp(instr.available_to) if instr.available_to else None
        af_ts = af_raw.tz_localize(None) if (af_raw is not None and af_raw.tzinfo is not None) else af_raw
        at_ts = at_raw.tz_localize(None) if (at_raw is not None and at_raw.tzinfo is not None) else at_raw
        if at_ts is not None and window_start_ts is not None and at_ts < window_start_ts:
            continue  # fully delisted before window started
        if af_ts is not None and window_end_ts is not None and af_ts > window_end_ts:
            continue  # not yet listed when window ended
        # Canonical venue/chain derivation — gotcha #3 (defi-canonical-naming-ssot.md).
        # The instruments-service catalog stores legacy combined venue='AAVEV3-ARBITRUM'
        # with blank chain=''; MTDS captures use canonical venue='AAVE_V3' + chain='ARBITRUM'.
        # Fix: (1) canonicalise protocol spelling (AAVEV3→AAVE_V3) via VenueMapping,
        # (2) if chain is blank and the result is PROTOCOL-CHAIN form, split to derive both.
        canonical_venue_str = VenueMapping._canonicalise_defi_protocol_spelling(instr.venue)
        if not instr.chain and "-" in canonical_venue_str:
            # Split on the last '-' to separate PROTOCOL from CHAIN.
            _proto, _chain = canonical_venue_str.rsplit("-", 1)
            canonical_venue = _proto
            chain_upper = _chain.upper()
        else:
            canonical_venue = canonical_venue_str
            chain_upper = instr.chain.upper() if instr.chain else ""
        chain_genesis_str = CHAIN_GENESIS_DATES.get(chain_upper)
        chain_genesis_ts = pd.Timestamp(chain_genesis_str) if chain_genesis_str else None
        # G1-ENUM: filter data_types to those valid for this instrument's shape.
        # Pass a proxy entry with canonical venue so the validity matrix lookup works.
        _instr_canonical = instr._replace(venue=canonical_venue, chain=chain_upper)
        row_dts = _row_data_types("defi", _instr_canonical, data_types)
        if not row_dts:
            continue  # instrument type not in PROTOCOL_CAPABILITIES → skip entirely
        for d in date_axis:
            d_ts = pd.Timestamp(d)
            iso = d.isoformat()
            # chain genesis takes priority
            if chain_genesis_ts is not None and d_ts < chain_genesis_ts:
                reason = "EXPECTED_PRE_GENESIS_CHAIN"
            elif af_ts is not None and d_ts < af_ts:
                reason = "EXPECTED_INSTRUMENT_NOT_LISTED"
            elif at_ts is not None and d_ts > at_ts:
                reason = "EXPECTED_INSTRUMENT_DELISTED"
            else:
                if present_set is None:
                    continue  # legacy mode: alive on this day — skip
                for dt in row_dts:
                    row_key = tuple(
                        {
                            "venue": canonical_venue,
                            "chain": chain_upper,
                            "data_type": dt,
                            "instrument_type": instr.instrument_type,
                            "instrument_id": instr.instrument_id,
                            "league_id": "",
                            "date": iso,
                        }.get(c, "")
                        for c in _pcols
                    )
                    if row_key not in present_set:
                        yield ExpectedRow(
                            asset_group="defi",
                            venue=canonical_venue,
                            chain=chain_upper,
                            data_type=dt,
                            instrument_type=instr.instrument_type,
                            instrument_id=instr.instrument_id,
                            league_id="",
                            date=iso,
                            reason="",
                            capture_status="expected_unattempted",
                        )
                continue
            for dt in row_dts:
                yield ExpectedRow(
                    asset_group="defi",
                    venue=canonical_venue,
                    chain=chain_upper,
                    data_type=dt,
                    instrument_type=instr.instrument_type,
                    instrument_id=instr.instrument_id,
                    league_id="",
                    date=iso,
                    reason=reason,
                )


def _canonical_writer_instrument_type(asset_group: str, instr: InstrumentCatalogEntry) -> str:
    """Return the CANONICAL bundled instrument_type the MTDS WRITER records at.

    Shard-grain SSOT (mirrors the defi PROTOCOL-vs-PROTOCOL-CHAIN fix at
    instruments-service@38cec01): the seeded ``expected_unattempted`` cell MUST
    carry the SAME ``instrument_type`` the writer stamps for a captured cell, or
    the shard atoms differ and a capture can never convert the seed (the seed
    stays permanently ``expected_unattempted`` and deflates honest-coverage).

    The IS catalogue carries the RAW leaf type UPPERCASE (``FUTURE`` / ``EQUITY``
    / ``ETF`` / ``SPOT_PAIR`` / ``INDEX``), but MTDS writes captured cells at the
    CANONICAL BUNDLED grain LOWERCASE (``future`` / ``equity`` / ``etf`` /
    ``combo`` / ``futures_chain`` / ``options_chain``) — the writer reads the
    ``instrument_type=`` hive path segment which the capture emits in canonical
    form. The UAC ``bundle_instrument_type_for_leaf`` / ``grain_for_instrument_type``
    helpers are the SSOT for that mapping (the same pair ``_rollup_bundle_grain``
    uses to collapse OPTION/COMBO leaves to one per-underlying ``options_chain``
    entry); so:

    * a bundle leaf (``option`` at any tradfi venue → ``options_chain``; ``combo`` at
      a tradfi venue → ``combo``; a CME/ICE-venue-overlaid ``future`` → ``futures_chain``)
      → its bundle instrument_type. NOTE (2026-06-22): TradFi ``combo`` rolls up to its
      OWN ``instrument_type=combo`` (the writer keeps a distinct ``combo`` partition),
      and TradFi ``future`` at CME/ICE bundles to ``futures_chain`` via the
      ``FUTURE_BUNDLE_VENUES["tradfi"]`` overlay — both matching the writer grain;
    * a passthrough leaf (``future`` at a NON-bundling venue / ``equity`` / ``etf`` /
      ``spot_pair`` / ``index``) → the canonical-lowercase leaf type (``.strip().lower()``);
      for tradfi the lowercase form already equals the UAC canonical alias for every
      type (``FUTURE``→``future`` etc — no per-type aliasing needed);
    * an already-canonical bundle entry (``options_chain`` / ``futures_chain`` — e.g.
      the synthetic rows ``_rollup_bundle_grain`` already produced, or a bundle entry
      direct from the catalogue) → returned unchanged (``bundle_instrument_type_for_leaf``
      returns ``None`` for the ``options_chain`` / ``futures_chain`` bundle types, so the
      ``.lower()`` fall-through keeps it intact; a synthetic ``combo`` bundle entry instead
      resolves to ``combo`` via the leaf map, which is likewise its correct canonical type).

    De-dup note: leaf types keep a per-contract ``instrument_id``, so two leaves
    never collapse to one cell here; the OPTION/COMBO→underlying collapse (which
    DOES merge leaves into one cell per ``(venue, underlying)``) is handled
    upstream by ``_rollup_bundle_grain``, so this normaliser never double-counts.
    """
    bundle_it = bundle_instrument_type_for_leaf(asset_group, instr.instrument_type, instr.venue)
    if bundle_it is not None:
        return bundle_it
    return (instr.instrument_type or "").strip().lower()


def _enumerate_v2_tradfi(
    catalog: list[InstrumentCatalogEntry],
    date_axis: list[date],
    data_types: list[str],
    *,
    present_set: set[tuple[str, ...]] | None = None,
    present_cols: list[str] | None = None,
) -> Iterator[ExpectedRow]:
    """Per-instrument tradfi v2 enumerator.

    Tradfi instruments respect available_from/available_to lifecycle bounds.
    Weekend and holiday dates fall through to the pipeline (v1 handles them
    at venue-grain; v2 only adds per-instrument rows for the non-trading-day
    windows outside the instrument lifecycle).

    The seeded ``instrument_type`` is the CANONICAL WRITER grain (lowercase
    ``future``/``equity``/``etf``/``combo``/``futures_chain``), NOT the raw
    UPPERCASE catalogue leaf (``FUTURE``/``EQUITY``/…) — see
    :func:`_canonical_writer_instrument_type`. Without this, the seeded shard
    atom (``FUTURE`` + uppercase) can NEVER be converted by the real capture
    (``future`` lowercase) → ~253k tradfi cells sit permanently
    ``expected_unattempted`` and deflate honest-coverage even though the data IS
    captured (same shard-grain-mismatch class as the defi PROTOCOL-CHAIN bug,
    instruments-service@38cec01).

    * date < available_from    → EXPECTED_INSTRUMENT_NOT_LISTED (empty_confirmed)
    * date > available_to      → EXPECTED_INSTRUMENT_DELISTED (empty_confirmed)
    * alive AND no manifest row (present_set provided) → expected_unattempted
    * alive AND present_set not provided → skip (legacy mode)
    """
    _pcols = present_cols or ["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"]
    window_start_ts = pd.Timestamp(date_axis[0]) if date_axis else None
    window_end_ts = pd.Timestamp(date_axis[-1]) if date_axis else None
    for instr in catalog:
        af_ts = pd.Timestamp(instr.available_from) if instr.available_from else None
        at_ts = pd.Timestamp(instr.available_to) if instr.available_to else None
        if at_ts is not None and window_start_ts is not None and at_ts < window_start_ts:
            continue  # fully delisted before window started
        if af_ts is not None and window_end_ts is not None and af_ts > window_end_ts:
            continue  # not yet listed when window ended
        # G1-ENUM: filter data_types to those valid for this instrument's shape.
        row_dts = _row_data_types("tradfi", instr, data_types)
        if not row_dts:
            continue  # e.g. unknown TradFi instrument type → skip entirely
        # Shard-grain SSOT: seed at the CANONICAL WRITER instrument_type (lowercase
        # bundled grain), not the raw UPPERCASE catalogue leaf — else the seed can
        # never be converted by the real capture. (_row_data_types above is
        # case-insensitive, so it is unaffected by reading instr.instrument_type.)
        canon_it = _canonical_writer_instrument_type("tradfi", instr)
        # Shard-grain SSOT (axis-3, 2026-06-22): for the per-underlying BUNDLE
        # instrument_types (futures_chain / combo / options_chain — the synthetic
        # entries _rollup_bundle_grain produces at instrument_id=<underlying>), the
        # MTDS writer records the captured cell with instrument_id="" + underlying=<U>
        # (market-tick-data-service venue_fetch.py:318-320 → manifest_finalize.py
        # base_row_key, the _UNDERLYING_PARTITIONED_TYPES set). So the seed MUST mirror
        # that: blank instrument_id, the underlying carried in the ``underlying``
        # column. A LEAF type (future at a non-bundling venue / equity / etf / index)
        # keeps its real instrument_id and a blank underlying — unchanged. The
        # discriminator is the UAC GRAIN axis (the same SSOT _rollup_bundle_grain uses),
        # NOT a hardcoded type set, so it tracks the writer automatically.
        is_bundle = grain_for_instrument_type("tradfi", canon_it) == GRAIN_BUNDLE_BY_UNDERLYING
        if is_bundle:
            seed_instrument_id = ""
            seed_underlying = instr.underlying or instr.instrument_id
        else:
            seed_instrument_id = instr.instrument_id
            seed_underlying = ""
        for d in date_axis:
            d_ts = pd.Timestamp(d)
            iso = d.isoformat()
            if af_ts is not None and d_ts < af_ts:
                reason = "EXPECTED_INSTRUMENT_NOT_LISTED"
            elif at_ts is not None and d_ts > at_ts:
                reason = "EXPECTED_INSTRUMENT_DELISTED"
            else:
                if present_set is None:
                    continue  # legacy mode: alive on this day — skip
                for dt in row_dts:
                    row_key = tuple(
                        {
                            "venue": instr.venue,
                            "chain": "",
                            "data_type": dt,
                            "instrument_type": canon_it,
                            "instrument_id": seed_instrument_id,
                            "underlying": seed_underlying,
                            "league_id": "",
                            "date": iso,
                        }.get(c, "")
                        for c in _pcols
                    )
                    if row_key not in present_set:
                        yield ExpectedRow(
                            asset_group="tradfi",
                            venue=instr.venue,
                            chain="",
                            data_type=dt,
                            instrument_type=canon_it,
                            instrument_id=seed_instrument_id,
                            league_id="",
                            date=iso,
                            reason="",
                            capture_status="expected_unattempted",
                            underlying=seed_underlying,
                        )
                continue
            for dt in row_dts:
                yield ExpectedRow(
                    asset_group="tradfi",
                    venue=instr.venue,
                    chain="",
                    data_type=dt,
                    instrument_type=canon_it,
                    instrument_id=seed_instrument_id,
                    league_id="",
                    date=iso,
                    reason=reason,
                    underlying=seed_underlying,
                )


def _enumerate_v2_sports(
    catalog: list[InstrumentCatalogEntry],
    date_axis: list[date],
    data_types: list[str],
    *,
    present_set: set[tuple[str, ...]] | None = None,
    present_cols: list[str] | None = None,
) -> Iterator[ExpectedRow]:
    """Per-LEAGUE sports v2 enumerator (league-grain — NOT per-fixture).

    The captured sports manifest atom is per-``(league_id, data_type, date)``
    (slot-4 finding 2026-06-07 on the canonical ``instruments-store-sports-prd``
    ``_index``): ``league_id`` populated 97.6%, ``venue`` / ``instrument_id`` /
    ``instrument_type`` ~blank. So the present-set match + the seeded
    ``expected_unattempted`` atom are LEAGUE-grain (``_SPORTS_PRESENT_COLS`` =
    ``data_type, league_id, date``); ``venue`` / ``instrument_id`` /
    ``instrument_type`` are blanked on the yielded row so the seeded atom is
    indistinguishable from the captured atom (a fixture-grain row would never
    match → every cell inflates the coverage denominator).

    Per (league, data_type, date), with the league's lifecycle bounds from the
    catalogue (``available_from`` = first day the league appears,
    ``available_to`` = last day / ``None`` if still active):

    * date < the data_type's source coverage start → SKIP — those dates are
      owned by the v1 ``_enumerate_sports`` pre-coverage rows
      (``EXPECTED_PRE_SOURCE_COVERAGE_START``, league_id="" grain). v2 must NOT
      re-emit them or the (data_type, date) cell is double-counted at two grains.
    * date < available_from   → EXPECTED_INSTRUMENT_NOT_LISTED (empty_confirmed)
    * date > available_to      → EXPECTED_INSTRUMENT_DELISTED (empty_confirmed)
    * alive AND no manifest row (present_set provided) → expected_unattempted
    * alive AND present_set not provided → skip (legacy mode)

    ``data_types`` is the captured sports data_types axis (``_sports_data_types()``
    = ``SPORTS_DATA_TYPE_TO_SOURCE`` keys) — passed in by ``enumerate_v2`` /
    ``main``. A data_type with no source mapping (e.g. a test stub) gets no
    coverage filter.
    """
    from unified_api_contracts.registry.sports_per_source_rules import is_expected_for_source
    from unified_api_contracts.sports import (
        SPORTS_DATA_TYPE_TO_SOURCE,
        get_entity_league_coverage,
        get_source_coverage_start,
    )
    from unified_api_contracts.registry.sports_per_source_rules import is_expected_for_source

    _pcols = present_cols or list(_SPORTS_PRESENT_COLS)
    window_start_ts = pd.Timestamp(date_axis[0]) if date_axis else None
    window_end_ts = pd.Timestamp(date_axis[-1]) if date_axis else None

    # Pre-resolve each data_type's source key (used by is_expected_for_source in alive branch).
    dt_source: dict[str, str | None] = {}
    # Pre-resolve each data_type's source coverage start once (None = unmapped).
    coverage_starts: dict[str, pd.Timestamp | None] = {}
    # Pre-resolve each data_type's per-LEAGUE entity coverage once. ``None`` = the
    # source covers ALL leagues; a frozenset = the source covers ONLY those
    # canonical leagues (e.g. XG / XG_SHOTS → Understat's ~5 leagues). Seeding a
    # data_type for a league outside its coverage is a FALSE expected_unattempted
    # (the source legitimately doesn't cover that league) — gate it out below.
    entity_coverage: dict[str, frozenset[str] | None] = {}
    for dt in data_types:
        source = SPORTS_DATA_TYPE_TO_SOURCE.get(dt)
        dt_source[dt] = source
        cov = get_source_coverage_start(source, dt) if source is not None else None
        coverage_starts[dt] = pd.Timestamp(cov) if cov is not None else None
        _ec = get_entity_league_coverage(dt)
        entity_coverage[dt] = frozenset(x.upper() for x in _ec) if _ec is not None else None

    for instr in catalog:
        af_ts = pd.Timestamp(instr.available_from) if instr.available_from else None
        at_ts = pd.Timestamp(instr.available_to) if instr.available_to else None
        if at_ts is not None and window_start_ts is not None and at_ts < window_start_ts:
            continue  # league fully delisted before window started
        if af_ts is not None and window_end_ts is not None and af_ts > window_end_ts:
            continue  # league not yet listed when window ended
        league_id = instr.league_id or instr.instrument_id
        # G1-ENUM: filter data_types to those valid for this league instrument's shape.
        row_dts = _row_data_types("sports", instr, data_types)
        if not row_dts:
            continue  # unmapped sports instrument type → skip entirely
        for dt in row_dts:
            if dt in _RETIRED_SPORTS_DATA_TYPES:
                continue  # retired — not a coverage gap, skip entirely
            cov_ts = coverage_starts.get(dt)
            # Per-league entity coverage: the source does NOT cover this league for
            # this data_type (e.g. XG for a non-Understat league) — emit
            # EXPECTED_NO_PROVIDER_COVERAGE (empty_confirmed) so the shard is
            # excluded from the denominator honestly, rather than inflating
            # expected_unattempted with cells the provider can never supply.
            _cov_leagues = entity_coverage.get(dt)
            if _cov_leagues is not None and league_id.upper() not in _cov_leagues:
                for d in date_axis:
                    d_ts = pd.Timestamp(d)
                    if cov_ts is not None and d_ts < cov_ts:
                        continue  # pre-coverage dates owned by v1 (league_id="" grain)
                    yield ExpectedRow(
                        asset_group="sports",
                        venue="",
                        chain="",
                        data_type=dt,
                        instrument_type="",
                        instrument_id="",
                        league_id=league_id,
                        date=d.isoformat(),
                        reason="EXPECTED_NO_PROVIDER_COVERAGE",
                    )
                continue
            for d in date_axis:
                d_ts = pd.Timestamp(d)
                iso = d.isoformat()
                # Pre-source-coverage dates are owned by v1 (league_id="" grain).
                if cov_ts is not None and d_ts < cov_ts:
                    continue
                if af_ts is not None and d_ts < af_ts:
                    reason = "EXPECTED_INSTRUMENT_NOT_LISTED"
                elif at_ts is not None and d_ts > at_ts:
                    reason = "EXPECTED_INSTRUMENT_DELISTED"
                else:
                    # Per-day source-rule gate: footystats off-season, transfer-window,
                    # understat-league whitelist (already guarded by entity_coverage above
                    # for understat — this catches any residual + footystats season logic).
                    _src = dt_source.get(dt)
                    if _src is not None:
                        _in_scope, _oos_reason = is_expected_for_source(_src, league_id, d, data_type=dt)
                        if not _in_scope and _oos_reason is not None:
                            yield ExpectedRow(
                                asset_group="sports",
                                venue="",
                                chain="",
                                data_type=dt,
                                instrument_type="",
                                instrument_id="",
                                league_id=league_id,
                                date=iso,
                                reason=_oos_reason,
                            )
                            continue
                    if present_set is None:
                        continue  # legacy mode: alive on this day — skip
                    row_key = tuple(
                        {
                            "venue": "",
                            "chain": "",
                            "data_type": dt,
                            "instrument_type": "",
                            "instrument_id": "",
                            "league_id": league_id,
                            "date": iso,
                        }.get(c, "")
                        for c in _pcols
                    )
                    if row_key not in present_set:
                        yield ExpectedRow(
                            asset_group="sports",
                            venue="",
                            chain="",
                            data_type=dt,
                            instrument_type="",
                            instrument_id="",
                            league_id=league_id,
                            date=iso,
                            reason="",
                            capture_status="expected_unattempted",
                        )
                    continue
                yield ExpectedRow(
                    asset_group="sports",
                    venue="",
                    chain="",
                    data_type=dt,
                    instrument_type="",
                    instrument_id="",
                    league_id=league_id,
                    date=iso,
                    reason=reason,
                )


def _enumerate_v2_prediction(
    catalog: list[InstrumentCatalogEntry],
    date_axis: list[date],
    data_types: list[str],
    *,
    present_set: set[tuple[str, ...]] | None = None,
    present_cols: list[str] | None = None,
) -> Iterator[ExpectedRow]:
    """Per-market prediction v2 enumerator.

    Prediction instruments have ``market_created_at`` and ``settlement_time``
    lifecycle bounds. Dates before creation → EXPECTED_INSTRUMENT_NOT_LISTED;
    dates after settlement → EXPECTED_INSTRUMENT_DELISTED.

    When ``market_created_at`` / ``settlement_time`` are absent, falls back
    to available_from / available_to.

    * date < market_created_at → EXPECTED_INSTRUMENT_NOT_LISTED (empty_confirmed)
    * date > settlement_time   → EXPECTED_INSTRUMENT_DELISTED (empty_confirmed)
    * alive AND no manifest row (present_set provided) → expected_unattempted
    * alive AND present_set not provided → skip (legacy mode)

    **cqg-bundle grain ONLY (decision 338, 2026-06-19).** The prediction catalogue
    carries TWO grains: the cqg bundle (``data_type=prediction_canonical_question_group``,
    ``instrument_id=<cqg>``, ~45 rows) AND per-conditionId (``data_type=trades`` /
    ``market_lifecycle``, ~870K rows). Seeding ``expected_unattempted`` at per-conditionId
    grain emits >50M FALSE rows (435K conditionIds x ~574 days x 2 data_types) that NEVER
    match the per-conditionId-``trades`` captured present-set -> catastrophic denominator
    inflation. So when the catalogue contains ANY cqg-bundle rows, this enumerator FILTERS
    to those rows ONLY (the manifest's actual reconciliation grain for the bundle). The
    per-conditionId trades/market_lifecycle universe is a feature-layer concern, NOT an EU
    seed. If the catalogue has NO cqg-bundle rows (legacy / test), fall through to all rows
    unchanged (never silently drop a whole AG).
    """
    _pcols = present_cols or ["venue", "chain", "data_type", "instrument_type", "instrument_id", "league_id", "date"]
    window_start_ts = pd.Timestamp(date_axis[0]) if date_axis else None
    window_end_ts = pd.Timestamp(date_axis[-1]) if date_axis else None
    # decision 338: keep ONLY the cqg-bundle grain when present (else >50M conditionId blow-up).
    _cqg_rows = [c for c in catalog if c.data_type == _PREDICTION_CQG_DATA_TYPE]
    if _cqg_rows:
        logger.info(
            "prediction v2: cqg-bundle-grain filter active — %d cqg rows kept of %d catalogue rows "
            "(per-conditionId trades/market_lifecycle EXCLUDED; decision 338)",
            len(_cqg_rows),
            len(catalog),
        )
        catalog = _cqg_rows
    for instr in catalog:
        # Prefer market lifecycle fields; fall back to generic available_from/to
        created_str = instr.market_created_at or instr.available_from
        settled_str = instr.settlement_time or instr.available_to
        af_ts = pd.Timestamp(created_str) if created_str else None
        at_ts = pd.Timestamp(settled_str) if settled_str else None
        if at_ts is not None and window_start_ts is not None and at_ts < window_start_ts:
            continue  # fully settled before window started
        if af_ts is not None and window_end_ts is not None and af_ts > window_end_ts:
            continue  # not yet created when window ended
        # Grain-binding: a catalogue row may declare its OWN data_type (the
        # prediction multi-grain catalogue — cqg bundle vs per-conditionId
        # trades). When set, emit for THAT data_type only at this row's grain;
        # otherwise fall back to the full passed list (legacy / other AGs).
        # G1-ENUM: _row_data_types handles the instr.data_type path + any
        # future prediction matrix entries; for now prediction uses the
        # instr.data_type grain-binding path (which _row_data_types returns
        # as-is) or falls back to all data_types (unmapped → None → all).
        row_dts: list[str] = _row_data_types("prediction", instr, data_types)
        for d in date_axis:
            d_ts = pd.Timestamp(d)
            iso = d.isoformat()
            if af_ts is not None and d_ts < af_ts:
                reason = "EXPECTED_INSTRUMENT_NOT_LISTED"
            elif at_ts is not None and d_ts > at_ts:
                reason = "EXPECTED_INSTRUMENT_DELISTED"
            else:
                if present_set is None:
                    continue  # legacy mode: alive on this day — skip
                for dt in row_dts:
                    row_key = tuple(
                        {
                            "venue": instr.venue,
                            "chain": "",
                            "data_type": dt,
                            "instrument_type": instr.instrument_type,
                            "instrument_id": instr.instrument_id,
                            "league_id": "",
                            "date": iso,
                        }.get(c, "")
                        for c in _pcols
                    )
                    if row_key not in present_set:
                        yield ExpectedRow(
                            asset_group="prediction",
                            venue=instr.venue,
                            chain="",
                            data_type=dt,
                            instrument_type=instr.instrument_type,
                            instrument_id=instr.instrument_id,
                            league_id="",
                            date=iso,
                            reason="",
                            capture_status="expected_unattempted",
                        )
                continue
            for dt in row_dts:
                yield ExpectedRow(
                    asset_group="prediction",
                    venue=instr.venue,
                    chain="",
                    data_type=dt,
                    instrument_type=instr.instrument_type,
                    instrument_id=instr.instrument_id,
                    league_id="",
                    date=iso,
                    reason=reason,
                )


_V2_ENUMERATORS: dict[
    str,
    object,
] = {
    "cefi": _enumerate_v2_cefi,
    "defi": _enumerate_v2_defi,
    "tradfi": _enumerate_v2_tradfi,
    "sports": _enumerate_v2_sports,
    "prediction": _enumerate_v2_prediction,
}


def _derive_underlying(instrument_id: str) -> str:
    """Fallback underlying derivation when the catalogue ``underlying`` column is
    blank — the base asset is the token before the first ``-`` separator
    (``BTC-29MAR24-50000-C`` → ``BTC``; ``BTC-29MAR24`` → ``BTC``). Returns "" if
    no separator (cannot key a bundle → caller skips rather than mis-key)."""
    iid = instrument_id.strip()
    if "-" not in iid:
        return ""
    return iid.split("-", 1)[0]


def _rollup_bundle_grain(catalog: list[InstrumentCatalogEntry], asset_group: str) -> list[InstrumentCatalogEntry]:
    """Roll bundle-grain LEAF instruments up to ONE synthetic per-underlying
    bundle entry (G1-ENUM, ERA-B).

    For instrument_types the UAC GRAIN axis marks ``bundle_by_underlying`` AND
    that map to a bundle INSTRUMENT_TYPE (``option`` → ``options_chain``; tradfi
    ``combo`` → ``combo``; a CME/ICE ``future`` → ``futures_chain``), every leaf
    contract of an ``(venue, chain, underlying)`` collapses into ONE
    synthetic catalogue entry — ``instrument_type`` = the bundle instrument_type
    (``options_chain`` / ``futures_chain`` / ``combo``), ``instrument_id`` = the underlying,
    ``data_type`` = None so the enumerator resolves the bundle's data_type from the
    UAC validity matrix (``options_chain``/``futures_chain`` → ``trades``, Era-B) —
    emitting ONE candidate with data_type=trades, NOT data_type=options_chain.
    Lifecycle = the UNION of the leaves' ``[available_from, available_to]`` windows.
    So the enumerator yields ONE could-exist candidate per underlying instead of
    one per leaf contract (the slot-3/slot-6 over-fan: 72K OPTION + 64.8K COMBO
    leaves; tradfi ~563K false candidates).

    Generalises slot-4's league-grain roll-up (the sports catalogue is built at
    league grain; here the read-side pre-pass rolls option/combo leaves to the
    chain-bundle grain) — driven entirely by the UAC registry, no per-AG
    special-casing. Non-bundle-leaf instruments (incl. the ``options_chain`` /
    ``futures_chain`` bundle entries themselves) pass through unchanged; a
    passed-through bundle entry's data_type is likewise resolved to trades via the
    validity matrix.
    """
    passthrough: list[InstrumentCatalogEntry] = []
    # key (venue, chain, underlying, bundle_it) → [min available_from, max available_to (None = open)]
    bundles: dict[tuple[str, str, str, str], list[str | None]] = {}
    saw_open_end: set[tuple[str, str, str, str]] = set()
    # MVP roll-up (cefi_universe_capture_rule_2026_06_23): a bundle (options_chain /
    # futures_chain) is MVP iff ANY of its leaves is MVP, so the perp-gated mvp tag
    # survives the bundle collapse and the cefi expected-universe gate reads it
    # directly (the bundle's instrument_type is options_chain/futures_chain, NOT a
    # type the predicate recognises). For cefi we OR each leaf's mvp (catalogue
    # column when present, else the shared predicate) into the bundle key.
    bundle_mvp: dict[tuple[str, str, str, str], bool] = {}
    _perp_bases: set[tuple[str, str]] = _cefi_perp_bases(catalog) if asset_group == "cefi" else set()

    def _leaf_is_mvp(e: InstrumentCatalogEntry, base_override: str = "") -> bool:
        if e.mvp is not None:
            return e.mvp
        _base = (base_override or e.base_asset or e.underlying).strip().upper()
        return is_in_mvp_capture_universe(
            e.venue,
            _base,
            _mvp_capture_itype(e.instrument_type),
            has_perp_for_base=(_base_exchange(e.venue), _base) in _perp_bases,
        )

    for instr in catalog:
        # Venue-aware (F2): a bare FUTURE leaf bundles to futures_chain only at
        # DERIBIT/OKX; at BYBIT (+ venue-unknown) it stays a per-contract leaf.
        # option/combo bundle venue-agnostically (the venue arg is a no-op there).
        bundle_it = bundle_instrument_type_for_leaf(asset_group, instr.instrument_type, instr.venue)
        is_bundle_leaf = (
            grain_for_instrument_type(asset_group, instr.instrument_type, instr.venue) == GRAIN_BUNDLE_BY_UNDERLYING
            and bundle_it is not None
        )
        if not is_bundle_leaf:
            passthrough.append(instr)
            continue
        underlying = instr.underlying or _derive_underlying(instr.instrument_id)
        if not underlying:
            # Cannot key the bundle (no underlying) → drop the leaf rather than
            # mis-key a candidate (under-seed beats false over-seed). Logged once.
            logger.warning(
                "G1-ENUM bundle-grain: no underlying for %s leaf %r (venue=%s) — dropped from roll-up",
                asset_group,
                instr.instrument_id,
                instr.venue,
            )
            continue
        key = (instr.venue, instr.chain, underlying, bundle_it)  # pyright: ignore[reportArgumentType]
        if asset_group == "cefi":
            # Use the DERIVED underlying as the MVP base (the leaf may carry a blank
            # underlying — derived above from the instrument_id).
            bundle_mvp[key] = bundle_mvp.get(key, False) or _leaf_is_mvp(instr, underlying)
        if key not in bundles:
            bundles[key] = [instr.available_from, instr.available_to]
            if instr.available_to is None:
                saw_open_end.add(key)
        else:
            cur = bundles[key]
            if instr.available_from is not None and (cur[0] is None or instr.available_from < cur[0]):
                cur[0] = instr.available_from
            # available_to: an open end (None) on ANY leaf means the bundle is open.
            if instr.available_to is None:
                saw_open_end.add(key)
            elif key not in saw_open_end and (cur[1] is None or instr.available_to > cur[1]):
                cur[1] = instr.available_to
    synthetic: list[InstrumentCatalogEntry] = []
    for (venue, chain, underlying, bundle_it), (af, at_) in sorted(bundles.items()):
        synthetic.append(
            InstrumentCatalogEntry(
                instrument_id=underlying,
                instrument_type=bundle_it,
                venue=venue,
                chain=chain,
                league_id="",
                available_from=af,
                available_to=None if (venue, chain, underlying, bundle_it) in saw_open_end else at_,
                market_created_at=None,
                settlement_time=None,
                # ERA-B: data_type=None → enumerator resolves it from the validity
                # matrix (options_chain/futures_chain → trades), emitting exactly
                # one candidate with data_type=trades (NOT data_type=options_chain).
                data_type=None,
                underlying=underlying,
                base_asset=underlying,
                # Carry the rolled-up MVP tag so the cefi enumerator gate reads it
                # directly (the bundle instrument_type is options_chain/futures_chain,
                # which the predicate doesn't recognise). None for non-cefi.
                mvp=bundle_mvp.get((venue, chain, underlying, bundle_it)),
            )
        )
    return passthrough + synthetic


def enumerate_v2(
    *,
    asset_group: str,
    catalog: list[InstrumentCatalogEntry],
    date_axis: list[date],
    data_types: list[str] | None = None,
    present_set: set[tuple[str, ...]] | None = None,
    present_cols: list[str] | None = None,
) -> Iterator[ExpectedRow]:
    """Per-instrument-grain enumerator (v2).

    Cross-joins the instruments-service catalog with a date axis and a list of
    data_types to yield one ``ExpectedRow`` per
    ``(instrument_id, date, data_type)`` triple where the instrument is NOT
    alive on that date (``empty_confirmed``) OR is alive but has no manifest
    row (``expected_unattempted``, when ``present_set`` is provided).

    Args:
        asset_group: One of the five supported asset groups.
        catalog: Instrument lifecycle records read from the instruments-service
            catalog parquets. Build via :func:`_catalog_from_dataframe` or
            construct :class:`InstrumentCatalogEntry` objects directly in tests.
        date_axis: Ordered list of calendar dates to check. Generate from
            ``pd.date_range(start, end, freq="D")`` + ``.date`` conversion.
        data_types: Optional override list of data_type strings. Defaults to
            ``DATA_TYPES_BY_ASSET_GROUP[asset_group]``.
        present_set: Set of manifest row-key tuples already present in the
            manifest (built via :func:`_build_present_set`). When provided,
            alive-instrument dates with no manifest row yield
            ``expected_unattempted`` rows. When ``None``, alive dates are
            silently skipped (legacy mode — no-op for alive window).
        present_cols: Column order used to build ``present_set`` tuples (must
            match the order used in :func:`_build_present_set`). Defaults to
            ``["venue", "chain", "data_type", "instrument_type",
            "instrument_id", "league_id", "date"]``.

    Yields:
        :class:`ExpectedRow` instances with ``reason`` drawn from the
        ``EMPTY_CONFIRMED_REASONS`` closed set (``empty_confirmed`` rows) or
        ``reason=""`` with ``capture_status="expected_unattempted"``.

    Lifecycle rules applied per asset_group:
        - **cefi**: CEFI_VENUE_LAUNCH_DATES (pre-launch) > available_from/to
          (instrument lifecycle). Reasons: EXPECTED_PRE_VENUE_LAUNCH >
          EXPECTED_INSTRUMENT_NOT_LISTED > EXPECTED_INSTRUMENT_DELISTED.
        - **defi**: CHAIN_GENESIS_DATES > available_from/to. Reasons:
          EXPECTED_PRE_GENESIS_CHAIN > EXPECTED_INSTRUMENT_NOT_LISTED >
          EXPECTED_INSTRUMENT_DELISTED.
        - **tradfi**: available_from/to only. Non-trading-day (weekend/holiday)
          rows are covered by the v1 venue-level enumerator.
        - **sports**: available_from/to applied per league/fixture.
        - **prediction**: market_created_at/settlement_time (falls back to
          available_from/to). Reasons: EXPECTED_INSTRUMENT_NOT_LISTED /
          EXPECTED_INSTRUMENT_DELISTED.

    Gate G3 of ``manifest_evolution_SUPERSEDED_2026_05_21``. Ships per
    ``expected_universe_v2_design_2026_05_08.md`` Phase 1.A.
    Wave 3 (``expected_unattempted``): writegate plan Phase 3.D.5 item.
    """
    if asset_group not in _V2_ENUMERATORS:
        raise ValueError(
            f"enumerate_v2: unsupported asset_group={asset_group!r}; must be one of {sorted(_V2_ENUMERATORS)}"
        )
    # G1-ENUM bundle-grain roll-up (Era-B): collapse option/combo leaves → ONE
    # synthetic per-underlying options_chain instrument entry (data_type resolved
    # to trades via the validity matrix) BEFORE per-AG enumeration (no-op for
    # asset_groups/instrument_types without bundle-grain leaves).
    catalog = _rollup_bundle_grain(catalog, asset_group)
    if data_types:
        resolved_data_types = data_types
    elif asset_group == "sports":
        # Sports denominator iterates the captured provider data_types
        # (SPORTS_DATA_TYPE_TO_SOURCE), NOT DATA_TYPES_BY_ASSET_GROUP["sports"]
        # (the MTDS odds types) — see _sports_data_types().
        resolved_data_types = _sports_data_types()
    else:
        resolved_data_types = [str(dt) for dt in DATA_TYPES_BY_ASSET_GROUP.get(asset_group, [])]
    enumerator_func = _V2_ENUMERATORS[asset_group]
    yield from enumerator_func(
        catalog,
        date_axis,
        resolved_data_types,
        present_set=present_set,
        present_cols=present_cols,
    )


def _catalog_from_dataframe(df: pd.DataFrame) -> list[InstrumentCatalogEntry]:
    """Build a list of :class:`InstrumentCatalogEntry` from a catalog DataFrame.

    The DataFrame is expected to have at minimum ``instrument_id``, ``venue``,
    and ``instrument_type`` columns. All other fields default to empty string /
    None when absent. Used by both the production loader and unit tests.
    """

    def _safe_str(val: object) -> str:
        """Return empty string for NaN/None, else str(val)."""
        if val is None:
            return ""
        try:
            if pd.isna(val):
                return ""
        except (TypeError, ValueError):
            pass
        return str(val)

    def _opt_date(val: object) -> str | None:
        """Return ISO date string or None."""
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        return str(val)

    def _opt_bool(val: object) -> bool | None:
        """Return bool, or None when the ``mvp`` column is absent/NaN."""
        if val is None:
            return None
        try:
            if pd.isna(val):  # pyright: ignore[reportArgumentType]
                return None
        except (TypeError, ValueError):
            pass
        return bool(val)

    entries: list[InstrumentCatalogEntry] = []
    for row in df.itertuples(index=False, name=None):
        row_dict: dict[str, object] = dict(zip(df.columns, row, strict=True))
        # Support both canonical column names and instruments-service catalog aliases.
        # instruments-service catalog uses:
        #   instrument_key (not instrument_id)
        #   available_from_datetime (not available_from)
        #   available_to_datetime (not available_to)
        instrument_id = _safe_str(row_dict.get("instrument_id") or row_dict.get("instrument_key", ""))
        available_from = _opt_date(row_dict.get("available_from") or row_dict.get("available_from_datetime"))
        available_to = _opt_date(row_dict.get("available_to") or row_dict.get("available_to_datetime"))
        entries.append(
            InstrumentCatalogEntry(
                instrument_id=instrument_id,
                instrument_type=_safe_str(row_dict.get("instrument_type", "")),
                venue=_safe_str(row_dict.get("venue", "")),
                chain=_safe_str(row_dict.get("chain", "")),
                league_id=_safe_str(row_dict.get("league_id", "")),
                available_from=available_from,
                available_to=available_to,
                market_created_at=_opt_date(row_dict.get("market_created_at")),
                settlement_time=_opt_date(row_dict.get("settlement_time")),
                data_type=(_safe_str(row_dict.get("data_type", "")) or None),
                underlying=_safe_str(row_dict.get("underlying", "")),
                base_asset=_safe_str(row_dict.get("base_asset", "")),
                mvp=_opt_bool(row_dict.get("mvp")),
            )
        )
    return entries


# ---------------------------------------------------------------------------
# Manifest IO
# ---------------------------------------------------------------------------


def _download_manifest(bucket_name: str, asset_group: str) -> tuple[pd.DataFrame, str]:
    """Bulk-download the canonical manifest. Returns (df, local_path).

    If a pre-cached copy exists at /tmp/{asset_group}_manifest_cache.parquet
    (written by a preceding gsutil cp to avoid GCS SDK stream timeouts on
    large manifests), use it directly instead of re-downloading.
    """
    # Support both /tmp and home-dir caches (macOS sandbox writes home-dir on some calls)
    _home_cache = os.path.expanduser(f"~/tmp_manifest_cache/{asset_group}_manifest_cache.parquet")
    _tmp_cache = f"/tmp/{asset_group}_manifest_cache.parquet"
    cache_path = _home_cache if os.path.exists(_home_cache) else _tmp_cache
    if os.path.exists(cache_path):
        logger.info("Using pre-cached manifest at %s", cache_path)
        df = pd.read_parquet(cache_path)
        logger.info("Manifest rows: %d", len(df))
        return df, cache_path

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    logger.info("Loading manifest from gs://%s/%s", bucket_name, MANIFEST_BLOB)
    with tempfile.NamedTemporaryFile(
        prefix=f"enum-univ-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        local_path = tf.name
    blob.download_to_filename(local_path, timeout=600)
    df = pd.read_parquet(local_path)
    logger.info("Manifest rows: %d", len(df))
    return df, local_path


def _build_present_set(df: pd.DataFrame, asset_group: str) -> set[tuple[str, ...]]:
    """Build the set of present manifest row-key tuples at the per-asset_group grain.

    Sports uses LEAGUE-grain (``data_type, league_id, date``); every other group
    uses the full per-instrument grain — see :func:`_present_cols_for`.
    """
    if df.empty:
        return set()
    if "date" not in df.columns:
        logger.warning("Manifest missing 'date' column — cannot build present-set")
        return set()
    available = _present_cols_for(asset_group, list(df.columns))
    df_subset = df[available].fillna("").astype(str)
    return {tuple(row) for row in df_subset.itertuples(index=False, name=None)}


# ---------------------------------------------------------------------------
# Full-history range-encoding (Part 2 — scalable representation)
# ---------------------------------------------------------------------------

#: The non-date shard-key + state columns that define one range. Two ExpectedRows
#: collapse into the same range iff every one of these is equal and their dates are
#: contiguous (no gap > 1 day).
_RANGE_KEY_FIELDS: tuple[str, ...] = (
    "asset_group",
    "venue",
    "chain",
    "data_type",
    "instrument_type",
    "instrument_id",
    "underlying",
    "league_id",
    "reason",
    "capture_status",
)


@dataclass(frozen=True)
class RangeRow:
    """One range-encoded expected_unattempted span - ``[date_start, date_end]`` inclusive.

    Replaces ``(date_end - date_start + 1)`` per-day ``ExpectedRow``s with a SINGLE row.
    ``n_days`` is the materialised day count so a coverage consumer recovers the exact
    per-day denominator contribution WITHOUT expanding the range
    (``sum of n_days`` over ranges == the per-day EU count the naive full-history would have).
    """

    asset_group: str
    venue: str
    chain: str
    data_type: str
    instrument_type: str
    instrument_id: str
    underlying: str
    league_id: str
    reason: str
    capture_status: str
    date_start: str
    date_end: str
    n_days: int


def range_encode(rows: Iterable[ExpectedRow]) -> list[RangeRow]:
    """Collapse per-day ``ExpectedRow``s into contiguous ``RangeRow`` spans.

    Groups by :data:`_RANGE_KEY_FIELDS`; within a group, sorts the ISO dates and
    emits one :class:`RangeRow` per maximal run of consecutive calendar days (a gap
    of > 1 day starts a new span). This is the ~100x compaction that makes the full
    2018-to-today per-instrument universe (~190M day-rows fleet-wide) fit as ~1-3M range
    rows while keeping the coverage denominator EXACT (sum of ``n_days``).

    Pure + deterministic (sorted group keys + sorted dates) so re-runs are byte-stable
    - required for the gitignore'd-artifact / idempotent-shard contract.
    """
    by_key: dict[tuple[str, ...], list[str]] = {}
    for r in rows:
        key = tuple(getattr(r, f) for f in _RANGE_KEY_FIELDS)
        by_key.setdefault(key, []).append(r.date)

    out: list[RangeRow] = []
    one_day = pd.Timedelta(days=1)
    for key in sorted(by_key):
        dates = sorted(set(by_key[key]))
        if not dates:
            continue
        span_start = dates[0]
        prev = dates[0]
        prev_ts = pd.Timestamp(prev)
        run_days = 1
        kw = dict(zip(_RANGE_KEY_FIELDS, key, strict=True))
        for d in dates[1:]:
            d_ts = pd.Timestamp(d)
            if d_ts - prev_ts == one_day:
                run_days += 1
            else:
                out.append(RangeRow(date_start=span_start, date_end=prev, n_days=run_days, **kw))  # pyright: ignore[reportArgumentType]
                span_start = d
                run_days = 1
            prev = d
            prev_ts = d_ts
        out.append(RangeRow(date_start=span_start, date_end=prev, n_days=run_days, **kw))  # pyright: ignore[reportArgumentType]
    return out


def full_timeframe_coverage(
    index_df: pd.DataFrame,
    ranges_df: pd.DataFrame | None,
) -> dict[str, float | int]:
    """Compute the HONEST full-timeframe coverage denominator from the per-day ``_index``
    + the range-encoded EU companion (read-side reconciliation, Part 2).

    The main ``_index`` carries per-day cells (recent bounded window + any captured
    history). The companion carries the FULL 2018-to-today expected_unattempted universe as
    range spans. The full-timeframe denominator ADDS the companion's EU-days that are NOT
    already represented per-day in the ``_index`` window:

        captured  = sum of index cells where capture_status == captured
        empty     = sum of index cells where capture_status == empty_confirmed
        failed    = sum of index cells where capture_status == attempted_failed
        eu_index  = sum of index cells where capture_status == expected_unattempted (window)
        eu_full   = sum of ranges.n_days where capture_status == expected_unattempted
        # The companion is the COMPLETE EU universe; the window EU is a subset of it, so
        # use max(eu_index, eu_full) to avoid double-counting the overlap.
        denom     = captured + empty + failed + max(eu_index, eu_full)
        coverage% = 100 * captured / denom

    Returns a dict with the components + ``coverage_pct``. Pure (no IO) so it is the
    canonical reader a coverage consumer (deployment-api / data-status) calls.
    """
    cs = index_df["capture_status"] if "capture_status" in index_df.columns else pd.Series([], dtype=str)
    captured = int((cs == "captured").sum())
    empty = int((cs == "empty_confirmed").sum())
    failed = int((cs == "attempted_failed").sum())
    eu_index = int((cs == "expected_unattempted").sum())
    eu_full = 0
    if ranges_df is not None and not ranges_df.empty and "n_days" in ranges_df.columns:
        _eu = ranges_df
        if "capture_status" in ranges_df.columns:
            _eu = ranges_df[ranges_df["capture_status"] == "expected_unattempted"]
        eu_full = int(_eu["n_days"].fillna(0).astype(int).sum())
    eu = max(eu_index, eu_full)
    denom = captured + empty + failed + eu
    coverage_pct = round(100.0 * captured / denom, 2) if denom else 0.0
    return {
        "captured": captured,
        "empty_confirmed": empty,
        "attempted_failed": failed,
        "expected_unattempted_window": eu_index,
        "expected_unattempted_full_history": eu_full,
        "expected_unattempted_effective": eu,
        "denominator": denom,
        "coverage_pct": coverage_pct,
    }


def _write_range_artifact(
    *,
    ranges: list[RangeRow],
    asset_group: str,
    bucket_name: str,
    run_id: str,
) -> int:
    """Write the range-encoded full-history EU companion to
    ``gs://{bucket}/_index/expected_universe_ranges.parquet``.

    This is a SEPARATE artifact from the main ``_index/availability_index.parquet`` -
    the coverage consumer reads it ADDITIVELY for the full-timeframe denominator,
    keeping the hot-path per-day ``_index`` lean (recent window only). One stable blob
    per AG bucket (last-writer-wins; idempotent re-runs). Returns 0 on success.
    """
    attempted_at_iso = datetime.now(UTC).isoformat()
    records: list[dict[str, object]] = []
    for r in ranges:
        pipeline_mode, source, transport = _derive_pm_source_transport(asset_group, r.data_type)
        records.append(
            {
                "asset_group": r.asset_group,
                "venue": r.venue,
                "chain": r.chain,
                "data_type": r.data_type,
                "instrument_type": r.instrument_type,
                "instrument_id": r.instrument_id,
                "underlying": r.underlying,
                "league_id": r.league_id,
                "capture_status": r.capture_status,
                "error_reason": r.reason if r.capture_status == "empty_confirmed" else "",
                "date_start": r.date_start,
                "date_end": r.date_end,
                "n_days": r.n_days,
                "written_at": attempted_at_iso,
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "service_name": "instruments-service",
                "enumerator_run_id": run_id,
                "pipeline_mode": pipeline_mode,
                "source": source,
                "transport": transport,
            }
        )
    new_df = pd.DataFrame(records)
    with tempfile.NamedTemporaryFile(
        prefix=f"enum-univ-ranges-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        out_path = tf.name
    try:
        new_df.to_parquet(out_path, index=False)
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        out_blob = bucket.blob(EXPECTED_UNIVERSE_RANGES_BLOB)
        out_blob.upload_from_filename(out_path, timeout=600)
        logger.info(
            "Wrote %d range rows (%d total EU-days) to gs://%s/%s",
            len(records),
            sum(r.n_days for r in ranges),
            bucket_name,
            EXPECTED_UNIVERSE_RANGES_BLOB,
        )
    finally:
        with contextlib.suppress(OSError):
            os.unlink(out_path)
    return 0


def _row_key(row: ExpectedRow, available_cols: list[str]) -> tuple[str, ...]:
    """Build the manifest-aligned row key from an ExpectedRow."""
    field_map = {
        "venue": row.venue,
        "chain": row.chain,
        "data_type": row.data_type,
        "instrument_type": row.instrument_type,
        "instrument_id": row.instrument_id,
        "underlying": row.underlying,
        "league_id": row.league_id,
        "date": row.date,
    }
    return tuple(field_map.get(c, "") for c in available_cols)


# ---------------------------------------------------------------------------
# CLI + main
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Enumerate expected universe and write record_expected_empty rows "
            "for tuples with no manifest row. Phase 3.D.4 — see module "
            "docstring."
        ),
    )
    p.add_argument(
        "--asset-group",
        required=True,
        choices=sorted(SUPPORTED_ASSET_GROUPS),
        help="Asset group manifest to enumerate.",
    )
    p.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help=f"Window start (default: {DEFAULT_START_DATE}).",
    )
    p.add_argument(
        "--end-date",
        default=datetime.now(UTC).strftime("%Y-%m-%d"),
        help="Window end (default: today UTC).",
    )
    p.add_argument(
        "--bucket",
        default=None,
        help="Override the canonical bucket (default: per-asset-group SSOT).",
    )
    p.add_argument(
        "--apply-write",
        action="store_true",
        help="Default scan-only. Pass to actually write to per-VM manifest shard.",
    )
    p.add_argument(
        "--max-writes-per-run",
        type=int,
        default=1_000_000,
        help=(
            "Halt-safety cap (default 1M, bumped 2026-05-07 after defi scan-only run "
            "exceeded the prior 100k default). Aborts if scan finds more than this."
        ),
    )
    p.add_argument(
        "--report-dir",
        default=None,
        help="Override CSV report dir (default: tempfile.gettempdir()).",
    )
    p.add_argument(
        "--gcs-report-bucket",
        default=None,
        help=(
            "If set, upload the CSV report to gs://<bucket>/enumerator-reports/"
            "{vm_name_or_run_id}/<asset_group>-<ts>.csv before VM auto-shutdown. "
            "Use this on backfill VMs where local disk dies on shutdown so row-by-row "
            "inspection survives. Defaults to deployment-scripts-{PROJECT_ID} when "
            "VM_NAME is set; pass empty string to opt out."
        ),
    )
    p.add_argument(
        "--enumerator-version",
        choices=["v1", "v2"],
        default="v1",
        help=(
            "Enumerator version to use. "
            "v1 (default): venue-grain enumerator (Phase 3.D.4 writegate). "
            "v2: per-instrument-grain enumerator (Gate G3 manifest_evolution_master). "
            "v2 requires --catalog-path (GCS URI or local path to instruments-service "
            "catalog parquet). v2 default becomes active after G4 v8 schema lands."
        ),
    )
    p.add_argument(
        "--catalog-path",
        default=None,
        help=(
            "Path to instruments-service catalog parquet for v2 enumeration. "
            "Accepts local filesystem path or gs:// URI. Required when "
            "--enumerator-version=v2."
        ),
    )
    p.add_argument(
        "--data-types",
        default=None,
        help=(
            "Comma-separated data_type override (v2 only). Restricts the enumerated "
            "data_type axis to this explicit set — e.g. "
            "'--data-types prediction_canonical_question_group' to seed prediction "
            "expected_unattempted at the cqg-bundle grain ONLY (decision 338). "
            "Default: the per-asset_group canonical data_types."
        ),
    )
    p.add_argument(
        "--full-history",
        action="store_true",
        help=(
            "Full-history mode (Part 2). Enumerate the FULL --start-date..--end-date "
            "window and write a RANGE-ENCODED companion artifact "
            "(_index/expected_universe_ranges.parquet) instead of per-day rows into the "
            "per-VM _index shard. Collapses the ~190M-row naive full-history per-day "
            "universe into ~1-3M contiguous (shard-key, reason) spans (~100x), keeping "
            "the hot-path _index lean while the coverage denominator stays honest over "
            "2018->today. Requires --enumerator-version=v2 + --apply-write to write."
        ),
    )
    return p.parse_args()


def _upload_csv_report_to_gcs(
    *,
    local_path: Path,
    bucket_name: str,
    vm_name_or_run_id: str,
    asset_group: str,
    run_ts: str,
) -> str:
    """Upload the CSV report to GCS so it survives VM auto-shutdown.

    Returns the canonical ``gs://...`` URI of the uploaded blob.
    """
    blob_name = f"enumerator-reports/{vm_name_or_run_id}/{asset_group}-{run_ts}.csv"
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{blob_name}"


def _write_absent_rows(
    *,
    absent_rows: list[ExpectedRow],
    asset_group: str,
    bucket_name: str,
    apply_write: bool,
    report_dir: Path,
    report_path: Path,
    run_id: str,
    run_ts: str,
    gcs_report_bucket_arg: str | None,
    enumerator_version: str = "v1",
    manifest_df: pd.DataFrame | None = None,
) -> int:
    """Shared CSV-report + optional per-VM shard write path.

    Called by both v1 and v2 paths in ``main()``. Writes a CSV audit report,
    optionally uploads it to GCS, then (if ``--apply-write``) writes a
    per-VM manifest shard parquet to GCS.

    Args:
        absent_rows: Rows to report/write.
        asset_group: Target asset group (for record-keeping).
        bucket_name: GCS bucket holding the canonical manifest.
        apply_write: If False, scan-only (no GCS shard write).
        report_dir: Local directory for CSV report.
        report_path: Full path of the CSV report file.
        run_id: Unique run identifier used in events + shard paths.
        run_ts: Timestamp string ``YYYYMMDD-HHMMSS`` for report naming.
        gcs_report_bucket_arg: ``args.gcs_report_bucket`` value.
        enumerator_version: ``"v1"`` or ``"v2"`` for event logging.
        manifest_df: Optional manifest DataFrame (v1 passes it for column
            alignment; v2 passes None and the shard uses a minimal schema).

    Returns:
        Exit code (0 = success, 4 = env guard failure).
    """
    if apply_write:
        if os.environ.get("MANIFEST_PER_VM_SHARDS", "").lower() not in ("1", "true", "yes"):
            logger.error(
                "--apply-write requires MANIFEST_PER_VM_SHARDS=true (per-VM shard "
                "isolation rule, codex/02-data/availability-manifest-and-data-status.md)."
            )
            _emit_event(
                "ENUMERATOR_FAILED",
                reason="missing_per_vm_shards_env",
                run_id=run_id,
            )
            return 4
        if not os.environ.get("VM_NAME"):
            logger.error("--apply-write requires VM_NAME=<unique-tag>.")
            _emit_event("ENUMERATOR_FAILED", reason="missing_vm_name_env", run_id=run_id)
            return 4

    # Distribution by reason.
    reason_counts: dict[str, int] = {}
    for r in absent_rows:
        reason_counts[r.reason] = reason_counts.get(r.reason, 0) + 1
    logger.info("Distribution by reason:")
    for reason, count in sorted(reason_counts.items(), key=lambda kv: -kv[1]):
        logger.info("  %s: %d", reason, count)

    # CSV audit.
    with report_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(absent_rows[0]).keys()))
        writer.writeheader()
        writer.writerows(asdict(r) for r in absent_rows)
    logger.info("Would-write report: %s (%d rows)", report_path, len(absent_rows))

    # Upload CSV report to GCS so it survives VM auto-shutdown.
    gcs_report_uri: str | None = None
    vm_name_env = os.environ.get("VM_NAME", "")
    report_bucket: str | None
    if gcs_report_bucket_arg is None:
        report_bucket = f"deployment-scripts-{PROJECT_ID}" if vm_name_env else None
    elif gcs_report_bucket_arg == "":
        report_bucket = None
    else:
        report_bucket = gcs_report_bucket_arg
    if report_bucket:
        try:
            gcs_report_uri = _upload_csv_report_to_gcs(
                local_path=report_path,
                bucket_name=report_bucket,
                vm_name_or_run_id=vm_name_env or run_id,
                asset_group=asset_group,
                run_ts=run_ts,
            )
            logger.info("Uploaded CSV report to %s", gcs_report_uri)
        except Exception as exc:
            logger.warning("CSV report GCS upload failed (best-effort): %s", exc)
            _emit_event(
                "ENUMERATOR_REPORT_UPLOAD_FAILED",
                bucket=report_bucket,
                error=str(exc),
                run_id=run_id,
            )

    if not apply_write:
        logger.info("Scan-only mode; not writing manifest. Pass --apply-write to commit.")
        _emit_event(
            "ENUMERATOR_COMPLETED",
            enumerator_version=enumerator_version,
            asset_group=asset_group,
            candidates=len(absent_rows),
            written=0,
            report_path=str(report_path),
            gcs_report_uri=gcs_report_uri,
            run_id=run_id,
        )
        return 0

    # Write per-VM shard.
    vm_name = os.environ["VM_NAME"]
    per_vm_blob = f"_index/per_vm/{vm_name}.parquet"
    attempted_at_iso = datetime.now(UTC).isoformat()

    new_rows_records: list[dict[str, object]] = []
    for r in absent_rows:
        # #4 — stamp pipeline_mode + source + transport so seeded denominator
        # rows match the real rows they reconcile against (else CF-3 reads blank).
        pipeline_mode, source, transport = _derive_pm_source_transport(asset_group, r.data_type)
        record: dict[str, object] = {
            "asset_group": asset_group,
            "venue": r.venue,
            "chain": r.chain,
            "data_type": r.data_type,
            "instrument_type": r.instrument_type,
            "instrument_id": r.instrument_id,
            "underlying": r.underlying,
            "league_id": r.league_id,
            "date": r.date,
            "capture_status": r.capture_status,
            "error_reason": r.reason if r.capture_status == "empty_confirmed" else "",
            "attempted_at": attempted_at_iso,
            "written_at": attempted_at_iso,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "row_count": 0,
            "service_name": "instruments-service",
            "enumerator_run_id": run_id,
            "pipeline_mode": pipeline_mode,
            "source": source,
            "transport": transport,
        }
        new_rows_records.append(record)

    new_df = pd.DataFrame(new_rows_records)

    # Align columns with the canonical manifest where they overlap (v1 passes
    # manifest_df; v2 passes None and uses the minimal schema above).
    if manifest_df is not None:
        manifest_cols = list(manifest_df.columns)
        for col in manifest_cols:
            if col not in new_df.columns:
                canonical_dtype = manifest_df[col].dtype
                if pd.api.types.is_integer_dtype(canonical_dtype):
                    new_df[col] = pd.array([pd.NA] * len(new_df), dtype="Int64")
                    continue
                if pd.api.types.is_float_dtype(canonical_dtype):
                    new_df[col] = pd.array([pd.NA] * len(new_df), dtype="Float64")
                    continue
                if pd.api.types.is_bool_dtype(canonical_dtype):
                    new_df[col] = pd.array([pd.NA] * len(new_df), dtype="boolean")
                    continue
                if pd.api.types.is_datetime64_any_dtype(canonical_dtype):
                    new_df[col] = pd.array([pd.NaT] * len(new_df), dtype="datetime64[ns]")
                    continue
                non_null = manifest_df[col].dropna()
                if len(non_null) > 0:
                    sample_type = type(non_null.iloc[0])
                    if sample_type is bool:
                        new_df[col] = pd.array([pd.NA] * len(new_df), dtype="boolean")
                        continue
                    if sample_type is int:
                        new_df[col] = pd.array([pd.NA] * len(new_df), dtype="Int64")
                        continue
                    if sample_type is float:
                        new_df[col] = pd.array([pd.NA] * len(new_df), dtype="Float64")
                        continue
                new_df[col] = ""
        new_df = new_df.reindex(columns=manifest_cols + [c for c in new_df.columns if c not in manifest_cols])

    with tempfile.NamedTemporaryFile(
        prefix=f"enum-univ-out-{asset_group}-",
        suffix=".parquet",
        delete=False,
    ) as tf:
        out_path = tf.name
    start_write = time.time()
    try:
        new_df.to_parquet(out_path, index=False)
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        out_blob = bucket.blob(per_vm_blob)
        out_blob.upload_from_filename(out_path, timeout=600)
        logger.info("Uploaded per-VM shard to gs://%s/%s", bucket_name, per_vm_blob)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(out_path)

    elapsed = time.time() - start_write
    _emit_event(
        "ENUMERATOR_COMPLETED",
        enumerator_version=enumerator_version,
        asset_group=asset_group,
        candidates=len(absent_rows),
        written=len(new_rows_records),
        elapsed_secs=round(elapsed, 1),
        report_path=str(report_path),
        gcs_report_uri=gcs_report_uri,
        per_vm_blob=per_vm_blob,
        run_id=run_id,
    )
    logger.info(
        "Wrote %d rows to per-VM shard gs://%s/%s for VM=%s in %.1fs. "
        "Consolidator will merge into canonical manifest within ~5min.",
        len(new_rows_records),
        bucket_name,
        per_vm_blob,
        vm_name,
        elapsed,
    )
    return 0


def main() -> int:
    args = _parse_args()
    asset_group: str = args.asset_group
    bucket_name: str = args.bucket or _default_bucket_for(asset_group)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    run_id = f"enum-universe-{asset_group}-{run_ts}"

    report_dir = Path(args.report_dir) if args.report_dir else Path(tempfile.gettempdir())
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"enum-universe-{asset_group}-{run_ts}.csv"

    enumerator_version: str = args.enumerator_version
    apply_write: bool = bool(args.apply_write)
    max_writes_per_run: int = int(args.max_writes_per_run)
    start_date: str = str(args.start_date)
    end_date: str = str(args.end_date)
    gcs_report_bucket_arg: str | None = args.gcs_report_bucket
    catalog_path: str | None = args.catalog_path
    data_types_override: list[str] | None = (
        [dt.strip() for dt in str(args.data_types).split(",") if dt.strip()] if args.data_types else None
    )
    full_history: bool = bool(args.full_history)

    _emit_event(
        "ENUMERATOR_STARTED",
        enumerator="enumerate_expected_universe",
        enumerator_version=enumerator_version,
        asset_group=asset_group,
        bucket=bucket_name,
        start_date=start_date,
        end_date=end_date,
        apply_write=apply_write,
        max_writes_per_run=max_writes_per_run,
        data_types_override=data_types_override,
        full_history=full_history,
        run_id=run_id,
    )

    if full_history and enumerator_version != "v2":
        logger.error("--full-history requires --enumerator-version=v2 (range-encoding is v2-only)")
        _emit_event("ENUMERATOR_FAILED", reason="full_history_requires_v2", run_id=run_id)
        return 4

    # v2 path: load catalog + manifest, build date axis, delegate to enumerate_v2()
    if enumerator_version == "v2":
        if not catalog_path:
            logger.error("--enumerator-version=v2 requires --catalog-path <parquet path or gs:// URI>")
            _emit_event("ENUMERATOR_FAILED", reason="missing_catalog_path", run_id=run_id)
            return 4
        logger.info("v2 enumerator: loading catalog from %s", catalog_path)
        if catalog_path.startswith("gs://"):
            # Download via the google-cloud-storage client (ADC) — the same path
            # _download_manifest uses. NOT gcsfs ``token="cloud"`` (which is the
            # GCE metadata-server credential ONLY → fails on any non-GCE host, e.g.
            # a laptop run with ``ValueError: Invalid gcloud credentials``). ADC
            # resolves to the metadata server on a VM and to the application-default
            # credentials on a workstation, so this is portable laptop + VM + AWS.
            _gs_bucket, _gs_blob = catalog_path[len("gs://") :].split("/", 1)
            _cat_client = storage.Client(project=PROJECT_ID)
            _cat_bucket = _cat_client.bucket(_gs_bucket)
            _cat_blob = _cat_bucket.blob(_gs_blob)
            with tempfile.NamedTemporaryFile(
                prefix=f"enum-univ-catalog-{asset_group}-",
                suffix=".parquet",
                delete=False,
            ) as _cat_tf:
                _cat_local = _cat_tf.name
            _cat_blob.download_to_filename(_cat_local, timeout=600)
            catalog_df = pd.read_parquet(_cat_local)
        else:
            catalog_df = pd.read_parquet(catalog_path)
        logger.info("v2 catalog loaded: %d instruments", len(catalog_df))
        catalog = _catalog_from_dataframe(catalog_df)
        # Download manifest to build present_set for expected_unattempted detection.
        v2_manifest_df, v2_local_manifest = _download_manifest(bucket_name, asset_group)
        try:
            v2_present_set = _build_present_set(v2_manifest_df, asset_group)
            logger.info("v2 manifest present-set size: %d", len(v2_present_set))
            # Column order used in _build_present_set (must match present_set tuples).
            v2_present_cols = _present_cols_for(asset_group, list(v2_manifest_df.columns))
            # Build date_axis as list[date]
            date_axis_ts = pd.date_range(start_date, end_date, freq="D")
            date_axis: list[date] = [d.date() for d in date_axis_ts]
            # Sports denominator iterates the captured provider data_types
            # (SPORTS_DATA_TYPE_TO_SOURCE), NOT the MTDS odds types in
            # DATA_TYPES_BY_ASSET_GROUP["sports"] — see _sports_data_types().
            if data_types_override is not None:
                # Explicit override (e.g. prediction cqg-bundle grain only, decision 338).
                data_types_list = data_types_override
                logger.info("v2: data_type override active → %s", data_types_list)
            elif asset_group == "sports":
                data_types_list = _sports_data_types()
            else:
                data_types_list = [str(dt) for dt in DATA_TYPES_BY_ASSET_GROUP.get(asset_group, [])]
            # FULL-HISTORY (Part 2): enumerate the FULL --start..--end window. The
            # present_set is STILL passed (alive-and-present cells — the recent ~120d
            # _index window — correctly SKIP, never double-counted; alive-and-absent →
            # expected_unattempted; lifecycle-boundary days → empty_confirmed). The result
            # is range-encoded into the companion artifact rather than written per-day, so
            # the ~190M-day full-history collapses to ~1-3M spans. The bounded-window path
            # is identical but over the short window + writes per-day rows to the _index shard.
            # Wrap enumerate_v2 in an adapter that matches the absent_rows list
            # the existing write-path expects (list[ExpectedRow])
            v2_absent: list[ExpectedRow] = []
            for expected_row in enumerate_v2(
                asset_group=asset_group,
                catalog=catalog,
                date_axis=date_axis,
                data_types=data_types_list,
                present_set=v2_present_set,
                present_cols=v2_present_cols,
            ):
                v2_absent.append(expected_row)
                if len(v2_absent) > max_writes_per_run:
                    logger.error(
                        "Halt-safety triggered: would-write %d > max_writes_per_run %d. "
                        "Increase --max-writes-per-run after operator review.",
                        len(v2_absent),
                        max_writes_per_run,
                    )
                    _emit_event(
                        "ENUMERATOR_FAILED",
                        reason="max_writes_exceeded",
                        candidates=len(v2_absent),
                        cap=max_writes_per_run,
                        run_id=run_id,
                    )
                    return 5
            logger.info(
                "v2 enumeration complete: %d candidate rows (per-instrument grain)",
                len(v2_absent),
            )
            if not v2_absent:
                logger.info("v2: nothing to backfill — manifest already covers the expected per-instrument universe.")
                _emit_event(
                    "ENUMERATOR_COMPLETED",
                    enumerator_version="v2",
                    asset_group=asset_group,
                    candidates=0,
                    written=0,
                    run_id=run_id,
                )
                return 0
            # FULL-HISTORY: range-encode the per-day candidates into contiguous spans +
            # write the companion artifact (not the per-day _index shard). ~100x compaction.
            if full_history:
                ranges = range_encode(v2_absent)
                total_days = sum(r.n_days for r in ranges)
                logger.info(
                    "v2 full-history: %d per-day candidates → %d range rows (%d EU-days; %.0fx compaction)",
                    len(v2_absent),
                    len(ranges),
                    total_days,
                    (len(v2_absent) / len(ranges)) if ranges else 1.0,
                )
                if not apply_write:
                    logger.info("Scan-only full-history; pass --apply-write to commit the range companion.")
                    _emit_event(
                        "ENUMERATOR_COMPLETED",
                        enumerator_version="v2",
                        asset_group=asset_group,
                        candidates=len(v2_absent),
                        range_rows=len(ranges),
                        eu_days=total_days,
                        written=0,
                        full_history=True,
                        run_id=run_id,
                    )
                    return 0
                code = _write_range_artifact(
                    ranges=ranges, asset_group=asset_group, bucket_name=bucket_name, run_id=run_id
                )
                _emit_event(
                    "ENUMERATOR_COMPLETED",
                    enumerator_version="v2",
                    asset_group=asset_group,
                    candidates=len(v2_absent),
                    range_rows=len(ranges),
                    eu_days=total_days,
                    written=len(ranges),
                    full_history=True,
                    run_id=run_id,
                )
                return code
            # Route through shared write path (v1 passes manifest_df for column
            # alignment; v2 passes None — uses minimal schema).
            return _write_absent_rows(
                absent_rows=v2_absent,
                asset_group=asset_group,
                bucket_name=bucket_name,
                apply_write=apply_write,
                report_dir=report_dir,
                report_path=report_path,
                run_id=run_id,
                run_ts=run_ts,
                gcs_report_bucket_arg=gcs_report_bucket_arg,
                enumerator_version="v2",
            )
        finally:
            with contextlib.suppress(OSError):
                os.unlink(v2_local_manifest)

    # v1 path: download manifest, build present-set, enumerate expected universe.
    df, local_manifest = _download_manifest(bucket_name, asset_group)
    try:
        present_set = _build_present_set(df, asset_group)
        logger.info("Manifest present-set size: %d", len(present_set))

        # Determine which manifest columns exist for present-set comparison
        # (LEAGUE-grain for sports — must match _build_present_set above).
        available_cols = _present_cols_for(asset_group, list(df.columns))

        # Step 2: enumerate expected universe; filter to absent tuples.
        enumerator = _ENUMERATORS[asset_group]
        absent_rows: list[ExpectedRow] = []
        scan_start = time.time()
        for expected in enumerator(start_date, end_date):
            key = _row_key(expected, available_cols)
            if key in present_set:
                continue
            absent_rows.append(expected)
            if len(absent_rows) > max_writes_per_run:
                logger.error(
                    "Halt-safety triggered: would-write %d > max_writes_per_run %d. "
                    "Increase --max-writes-per-run after operator review.",
                    len(absent_rows),
                    max_writes_per_run,
                )
                _emit_event(
                    "ENUMERATOR_FAILED",
                    reason="max_writes_exceeded",
                    candidates=len(absent_rows),
                    cap=max_writes_per_run,
                    run_id=run_id,
                )
                return 5
        scan_secs = time.time() - scan_start
        logger.info(
            "Enumeration complete: %d candidate rows in %.1fs",
            len(absent_rows),
            scan_secs,
        )

        if not absent_rows:
            logger.info("Nothing to backfill. Manifest already covers the expected universe.")
            _emit_event(
                "ENUMERATOR_COMPLETED",
                enumerator_version="v1",
                asset_group=asset_group,
                candidates=0,
                written=0,
                run_id=run_id,
            )
            return 0

        # Delegate CSV report + GCS upload + optional per-VM shard write to
        # shared helper (same code path as v2; v1 passes manifest_df for column
        # alignment so the consolidator merge is schema-exact).
        return _write_absent_rows(
            absent_rows=absent_rows,
            asset_group=asset_group,
            bucket_name=bucket_name,
            apply_write=apply_write,
            report_dir=report_dir,
            report_path=report_path,
            run_id=run_id,
            run_ts=run_ts,
            gcs_report_bucket_arg=gcs_report_bucket_arg,
            enumerator_version="v1",
            manifest_df=df,
        )
    finally:
        with contextlib.suppress(OSError):
            os.unlink(local_manifest)


if __name__ == "__main__":
    sys.exit(main())
