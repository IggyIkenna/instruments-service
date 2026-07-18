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
from datetime import UTC, date, datetime, timedelta
from typing import TypeVar

import pandas as pd
from unified_api_contracts import (
    CEFI_BASE_ASSET_UNIVERSE,
    CEFI_EQUITY_PERP_BASE_UNIVERSE,
    TRADFI_ROOTS,
    VENUE_TO_ASSET_GROUP,
    build_pool_identity,
    is_in_mvp_capture_universe,
    is_mvp,
    tracks_equity,
)
from unified_api_contracts.internal import InstrumentRecord, InstrumentType
from unified_api_contracts.internal.reference.canonical_id_builder import build_instrument_id
from unified_api_contracts.predictions import build_cross_venue_mapping
from unified_trading_library import (
    GcsEventSink,
    StorageClient,
    get_config,
    get_storage_client,
    log_event,
    resolve_bucket_name,
    setup_events,
)

#: Real event-log wiring for CATALOGUE_SHRINK_BLOCKED (cefi_monotonicity_guard_alerting_
#: and_dark_venues_2026_07_07.md) — mirrors scripts/cross_asset_rescan.py's batch-mode
#: GcsEventSink pattern. setup_events() is called once in main() (the sole real CLI entry
#: point; tests call run_rollup()/promote_catalogue() directly and never reach this).
_EVENTS_PROJECT_ID = "central-element-323112"
_EVENTS_SERVICE = "instruments-service"


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

#: Sentinel ``league_id`` values that must NEVER be rolled up into a catalogue
#: row (2026-07-08/09 phantom-league fix, `A1` in
#: `instruments_docs_audit_outstanding_items_2026_07_08.md`).
#: ``api_football_reference.py:165`` is the only known writer of the literal
#: ``"UNKNOWN"`` value (frozen since the 2026-06-24 write-universe gate — no new
#: captures land under it), but the uppercase compare below also catches any
#: case-variant that might otherwise re-enter this roll-up.
#:
#: HISTORY NOTE (2026-07-13): until the 24-league de-registration ruling this
#: deliberately stayed a NARROW sentinel check because 22 raw numeric long-tail
#: league_ids plus ``LA_LIGA_2``/``RFPL``/``SCOTTISH_LEAGUE_CUP_185`` were
#: legitimate manifest leagues outside UAC ``LEAGUE_REGISTRY``. The operator
#: ruling de-registered all 24 (captured rows re-keyed or parked to
#: ``_audits/parked_league_rows_20260713.parquet``, index purged), so the
#: roll-ups now ALSO require :func:`_sports_league_registered` — sentinels stay
#: as a distinct, case-insensitive first-line guard.
SPORTS_LEAGUE_ID_SENTINELS = frozenset({"UNKNOWN"})


def _sports_league_registered(league_id: str) -> bool:
    """True when ``league_id`` is a UAC ``LEAGUE_REGISTRY`` member.

    2026-07-13 operator ruling (24-league de-registration): the sports universe
    is EXACTLY the registered-league set — the 94-league api_football trading
    universe plus the 7 non-football registry leagues (ATP/WTA/NBA/NFL/NHL/MLB/
    EUROLEAGUE). League_ids outside ``LEAGUE_REGISTRY`` (raw numeric
    api-football long-tail ids, alias strings like ``LA_LIGA_2``/``RFPL``/
    ``SCOTTISH_LEAGUE_CUP_185``) are de-registered: their index rows were
    re-keyed/parked+purged, but their GCS data objects remain in place by
    design — so BOTH sports roll-ups (league-grain manifest roll-up AND the
    fixture/team/player by_date walk) must gate on this or the catalogue would
    re-mint rows for de-registered leagues from the surviving objects, and the
    v2 enumerator would re-amplify them into manifest expected/empty rows.
    New captured writes are separately blocked by
    ``orchestrator/sports.py::_is_in_canonical_write_universe``.
    """
    from unified_api_contracts.sports import LEAGUE_REGISTRY

    return league_id in LEAGUE_REGISTRY


#: instrument_type stamped on sports FIXTURE/TEAM/PLAYER-grain catalogue rows
#: (2026-07-09 operator decision — extends the catalogue past the
#: league-grain-only scope `sports_catalog_league_grain_only_scope_2026_07_08.md`
#: documented). See :func:`build_sports_fixture_team_player_catalogue`.
SPORTS_FIXTURE_INSTRUMENT_TYPE = "fixture"
SPORTS_TEAM_INSTRUMENT_TYPE = "team"
SPORTS_PLAYER_INSTRUMENT_TYPE = "player"

#: ``sports_reference/by_date`` ``entity=`` names the fixture/team/player-grain
#: roll-up reads — ONE combined by_date walk covers all three (single-walk
#: discipline, codex/02-data/availability-manifest-and-data-status.md: a
#: separate whole-corpus walk per entity is review-blocking).
SPORTS_FIXTURE_ENTITY = "fixtures"
SPORTS_TEAM_ENTITY = "teams"
#: Real captured per-player source. API-Football ``entity=injuries`` rows carry
#: a real ``player_id``/``player_name``/``team_id`` (verified against real prod
#: GCS 2026-07-09). ``entity=fixture_lineups`` was considered first — it is the
#: full-roster source the SPORTS_INSTRUMENTS.md 11-step pipeline table implies —
#: but its real per-league parquet on GCS carries ONLY
#: ``formation``/``fixture_id``/``available_at``: the per-fixture-entity writer's
#: nested-column-drop guard (``sports_reference_fixtures.py::_write_per_fixture_entities``,
#: "Dropping N nested columns") strips the ``player_id``/``player_name`` fields
#: (nested under raw ``startXI``/``substitutes`` blocks) before they ever reach
#: GCS, so no player identity survives there today — a real, separate
#: data-completeness gap in the LINEUPS writer, not something this roll-up can
#: paper over. INJURIES is therefore the honest source: real, but a NARROWER
#: slice than a full roster (currently-injured players only). See
#: SPORTS_INSTRUMENTS.md's "Known gaps" section.
SPORTS_PLAYER_SOURCE_ENTITY = "injuries"

#: The three entities :func:`_iter_sports_ftp_snapshots` walks in one pass.
_SPORTS_FTP_ENTITIES = frozenset({SPORTS_FIXTURE_ENTITY, SPORTS_TEAM_ENTITY, SPORTS_PLAYER_SOURCE_ENTITY})

#: The canonical catalogue filename the launcher + v2 enumerator read.
CATALOG_FILENAME = "catalog.parquet"

#: Minimum trailing-window size for the incremental rollup, in days. MUST stay
#: >= ``_VENUE_RECENT_WINDOW`` (14) or the §7.3 per-venue thin-day median cannot
#: be computed inside the window and live perps mass-false-delist; +7 margin.
#: SSOT: plans/active/instruments_catalogue_incremental_rollup_2026_06_29.md.
WINDOW_DAYS_MIN = 21

#: Extra days added past the previous catalogue's age when SELF-WIDENING the
#: window (operator decision 2026-07-03): ``window_days =
#: max(WINDOW_DAYS_MIN, days_since_prev_catalogue + WINDOW_MARGIN_DAYS)`` — one
#: wide catch-up run is equivalent to replaying the daily incremental once per
#: missed day, so recovery after an outage stays EXACT (true available_from /
#: available_to for everything that listed/delisted during the gap).
WINDOW_MARGIN_DAYS = 7

#: Trailing-window size (days) for the sports FIXTURE/TEAM/PLAYER-grain roll-up's
#: by_date walk (:func:`build_sports_fixture_team_player_catalogue`). UNLIKE
#: league-grain (a cheap single manifest-index read, no by_date walk at all),
#: fixtures/teams/injuries are per-fixture/per-day objects with no GCS-side way
#: to prefix-scope a LISTING to just these three entities (the ``entity=``
#: segment sits after ``day=``/``pipeline_mode=`` in the path) — an unwindowed
#: full-history listing measured >180s against real prod GCS 2026-07-09,
#: BEFORE any downloads even start. A full historical fixture/team/player
#: backfill is therefore a genuine, separate, larger decision (mirrors the
#: "needs a decision, not a rushed fix" framing
#: `sports_catalog_league_grain_only_scope_2026_07_08.md` used) — this roll-up
#: defaults to a real, current, always-fresh trailing window (~13 months)
#: instead of silently doing nothing or hanging indefinitely.
SPORTS_FTP_WINDOW_DAYS = 400

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
    # Distinct venue-declared EXPIRY (dated FUTURE/OPTION/COMBO), stored separately
    # from the overloaded ``available_to`` (which for a non-expiring type is a
    # delisting / last-observed date, NOT an expiry). This is the already-parsed
    # ``_InstrumentAggregate.expiry`` (venue truth), emitted verbatim — NOT derived
    # from ``available_to``: for the ~74% of cefi dated rows where the venue declared
    # no expiry, ``available_to`` is a last-observed date, so deriving from it would
    # fabricate an expiry. Honest-NULL means "no venue-declared expiry captured"; the
    # ``available_to`` + type-filter path in ``catalogue_lifecycle.list_upcoming_expiries``
    # stays correct as the floor. Plan: data_status_page_ux_and_canonicalisation_2026_07_16 A2.
    "expiry",
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
    # Human-readable market question/title (uac InstrumentRecord.question). TODAY
    # populated only by the PREDICTION adapters (Polymarket ``question`` / Kalshi
    # ``title — yes_sub_title``) and emitted onto the per-market prediction roll-up
    # rows below. FORWARD-ONLY: the per-date parquet only carries ``question`` for
    # captures written after uac@c1de078a, so on pre-migration data it is honest-None
    # (never fabricated). Non-prediction rows (cefi/tradfi/defi/sports) get None via
    # the ``pd.DataFrame(rows, columns=CATALOG_COLUMNS)`` reindex — the main emission
    # is not wired for question (those instruments have no market-question axis).
    "question",
    # Per-instance cross-venue identity (prediction_canonical_identity_migration_
    # 2026_07_08.md todos 2 + 5): the Kalshi<->Polymarket SAME-MARKET join key
    # (unified_api_contracts.predictions.build_cross_venue_mapping()) for a
    # matched crypto/macro/index pair, OR the Sports-asset-group-aligned
    # fixture_id for a Polymarket sports market (build_fixture_id() — see
    # polymarket/parsing.py::_build_sports_id) for Prediction rows. For CeFi/DeFi
    # rows (canonical_instrument_id_cefi_defi_backfill_2026_07_14.md): mirrors
    # instrument_key, carried through from the adapter-populated per-date row's
    # own canonical_instrument_id field (no raw-code-to-human-name translation gap
    # to solve there, unlike TradFi/Databento's separate product-root use of this
    # same InstrumentRecord field, which is NOT surfaced through this column).
    "canonical_instrument_id",
    # MVP-scope tag (mvp_scope_catalogue_tagging_2026_06_08): per-entry boolean
    # computed via the UAC ``is_mvp(...)`` predicate over the rolled-up catalogue.
    # Read by deployment-api's ``scope=mvp`` coverage denominator + the data-status
    # MVP toggle. On-the-fly at roll-up time — never a baked rule (UAC owns the rule).
    "mvp",
    # Crypto-venue equity-identity tags (operator 2026-07-16): instrument_type stays
    # the BROAD mechanics type (a single-stock perp is PERPETUAL, a tokenized stock is
    # SPOT_PAIR) — the equity identity + real-equity linkage ride these two tags so
    # downstream can find the tradfi spot leg (basis arb) without inferring from the
    # type or symbol string. Stamped on-the-fly by ``_add_equity_tags`` (UAC owns the
    # universe + link map), same pattern as ``mvp``.
    #   tracks_equity  — the Databento DBEQ.BASIC real-equity ticker the instrument
    #                    tracks (METAUSDT→META, AAPLX→AAPL), or "" for a standalone/
    #                    pre-IPO symbol (SPCX) and every non-equity row.
    #   is_equity_perp — bool: the row is a crypto-venue EQUITY instrument (a
    #                    single-stock perp OR a tokenized-share spot), i.e. its base
    #                    resolves to a CEFI_EQUITY_PERP_BASE_UNIVERSE member.
    "tracks_equity",
    "is_equity_perp",
    # Margin type: "linear" (USDT/USDC-margined) or "inverse" (coin-margined/delivery).
    # Propagated from the per-date instruments parquet ``margin_type`` column
    # (populated by the IS cefi Tardis adapter). Empty for non-derivative instruments
    # (spot pairs, DeFi pools, etc.). cefi_universe_capture_rule 2026-06-24.
    "margin_type",
    # DUAL-FORM DeFi pool ids (defi_instrument_catalogue_and_capture_pipeline_2026_06_23,
    # operator Refinement 1 — keep BOTH forms per pool): the manifest-canonical
    # ``instrument_id`` above is ``pool_address.lower()`` (for DeFi POOL rows), while
    # ``glued_pair_id`` carries the HUMAN-READABLE UI id
    # ``UNISWAP_V3-ARBITRUM:POOL:AAVE-USDC:100`` (venue-chain glued + POOL + token0-token1
    # PAIR + FEE — with-underscore protocol spelling is canonical, operator decision
    # 2026-07-08/14, ``instrument_id_format_canonicalization_2026_07_08.md`` finding 2).
    # Built via the UAC SSOT converter ``build_pool_identity`` so the two
    # forms are reversible. Blank for non-DeFi-pool rows (cefi/tradfi/sports/prediction).
    "glued_pair_id",
    # On-chain pool contract address (DeFi POOL rows) — the canonical-id source +
    # the address the bidirectional converter needs to re-resolve from a glued-pair.
    # Blank for non-pool rows.
    "pool_address",
    # On-chain token contract addresses (DeFi rows) — projected from the per-date
    # InstrumentRecord (UAC internal.reference.instrument: base_asset_contract_address,
    # quote_asset_contract_address, atoken_address, debt_token_address). Surfaced so the
    # data-status UI can show + copy the contract address per token leg, and so the
    # SPOT_ASSET population (plan data_status_page_ux_and_canonicalisation_2026_07_16 P4-B)
    # can derive one SPOT_ASSET per unique (chain, token → contract_address) without a
    # re-fetch (the addresses already exist in the source rows). Blank for CeFi/tradfi/
    # sports/prediction rows + DeFi rows the adapter left unset.
    "base_asset_contract_address",
    "quote_asset_contract_address",
    "atoken_address",
    "debt_token_address",
    # Sports FIXTURE scheduling/display fields (operator round-3, 2026-07-17 —
    # "all the fixtures broken down by searching by date, league and/or team").
    # The ``entity=fixtures`` by_date snapshots that
    # :func:`build_sports_fixture_team_player_catalogue` ALREADY walks carry every
    # one of these (``timestamp`` / ``status_short`` / ``af_home_name`` /
    # ``af_away_name`` / ``venue_name`` / ``round``) — they were previously
    # DISCARDED, the roll-up keeping only the first/last-day lifecycle. That gap is
    # exactly why deployment-api's fixtures browser had to re-walk the per-day
    # parquets over a <=120-day window just to render a kickoff time. Carrying them
    # here makes the ONE rolled-up catalogue object a COMPLETE fixtures source over
    # the whole :data:`SPORTS_FTP_WINDOW_DAYS` window — no extra walk, no extra
    # read, the data was already in hand. Blank/None on every non-sports-fixture row.
    #
    # NOTE ``venue_name`` is the physical STADIUM (from the fixture snapshot), which
    # is a different axis from the ``venue`` column above (the bookmaker/exchange
    # association — honestly blank for sports reference rows, see SPORTS_INSTRUMENTS.md's
    # "``venue`` vs ``source``" note). The two never carry the same thing.
    #
    # Sourced from the LATEST-observed snapshot per fixture, because ``status``
    # legitimately evolves across days (NS -> 1H -> FT) — the newest observation is
    # the honest one (see ``_fixture_attr_day`` in the roll-up).
    #
    # MEASURED FILL (real GCS, 2026-07-17): kickoff_utc/status/home_team_name/
    # away_team_name = 76/76 of a sampled window; venue_name = 65/76 (the source
    # leaves it None for some fixtures — honest blank); ``round`` = **0/62 across
    # every sampled day AND every major league** (LA_LIGA / BUNDESLIGA /
    # ENG_CHAMPIONSHIP / DANISH_SUPERLIGA ...). The column is retained because it
    # IS a legitimate fixture field the upstream API supplies — the blank is an
    # instruments-service WRITER gap (it never stamps ``round`` into the
    # ``entity=fixtures`` snapshot), NOT a roll-up bug, and it is exactly why the
    # fixtures browser's ``round`` has always rendered blank (same source). Tracked
    # as a todo on data_status_page_ux_and_canonicalisation_2026_07_16 (P10). If
    # that writer is fixed, this column populates with no schema change.
    "kickoff_utc",
    "status",
    "home_team_name",
    "away_team_name",
    "venue_name",
    "round",
)

#: Per-date parquet columns holding the instrument identifier (first match wins).
_ID_COLUMNS: tuple[str, ...] = ("instrument_key", "instrument_id")

#: Concurrency for the by_date download (I/O-bound — MAX_WORKERS=16 per the coding standards).
MAX_DOWNLOAD_WORKERS = 16

#: Extract the ISO ``day=`` partition from a ``by_date`` blob path.
_DAY_RE = re.compile(r"(?:^|/)day=(\d{4}-\d{2}-\d{2})(?:/|$)")

#: Extract the ``entity=`` partition from a sports ``by_date`` blob path.
_ENTITY_RE = re.compile(r"(?:^|/)entity=([^/]+)(?:/|$)")

#: Extract the ``league=`` partition from a sports ``by_date`` blob path.
#: fixtures/teams/injuries are written per-league; absent on the rare legacy
#: unmapped-fallback blobs (callers treat a missing match as ``""``).
_LEAGUE_RE = re.compile(r"(?:^|/)league=([^/]+)(?:/|$)")

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


def _parse_truth_datetime(raw: str | None) -> datetime | None:
    """Parse a venue-truth lifecycle timestamp (``_PredLifecycle.settled``) → a
    UTC :class:`datetime`, or ``None`` for blank/unparseable values.

    Used to build the synthetic :class:`InstrumentRecord` views the cross-venue
    matcher (:func:`build_cross_venue_mapping`) needs for its
    ``expiry``-keyed settlement-bucket join (see ``build_prediction_catalogue_dataframe``).
    """
    if not raw:
        return None
    try:
        ts = pd.Timestamp(raw)
    except (ValueError, TypeError):
        return None
    if pd.isna(ts):
        return None
    py_dt = ts.to_pydatetime()
    return py_dt.replace(tzinfo=UTC) if py_dt.tzinfo is None else py_dt.astimezone(UTC)


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


#: Legacy lowercase → canonical UAC ``InstrumentType`` alias map applied by
#: :func:`_canonicalize_instrument_type` when a catalogue row's ``instrument_type``
#: is stamped from the raw per-date source value. Mirrors the documented legacy
#: mapping in ``InstrumentType``'s own docstring (UAC
#: ``unified_api_contracts._instrument_enums``) plus the additional lowercase
#: spellings measured live in production (2026-07-18: COINBASE-SPOT/BYBIT by_date
#: rows carry ``spot``/``spot_pair``/``perpetual``/``futures_chain`` alongside the
#: canonical uppercase form — the manifest-writer alias guard at
#: ``instruments_service.engine.orchestrator.writers._LEGACY_INSTRUMENT_TYPE_ALIASES``
#: only intercepts ``perpetual``/``spot`` going forward, so historical + still-live
#: variants outside that pair reach the roll-up unmapped). Keys are lowercased
#: before lookup.
_INSTRUMENT_TYPE_LEGACY_ALIASES: dict[str, str] = {
    "spot": InstrumentType.SPOT_PAIR.value,
    "spot_pair": InstrumentType.SPOT_PAIR.value,
    "perp": InstrumentType.PERPETUAL.value,
    "perpetual": InstrumentType.PERPETUAL.value,
    "futures": InstrumentType.FUTURE.value,
    "futures_chain": InstrumentType.FUTURE.value,
    "future": InstrumentType.FUTURE.value,
    "option": InstrumentType.OPTION.value,
    "pool": InstrumentType.POOL.value,
    "lending_market": InstrumentType.LENDING.value,
    "lending": InstrumentType.LENDING.value,
    "lst": InstrumentType.LST.value,
    "yield": InstrumentType.YIELD_BEARING.value,
    "etf": InstrumentType.ETF.value,
}

#: Honest-absence sentinel for a catalogue row whose source ``instrument_type`` is
#: blank, missing, or the literal string ``"None"`` (a stray ``str(None)`` stamp
#: from an upstream writer bug — measured live in production 2026-07-18).
#: Deliberately NOT a member of :class:`InstrumentType` (the closed contract-
#: mechanics vocabulary this roll-up otherwise canonicalises onto) — it is a
#: roll-up-only marker meaning "no real type was ever fabricated here", mirroring
#: the existing :data:`SPORTS_LEAGUE_ID_SENTINELS` "UNKNOWN" convention above.
INSTRUMENT_TYPE_UNKNOWN = "UNKNOWN"


def _canonicalize_instrument_type(raw: str | None) -> str:
    """Canonicalise a raw per-date ``instrument_type`` value to the UAC vocabulary.

    Deterministic + pure — applied at catalogue-row EMISSION time only (never
    mutates ``agg.meta``, so every internal consumer that reads the raw meta value
    — :func:`_canonicalize_cefi_future_id`, :func:`_canonicalize_cefi_perp_id`,
    the perp-family checks — is unaffected; they already defensively
    ``.strip().upper()`` at their own read sites):

      1. blank / ``None`` / the literal string ``"None"`` → the single honest
         :data:`INSTRUMENT_TYPE_UNKNOWN` sentinel (never fabricated, never the
         literal ``"None"``);
      2. a known legacy lowercase spelling (:data:`_INSTRUMENT_TYPE_LEGACY_ALIASES`,
         case-insensitive) → its canonical UAC ``InstrumentType`` value;
      3. anything else → uppercased unchanged (round-trips an already-canonical
         value; preserves a genuinely new/unrecognised venue-side type for triage
         instead of silently swallowing it into UNKNOWN).

    SSOT: ``unified_api_contracts._instrument_enums.InstrumentType`` docstring
    (legacy-value mapping) + measured live COINBASE-SPOT/BYBIT by_date rows
    (2026-07-18): COINBASE-SPOT carried ``['', 'SPOT_PAIR', 'spot', 'spot_pair']``,
    BYBIT carried ``['', 'FUTURE', 'PERPETUAL', 'SPOT_PAIR', 'futures_chain',
    'perpetual']``, and the literal string ``'None'`` also appears.
    """
    text = (raw or "").strip()
    if not text or text == "None":
        return INSTRUMENT_TYPE_UNKNOWN
    lowered = text.lower()
    aliased = _INSTRUMENT_TYPE_LEGACY_ALIASES.get(lowered)
    if aliased is not None:
        return aliased
    return text.upper()


#: DeFi pool instrument_type values (lowercased) the dual-form id applies to.
#: A pool's canonical manifest atom is ``pool_address.lower()``; the glued-pair
#: id is the human-readable UI form. Other DeFi instrument_types (lending / lst /
#: spot_asset / perpetual) key on a contract address too but are not pools.
_DEFI_POOL_ITYPES: frozenset[str] = frozenset({"pool"})

#: CeFi perpetual-family instrument_type values (UPPERCASE) that share ONE
#: canonical (venue, raw_symbol, margin) lifecycle across the 2026-07 id-convention
#: churn. A single live perp appeared under up to THREE id strings as the format
#: canonicalised (``VENUE:PERP:BTC`` → ``VENUE:PERPETUAL:BTC-USD`` →
#: ``VENUE:PERPETUAL:BTC-USD@LIN``; instrument_id_format_canonicalization_2026_07_08),
#: but the ``instrument_type`` COLUMN (PERPETUAL), ``raw_symbol`` (venue-native, e.g.
#: BTC / BTCUSDT), ``venue`` and ``margin_type`` are STABLE across all three forms —
#: so the lineage key below collapses them to ONE row keyed on the underlying, not
#: the churning id string (the HYPERLIQUID/ASTER ~176-of-534 stale-dup class).
#: Crypto-venue equity perps are typed PERPETUAL (operator 2026-07-16 — no distinct
#: EQUITY_PERP/TOKENIZED_EQUITY type), so they already ride this family via PERPETUAL;
#: tokenized-share spots stay SPOT_PAIR and key on the id string like any spot pair.
#: SSOT: codex/04-architecture/
#: instrument-universe-registry-consolidation.md + the plan cited in run_rollup.
_PERP_FAMILY_ITYPES: frozenset[str] = frozenset({"PERPETUAL"})


def _cefi_perp_lineage_key(instrument_id: str, instrument_type: str, raw_symbol: str, margin_type: str) -> str | None:
    """Semantic collapse key for a CeFi perp-family row, or ``None`` when N/A.

    Collapses the ``VENUE:PERP:BTC`` → ``VENUE:PERPETUAL:BTC-USD`` →
    ``VENUE:PERPETUAL:BTC-USD@LIN`` convention chain onto ONE lineage keyed on the
    UNDERLYING ``(venue, raw_symbol, margin)`` — matching the operator spec
    "(venue, base, instrument_type, quote, margin)" where ``raw_symbol`` is the
    venue-native encoding of base+quote (BTCUSDT vs BTCUSDC keep distinct keys, so
    genuinely-different perps never over-collapse; verified 0 live-instrument
    collisions across the full prod cefi catalogue). The venue prefix is read from
    the STABLE first segment of ``instrument_id`` (not the ``venue`` FIELD, which
    can carry era-specific spelling drift — the DERIBIT-COMBO ghost lesson).

    Returns ``None`` (caller falls back to the id-string key = current behaviour)
    when the row is not a perp-family type, or ``raw_symbol`` is blank (no
    venue-native symbol to key on → cannot safely collapse). Pure + idempotent.
    """
    if (instrument_type or "").strip().upper() not in _PERP_FAMILY_ITYPES:
        return None
    rs = (raw_symbol or "").strip().upper()
    if not rs:
        return None
    venue_prefix = str(instrument_id).split(":", 1)[0].strip().upper()
    if not venue_prefix:
        return None
    marker = (margin_type or "").strip().upper()
    return f"cefiperp::{venue_prefix}::{rs}::{marker}"


def _cefi_equity_tags(instrument_type: str, base_asset: str) -> tuple[bool, str]:
    """Return the ``(is_equity_perp, tracks_equity)`` catalogue tags for a CeFi row.

    Operator decision 2026-07-16 (broad instrument_type + equity tags, superseding
    the cefi_completion_program_2026_07_15 EQUITY_PERP/TOKENIZED_EQUITY *type*
    refinement): ``instrument_type`` stays the BROAD contract-mechanics type — a
    crypto-venue single-stock perp is ``PERPETUAL`` and a tokenized stock is
    ``SPOT_PAIR``. The equity identity + real-equity linkage instead ride two
    dedicated catalogue tags so the system can still find the tradfi spot leg
    (basis arb) / cross-venue dispersion without inferring it from the type or the
    symbol string:

      * ``is_equity_perp`` — ``True`` iff the row is a crypto-venue EQUITY
        instrument (a single-stock perp OR a tokenized-share spot), i.e. its base
        resolves to a ``CEFI_EQUITY_PERP_BASE_UNIVERSE`` member. This is the durable
        "it's an equity instrument" flag (the operator's label; it deliberately
        covers BOTH the perp form and the tokenized-spot form).
      * ``tracks_equity`` — the Databento ``DBEQ.BASIC`` real-equity ticker the
        instrument tracks (``crypto_equity_link.tracks_equity`` — METAUSDT→META,
        AAPLX→AAPL), or ``""`` for a standalone/pre-IPO symbol with no real-equity
        twin (e.g. SPCX) and for every non-equity row.

    Discrimination MIRRORS the pre-2026-07-16 ``_refine_cefi_instrument_type`` so the
    exact same rows are flagged (the ~715 crypto-venue equity instruments):

      * ``PERPETUAL`` whose base ∈ ``CEFI_EQUITY_PERP_BASE_UNIVERSE`` → equity perp
        (equities + commodity RAW forms XAU/XAG/… + index/ETF SPX/SPY/…). tracks
        ``tracks_equity(base)`` (``""`` for standalone SPCX/OPENAI/ANTHROPIC etc.).
      * ``SPOT_PAIR`` tokenized-share form (base ``<TICKER>X`` where ``TICKER`` ∈
        the equity universe and the base is NOT itself a crypto base) → equity
        (Bybit ``AAPLX`` → strip ``X`` → ``AAPL`` ∈ universe). tracks
        ``tracks_equity(TICKER)``.

    Any other row → ``(False, "")``. Pure + idempotent. instrument_type is NEVER
    changed (the row keeps its broad mechanics type + stable id / lineage key).
    """
    itype = (instrument_type or "").strip().upper()
    base = (base_asset or "").strip().upper()
    if not base:
        return (False, "")
    if itype == "PERPETUAL" and base in CEFI_EQUITY_PERP_BASE_UNIVERSE:
        return (True, tracks_equity(base) or "")
    if (
        itype == "SPOT_PAIR"
        and len(base) > 1
        and base.endswith("X")
        and base not in CEFI_BASE_ASSET_UNIVERSE
        and base[:-1] in CEFI_EQUITY_PERP_BASE_UNIVERSE
    ):
        return (True, tracks_equity(base[:-1]) or "")
    return (False, "")


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


#: Margin-type word (as carried in the by_date snapshot's ``margin_type`` column) →
#: the operator-decided ``@LIN``/``@INV`` marker token
#: (instrument_id_format_canonicalization_2026_07_08.md finding 1). Shared by BOTH
#: roll-up id rebuilds (:func:`_canonicalize_cefi_future_id` dated-FUTURE +
#: :func:`_canonicalize_cefi_perp_id` legacy-PERP) — one mapping, one spelling.
_CEFI_MARGIN_MARKER: dict[str, str] = {"linear": "LIN", "inverse": "INV"}


def _canonicalize_cefi_future_id(instrument_id: str, meta: dict[str, str | None]) -> str:
    """Rebuild a legacy raw-wire-form CeFi dated-FUTURE ``instrument_id`` to canonical shape.

    Some ``by_date`` snapshot rows (captured before the Tardis reference-data adapter's
    2026-07-09 canonicalization fix) still carry the raw wire-form id the adapter used to
    stamp, e.g. ``BINANCE-FUTURES:FUTURE:ETHUSDT_260626`` (a ``VENUE:TYPE:`` prefix glued onto
    the venue's raw concatenated-symbol+YYMMDD body), instead of the dash-canonical
    ``VENUE:FUTURE:BASE-QUOTE@MARKER-YYYYMMDD`` every other dated-futures venue in the SAME
    roll-up produces (KRAKEN-FUTURES/BYBIT). The parquet FILE content and the current adapter
    are already correct for a fresh capture — this roll-up was the one place still passing a
    legacy row's raw id straight through
    (cefi_mtds_writer_raw_symbol_vs_canonical_eu_namespace_mismatch_2026_07_15.md). Rebuilds via
    the SAME shared UAC builder (:func:`build_instrument_id`) the adapter itself uses, so a
    fresh capture and a legacy-snapshot roll-up always agree on the identical canonical shape —
    mirrors the ``_cefi_perp_lineage_key``/``_canonical_instrument_id`` precedent above for
    other id-convention chains.

    Returns ``instrument_id`` unchanged when it is not a CeFi FUTURE row, already carries the
    ``@`` marker (idempotent no-op — covers Kraken/Bybit/Deribit already-canonical rows), or a
    field the rebuild needs (base_asset/quote_asset/margin_type/expiry) is missing — degrade,
    never guess. Pure + idempotent.
    """
    if (meta.get("instrument_type") or "").strip().upper() != "FUTURE":
        return instrument_id
    if "@" in instrument_id:
        return instrument_id
    venue = (meta.get("venue") or "").strip()
    base = (meta.get("base_asset") or "").strip()
    quote = (meta.get("quote_asset") or "").strip()
    marker = _CEFI_MARGIN_MARKER.get((meta.get("margin_type") or "").strip().lower())
    expiry = _parse_truth_date(meta.get("expiry"))
    if not venue or not base or not quote or marker is None or expiry is None:
        return instrument_id
    return build_instrument_id(
        venue, InstrumentType.FUTURE, f"{base}-{quote}", expiry_date=expiry, margin_marker=marker
    )


#: The LEGACY CeFi perp-family type token: the pre-2026-07 on-chain adapters stamped
#: ``VENUE:PERP:<raw_symbol>`` where the canonical form is
#: ``VENUE:PERPETUAL:BASE-QUOTE@MARKER``. Used ONLY as the rebuild TRIGGER — a row whose
#: id already carries the canonical ``PERPETUAL`` token is left alone (including the 586
#: marker-less ``VENUE:PERPETUAL:BASE-QUOTE`` rows, which are a SEPARATE catalogue-
#: completeness concern tracked as blueprint open-q #19 — deliberately NOT in scope here).
_LEGACY_CEFI_PERP_TYPE_TOKEN = "PERP"


def _canonicalize_cefi_perp_id(instrument_id: str, meta: dict[str, str | None]) -> str:
    """Rebuild a legacy ``VENUE:PERP:<raw>``-form CeFi perp ``instrument_id`` to canonical shape.

    The on-chain perp venues (HYPERLIQUID / ASTER / EXTENDED-STARKNET / LIGHTER-ZKSYNC)
    stamped ``VENUE:PERP:<raw_symbol>`` until the adapter's id-format canonicalization
    (``instrument_id_format_canonicalization_2026_07_08``) switched them to the canonical
    ``VENUE:PERPETUAL:BASE-QUOTE@MARKER``. A perp that was still LIVE after the switch
    collapses onto its canonical form via :func:`_cefi_perp_lineage_key` (the lineage key
    is stable across the churn), so the roll-up emits the canonical id. But a perp
    DELISTED *before* the switch has no post-fix snapshot at all — its most-recent
    ``by_date`` row carries the legacy id, and the roll-up passed that raw id straight
    through, so the catalogue kept 9 stale ``:PERP:`` ids (HYPERLIQUID ARK/DOOD/FTT/MATIC/IP,
    ASTER IPUSDT, EXTENDED-STARKNET IP-USD/TON-USD, LIGHTER-ZKSYNC IP — all with
    ``available_to`` set, i.e. delisted). This is the SAME legacy-snapshot class the dated-
    FUTURE fix (:func:`_canonicalize_cefi_future_id`, instruments-service@79d4dbcb) closed,
    so it is fixed the SAME way and in the same place: at roll-up time, via the SAME shared
    UAC builder (:func:`build_instrument_id`) the adapter itself uses — a fresh capture and a
    legacy-snapshot roll-up therefore always agree on the identical canonical shape.

    The legacy rows carry every field the rebuild needs (verified against the live
    ``by_date`` corpus 2026-07-17: ``base_asset`` + ``quote_asset`` USD/USDT/USDC +
    ``margin_type=linear`` are all populated), and the rebuilt ids byte-match their live
    canonical siblings (``HYPERLIQUID:PERPETUAL:ARK-USD@LIN``, ``ASTER:PERPETUAL:IP-USDT@LIN``,
    ``EXTENDED-STARKNET:PERPETUAL:IP-USD@LIN``, ``LIGHTER-ZKSYNC:PERPETUAL:IP-USDC@LIN``).

    Returns ``instrument_id`` unchanged when the row is not a perp-family type, the id does
    NOT carry the legacy ``PERP`` token (idempotent no-op — every already-canonical
    ``PERPETUAL`` row, marker or not), or a field the rebuild needs
    (venue/base_asset/quote_asset/margin_type) is missing — degrade, never guess. Pure +
    idempotent.
    """
    if (meta.get("instrument_type") or "").strip().upper() not in _PERP_FAMILY_ITYPES:
        return instrument_id
    parts = instrument_id.split(":")
    if len(parts) < 3 or parts[1].strip().upper() != _LEGACY_CEFI_PERP_TYPE_TOKEN:
        return instrument_id
    venue = (meta.get("venue") or "").strip()
    base = (meta.get("base_asset") or "").strip()
    quote = (meta.get("quote_asset") or "").strip()
    marker = _CEFI_MARGIN_MARKER.get((meta.get("margin_type") or "").strip().lower())
    if not venue or not base or not quote or marker is None:
        return instrument_id
    return build_instrument_id(venue, InstrumentType.PERPETUAL, f"{base}-{quote}", margin_marker=marker)


def _canonicalize_cefi_rollup_id(instrument_id: str, meta: dict[str, str | None]) -> str:
    """Apply EVERY roll-up CeFi id-canonicalization step to ``instrument_id``, in order.

    The SINGLE source of the roll-up's id-rebuild chain, so the emitted ``instrument_id``
    and its ``canonical_instrument_id`` mirror can never drift apart (the 511-row
    ``instrument_id != canonical_instrument_id`` defect was exactly that drift: the
    dated-FUTURE rebuild ran on the emitted id only, leaving the mirror on the stale
    raw-glued form). Every no-op guard lives in the individual helpers, so this is a
    no-op for non-CeFi / already-canonical rows. Pure + idempotent.
    """
    if not instrument_id:
        return instrument_id
    return _canonicalize_cefi_perp_id(_canonicalize_cefi_future_id(instrument_id, meta), meta)


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

    CeFi perp-family rows (PERPETUAL — incl. crypto-venue equity perps, operator
    2026-07-16 no distinct EQUITY_PERP type) key on the
    canonical ``(venue, raw_symbol, margin)`` lineage (:func:`_cefi_perp_lineage_key`)
    so the ``VENUE:PERP:BTC`` → ``VENUE:PERPETUAL:BTC-USD`` → ``…@LIN`` id-convention
    chain collapses to ONE continuous lifecycle instead of THREE stale-dup listings
    (the HYPERLIQUID/ASTER ~176-of-534 churn duplicates). ``raw_symbol``-blank rows
    have no venue-native key so they fall through to the id-string behaviour.

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
    # CeFi perp-family: collapse the id-convention rename chain onto the underlying.
    perp_key = _cefi_perp_lineage_key(
        instrument_id,
        str(row.get("instrument_type") or ""),
        str(row.get("raw_symbol") or ""),
        str(row.get("margin_type") or ""),
    )
    if perp_key is not None:
        return perp_key
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

#: Venues dropped from the URDI adapter registry (operator ruling) — a live capture
#: for one of these can no longer happen, but stale historical ``by_date`` rows
#: persist in GCS and must not re-mint a catalogue row on every regen. Both the bare
#: and the ``-SOLANA``-suffixed spelling are excluded (whichever form a given row's
#: raw ``venue`` column happens to carry — checked upper-cased, exact match).
#: SSOT: ``unified_api_contracts.registry.venue_adapter_keys`` — DRIFT/PACIFICA
#: removed 2026-07-16, MANGO-SOLANA/ZETA-SOLANA/FLASH-SOLANA removed 2026-07-15
#: (all Solana perp DEXes except Jupiter, which is not integrated; operator ruling).
#: DRIFT alone carried 3,556 stale rows in the IS defi availability index as of
#: 2026-07-18 (0 in MTDS — never had live market-data capture).
_REMOVED_VENUES: frozenset[str] = frozenset(
    {
        "DRIFT",
        "DRIFT-SOLANA",
        "PACIFICA",
        "PACIFICA-SOLANA",
        "MANGO-SOLANA",
        "MANGO",
        "ZETA-SOLANA",
        "ZETA",
        "FLASH-SOLANA",
        "FLASH",
    }
)

#: Bare-vs-chain duplicate venue spellings collapsed to ONE canonical combined form —
#: same protocol, two venue-column spellings measured live in the IS defi
#: availability index (2026-07-18): JITO / JITO-SOLANA, RAYDIUM / RAYDIUM-SOLANA,
#: MARINADE / MARINADE-SOLANA. Deliberately narrow (exact match, these 3 known pairs
#: only) — NOT a blanket bare-venue-implies-Solana rule. Canonical form = the
#: ``-SOLANA``-suffixed combined spelling: the registered ``venue_prefix`` convention
#: in ``unified_api_contracts.registry.venue_adapter_keys`` only carries the suffixed
#: form (``JITO-SOLANA`` / ``RAYDIUM-SOLANA`` / ``MARINADE-SOLANA``) — the bare
#: spellings are NOT registered adapter keys at all.
_DUPLICATE_VENUE_ALIASES: dict[str, str] = {
    "JITO": "JITO-SOLANA",
    "RAYDIUM": "RAYDIUM-SOLANA",
    "MARINADE": "MARINADE-SOLANA",
}


def _is_removed_venue(venue: str) -> bool:
    """True when ``venue`` (any case) is a registry-removed venue (bare or suffixed).

    See :data:`_REMOVED_VENUES`. Pure; blank input is never "removed" (the caller
    already treats blank venue as its own no-op path).
    """
    return venue.strip().upper() in _REMOVED_VENUES


def _canonical_bare_venue_chain(venue: str, chain: str) -> tuple[str, str]:
    """Map any DeFi venue drift form ``(venue, chain)`` → canonical ``(bare_protocol, chain)``.

    1. On-chain CeFi perp CLOBs (LIGHTER-ZKSYNC / EXTENDED-STARKNET -- PACIFICA (Solana)
       was a third example here until removed 2026-07-16, operator ruling: all Solana
       perp DEXes dropped except Jupiter, not integrated) are GLUED ``VENUE-CHAIN``
       strings whose suffix IS a KNOWN_CHAIN, but they are
       CeFi venues (``VENUE_TO_ASSET_GROUP == "cefi"``, like HYPERLIQUID/ASTER) — NOT
       DeFi pools. Applying the DeFi split desynchronises them from the by_date PATH
       (``venue=LIGHTER-ZKSYNC``), the IS manifest writer (``writers._canonical_manifest_venue_chain``
       @ instruments-service@24c0dd5 keeps them glued) and the ``instrument_key``
       prefix (``LIGHTER-ZKSYNC:PERP:...``). Detect via UAC ``VENUE_TO_ASSET_GROUP``
       and return the full cefi venue with the incoming chain preserved. Ref:
       ``plans/active/instruments_foundation_completeness_2026_06_24.md`` §G1.3 follow-up.
    2. ghost-normalise the full venue (``AAVEV3-ARBITRUM`` → ``AAVE_V3-ARBITRUM``) via the
       UAC authority ``canonicalize_defi_venue_combined`` (no-op for bare/non-DeFi venues).
    3. if the result is glued (ends ``-<KNOWN_CHAIN>``), split → bare protocol + that chain.
    4. else keep the venue; preserve the existing chain column (upper).
    Pure + idempotent. A non-DeFi or already-bare canonical venue passes through unchanged.
    """
    from unified_api_contracts.registry.capability_declarations._defi import canonicalize_defi_venue_combined

    v = str(venue).strip()
    c = str(chain).strip().upper()
    if not v:
        return v, c
    # Bare-vs-chain duplicate venue collapse (JITO/RAYDIUM/MARINADE — see
    # _DUPLICATE_VENUE_ALIASES) BEFORE the cefi/ghost-normalisation checks below, so
    # a bare-spelling row with no separate chain column still resolves to the SAME
    # (bare_protocol, chain) pair as its ``-SOLANA``-suffixed sibling.
    v = _DUPLICATE_VENUE_ALIASES.get(v.upper(), v)
    if VENUE_TO_ASSET_GROUP.get(v) == "cefi":
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

        # Collapse the bare-vs-chain duplicate spelling BEFORE ghost-normalisation
        # (see _DUPLICATE_VENUE_ALIASES) so JITO/JITO-SOLANA etc. share ONE liveness
        # window key — otherwise the two spellings' §7.3 last-full-day anchors track
        # independently even though _canonical_bare_venue_chain later emits the same
        # (bare_protocol, chain) pair for both.
        _aliased = _DUPLICATE_VENUE_ALIASES.get(raw.upper(), raw)
        canonical = canonicalize_defi_venue_combined(_aliased)
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
            if _venue and _is_removed_venue(_venue):
                # Registry-removed venue (operator ruling) — stale historical rows
                # must not re-mint a catalogue row on every regen. See
                # _REMOVED_VENUES.
                continue
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
            _is_perp_family = str(_meta.get("instrument_type") or "").strip().upper() in _PERP_FAMILY_ITYPES
            if not _is_perp_family:
                # BUG #4 (B): keep the EARLIEST declared listing date seen across snapshots.
                if declared is not None and (existing.declared_from is None or declared < existing.declared_from):
                    existing.declared_from = declared
            elif existing.declared_from is None and declared is not None:
                # Perp-family with no declared date yet — seed it until the winning
                # (most-recent) form arrives and takes over below.
                existing.declared_from = declared
            # Metadata follows the most-recent definition of the instrument.
            if day >= existing.meta_day:
                existing.meta_day = day
                existing.meta = _meta
                # §7.3: venue-truth lifecycle dates follow the most-recent snapshot
                # (the freshest exchange-declared expiry / delisting for this id).
                existing.expiry = _expiry
                existing.delisted_at = _delisted
                if _is_perp_family and declared is not None:
                    # CeFi perp-family lineage collapse (HYPERLIQUID/ASTER 2026-07 id
                    # convention churn): declared_from follows the WINNING (most-recent)
                    # form so a dead old-convention form's spurious genesis date (the
                    # ASTER ``PERP:*USDT`` uniform venue-launch 2023-07-22, which
                    # PREDATES the tokens it is stamped on) does NOT drag the collapsed
                    # lifecycle's available_from below the live form's true per-instrument
                    # listing date. None-guarded (the elif above seeds an earlier date)
                    # so a fresh form lacking a declared date never wipes a known one.
                    existing.declared_from = declared

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
        # Legacy raw-wire-form CeFi ids captured before the adapter's canonicalization
        # fixes — rebuild to the canonical shape at roll-up time: dated-FUTURE
        # (BINANCE-FUTURES et al, 2026-07-09 fix) + legacy ``:PERP:``-token on-chain perps
        # (HYPERLIQUID/ASTER/EXTENDED-STARKNET/LIGHTER-ZKSYNC delisted before the
        # 2026-07-08 fix). No-op for already-canonical / non-CeFi rows. See
        # _canonicalize_cefi_rollup_id's docstring.
        canonical_id = _canonicalize_cefi_rollup_id(canonical_id, agg.meta)
        # instrument_type stays the BROAD contract-mechanics type (operator
        # 2026-07-16, superseding the cefi_completion_program_2026_07_15
        # EQUITY_PERP/TOKENIZED_EQUITY *type* refinement): a crypto-venue single-stock
        # perp stays PERPETUAL, a tokenized stock stays SPOT_PAIR. The equity identity
        # + real-equity linkage ride the ``is_equity_perp`` / ``tracks_equity`` tags,
        # stamped by ``_add_equity_tags`` on the finalized frame (see CATALOG_COLUMNS).
        rows.append(
            {
                "instrument_id": canonical_id,
                # Canonicalised at EMISSION only (see _canonicalize_instrument_type's
                # docstring) — agg.meta itself stays raw so _canonicalize_cefi_rollup_id
                # above (called with agg.meta, not this row dict) keeps its existing
                # behaviour byte-for-byte.
                "instrument_type": _canonicalize_instrument_type(agg.meta["instrument_type"]),
                "venue": bare_venue,
                "chain": chain,
                "league_id": agg.meta["league_id"] or "",
                "available_from": available_from.isoformat(),
                "available_to": available_to,
                # A2: the CLEAN venue-declared expiry, separate from the overloaded
                # available_to above (which may be a delisting/last-observed here).
                # Honest-NULL when the venue declared no expiry (perps, spot, or a
                # dated contract whose expiry we never captured from source).
                "expiry": agg.expiry.isoformat() if agg.expiry is not None else None,
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
                # CeFi/DeFi canonical_instrument_id — the adapter-populated per-date
                # value when present, else instrument_key itself (already carried
                # through in agg.meta for every row). Both are the IDENTICAL value
                # by design (canonical_instrument_id_cefi_defi_backfill_2026_07_14.md
                # — CeFi/DeFi have no raw-code-to-human-name translation gap, so
                # canonical_instrument_id always mirrors instrument_key), so this
                # fallback backfills every historical row captured BEFORE the
                # adapter fix shipped with the exact value a fresh capture would
                # have produced — no separate migration script needed.
                #
                # The mirror is run through the SAME ``_canonicalize_cefi_rollup_id``
                # chain as the emitted ``instrument_id`` above. Without this the two
                # DRIFT on exactly the rows the chain rebuilds: the 2026-07-17 live
                # catalogue carried 511 cefi rows (BINANCE-DELIVERY/OKX-FUTURES/
                # COINBASE-CDE/BINANCE-FUTURES/DERIBIT dated FUTUREs) whose
                # ``instrument_id`` was the CORRECT canonical form while this mirror
                # still held the stale raw-glued one
                # (``…:FUTURE:ADA-USD@INV-20200926`` vs ``…:FUTURE:ADAUSD_200925``) —
                # the roll-up rebuilt the id but never re-applied the mirror.
                # Canonicalising the SOURCE (rather than blanket-copying the emitted
                # ``canonical_id``) is deliberate: it keeps the DeFi POOL contract
                # intact, where ``instrument_id`` is re-keyed to ``pool_address`` but
                # ``canonical_instrument_id`` mirrors the glued ``instrument_key``
                # (test_rollup_defi_pool_row_backfills_canonical_instrument_id_from_
                # instrument_key), and preserves honest-blank for an id-less row.
                "canonical_instrument_id": _canonicalize_cefi_rollup_id(
                    agg.meta.get("canonical_instrument_id") or agg.meta.get("instrument_key") or "", agg.meta
                ),
                "glued_pair_id": glued_pair_id,
                "pool_address": pool_address,
                # On-chain token contract addresses (DeFi) — projected from the source
                # row without a re-fetch (P4-B). Blank for non-DeFi + unset DeFi rows.
                "base_asset_contract_address": agg.meta.get("base_asset_contract_address") or "",
                "quote_asset_contract_address": agg.meta.get("quote_asset_contract_address") or "",
                "atoken_address": agg.meta.get("atoken_address") or "",
                "debt_token_address": agg.meta.get("debt_token_address") or "",
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
        # On-chain token contract addresses (DeFi) — carried through so the catalogue
        # row can surface them without a re-fetch (P4-B). Blank when the adapter/source
        # row didn't populate them (non-DeFi rows, or DeFi rows pre the address backfill).
        "base_asset_contract_address": _str_field(row, "base_asset_contract_address"),
        "quote_asset_contract_address": _str_field(row, "quote_asset_contract_address"),
        "atoken_address": _str_field(row, "atoken_address"),
        "debt_token_address": _str_field(row, "debt_token_address"),
        "pool_fee_tier": _opt_field(row, "pool_fee_tier"),
        # The original glued ``instrument_key`` — carried so ``_defi_pool_dual_form``
        # can recover the faithful raw fee token for the human-readable glued_pair_id
        # even though the lifecycle is keyed by the canonical pool identity.
        "instrument_key": _str_field(row, "instrument_key"),
        # The row's RESOLVED id (instrument_key OR instrument_id) — the non-pool
        # pass-through canonical id (a catalogue keyed only on instrument_id keeps it).
        "_resolved_id": _row_id(row) or "",
        # Adapter-populated canonical_instrument_id (CeFi/DeFi: mirrors instrument_key
        # — no raw-code-to-human-name translation gap to solve, unlike TradFi/Databento's
        # product-root use of this field). Carried through so the catalogue row below
        # doesn't have to re-derive it. SSOT: unified-trading-pm/plans/active/
        # canonical_instrument_id_cefi_defi_backfill_2026_07_14.md.
        "canonical_instrument_id": _str_field(row, "canonical_instrument_id"),
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
    #: Venue-native fields carried straight through from the per-date InstrumentRecord
    #: (Kalshi ``event_ticker`` / Polymarket ``slug`` for ``raw_symbol``; Kalshi
    #: ``series_ticker`` / Polymarket's synthesized category label for ``base_asset`` —
    #: see ``instruments_service.reference_data.adapters.prediction.*``). BUG FIX
    #: 2026-07-08: these were captured on every per-date row but never threaded through
    #: this conditionId-grain accumulator, so ``_emit`` always wrote them blank and the
    #: real, adapter-populated values never survived into ``prod/catalog.parquet`` (100%
    #: NULL across 2.49M rows). Only meaningful at the per-conditionId (CID) grain — a
    #: cqg spans many conditionIds with different raw_symbol/base_asset, so the cqg-grain
    #: accumulator intentionally leaves these at their "" default (honest absence, not a
    #: per-market value). See ``docs/PREDICTION_INSTRUMENTS.md`` § "Canonical identity model".
    raw_symbol: str = ""
    base_asset: str = ""
    #: ``InstrumentRecord.underlying`` carried straight through from the per-date
    #: row (populated by the adapters as of
    #: ``prediction_canonical_identity_migration_2026_07_08.md`` todo 1 —
    #: BTC/CPI/TRUMP/… for a classified subject, "" for sports (no scalar
    #: underlying) or a genuinely-unclassified market). Per-conditionId grain
    #: only, same reasoning as raw_symbol/base_asset above.
    underlying: str = ""
    #: ``InstrumentRecord.canonical_instrument_id`` carried straight through from
    #: the per-date row when the ADAPTER already populated one (Polymarket sports
    #: fixture_id — todo 5). The cross-venue join below (todo 2) can also assign
    #: one post-hoc for a matched crypto/macro pair; ``_emit`` prefers the
    #: cross-venue match, falling back to this adapter-populated value.
    canonical_instrument_id: str = ""
    #: ``InstrumentRecord.question`` — the human-readable market question/title
    #: carried straight through from the per-date row (Polymarket ``question`` /
    #: Kalshi ``title — yes_sub_title``, populated as of uac@c1de078a). FORWARD-ONLY:
    #: honest ``None`` for any per-date row captured before that field landed (the
    #: field is genuinely absent, not blank), so this stays ``None`` rather than "".
    #: Per-conditionId grain only — a cqg spans many markets with distinct questions.
    question: str | None = None


def _merge_lifecycle(
    acc: dict[tuple[str, str], _PredLifecycle],
    key: tuple[str, str],
    day: date,
    venue: str,
    instrument_type: str,
    created: str | None,
    settled: str | None,
    raw_symbol: str = "",
    base_asset: str = "",
    underlying: str = "",
    canonical_instrument_id: str = "",
    question: str | None = None,
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
            raw_symbol=raw_symbol,
            base_asset=base_asset,
            underlying=underlying,
            canonical_instrument_id=canonical_instrument_id,
            question=question,
        )
        return
    if day < cur.first_day:
        cur.first_day = day
    if day > cur.last_day:
        cur.last_day = day
        # Metadata (instrument_type / raw_symbol / base_asset / underlying /
        # canonical_instrument_id) follows the most-recent observation, same
        # convention as instrument_type below.
        if instrument_type:
            cur.instrument_type = instrument_type
        if raw_symbol:
            cur.raw_symbol = raw_symbol
        if base_asset:
            cur.base_asset = base_asset
        if underlying:
            cur.underlying = underlying
        if canonical_instrument_id:
            cur.canonical_instrument_id = canonical_instrument_id
        if question:
            cur.question = question
    else:
        # An earlier day's row may be the only one carrying a value (e.g. a market's
        # last snapshot before delisting had a transiently-blank field) — backfill
        # rather than leaving an available value unused.
        if not cur.raw_symbol and raw_symbol:
            cur.raw_symbol = raw_symbol
        if not cur.base_asset and base_asset:
            cur.base_asset = base_asset
        if not cur.underlying and underlying:
            cur.underlying = underlying
        if not cur.canonical_instrument_id and canonical_instrument_id:
            cur.canonical_instrument_id = canonical_instrument_id
        if not cur.question and question:
            cur.question = question
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
            # Venue-native fields the adapters DO populate at InstrumentRecord
            # construction (Kalshi event_ticker / Polymarket slug for raw_symbol;
            # Kalshi series_ticker / Polymarket's synthesized category label for
            # base_asset) — present on every per-date row via _records_to_dataframe's
            # model_dump(), just never read here before this fix. Per-conditionId
            # grain only (see _PredLifecycle docstring for why the cqg grain skips them).
            raw_symbol = _str_field(row, "raw_symbol")
            base_asset = _str_field(row, "base_asset")
            # underlying / canonical_instrument_id (prediction_canonical_identity_
            # migration_2026_07_08.md todos 1 + 5): real, adapter-populated fields as
            # of the 2026-07-09 underlying/fixture_id fix — "" (honest absence) for
            # any per-date row captured before that fix, or for a market with no
            # scalar underlying / no resolvable sports fixture_id. Per-conditionId
            # grain only, same reasoning as raw_symbol/base_asset above.
            underlying = _str_field(row, "underlying")
            canonical_instrument_id = _str_field(row, "canonical_instrument_id")
            # question (uac InstrumentRecord.question): human-readable market
            # question/title, honest-None (via _opt_field) when the per-date row
            # predates the field or the adapter resolved none — FORWARD-ONLY, never
            # fabricated. Per-conditionId grain only (a cqg has no single question).
            question = _opt_field(row, "question")
            cqg_itype = cqg_itype or itype
            if created and (cqg_created is None or created < cqg_created):
                cqg_created = created
            if settled and (cqg_settled is None or settled > cqg_settled):
                cqg_settled = settled
            _merge_lifecycle(
                cid_acc,
                (venue_str, cid),
                day,
                venue_str,
                itype,
                created,
                settled,
                raw_symbol,
                base_asset,
                underlying,
                canonical_instrument_id,
                question,
            )
        # cqg grain only when the writer emits a cqg (249-b, gated on decision
        # 338). Currently always empty → no cqg rows, conditionId grain only.
        if saw_member and cqg_str:
            _merge_lifecycle(cqg_acc, (venue_str, cqg_str), day, venue_str, cqg_itype, cqg_created, cqg_settled)

    if not all_days:
        return pd.DataFrame(columns=list(CATALOG_COLUMNS))
    latest_day = max(all_days)

    # Cross-venue Kalshi<->Polymarket same-market join
    # (prediction_canonical_identity_migration_2026_07_08.md todo 2 /
    # docs/PREDICTION_INSTRUMENTS.md § "Canonical identity model" §3 item 3): the
    # existing cross_venue_mapping.build_cross_venue_mapping() matcher wired into
    # this real, scheduled roll-up step (runs every catalogue regen) rather than
    # left a pure function with no caller. Builds minimal InstrumentRecord views
    # from the accumulated per-conditionId lifecycle (instrument_key / venue /
    # raw_symbol / expiry are all it needs — see cross_venue_mapping.py's own
    # module docstring on what an InstrumentRecord carries for a prediction
    # market), runs the matcher, and indexes the result by BOTH venues'
    # instrument_key so ``_emit`` can look a matched conditionId's
    # canonical_event_id up below. No ``titles`` map is passed — todo 4's real,
    # documented decision: no per-instrument title is persisted anywhere the
    # offline roll-up can reach (InstrumentRecord dropped the ``symbol`` field —
    # see cross_venue_mapping.py's docstring), so sports pairs are honestly
    # absent here (matches the matcher's own no-titles-supplied default; the
    # Polymarket sports canonical_instrument_id set at adapter time (todo 5,
    # ``polymarket/parsing.py::_build_sports_id``) is preserved below via the
    # ``lc.canonical_instrument_id`` fallback since this dict has no entry for it).
    kalshi_recs: list[InstrumentRecord] = []
    poly_recs: list[InstrumentRecord] = []
    for (venue_str, cid), lc in cid_acc.items():
        rec = InstrumentRecord(
            instrument_key=cid,
            venue=venue_str,
            instrument_type=InstrumentType.PREDICTION_MARKET,
            raw_symbol=lc.raw_symbol,
            base_asset=lc.base_asset,
            expiry=_parse_truth_datetime(lc.settled),
        )
        if venue_str.upper() == "KALSHI":
            kalshi_recs.append(rec)
        elif venue_str.upper() == "POLYMARKET":
            poly_recs.append(rec)

    canonical_event_id_by_key: dict[str, str] = {}
    if kalshi_recs and poly_recs:
        matched_pairs = build_cross_venue_mapping(kalshi_recs, poly_recs)
        for mapping in matched_pairs:
            if mapping.kalshi_market_ticker:
                canonical_event_id_by_key[mapping.kalshi_market_ticker] = mapping.canonical_event_id
            if mapping.polymarket_condition_id:
                canonical_event_id_by_key[mapping.polymarket_condition_id] = mapping.canonical_event_id
        _emit_event(
            "PREDICTION_CROSS_VENUE_MAPPING_BUILT",
            kalshi_count=len(kalshi_recs),
            polymarket_count=len(poly_recs),
            matched_pairs=len(matched_pairs),
        )

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
                # BUG FIX 2026-07-08 (see _PredLifecycle docstring): raw_symbol / base_asset
                # are real, adapter-populated venue-native fields (Kalshi event_ticker/
                # series_ticker, Polymarket slug/category label) — now threaded through
                # from the per-date rows instead of always emitting blank. "" (not the prior
                # implicit NaN) at the cqg grain — a canonical_question_group has no single
                # per-market raw_symbol/base_asset (honest absence, not unpopulated).
                "raw_symbol": lc.raw_symbol,
                "base_asset": lc.base_asset,
                # `question` (uac InstrumentRecord.question): the human-readable
                # market question/title threaded straight through from the per-date
                # row (Polymarket ``question`` / Kalshi ``title — yes_sub_title``).
                # FORWARD-ONLY: honest ``None`` for rows captured before uac@c1de078a
                # or for a market the adapter resolved no question for — never
                # fabricated. cqg-grain rows carry ``None`` (lc.question default) — a
                # family has no single per-market question.
                "question": lc.question,
                # `underlying` (prediction_canonical_identity_migration_2026_07_08.md
                # todo 1): real, adapter-populated value threaded straight through
                # from the per-date row (see _PredLifecycle.underlying) as of the
                # 2026-07-09 fix — "" (honest absence) for rows captured before that
                # fix, sports markets (no scalar underlying), or a
                # genuinely-unclassified market. See docs/PREDICTION_INSTRUMENTS.md
                # "Canonical identity model" for the full decision.
                "underlying": lc.underlying,
                # `canonical_instrument_id` (todo 2 + todo 5): prefer the cross-venue
                # Kalshi<->Polymarket match computed above (crypto/macro/index same-
                # market pairs — keyed by this row's own instrument_key, i.e. the
                # wrapped conditionId/ticker); fall back to the adapter-populated
                # value (Polymarket sports fixture_id) when no cross-venue match
                # exists for this instrument. "" (honest absence, never a guessed or
                # false pair) when neither mechanism resolves one. cqg-grain rows
                # never get one (a family has no single per-instance identity).
                "canonical_instrument_id": (
                    canonical_event_id_by_key.get(entity_id) or lc.canonical_instrument_id
                    if data_type != _PREDICTION_CQG_DATA_TYPE
                    else ""
                ),
                # Non-DeFi grain → no dual-form pool ids + no on-chain addresses.
                "glued_pair_id": "",
                "pool_address": "",
                "base_asset_contract_address": "",
                "quote_asset_contract_address": "",
                "atoken_address": "",
                "debt_token_address": "",
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

    Excludes :data:`SPORTS_LEAGUE_ID_SENTINELS` (e.g. the ``"UNKNOWN"`` phantom
    pseudo-league) BEFORE the roll-up — this was previously unguarded and minted
    a real, persisted ``instrument_id="UNKNOWN"/league_id="UNKNOWN"`` catalogue
    row that a downstream v2-enumerator bug then amplified into thousands of
    manifest rows (root-caused + fixed 2026-07-09). Since the 2026-07-13
    24-league de-registration ruling it ALSO excludes any league_id outside UAC
    ``LEAGUE_REGISTRY`` (see :func:`_sports_league_registered`) — the registered
    universe is now exactly the could-exist universe.

    Args:
        manifest_df: the canonical sports ``_index`` (needs ``league_id`` /
            ``data_type`` / ``date`` columns). Read via
            :func:`_read_sports_manifest_index`.

    Returns:
        A DataFrame with :data:`CATALOG_COLUMNS`, one row per distinct current
        canonical league (sentinel league_ids excluded), sorted by ``league_id``
        for deterministic output.
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
    df = df[
        (df["league_id"] != "")
        & (~df["league_id"].str.upper().isin(SPORTS_LEAGUE_ID_SENTINELS))
        & (df["league_id"].map(_sports_league_registered))
        & (df["data_type"].isin(current))
    ]
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
# Sports FIXTURE/TEAM/PLAYER-GRAIN roll-up — FROM REAL CAPTURED REFERENCE DATA
#
# Distinct from the league-grain could-exist system above (which is seeded
# from the MANIFEST — a theoretical "should exist" universe used to compute
# expected_unattempted gaps). These three grains are seeded from real OBSERVED
# captures only (the entity=fixtures / entity=teams / entity=injuries parquets
# the 11-step pipeline in SPORTS_INSTRUMENTS.md already writes) — a roll-up of
# what actually got captured, mirroring build_catalogue_dataframe's cefi/defi/
# tradfi by_date-snapshot pattern, NOT a could-exist projection. They therefore
# never feed expected_unattempted seeding for fixture/team/player grain — the
# sports manifest itself is still league-grain-only (2026-07-08 finding), so a
# could-exist projection at these finer grains would inflate the coverage
# denominator exactly as `sports_catalog_league_grain_only_scope_2026_07_08.md`
# warned. `enumerate_expected_universe.py::_enumerate_v2_sports` was updated
# alongside this change to only process instrument_type="league" catalogue rows
# for that reason — a fixture/team/player row's ``league_id`` must never be
# treated as a per-league lifecycle window by that enumerator.
# ---------------------------------------------------------------------------


def _split_full_name(display_name: str) -> tuple[str, str]:
    """Split a "First Last" display name into ``(last_name, first_name)``.

    Feeds UAC's ``build_player_id(last_name, first_name)``. Whitespace-delimited:
    the LAST token is the surname, everything before it is the given name(s)
    (``"Bukayo Saka"`` -> ``("Saka", "Bukayo")``). A single-token name (e.g.
    ``"Neymar"``) returns ``(name, "")`` so ``build_player_id`` falls back to the
    bare name, matching its own documented single-name-player convention.
    """
    parts = display_name.split()
    if len(parts) < 2:
        return display_name.strip(), ""
    return parts[-1], " ".join(parts[:-1])


def _sports_attr_str(raw: object) -> str:
    """Normalise a sports fixture-snapshot cell to a clean string ("" when absent).

    Same missing-value idiom as :func:`_opt_field` / :func:`_row_id` (guard
    ``pd.isna`` in a try/except — the snapshots carry None/NaN/NaT freely, e.g.
    ``venue_id``/``venue_city`` are frequently None), but collapses to the honest
    empty string rather than None, and refuses to emit the STRINGIFIED sentinels
    ``"None"``/``"nan"``/``"NaT"`` — writing those into the catalogue would be a
    fabricated value, not an absent one (never silent placeholders).
    """
    if raw is None:
        return ""
    try:
        if pd.isna(raw):  # pyright: ignore[reportArgumentType]
            return ""
    except (TypeError, ValueError):
        pass
    text = str(raw).strip()
    return "" if text.lower() in {"none", "nan", "nat"} else text


def _sports_grain_rollup_to_df(
    first_day: dict[str, date],
    last_day: dict[str, date],
    row_league: dict[str, str],
    all_days: set[date],
    instrument_type: str,
    extra_attrs: dict[str, dict[str, str]] | None = None,
) -> pd.DataFrame:
    """Shared first/last-day lifecycle -> :data:`CATALOG_COLUMNS` row assembly.

    Mirrors :func:`build_sports_catalogue_dataframe`'s lifecycle convention:
    ``available_to=None`` means "present on the latest scanned day" (still
    active); otherwise the last day it was observed. Shared by the fixture/
    team/player-grain folding loop in
    :func:`build_sports_fixture_team_player_catalogue`.

    ``extra_attrs`` (id -> {column: value}) merges per-instrument columns onto
    the assembled row — used by the FIXTURE grain to carry the scheduling/display
    fields (``kickoff_utc``/``status``/team names/``venue_name``/``round``) that
    the by_date snapshot already had. Only keys in :data:`CATALOG_COLUMNS`
    survive (``pd.DataFrame(..., columns=cols)`` drops the rest); ids absent from
    the mapping simply keep the honest blank the other grains get.
    """
    cols = list(CATALOG_COLUMNS)
    if not all_days:
        return pd.DataFrame(columns=cols)
    latest_day = max(all_days)
    rows: list[dict[str, str | None]] = []
    for iid in sorted(first_day):
        available_to = None if last_day[iid] >= latest_day else last_day[iid].isoformat()
        row: dict[str, str | None] = {
            "instrument_id": iid,
            "instrument_type": instrument_type,
            "venue": "",
            "chain": "",
            "league_id": row_league[iid],
            "available_from": first_day[iid].isoformat(),
            "available_to": available_to,
            "market_created_at": None,
            "settlement_time": None,
            "data_type": None,
            "glued_pair_id": "",
            "pool_address": "",
        }
        if extra_attrs:
            row.update(extra_attrs.get(iid, {}))
        rows.append(row)
    return pd.DataFrame(rows, columns=cols)


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
    since: date | None = None,
    max_blobs: int | None = None,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
) -> Iterator[tuple[date, pd.DataFrame]]:
    """Yield ``(day, frame)`` for every ``by_date`` instruments parquet under ``prefix``.

    Downloads run concurrently (I/O-bound, ``max_workers`` threads) — the full
    ``by_date`` history is thousands of parquets and single-threaded download does
    not complete in a reasonable window. A per-blob download/read error propagates
    (the run fails loud rather than silently producing an under-counted catalogue
    the monotonic guard would then reject anyway).

    ``since`` restricts the walk to a DATE-FLOORED PREFIX LIST: one ``day=<D>/``
    listing per day from ``since`` through today (UTC) instead of the whole-corpus
    walk — the incremental rollup's window read (single-walk discipline: bounded
    per-day listings, never a second full-corpus walk). ``since=None`` keeps the
    full-history walk (``--mode full`` / cold start).

    ``max_blobs`` truncates the walk to the first N parquets (path-sorted, for
    determinism) — DIAGNOSTIC ONLY: a truncated walk yields an INCOMPLETE catalogue
    with wrong ``available_from`` / ``available_to``, so the caller forces dry-run
    when it is set (never promotable).
    """
    walk_prefix = prefix.rstrip("/") + "/"

    def _list_window_blobs() -> Iterator[object]:
        """Per-day prefix listings for ``day=>=since`` (bounded, not a corpus walk)."""
        assert since is not None
        day = since
        today = datetime.now(UTC).date()
        while day <= today:
            yield from storage.list_blobs(bucket, prefix=f"{walk_prefix}day={day.isoformat()}/")
            day += timedelta(days=1)

    blob_iter = storage.list_blobs(bucket, prefix=walk_prefix) if since is None else _list_window_blobs()
    targets: list[tuple[date, str]] = []
    for blob in blob_iter:
        name = str(getattr(blob, "name", ""))
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
    since: date | None = None,
    max_blobs: int | None = None,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
) -> Iterator[tuple[date, str, str, pd.DataFrame]]:
    """Yield ``(day, venue, cqg, frame)`` for every prediction ``by_date`` blob.

    ``since`` restricts the walk to per-day ``day=<D>/`` prefix listings from
    ``since`` through today (the incremental window read — mirrors
    :func:`_iter_by_date_snapshots`); ``since=None`` keeps the full-history walk.

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

    def _list_window_blobs() -> Iterator[object]:
        """Per-day prefix listings for ``day=>=since`` (bounded, not a corpus walk)."""
        assert since is not None
        day = since
        today = datetime.now(UTC).date()
        while day <= today:
            yield from storage.list_blobs(bucket, prefix=f"{walk_prefix}day={day.isoformat()}/")
            day += timedelta(days=1)

    blob_iter = storage.list_blobs(bucket, prefix=walk_prefix) if since is None else _list_window_blobs()
    targets: list[tuple[date, str, str, str]] = []
    for blob in blob_iter:
        name = str(getattr(blob, "name", ""))
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


def _iter_sports_ftp_snapshots(
    storage: StorageClient,
    bucket: str,
    prefix: str,
    *,
    since: date | None = None,
    max_blobs: int | None = None,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
) -> Iterator[tuple[str, date, str, pd.DataFrame]]:
    """Yield ``(entity, day, league_id, frame)`` for fixture/team/injuries by_date parquets.

    ONE combined prefix walk covers all three :data:`_SPORTS_FTP_ENTITIES`
    (single-walk discipline — a separate whole-corpus walk per entity is
    review-blocking per codex/02-data/availability-manifest-and-data-status.md)
    — mirrors :func:`_iter_sports_by_date_snapshots` (the existing
    ``entity=leagues`` walk) but additionally parses the ``league={L}``
    partition segment, which fixtures/teams/injuries all carry (``leagues``
    does not — that entity is a bare per-day file with ``league_id`` on the
    FRAME instead, which is why it keeps its own dedicated walk function).

    ``since`` restricts the walk to a DATE-FLOORED PREFIX LIST — one
    ``day=<D>/`` listing per day from ``since`` through today (UTC), mirroring
    :func:`_iter_by_date_snapshots`'s ``since`` window read. This is NOT just a
    download-time optimisation: an UNWINDOWED ``since=None`` walk lists the
    ENTIRE ``sports_reference/by_date/`` tree (every entity — footystats/
    understat/transfermarkt/standings/etc., not just fixtures/teams/injuries),
    because the ``entity=`` segment sits AFTER ``day=``/``pipeline_mode=`` in
    the path, so GCS cannot prefix-scope the LISTING itself to just these three
    entities — only client-side filtering AFTER listing. Measured against real
    prod GCS 2026-07-09: an unwindowed listing of the full multi-year history
    did not complete in 180s (before any downloads even start). ``since=None``
    is kept for callers that genuinely want the full history (accepting that
    cost) — the default caller, :func:`build_sports_fixture_team_player_catalogue`,
    always passes a bounded ``since``.

    ``league_id`` here is always the PATH value: fixtures carries no canonical
    ``league_id`` column at all, and injuries' own ``league_id`` column is the
    RAW numeric api-football id (never overwritten with the canonical value the
    partition path already carries) — reading the path uniformly for all three
    entities avoids a silent canonical/raw mismatch between them. A raw numeric
    path value (no canonical name mapping) is a DE-REGISTERED league since the
    2026-07-13 ruling — the caller drops it via
    :func:`_sports_league_registered` alongside the
    :data:`SPORTS_LEAGUE_ID_SENTINELS` check (the pre-ruling convention of
    keeping unmapped leagues no longer applies; their data objects stay on GCS
    but must not re-mint catalogue rows). Blobs with no ``league=`` segment
    (the rare legacy unmapped-fallback files) yield ``league_id=""`` — callers
    skip those (a catalogue row needs a real league to be honest).
    """
    walk_prefix = prefix.rstrip("/") + "/"

    def _list_window_blobs() -> Iterator[object]:
        """Per-day prefix listings for ``day=>=since`` (bounded, not a corpus walk)."""
        assert since is not None
        day = since
        today = datetime.now(UTC).date()
        while day <= today:
            yield from storage.list_blobs(bucket, prefix=f"{walk_prefix}day={day.isoformat()}/")
            day += timedelta(days=1)

    blob_iter = storage.list_blobs(bucket, prefix=walk_prefix) if since is None else _list_window_blobs()
    targets: list[tuple[str, date, str, str]] = []
    for blob in blob_iter:
        name = str(getattr(blob, "name", ""))
        if not name.endswith(".parquet"):
            continue
        entity_m = _ENTITY_RE.search(name)
        if entity_m is None or entity_m.group(1) not in _SPORTS_FTP_ENTITIES:
            continue
        day_m = _DAY_RE.search(name)
        if day_m is None:
            logger.warning("Skipping sports %s blob with no day= partition: %s", entity_m.group(1), name)
            continue
        league_m = _LEAGUE_RE.search(name)
        targets.append(
            (entity_m.group(1), date.fromisoformat(day_m.group(1)), league_m.group(1) if league_m else "", name)
        )

    targets.sort(key=lambda item: item[3])
    if max_blobs is not None:
        targets = targets[:max_blobs]
    logger.info(
        "Found %d sports fixture/team/player-source by_date parquet(s) to roll up (workers=%d)",
        len(targets),
        max_workers,
    )

    def _load(item: tuple[str, date, str, str]) -> tuple[str, date, str, pd.DataFrame]:
        entity, day, league_id, name = item
        payload = storage.download_bytes(bucket, name)
        return entity, day, league_id, pd.read_parquet(io.BytesIO(payload))

    # Memory-bounded sliding window — see _bounded_parallel_load. Streamed
    # straight into build_sports_fixture_team_player_catalogue's accumulator
    # dicts (never buffered into a list here), so peak memory stays
    # O(max_workers) frames + O(distinct fixture/team/player count), NOT
    # O(len(targets)) — the fixture/team/injuries by_date corpus can span
    # hundreds of thousands of small blobs over the full history.
    yield from _bounded_parallel_load(targets, _load, max_workers=max_workers)


def build_sports_fixture_team_player_catalogue(
    storage: StorageClient,
    bucket: str,
    *,
    by_date_prefix: str = SPORTS_BY_DATE_PREFIX,
    since: date | None = None,
    max_blobs: int | None = None,
) -> pd.DataFrame:
    """Roll real captured fixture/team/player reference data into catalogue rows.

    Extends the sports could-exist catalogue past league-grain-only (2026-07-09
    operator decision — supersedes the scoping question left open in
    `sports_catalog_league_grain_only_scope_2026_07_08.md`). Reads REAL captured
    fixture/team/injuries reference data already written to
    ``sports_reference/by_date/`` (the same GCS objects the 11-step pipeline in
    SPORTS_INSTRUMENTS.md documents) and rolls each up into its own grain —
    the by_date-snapshot OBSERVED-capture pattern :func:`build_catalogue_dataframe`
    already uses for cefi/defi/tradfi, deliberately NOT the manifest-derived
    could-exist pattern :func:`build_sports_catalogue_from_manifest` uses for
    league-grain (see the module comment above this function for why: the
    sports manifest is still league-grain-only, so a could-exist projection at
    finer grain would inflate the coverage denominator).

    Instrument ids: fixtures use UAC's canonical ``LEAGUE:HOME_v_AWAY:DATE``
    shape (``build_fixture_id``); teams reuse the canonical ``team_id`` the
    ``entity=teams`` writer already stamps (``build_team_id``); players use
    ``build_player_id`` over a display-name split (:func:`_split_full_name`).
    ``venue`` is left blank for all three, same as league-grain — these are
    reference-data rows with no bookmaker association, the honest empty value
    (see SPORTS_INSTRUMENTS.md's "``venue`` vs ``source``" note).

    Streams the ONE combined walk (:func:`_iter_sports_ftp_snapshots`) directly
    into three grains' first/last-day accumulator dicts rather than buffering
    per-entity snapshot lists first — see that function's docstring on why
    (avoids reintroducing the O(len(items))-frames-in-memory failure mode
    :func:`_bounded_parallel_load` exists to prevent).

    ``since`` bounds the walk to a trailing window (default
    :data:`SPORTS_FTP_WINDOW_DAYS` days back from today when not given) — see
    that constant's docstring for why an unwindowed full-history walk is
    impractical here. Pass ``since=`` a fixed early date (or monkeypatch the
    default) for a deliberate one-off full-history backfill run.
    """
    from unified_api_contracts.sports import build_fixture_id, build_player_id, build_team_id

    if since is None:
        since = datetime.now(UTC).date() - timedelta(days=SPORTS_FTP_WINDOW_DAYS)

    fixture_first: dict[str, date] = {}
    fixture_last: dict[str, date] = {}
    fixture_league: dict[str, str] = {}
    # Scheduling/display fields carried onto the FIXTURE rows (see CATALOG_COLUMNS'
    # sports-fixture block). ``_fixture_attr_day`` tracks which snapshot day each
    # id's attrs came from so the LATEST observation wins — ``status`` evolves
    # (NS -> 1H -> FT) and a stale "NS" on a played fixture would be a lie.
    fixture_attrs: dict[str, dict[str, str]] = {}
    fixture_attr_day: dict[str, date] = {}
    team_first: dict[str, date] = {}
    team_last: dict[str, date] = {}
    team_league: dict[str, str] = {}
    player_first: dict[str, date] = {}
    player_last: dict[str, date] = {}
    player_league: dict[str, str] = {}
    all_days: set[date] = set()

    for entity, day, league_id, frame in _iter_sports_ftp_snapshots(
        storage, bucket, by_date_prefix, since=since, max_blobs=max_blobs
    ):
        all_days.add(day)
        league_id = league_id.strip()
        if (
            frame.empty
            or not league_id
            or league_id.upper() in SPORTS_LEAGUE_ID_SENTINELS
            or not _sports_league_registered(league_id)
        ):
            continue
        records: list[dict[str, object]] = frame.to_dict("records")  # pyright: ignore[reportAssignmentType]

        if entity == SPORTS_FIXTURE_ENTITY:
            for row in records:
                home = str(row.get("af_home_name") or "").strip()
                away = str(row.get("af_away_name") or "").strip()
                date_str = str(row.get("date") or "").strip()
                if not home or not away or not date_str:
                    continue
                home_id = build_team_id(home)
                away_id = build_team_id(away)
                if not home_id or not away_id:
                    continue
                fid = build_fixture_id(league_id, home_id, away_id, date_str)
                if fid not in fixture_first or day < fixture_first[fid]:
                    fixture_first[fid] = day
                if fid not in fixture_last or day > fixture_last[fid]:
                    fixture_last[fid] = day
                fixture_league[fid] = league_id
                # Latest-snapshot-wins (see _fixture_attr_day above).
                if fid not in fixture_attr_day or day >= fixture_attr_day[fid]:
                    fixture_attr_day[fid] = day
                    fixture_attrs[fid] = {
                        "kickoff_utc": _sports_attr_str(row.get("timestamp")),
                        "status": _sports_attr_str(row.get("status_short")),
                        "home_team_name": home,
                        "away_team_name": away,
                        "venue_name": _sports_attr_str(row.get("venue_name")),
                        "round": _sports_attr_str(row.get("round")),
                    }
        elif entity == SPORTS_TEAM_ENTITY:
            for row in records:
                tid = str(row.get("team_id") or "").strip()
                if not tid:
                    continue
                if tid not in team_first or day < team_first[tid]:
                    team_first[tid] = day
                if tid not in team_last or day > team_last[tid]:
                    team_last[tid] = day
                team_league[tid] = league_id
        else:  # SPORTS_PLAYER_SOURCE_ENTITY ("injuries")
            for row in records:
                raw_name = str(row.get("player_name") or "").strip()
                if not raw_name:
                    continue
                last_name, first_name = _split_full_name(raw_name)
                pid = build_player_id(last_name, first_name)
                if not pid:
                    continue
                if pid not in player_first or day < player_first[pid]:
                    player_first[pid] = day
                if pid not in player_last or day > player_last[pid]:
                    player_last[pid] = day
                player_league[pid] = league_id

    fixture_df = _sports_grain_rollup_to_df(
        fixture_first,
        fixture_last,
        fixture_league,
        all_days,
        SPORTS_FIXTURE_INSTRUMENT_TYPE,
        extra_attrs=fixture_attrs,
    )
    team_df = _sports_grain_rollup_to_df(team_first, team_last, team_league, all_days, SPORTS_TEAM_INSTRUMENT_TYPE)
    player_df = _sports_grain_rollup_to_df(
        player_first, player_last, player_league, all_days, SPORTS_PLAYER_INSTRUMENT_TYPE
    )
    return pd.concat([fixture_df, team_df, player_df], ignore_index=True)


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
        # Real event-log emission (was best-effort logger.info-only via _emit_event —
        # cefi_monotonicity_guard_alerting_and_dark_venues_2026_07_07.md). CRITICAL
        # severity + this event name route through UAC's DP-CATALOG-002 rule to
        # alerting-service's Slack/PagerDuty/Telegram paging path.
        log_event(
            "CATALOGUE_SHRINK_BLOCKED",
            severity="CRITICAL",
            details={
                "bucket": bucket,
                "env": env,
                "new_count": new_count,
                "current_count": current_count,
                "hint": "re-run a complete regeneration, or pass --allow-catalogue-shrink for a corrective shrink",
            },
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
# TradFi option contract-code resolver
# ---------------------------------------------------------------------------


def _tradfi_contract_code_to_root(contract_code: str) -> str:
    """Resolve a CME contract code to its TradFi root symbol for mvp tagging.

    The IS ``underlying`` column for CME OPTION rows stores the specific futures
    contract code (e.g., ``ESZ5`` = ES Dec-2025 future) rather than the root symbol
    (``ES``) that ``is_mvp`` checks against. Find the longest prefix that is a known
    TRADFI_ROOT; fall back to the original code so ``is_mvp`` correctly returns False
    for genuinely non-MVP underliers (e.g., ``ECNQ`` event contracts stay False).
    """
    if contract_code in TRADFI_ROOTS:
        return contract_code
    for n in range(len(contract_code) - 1, 1, -1):
        prefix = contract_code[:n]
        if prefix in TRADFI_ROOTS:
            return prefix
    return contract_code


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
            # PERPETUAL is the sole perp itype (operator 2026-07-16: crypto-venue
            # equity perps are typed PERPETUAL too, no distinct EQUITY_PERP type).
            if _itype == "PERPETUAL":
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

        # TradFi OPTION: the IS ``underlying`` column stores the specific futures
        # contract code (e.g., ``ESZ5``) not the root symbol (``ES``) that
        # ``is_mvp`` checks against. Resolve to root via TRADFI_ROOTS SSOT.
        if asset_group == "tradfi" and base and _cell(row, "instrument_type").strip().upper() == "OPTION":
            base = _tradfi_contract_code_to_root(base)

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


def _add_equity_tags(df: pd.DataFrame, asset_group: str) -> pd.DataFrame:
    """Stamp the crypto-venue equity-identity tags ``tracks_equity`` + ``is_equity_perp``.

    Operator decision 2026-07-16 (broad instrument_type + equity tags): the catalogue
    ``instrument_type`` stays the BROAD contract-mechanics type (a single-stock perp is
    ``PERPETUAL``, a tokenized stock is ``SPOT_PAIR``); the equity identity + real-equity
    linkage ride these two tags so downstream can find the tradfi spot leg (basis arb)
    without inferring it from the type or the symbol string. Derived on-the-fly from
    (``instrument_type``, ``base_asset``) via :func:`_cefi_equity_tags` over the
    rolled-up frame — never baked (UAC owns ``CEFI_EQUITY_PERP_BASE_UNIVERSE`` + the
    ``tracks_equity`` link map), mirroring the :func:`_add_mvp_column` pattern (so the
    tags self-heal on every rebuild + incremental run rather than persisting stale).

    Only ``cefi`` rows can be equity instruments; every other asset group carries
    ``(is_equity_perp=False, tracks_equity="")``.
    """
    out = df.copy()
    if df.empty:
        out["tracks_equity"] = pd.Series([], dtype="object")
        out["is_equity_perp"] = pd.Series([], dtype="bool")
        return out
    if asset_group != "cefi":
        out["tracks_equity"] = ""
        out["is_equity_perp"] = False
        return out

    def _cell(row: pd.Series[object], col: str) -> str:
        """NaN/None/empty-safe string cell (``str(np.nan)`` is ``"nan"`` — guard it)."""
        raw = row.get(col)
        if raw is None:
            return ""
        try:
            if pd.isna(raw):  # pyright: ignore[reportArgumentType]
                return ""
        except (TypeError, ValueError):
            pass
        return str(raw)

    is_equity_perp_vals: list[bool] = []
    tracks_equity_vals: list[str] = []
    for _, row in out.iterrows():
        is_equity, ticker = _cefi_equity_tags(_cell(row, "instrument_type"), _cell(row, "base_asset"))
        is_equity_perp_vals.append(is_equity)
        tracks_equity_vals.append(ticker)
    out["tracks_equity"] = tracks_equity_vals
    out["is_equity_perp"] = is_equity_perp_vals
    return out


# ---------------------------------------------------------------------------
# Incremental (trailing-window + frozen-tail) engine
# ---------------------------------------------------------------------------
# The daily full rebuild re-reads the ENTIRE multi-year by_date history every
# run (O(all-history), 2h17m tradfi) and outgrew the Cloud Run 3600s budget —
# 3 of 5 asset groups froze at the timeout (2026-06-29). The incremental path
# loads the previous catalog.parquet (all-time available_from + frozen
# available_to), re-reads ONLY a trailing window of by_date days through the
# UNCHANGED build_catalogue_dataframe (§7.3 liveness verbatim), and upserts.
# Plan: instruments_catalogue_incremental_rollup_2026_06_29.md.


def compute_window_start(
    today: date,
    prev_catalogue_mtime: datetime | None,
    *,
    window_days_min: int = WINDOW_DAYS_MIN,
) -> date:
    """Return the first day of the SELF-WIDENING trailing window.

    ``window_days = max(window_days_min, days_since_prev_catalogue + margin)`` —
    the widening term reaches back past the previous catalogue's frontier, so one
    catch-up run after an outage observes every day the dailies missed (equivalent
    to replaying them one by one; operator decision 2026-07-03). An unknown mtime
    (metadata unavailable) degrades to the minimum window — the weekly ``--mode
    full`` self-heal corrects any residual drift.
    """
    window_days = window_days_min
    if prev_catalogue_mtime is not None:
        age_days = max(0, (today - prev_catalogue_mtime.date()).days)
        window_days = max(window_days_min, age_days + WINDOW_MARGIN_DAYS)
    return today - timedelta(days=window_days)


def _load_previous_catalogue(
    storage: StorageClient,
    bucket: str,
    canonical_blob: str,
) -> tuple[pd.DataFrame, datetime | None] | None:
    """Load the current canonical catalogue + its mtime, or None when absent (cold start)."""
    if not storage.blob_exists(bucket, canonical_blob):
        return None
    payload = storage.download_bytes(bucket, canonical_blob)
    df = pd.read_parquet(io.BytesIO(payload))
    mtime: datetime | None = None
    get_meta = getattr(storage, "get_blob_metadata", None)
    if callable(get_meta):
        meta = get_meta(bucket, canonical_blob)
        raw_mtime = getattr(meta, "last_modified", None)
        if raw_mtime:
            try:
                mtime = pd.Timestamp(raw_mtime).to_pydatetime()
                if mtime.tzinfo is None:
                    mtime = mtime.replace(tzinfo=UTC)
            except (ValueError, TypeError):
                mtime = None
    return df, mtime


def _incremental_merge_keys(df: pd.DataFrame, *, asset_group: str) -> pd.Series[str]:
    """Vectorised per-row merge identity, shared by prev-catalogue + window frames.

    MUST mirror the full rebuild's PER-ASSET-GROUP aggregate identity EXACTLY
    (full-rebuild parity is the merge's correctness contract):

    * DeFi POOL rows → the canonical dual-form pool identity
      ``pool::<CHAIN>::<addr.lower()>`` (mirrors ``_aggregate_key`` /
      ``_defi_pool_dual_form`` — NOT the raw ``instrument_id``, which is the
      bare lowercased address and could in principle collide across chains).
    * prediction → ``venue::instrument_id::data_type``: the prediction rollup
      dedups by ``(venue, conditionId)`` per grain, and cqg rows legitimately
      exist on MULTIPLE venues under the SAME id (``BNB_PRICE_RANGE_DAILY`` on
      both KALSHI and POLYMARKET — 31 real cross-venue pairs in prod), so venue
      IS identity here.
    * cefi perp-family (PERPETUAL — incl. crypto-venue equity perps, raw_symbol
      present) → the ``(venue, raw_symbol, margin)`` lineage key
      (:func:`_cefi_perp_lineage_key`), mirroring ``_aggregate_key`` — so the
      2026-07 id-convention churn (``VENUE:PERP:BTC`` → ``VENUE:PERPETUAL:BTC-USD``
      → ``…@LIN``) collapses to ONE lifecycle instead of 3 stale-dup listings.
      Here the venue prefix is read from the STABLE ``instrument_id`` first
      segment (NOT the drifting ``venue`` field), for the same reason as below.
    * cefi / tradfi / defi non-pool (not perp-family, or raw_symbol blank) →
      ``instrument_id`` alone, which IS ``_aggregate_key`` for these rows (the id
      embeds the canonical venue prefix). The ``venue`` FIELD must NOT be part of
      the key: it carries the era-specific raw spelling (``DERIBIT-COMBO`` in old
      rows vs ``DERIBIT`` in the window for the SAME ``instrument_id``), so keying
      on it splits one lifecycle into ghost duplicates the full rebuild unifies —
      the 122-dupe cefi ``CATALOGUE_SHRINK_BLOCKED`` on the first weekly self-heal
      (2026-07-04).
    """

    def _col(name: str) -> pd.Series[str]:
        if name in df.columns:
            return df[name].fillna("").astype(str)
        return pd.Series([""] * len(df), index=df.index, dtype=str)

    if asset_group == "prediction":
        return _col("venue") + "::" + _col("instrument_id") + "::" + _col("data_type")
    instrument_id = _col("instrument_id")
    pool_address = _col("pool_address").str.lower()
    chain = _col("chain").str.upper()
    is_pool = (pool_address != "") & (chain != "")
    pool_key = "pool::" + chain + "::" + pool_address
    # CeFi perp-family lineage collapse — mirrors _aggregate_key. Venue prefix from
    # the stable instrument_id first segment; raw_symbol-blank rows fall through.
    itype = _col("instrument_type").str.strip().str.upper()
    raw_symbol = _col("raw_symbol").str.strip().str.upper()
    venue_prefix = instrument_id.str.split(":", n=1).str[0].str.strip().str.upper()
    margin = _col("margin_type").str.strip().str.upper()
    is_perp = itype.isin(_PERP_FAMILY_ITYPES) & (raw_symbol != "") & (venue_prefix != "")
    perp_key = "cefiperp::" + venue_prefix + "::" + raw_symbol + "::" + margin
    key = perp_key.where(is_perp, instrument_id)
    return pool_key.where(is_pool, key)


def _merge_incremental(
    prev_df: pd.DataFrame,
    window_df: pd.DataFrame,
    *,
    window_start: date,
    asset_group: str,
) -> pd.DataFrame:
    """Upsert the window recompute onto the previous catalogue (frozen tail kept).

    Branches (plan §The fix, step 3):
      1. window row known in prev  → take the window row (fresh §7.3
         ``available_to`` + metadata) but ``available_from`` = min(prev, window)
         (immutable-once-set; min also absorbs a newly-declared earlier listing
         date exactly like the full rebuild's BUG#4 rule).
      2. window-only row (new listing) → append as-is.
      3. active-in-prev, absent from the ENTIRE window, venue STILL PRESENT in
         the window → newly delisted; close ``available_to`` at
         ``window_start - 1`` (tightest provable bound — near-dead code under the
         self-widening window; healed exactly by the weekly full rebuild).
      4. every other prev row (frozen tail — incl. every row of a venue with NO
         window presence: a venue-level capture outage is NOT a delisting, §7.3
         keeps a stopped venue's instruments active exactly like the full
         rebuild) → copied through unchanged.

    The output row set is prev UNION window-new, so ``len(merged) >= len(prev)`` and
    the monotonic guard passes by construction.
    """
    # Derived/finalization columns (mvp + the equity-identity tags) are re-stamped
    # on the MERGED frame by _add_mvp_column / _add_equity_tags — drop them from both
    # inputs so the merge carries only the stable rollup columns (full-rebuild parity:
    # mvp's cefi perp-gate is computed over the whole catalogue, and the equity tags
    # self-heal from instrument_type + base_asset every run).
    out_columns = [c for c in CATALOG_COLUMNS if c not in ("mvp", "tracks_equity", "is_equity_perp")]
    prev = prev_df.copy()
    # The prev catalogue carries the derived tags; drop them (see above).
    prev = prev.drop(columns=[c for c in prev.columns if c not in out_columns])
    for col in out_columns:
        if col not in prev.columns:
            prev[col] = ""
    window = window_df.copy()
    for col in out_columns:
        if col not in window.columns:
            window[col] = ""

    prev_keys = _incremental_merge_keys(prev, asset_group=asset_group)
    window_keys = _incremental_merge_keys(window, asset_group=asset_group)
    prev_key_set = set(prev_keys)
    window_key_set = set(window_keys)

    # Branch 1+2 — the window recompute, with available_from carried for known rows.
    prev_af = pd.Series(prev["available_from"].astype(str).values, index=prev_keys)
    prev_af = prev_af[~prev_af.index.duplicated(keep="first")]
    known_mask = window_keys.isin(prev_key_set).to_numpy()
    updated = window[known_mask].copy()
    if not updated.empty:
        carried = prev_af.reindex(_incremental_merge_keys(updated, asset_group=asset_group).to_numpy()).to_numpy()
        own = updated["available_from"].astype(str).to_numpy()
        # ISO dates compare lexicographically == chronologically; keep the earlier.
        updated["available_from"] = [
            min(c, o) if isinstance(c, str) and c else o for c, o in zip(carried, own, strict=True)
        ]
    fresh = window[~known_mask]

    # Branch 3+4 — prev rows absent from the window.
    tail = prev[~prev_keys.isin(window_key_set).to_numpy()].copy()
    if not tail.empty:
        # §7.3 venue-truth: only close an instrument when its venue DID capture
        # in the window (instrument-level absence). Venue-level absence = capture
        # outage / stopped venue → preserve prev state (full-rebuild parity).
        window_venues = {_merge_canonical_venue(v) for v in window["venue"].fillna("").astype(str) if v}
        tail_venues = tail["venue"].fillna("").astype(str).map(_merge_canonical_venue)
        active = tail["available_to"].isna() | (tail["available_to"].astype(str).str.strip() == "")
        newly_delisted = active & tail_venues.isin(window_venues)
        n_delisted = int(newly_delisted.sum())
        if n_delisted:
            close_day = (window_start - timedelta(days=1)).isoformat()
            tail.loc[newly_delisted, "available_to"] = close_day
            logger.info(
                "Incremental merge: %d active instrument(s) absent from the whole window "
                "(venue still capturing) → closed available_to=%s",
                n_delisted,
                close_day,
            )

    merged = pd.concat([tail[out_columns], updated[out_columns], fresh[out_columns]], ignore_index=True)
    logger.info(
        "Incremental merge: %d prev rows → %d merged (%d updated in-window, %d new listings, %d frozen-tail)",
        len(prev),
        len(merged),
        len(updated),
        len(fresh),
        len(tail),
    )
    return merged


#: Coverage-horizon warn threshold: the newest by_date day in the window older
#: than this many days means the UPSTREAM download cron is broken — the
#: catalogue can only be as fresh as the snapshots that feed it.
_STALE_BY_DATE_MAX_AGE_DAYS = 3


def _warn_coverage_horizon(day_counts: dict[date, int], today: date, asset_group: str) -> None:
    """Emit ``CATALOGUE_STALE_BY_DATE`` when the by_date feed itself looks unhealthy.

    Two signals (the originating plan's never-built NICE-TO-HAVE, shipped with
    the incremental rollup since the window read makes both trivial):
      * the newest window day is > ``_STALE_BY_DATE_MAX_AGE_DAYS`` old (download
        cron down — catch-up self-heals via the widening window, but the
        operator should fix the producer);
      * the newest day's instrument count dropped sharply (<50% of the window
        median — a partial capture; §7.3's thin-day guard already refuses to
        delist off it, this makes the condition VISIBLE).
    """
    # Clamp to days <= today: the prediction writer emits FUTURE-dated day=
    # partitions (event/settlement-dated dirs out to 2028+), which would make
    # ``max(day_counts)`` land in the future and BLIND both checks below — the
    # exact blind spot that hid the 2026-07-01→06 prediction capture outage
    # (is-daily-enum-prediction failing daily; catalogue stayed green on §7.3
    # thin-day semantics while its feed starved).
    past_counts = {d: c for d, c in day_counts.items() if d <= today}
    if not past_counts:
        _emit_event("CATALOGUE_STALE_BY_DATE", asset_group=asset_group, reason="no_window_data")
        logger.warning("CATALOGUE_STALE_BY_DATE: %s window contained no by_date data at all", asset_group)
        return
    day_counts = past_counts
    latest = max(day_counts)
    age_days = (today - latest).days
    if age_days > _STALE_BY_DATE_MAX_AGE_DAYS:
        _emit_event(
            "CATALOGUE_STALE_BY_DATE",
            asset_group=asset_group,
            reason="latest_day_too_old",
            latest_day=latest.isoformat(),
            age_days=age_days,
        )
        logger.warning(
            "CATALOGUE_STALE_BY_DATE: %s newest by_date day is %s (%dd old) — upstream download cron unhealthy",
            asset_group,
            latest.isoformat(),
            age_days,
        )
    counts = sorted(day_counts.values())
    mid = len(counts) // 2
    median = counts[mid] if len(counts) % 2 else (counts[mid - 1] + counts[mid]) / 2
    if median and day_counts[latest] < 0.5 * median:
        _emit_event(
            "CATALOGUE_STALE_BY_DATE",
            asset_group=asset_group,
            reason="latest_day_sharp_count_drop",
            latest_day=latest.isoformat(),
            latest_count=day_counts[latest],
            window_median=median,
        )
        logger.warning(
            "CATALOGUE_STALE_BY_DATE: %s newest day %s has %d rows vs window median %s — partial capture",
            asset_group,
            latest.isoformat(),
            day_counts[latest],
            median,
        )


def _tee_day_counts(
    snapshots: Iterator[tuple[date, pd.DataFrame]],
    day_counts: dict[date, int],
) -> Iterator[tuple[date, pd.DataFrame]]:
    """Pass ``(day, frame)`` through while accumulating per-day row counts."""
    for day, frame in snapshots:
        day_counts[day] = day_counts.get(day, 0) + len(frame)
        yield day, frame


def _tee_prediction_day_counts(
    snapshots: Iterator[tuple[date, str, str, pd.DataFrame]],
    day_counts: dict[date, int],
) -> Iterator[tuple[date, str, str, pd.DataFrame]]:
    """Prediction-shaped variant of :func:`_tee_day_counts` (4-tuple items)."""
    for day, venue, cqg, frame in snapshots:
        day_counts[day] = day_counts.get(day, 0) + len(frame)
        yield day, venue, cqg, frame


_MERGE_VENUE_CACHE: dict[str, str] = {}


def _merge_canonical_venue(raw: str) -> str:
    """Ghost-normalised venue key for the merge's venue-presence set.

    Mirrors ``build_catalogue_dataframe._canonical_venue_key`` (lazy UAC import,
    cached) so an old ghost-spelled venue in the prev catalogue matches its
    canonical form in the window frame.
    """
    if not raw:
        return raw
    cached = _MERGE_VENUE_CACHE.get(raw)
    if cached is not None:
        return cached
    from unified_api_contracts.registry.capability_declarations._defi import canonicalize_defi_venue_combined

    canonical = canonicalize_defi_venue_combined(raw)
    _MERGE_VENUE_CACHE[raw] = canonical
    return canonical


def _merge_sports_ftp_with_frozen_tail(
    storage: StorageClient,
    bucket: str,
    *,
    by_date_prefix: str,
    since: date,
    max_blobs: int | None,
    prev_catalogue: tuple[pd.DataFrame, datetime | None] | None,
) -> pd.DataFrame:
    """Sports fixture/team/player-grain roll-up with a FROZEN TAIL (2026-07-15 fix).

    :func:`build_sports_fixture_team_player_catalogue`'s trailing
    :data:`SPORTS_FTP_WINDOW_DAYS` window is unconditionally full-rebuilt on
    every run (sports is exempt from the generic ``--mode incremental`` engine
    — see :func:`run_rollup`'s mode-resolution comment). The by_date WALK is
    windowed, but with no memory of what a PRIOR run already rolled up: an
    instrument whose last-observed day ages past the window's leading edge
    (``since``) simply has NO blob in the fresh walk at all, so its row
    vanishes from the recompute ENTIRELY — not just its ``available_to``
    closing, the whole row disappears — silently shrinking the catalogue's row
    count run over run. That contradicts this module's own documented
    contract ("a cumulative, all-instruments-ever lifecycle catalogue, NOT a
    current snapshot" — module docstring) and can permanently jam the
    monotonic guard.

    Confirmed root cause of the 2026-07-15 ``CATALOGUE_SHRINK_BLOCKED`` sports
    incident (27216 → 27210): 9 single-day-only fixture/player rows (their
    ONLY captured occurrence across the whole history was 2025-06-09) aged off
    the window's bottom edge the moment ``since`` advanced past that day,
    while only 3 new same-day USL_CHAMPIONSHIP fixtures were gained at the top
    — net -6. Not a legitimate league de-registration (unrelated to
    :func:`_sports_league_registered`) and not upstream flakiness (identical
    49,916-blob / 27,210-row result on both same-day retries) — a real gap in
    this roll-up's windowing.

    Fix: reuse the generic frozen-tail merge (:func:`_merge_incremental`) —
    its default (non-prediction, non-DeFi-pool) merge key is the bare
    ``instrument_id``, which IS the fixture_id/team_id/player_id identity
    these rows already carry, so no sports-specific key branch is needed. Its
    venue-presence-gated close (branch 3) is a natural no-op here (sports FTP
    rows carry ``venue=""`` always, by design — see
    :func:`build_sports_fixture_team_player_catalogue`'s docstring), so an
    aged-off row is carried through FROZEN — unchanged ``available_from`` /
    ``available_to`` — rather than closed again. league-grain rows (the
    manifest-derived could-exist universe) are untouched by this helper; the
    caller filters them out of ``prev_catalogue`` before calling in.
    """
    window_df = build_sports_fixture_team_player_catalogue(
        storage, bucket, by_date_prefix=by_date_prefix, since=since, max_blobs=max_blobs
    )
    if prev_catalogue is None:
        return window_df
    prev_df, _prev_mtime = prev_catalogue
    if prev_df.empty:
        return window_df
    prev_ftp_df = prev_df[
        prev_df["instrument_type"].isin(
            (SPORTS_FIXTURE_INSTRUMENT_TYPE, SPORTS_TEAM_INSTRUMENT_TYPE, SPORTS_PLAYER_INSTRUMENT_TYPE)
        )
    ].copy()
    if prev_ftp_df.empty:
        return window_df
    logger.info(
        "Sports FTP frozen-tail merge: %d prev FTP rows + %d fresh window rows",
        len(prev_ftp_df),
        len(window_df),
    )
    return _merge_incremental(prev_ftp_df, window_df, window_start=since, asset_group="sports")


def run_rollup(
    asset_group: str,
    *,
    allow_shrink: bool,
    dry_run: bool,
    mode: str = "incremental",
    by_date_prefix: str = DEFAULT_BY_DATE_PREFIX,
    max_blobs: int | None = None,
    since: date | None = None,
    storage: StorageClient | None = None,
) -> int:
    """Roll up the per-date definitions for ``asset_group`` and promote the catalogue.

    ``since`` overrides the sports FTP roll-up's window start (default
    ``today - SPORTS_FTP_WINDOW_DAYS``). It exists for the deliberate ONE-OFF
    full-history backfill the module docstring describes but previously offered
    no supported path to run: because the window start was hardcoded here, the
    sports catalogue could only ever hold ~13 months, and the frozen-tail merge
    can only PRESERVE rows already present — it cannot recover history that was
    never rolled up. Pass an early date once (e.g. 2019-01-01) to populate the
    full corpus; every subsequent windowed cron run then carries those rows
    forward via the frozen tail, so the cost is paid once, not per run.
    """
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

    # Mode resolution. Sports never walks by_date (the league catalogue is a
    # single manifest _index read — nothing to make incremental).
    if mode == "incremental" and asset_group == "sports":
        logger.info("mode=incremental is a no-op for sports — running the manifest single-read path")
        mode = "full"
    prev_catalogue: tuple[pd.DataFrame, datetime | None] | None = None
    canonical_blob, _ = _catalogue_object_paths(env)
    if mode == "incremental":
        prev_catalogue = _load_previous_catalogue(storage, bucket, canonical_blob)
        if prev_catalogue is None:
            # Cold start: no previous catalogue to merge onto → full rebuild.
            logger.info(
                "No previous catalogue at gs://%s/%s — cold start, falling back to --mode full", bucket, canonical_blob
            )
            mode = "full"

    _emit_event(
        "CATALOGUE_ROLLUP_STARTED",
        run_id=run_id,
        asset_group=asset_group,
        bucket=bucket,
        env=env,
        mode=mode,
        by_date_prefix=by_date_prefix,
    )
    logger.info(
        "Rolling up %s catalogue from gs://%s/%s/ (env=%s mode=%s)",
        asset_group,
        bucket,
        by_date_prefix.rstrip("/"),
        env,
        mode,
    )

    # Phase C: by_date listing + download-pool
    print(
        f"[BISECT-C] by_date listing / download-pool asset_group={asset_group} bucket={bucket} mode={mode}", flush=True
    )
    if asset_group == "sports":
        # League-grain could-exist universe — derived from the canonical MANIFEST
        # (the namespace-correct superset), NOT the entity=leagues slice (whose RAW
        # NUMERIC api-football league_ids do not match the manifest's canonical
        # namespace → 131/606 coverage + numeric over-seed; slot-4 2026-06-07).
        # The captured manifest atom is per-(league_id, data_type, date).
        league_df = build_sports_catalogue_from_manifest(_read_sports_manifest_index(storage, bucket))
        # Fixture/team/player-grain — real OBSERVED captures rolled up from
        # sports_reference/by_date (2026-07-09, extends past league-grain-only;
        # see build_sports_fixture_team_player_catalogue's docstring for why this
        # is architecturally distinct from the manifest-derived league-grain path
        # above and safe to concat onto the same catalogue). FROZEN-TAIL merge
        # (2026-07-15 fix, _merge_sports_ftp_with_frozen_tail) onto the previous
        # catalogue's FTP rows — the trailing SPORTS_FTP_WINDOW_DAYS window is
        # unconditionally recomputed every run with no memory of a prior run, so
        # without the frozen tail an instrument that ages off the window's bottom
        # edge silently vanishes from the catalogue entirely (not just closes),
        # shrinking the row count and jamming the monotonic guard — see that
        # helper's docstring for the confirmed 2026-07-15 incident (27216→27210).
        # ``--since`` (one-off full-history backfill) overrides the trailing
        # window; absent it, the affordable cron default. See run_rollup's docstring.
        sports_since = since if since is not None else datetime.now(UTC).date() - timedelta(days=SPORTS_FTP_WINDOW_DAYS)
        logger.info(
            "Sports FTP window start: %s (%s)",
            sports_since.isoformat(),
            "explicit --since (full-history backfill)"
            if since is not None
            else f"default {SPORTS_FTP_WINDOW_DAYS}d trailing",
        )
        sports_prev_catalogue = _load_previous_catalogue(storage, bucket, canonical_blob)
        ftp_df = _merge_sports_ftp_with_frozen_tail(
            storage,
            bucket,
            by_date_prefix=by_date_prefix,
            since=sports_since,
            max_blobs=max_blobs,
            prev_catalogue=sports_prev_catalogue,
        )
        df = pd.concat([league_df, ftp_df], ignore_index=True) if not ftp_df.empty else league_df
    elif mode == "incremental" and prev_catalogue is not None:
        # Trailing-window read (self-widening) + frozen-tail merge — the O(window)
        # replacement for the O(all-history) walk. §7.3 liveness (generic) / the
        # settlement-date convention (prediction) run verbatim on the window via
        # the unchanged per-AG builders.
        prev_df, prev_mtime = prev_catalogue
        window_start = compute_window_start(datetime.now(UTC).date(), prev_mtime)
        logger.info(
            "Incremental window: day>=%s (prev catalogue rows=%d mtime=%s)",
            window_start.isoformat(),
            len(prev_df),
            prev_mtime.isoformat() if prev_mtime else "unknown",
        )
        window_day_counts: dict[date, int] = {}
        if asset_group == "prediction":
            window_df = build_prediction_catalogue_dataframe(
                _tee_prediction_day_counts(
                    _iter_prediction_by_date_snapshots(
                        storage, bucket, by_date_prefix, since=window_start, max_blobs=max_blobs
                    ),
                    window_day_counts,
                )
            )
        else:
            window_df = build_catalogue_dataframe(
                _tee_day_counts(
                    _iter_by_date_snapshots(storage, bucket, by_date_prefix, since=window_start, max_blobs=max_blobs),
                    window_day_counts,
                )
            )
        if window_df.empty:
            logger.warning(
                "Incremental window produced 0 rows (no by_date data since %s) — catalogue preserved unchanged",
                window_start.isoformat(),
            )
        # Coverage-horizon health of the by_date FEED itself (stale latest day /
        # sharp count drop) — visible even though the merge below stays safe.
        _warn_coverage_horizon(window_day_counts, datetime.now(UTC).date(), asset_group)
        df = _merge_incremental(prev_df, window_df, window_start=window_start, asset_group=asset_group)
    elif asset_group == "prediction":
        # Multi-grain roll-up: the cqg bundle is per-canonical_question_group while
        # trades/market_lifecycle are per-conditionId. Parse the cqg from the path.
        df = build_prediction_catalogue_dataframe(
            _iter_prediction_by_date_snapshots(storage, bucket, by_date_prefix, max_blobs=max_blobs)
        )
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

    # Crypto-venue equity-identity tags (operator 2026-07-16): instrument_type stays
    # the broad mechanics type; the equity identity + real-equity linkage ride the
    # tracks_equity / is_equity_perp tags (see _add_equity_tags / CATALOG_COLUMNS).
    df = _add_equity_tags(df, asset_group)
    _eq_count = int(df["is_equity_perp"].sum()) if not df.empty else 0
    logger.info("Equity-tagged catalogue: %d / %d rows flagged is_equity_perp", _eq_count, len(df))

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
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help=(
            "incremental (default): load the previous catalog.parquet + re-read only the self-widening "
            "trailing window of by_date days, then upsert (frozen tail untouched). "
            "full: re-aggregate the entire by_date history (cold start / weekly self-heal / rollback)."
        ),
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
        "--since",
        default=None,
        help=(
            "SPORTS ONLY — override the FTP roll-up's window start (YYYY-MM-DD). Default is a "
            f"{SPORTS_FTP_WINDOW_DAYS}d trailing window, which is why the sports catalogue holds only "
            "~13 months. Pass an early date (e.g. 2019-01-01) for the deliberate ONE-OFF full-history "
            "backfill; the frozen-tail merge then carries those rows forward on every subsequent "
            "windowed run, so the (multi-hour) full walk is paid once, not per run."
        ),
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
    mode: str = args.mode
    by_date_prefix: str = args.by_date_prefix
    max_blobs: int | None = args.max_blobs

    # --since is sports-only (the other asset groups' window is driven by --mode +
    # the incremental engine). Fail LOUD on a bad date rather than silently
    # falling back to the 400d default — a typo on a multi-hour backfill that
    # quietly rolled up 13 months instead of 8 years would look like success.
    since: date | None = None
    if args.since is not None:
        try:
            since = date.fromisoformat(str(args.since).strip())
        except ValueError:
            logger.error("--since must be YYYY-MM-DD (got %r)", args.since)
            return 2
        if asset_group != "sports":
            logger.error("--since is sports-only (got --asset-group %s)", asset_group)
            return 2

    # Real event-log wiring so CATALOGUE_SHRINK_BLOCKED actually leaves the process
    # (was best-effort logger.info-only — cefi_monotonicity_guard_alerting_and_dark_
    # venues_2026_07_07.md). setup_events() must run before promote_catalogue()'s
    # log_event() call or it raises (batch mode requires an explicit sink).
    setup_events(
        service_name=_EVENTS_SERVICE,
        mode="batch",
        sink=GcsEventSink(
            project_id=_EVENTS_PROJECT_ID,
            bucket=f"{_EVENTS_PROJECT_ID}-events",
            service_name=_EVENTS_SERVICE,
        ),
    )
    return run_rollup(
        asset_group,
        allow_shrink=allow_shrink,
        dry_run=dry_run,
        mode=mode,
        by_date_prefix=by_date_prefix,
        max_blobs=max_blobs,
        since=since,
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
