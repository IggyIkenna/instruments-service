"""Venue/date core: adapter epoch lookup, shard skip policy, venue availability windows, instrument date filtering, asset-group venue resolution.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators, constants and mutable module state resolve through
``_orch`` — the live ``instruments_service.engine.orchestrator`` package
namespace — so the package keeps the original module's single-namespace
semantics: ``unittest.mock.patch("instruments_service.engine.orchestrator.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split, and mutable caches remain package-level attributes.
"""

# Package-internal access: the orchestrator package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING

from unified_api_contracts import VENUES_BY_ASSET_GROUP

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_SPORTS_PROVIDER_VENUES",
    "_TRADFI_NON_VENUE_KEYS",
    "_VENUE_ADAPTER_EPOCH",
    "_get_venue_epoch",
    "_should_skip_shard",
    "earliest_venue_date",
    "expand_cefi_tardis_endpoints",
    "filter_instruments_by_date",
    "get_venues_for_asset_groups",
    "is_venue_available",
    "reject_junk_instruments",
]


# ---------------------------------------------------------------------------
# Adapter epoch versioning
# ---------------------------------------------------------------------------
# When adapter filtering logic changes (e.g. adding DEFI_MAJOR_ASSET_SYMBOLS
# filter, changing TVL thresholds), old manifest HWM entries become invalid.
# The epoch date marks when the current adapter version started — manifest
# entries BEFORE this date are ignored for monotonicity comparison.
#
# Bump the epoch date when adapter logic changes for a venue.
# Format: venue name → YYYY-MM-DD of the first run with new logic.
_VENUE_ADAPTER_EPOCH: dict[str, str] = {
    # 2026-04-02: removed DEFI_MAJOR_ASSET_SYMBOLS filter from all DeFi adapters
    # and TVL threshold from Uniswap V3 GraphQL query. Filtering now handled
    # post-fetch by filter_defi_instruments_by_relevance(). Manifest tracks
    # true pre-filter counts for monotonicity. Old filtered counts are lower
    # but new unfiltered counts are strictly >=, so no false regressions.
    "AAVE_V3": "2026-04-02",
    "UNISWAP_V2": "2026-04-02",
    # 2026-04-04: Uniswap V3/V4 and Balancer adapters had _FETCH_LIMIT=1000
    # with no pagination — actual pool counts exceed 1000. Pagination added
    # (skip-based, up to 6000 pools). Epoch bumped past all capped entries.
    "UNISWAP_V3": "2026-04-05",
    "UNISWAP_V4": "2026-04-05",
    "BALANCER": "2026-04-05",
    # 2026-04-04: Curve adapter was hardcoded to Ethereum API, ignoring chain
    # parameter — CURVE-AVALANCHE and CURVE-OPTIMISM had Ethereum pool counts.
    # Adapter fixed to use per-chain API URLs. Epoch bumped past today's bad entries.
    "CURVE": "2026-04-05",
    "COMPOUND_V3": "2026-04-02",
    "MORPHO": "2026-04-02",
    "FLUID": "2026-04-02",
    # DEX perp venues (L2 + Solana) — epoch from when adapters were registered
    "LIGHTER": "2026-05-12",
    "PACIFICA": "2026-05-12",
    "EXTENDED": "2026-05-12",
    # Solana adapters, LST, and yield venues — epoch from first run
    "DRIFT": "2026-04-02",
    "KAMINO": "2026-04-02",
    "ORCA": "2026-04-02",
    "RAYDIUM": "2026-04-02",
    "MARINADE": "2026-04-02",
    "JITO": "2026-04-02",
    "LIDO": "2026-04-02",
    "ETHERFI": "2026-04-02",
    "ETHENA": "2026-04-02",
}


# ---------------------------------------------------------------------------
# CeFi Tardis grain-adapter
# ---------------------------------------------------------------------------
# UAC's VENUES_BY_ASSET_GROUP["cefi"] carries the bare execution-context canonical
# venue names (e.g. "OKX"). IS fetches instruments via the Tardis
# endpoint API, which exposes SEPARATE endpoints per OKX market type:
#   OKX → okex (spot), okex-swap (perps/funding), okex-futures (fixed-expiry)
# Mapping bare OKX to all three ensures IS captures the full OKX universe.
# COINBASE-SPOT / COINBASE-FUTURES are already separate UAC cefi entries
# (coinbase_bare_name_migration_2026_07_06.md S2/S3, 2026-07-10 — bare
# COINBASE removed from UAC) and pass through unchanged.
#
# Existing comment in the former _CEFI_VENUES:
#   "OKX: 3 separate Tardis exchanges — okex (spot), okex-swap (perps), okex-futures
#   (fixed-expiry). Do NOT add bare OKX — duplicate data."
# This function encodes that rule explicitly so IS never needs a parallel list.


def expand_cefi_tardis_endpoints(canonical_venues: list[str]) -> list[str]:
    """Map UAC canonical CeFi venues to the Tardis-endpoint venues IS fetches.

    UAC's ``VENUES_BY_ASSET_GROUP["cefi"]`` carries the bare execution-context
    canonical venue name ``"OKX"`` (bare ``"COINBASE"`` REMOVED,
    coinbase_bare_name_migration_2026_07_06.md S3, 2026-07-10 — ``COINBASE-SPOT``
    is the sole canonical cefi spot key now).  IS fetches instruments via the
    Tardis per-endpoint API, which requires the split forms for multi-endpoint
    venues.  This function encodes the grain mapping so IS no longer maintains a
    parallel hand-edited venue list.

    Mapping rules:
    - ``"OKX"`` → ``["OKX-SPOT", "OKX-SWAP", "OKX-FUTURES"]``
      OKX: 3 separate Tardis exchanges (okex/okex-swap/okex-futures).
      Do NOT add bare "OKX" — it maps to the same Tardis exchange as OKX-SPOT
      (duplicate data).
    - Every other UAC cefi venue → passthrough (already the correct Tardis endpoint
      name, e.g. ``BINANCE-SPOT``, ``COINBASE-SPOT``, ``KALSHI-PERP``,
      ``POLYMARKET-PERP``).  Bare ``COINBASE`` no longer appears in
      ``VENUES_BY_ASSET_GROUP["cefi"]`` (coinbase_bare_name_migration_2026_07_06.md
      S3, 2026-07-10) — ``COINBASE-SPOT`` is the sole canonical cefi spot key and
      passes through unchanged, same as ``COINBASE-FUTURES``.

    Args:
        canonical_venues: A list of UAC canonical cefi venue names
            (e.g. ``VENUES_BY_ASSET_GROUP["cefi"]``).

    Returns:
        The expanded list of Tardis-endpoint venue names IS uses for enumeration.
        Includes ``KALSHI-PERP`` and ``POLYMARKET-PERP`` automatically because they
        appear in the UAC cefi list (fixing the former ``_CEFI_VENUES`` omission).
    """
    result: list[str] = []
    for venue in canonical_venues:
        if venue == "OKX":
            result.extend(["OKX-SPOT", "OKX-SWAP", "OKX-FUTURES"])
        else:
            result.append(venue)
    return result


# ---------------------------------------------------------------------------
# TradFi non-venue filter
# ---------------------------------------------------------------------------
# UAC keeps YAHOO_FINANCE in VENUES_BY_ASSET_GROUP["tradfi"] as a legacy
# source-as-venue artifact (see UAC market_data_categories.py ~line 293:
# "legacy source-as-venue (rolling VIX 15m / KRW-USD daily)").  It is NOT a
# real fetchable venue — instruments are NOT discoverable via a Yahoo Finance
# venue endpoint; the data comes as a Yahoo Finance sourced field on ICE/FX
# instruments.  IS excludes it from the enumeration list.
_TRADFI_NON_VENUE_KEYS: frozenset[str] = frozenset({"YAHOO_FINANCE"})


_SPORTS_PROVIDER_VENUES: dict[str, list[str]] = {
    "API_FOOTBALL": ["API_FOOTBALL"],
    "API_FOOTBALL_ENRICHMENT": ["API_FOOTBALL"],
    "OPEN_METEO": ["OPEN_METEO"],
    "TRANSFERMARKT": ["TRANSFERMARKT"],
    "SOCCER_FOOTBALL_INFO": ["SOCCER_FOOTBALL_INFO"],
    "UNDERSTAT": ["UNDERSTAT"],
    "FOOTYSTATS": ["FOOTYSTATS"],
}


def _get_venue_epoch(venue: str) -> str | None:
    """Return the adapter epoch date for a venue, or None if no epoch set.

    Matches by venue prefix: 'AAVE_V3-ETHEREUM' matches epoch key 'AAVE_V3'.
    """
    for prefix, epoch in _orch._VENUE_ADAPTER_EPOCH.items():
        if venue.startswith(prefix):
            return epoch
    return None


def _should_skip_shard(
    manifest: _orch.ManifestWriter,
    *,
    row_key: dict[str, str],
    force: bool,
) -> bool:
    """Return True if this shard already has a captured/empty_confirmed row.

    ``attempted_failed`` rows are NOT skipped — operator can decide via
    inspection whether the underlying error has been resolved.  ``force``
    bypasses the skip entirely (re-attempt the shard).
    """
    if force:
        return False
    prev: _orch.ManifestRow | None = manifest.lookup(row_key)
    if prev is None:
        return False
    if prev.capture_status in (_orch.CaptureStatus.CAPTURED.value, _orch.CaptureStatus.EMPTY_CONFIRMED.value):
        return True
    # expected_unattempted with EXPECTED_* reason = known-empty sentinel → skip (same as empty_confirmed)
    if prev.capture_status == _orch.CaptureStatus.EXPECTED_UNATTEMPTED.value:
        return (prev.error_reason or "").startswith("EXPECTED_")
    return False


def filter_instruments_by_date(
    records: list,
    date_dt: _orch.datetime,
    defi_venues: frozenset[str] | None = None,
) -> list:
    """Return only instruments active on the given UTC datetime.

    An instrument is active on `date_dt` when:
    - available_from_datetime is None OR available_from_datetime <= date_dt
    - available_to_datetime   is None OR available_to_datetime   >= date_dt

    This is required because URDI adapters return the full historical universe.
    function reduces them to only the instruments tradeable on the requested day.

    Args:
        records: InstrumentRecord list from URDI.
        date_dt: UTC datetime representing the requested processing date.
        defi_venues: Optional set of DeFi venue names (uppercase). When provided,
            a WARNING is emitted for any DeFi instrument where available_from_datetime=None
            because on-chain creation timestamps are expected for all DeFi instruments
            and absence indicates the URDI adapter did not provide them (data quality
            is degraded — the instrument will still be included but with unknown
            listing date).
    """
    result = []
    for r in records:
        since: _orch.datetime | None = getattr(r, "available_from_datetime", None)
        until: _orch.datetime | None = getattr(r, "available_to_datetime", None)
        since_ok = since is None or since <= date_dt
        until_ok = until is None or until >= date_dt
        if since_ok and until_ok:
            if defi_venues is not None and since is None:
                venue = (getattr(r, "venue", None) or "").upper()
                if venue in defi_venues:
                    key = getattr(r, "instrument_key", repr(r))
                    _orch.logger.error(
                        "DeFi instrument %s has available_from_datetime=None — "
                        "URDI adapter MUST provide creation timestamp "
                        "(protocol floor date or on-chain); "
                        "instrument included but date accuracy is UNKNOWN",
                        key,
                    )
            result.append(r)
    return result


#: Known test / placeholder instrument BASES that leak from venue test listings
#: (e.g. exchange-internal smoke symbols). Compared case-insensitively against the
#: record's base_asset / raw_symbol / instrument_key. Non-ASCII rejection (below)
#: already catches the CJK/meme junk (龙虾 / 币安人生 / 我踏马来了); this set covers
#: ASCII placeholders venues occasionally list. Extend as new junk is observed.
_KNOWN_TEST_BASES: frozenset[str] = frozenset({"TEST", "TESTUSDT", "DUMMY", "PLACEHOLDER"})


def _is_junk_instrument(record: object) -> tuple[bool, str]:
    """Return ``(is_junk, reason)`` for a fetched instrument record (§1.5 noise guard).

    A capture-correctness reject (G1.4): an instrument whose identity carries a
    NON-ASCII character (the 2026-06-24 audit found CJK/meme test bases —
    龙虾/币安人生/我踏马来了 — on BINANCE/BITGET/ASTER) or a known ASCII test base is
    junk and must NOT enter ``by_date/``. Checks ``base_asset``, ``raw_symbol`` and
    ``instrument_key`` so the reject fires regardless of which field carries the junk.
    Pure + side-effect-free (the caller logs + drops).
    """
    base = (getattr(record, "base_asset", None) or "").strip()
    raw_symbol = (getattr(record, "raw_symbol", None) or "").strip()
    key = (getattr(record, "instrument_key", None) or "").strip()
    for field in (base, raw_symbol, key):
        if field and not field.isascii():
            return True, f"non-ascii ({field!r})"
    if base.upper() in _KNOWN_TEST_BASES:
        return True, f"known-test-base ({base!r})"
    return False, ""


def reject_junk_instruments(records: list) -> list:
    """Drop junk/test/non-ASCII instruments at capture time (§1.5 noise guard, G1.4).

    Applied to EVERY asset group right after the date filter, so junk symbols never
    reach ``by_date/`` (and therefore never reach the catalogue roll-up / coverage /
    MTDS). Rejected records are logged at WARNING with the reason; the surviving
    list is returned. A no-op when no junk is present.
    """
    kept: list = []
    rejected = 0
    for r in records:
        is_junk, reason = _is_junk_instrument(r)
        if is_junk:
            rejected += 1
            _orch.logger.warning(
                "Junk-symbol reject (§1.5/G1.4): venue=%s instrument_key=%s — %s",
                getattr(r, "venue", "?"),
                getattr(r, "instrument_key", "?"),
                reason,
            )
            continue
        kept.append(r)
    if rejected:
        _orch.logger.info("Junk-symbol guard: rejected %d junk/test instrument(s)", rejected)
    return kept


def get_venues_for_asset_groups(asset_groups: list[str]) -> list[str]:
    """Return the IS instrument-fetch venue list for the requested asset groups (CEFI, DEFI, …).

    CEFI: applies ``expand_cefi_tardis_endpoints`` to ``VENUES_BY_ASSET_GROUP["cefi"]`` so
        bare UAC canonical aliases (``OKX``) are expanded to the Tardis
        per-endpoint forms IS actually fetches from.  ``KALSHI-PERP`` and
        ``POLYMARKET-PERP`` are included automatically (they are UAC cefi venues).

    TRADFI: reads ``VENUES_BY_ASSET_GROUP["tradfi"]`` minus ``_TRADFI_NON_VENUE_KEYS``
        (currently ``YAHOO_FINANCE`` — a legacy source-as-venue artifact, NOT a real
        fetchable venue; see UAC market_data_categories.py ~line 293).

    DEFI: uses the assembled ``_DEFI_VENUES`` list built from ``_build_defi_venues()``
        (subgraph-derived union static union Solana). Decision D (operator 2026-06-29): IS
        keeps its own assembled subset; not a direct UAC read for defi.

    SPORTS: IS owns reference-data providers (API_FOOTBALL/FOOTYSTATS/UNDERSTAT/
        TRANSFERMARKT/SOCCER_FOOTBALL_INFO/OPEN_METEO).  These are DISJOINT from the
        UAC sports venues (ODDS_API/PINNACLE/BETFAIR*/DRAFTKINGS/FANDUEL), which are
        odds/market-data venues that live in MTDS, not IS.  Decision C (operator
        2026-06-29): two separate registries; this list is IS-owned and EXEMPT from the
        set-equality invariant with UAC.

    PREDICTION: reads ``VENUES_BY_ASSET_GROUP["prediction"]`` directly (UAC SSOT).
        Previously a local literal ``["POLYMARKET", "KALSHI"]``; now UAC-sourced so any
        new prediction venues added to UAC are auto-included.
    """
    venues: list[str] = []
    for cat in asset_groups:
        cat_upper = cat.upper()
        if cat_upper in ("CEFI", "ALL"):
            venues.extend(expand_cefi_tardis_endpoints(VENUES_BY_ASSET_GROUP["cefi"]))
        if cat_upper in ("TRADFI", "ALL"):
            venues.extend(v for v in VENUES_BY_ASSET_GROUP["tradfi"] if v not in _TRADFI_NON_VENUE_KEYS)
        if cat_upper in ("DEFI", "ALL"):
            venues.extend(_orch._DEFI_VENUES)
        if cat_upper in ("SPORTS", "ALL"):
            # IS owns fixtures + slow-moving reference data (teams, leagues, players,
            # referees, venues) via API-Football and enrichment providers.
            # Betting market instruments (the actual tradeable positions) come from
            # market-tick-data-service via Odds API — documented exception because
            # markets are only discoverable alongside odds data.  The UAC sports venues
            # (ODDS_API/PINNACLE/BETFAIR*/DRAFTKINGS/FANDUEL) are MTDS-owned, not IS.
            # Enrichment providers (no instruments — reference data for features):
            # FootyStats (match stats), Understat (xG), Transfermarkt (player values),
            # SoccerFootball.info (standings), Open-Meteo (weather).
            venues.extend(
                [
                    "API_FOOTBALL",
                    "FOOTYSTATS",
                    "UNDERSTAT",
                    "TRANSFERMARKT",
                    "SOCCER_FOOTBALL_INFO",
                    "OPEN_METEO",
                ]
            )
        if cat_upper in ("PREDICTION", "ALL"):
            # Prediction markets (binary / multi-outcome): POLYMARKET + KALSHI.
            # Sourced from UAC VENUES_BY_ASSET_GROUP["prediction"] (was a local literal
            # — wired to UAC per instrument_universe_registry_consolidation_2026_06_29).
            # No auth required — Gamma API (Polymarket) and public API (Kalshi) are keyless.
            venues.extend(VENUES_BY_ASSET_GROUP["prediction"])
    return list(dict.fromkeys(venues))


def is_venue_available(venue: str, date: str) -> bool:
    """Return True if the venue's discovery API can produce instruments on this date.

    Uses ``get_instrument_discovery_start`` rather than raw ``venue_start_dates``
    so HYPERLIQUID (and any future venue with a discovery-API gap narrower than
    its market-data archive) gates on the date the discovery endpoint actually
    has data — not the market-data archive earliest date. Pre-2026-05-05 this
    used ``venue_start_dates["HYPERLIQUID"] = 2023-04-15`` and produced 200
    phantom ``attempted_failed`` rows for the April-October 2023 window where
    the discovery API legitimately returns nothing.
    """
    launch_date = _orch._VENUE_MAPPING.get_instrument_discovery_start(venue)
    if launch_date is None:
        return True  # Unknown venue — assume always available
    return date >= launch_date


def earliest_venue_date(venues: list[str]) -> str | None:
    """Return the earliest discovery-start date across the given venues, or None."""
    dates = [d for v in venues if (d := _orch._VENUE_MAPPING.get_instrument_discovery_start(v)) is not None]
    return min(dates) if dates else None
