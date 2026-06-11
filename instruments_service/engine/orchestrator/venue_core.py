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

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_CEFI_VENUES",
    "_SPORTS_PROVIDER_VENUES",
    "_TRADFI_VENUES",
    "_VENUE_ADAPTER_EPOCH",
    "_get_venue_epoch",
    "_should_skip_shard",
    "earliest_venue_date",
    "filter_instruments_by_date",
    "get_venues_for_asset_groups",
    "is_venue_available",
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


_CEFI_VENUES: list[str] = [
    "BINANCE-SPOT",
    "BINANCE-FUTURES",
    "BYBIT",
    # OKX: 3 separate Tardis exchanges — okex (spot), okex-swap (perps), okex-futures (fixed-expiry)
    # Do NOT add bare "OKX" — it maps to same Tardis exchange as OKX-SPOT (duplicate data).
    "OKX-SPOT",
    "OKX-SWAP",
    "OKX-FUTURES",
    "DERIBIT",
    # DERIBIT-COMBO: live multi-leg options strategies (straddles, strangles, spreads, condors).
    # Historical combos are covered by DERIBIT → Tardis. This venue fetches LIVE active combos
    # from the Deribit public REST API (kind=combo, expired=false).
    "DERIBIT-COMBO",
    "COINBASE-SPOT",
    "HYPERLIQUID",
    "UPBIT",
    "ASTER",
    # Tier-3 CeFi (Tardis archive — factory entries exist, added to orchestrator 2026-05-12)
    "KRAKEN-FUTURES",
    "BITFINEX-FUTURES",
    # Tier-3 CeFi (Tardis archive — added 2026-05-22: factory entries existed, missing from batch)
    "BITGET-SPOT",
    "BITGET-FUTURES",
    "BITFINEX-SPOT",
]


_TRADFI_VENUES: list[str] = [
    "CME",
    "NASDAQ",
    "NYSE",
    "CBOE",
    "ICE",
    "FX",
]


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


def get_venues_for_asset_groups(asset_groups: list[str]) -> list[str]:
    """Return UAC canonical venue names for the requested asset groups (CEFI, DEFI, …)."""
    venues: list[str] = []
    for cat in asset_groups:
        cat_upper = cat.upper()
        if cat_upper in ("CEFI", "ALL"):
            venues.extend(_orch._CEFI_VENUES)
        if cat_upper in ("TRADFI", "ALL"):
            venues.extend(_orch._TRADFI_VENUES)
        if cat_upper in ("DEFI", "ALL"):
            venues.extend(_orch._DEFI_VENUES)
        if cat_upper in ("SPORTS", "ALL"):
            # instruments-service owns fixtures + slow-moving reference data
            # (teams, leagues, players, referees, venues) via API-Football.
            # Betting market instruments (the actual tradeable positions) come from
            # market-tick-data-service via Odds API — documented exception because
            # markets are only discoverable alongside odds data.
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
            # POLYMARKET + KALSHI: prediction market instruments (crypto up/down, soccer, macro).
            # No auth required — Gamma API (Polymarket) and public API (Kalshi) are keyless.
            venues.extend(["POLYMARKET", "KALSHI"])
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
