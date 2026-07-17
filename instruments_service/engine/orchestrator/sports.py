"""Sports canonicalisation helpers: league ids, API-Football lifecycle columns, fixture flattening, core-entity caches, per-league date skip policy.

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

import functools as _functools
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_af_id_from_canonical",
    "_canonical_league_id",
    "_empty_lifecycle_columns",
    "_flatten_canonical_fixture_for_disk",
    "_is_in_canonical_write_universe",
    "_lifecycle_columns_from_af_response",
    "_pipeline_mode_for_sports_data_type",
    "_set_cached_leagues",
    "_set_cached_standings",
    "_set_cached_teams",
    "_should_skip_date_for_per_league",
]


def _pipeline_mode_for_sports_data_type(data_type: str) -> _orch.PipelineMode:
    """Return the batch ``PipelineMode`` for a sports/prediction data_type.

    Used by orchestrator code paths that emit per-data_type manifest rows in
    loops (zero-fixture fast-path, sports reference data fan-out, enrichment
    fast-path). Raises ``KeyError`` for unknown data_types — keeps the
    enum-set wired and surfaces typos at runtime instead of silently picking
    an empty string.
    """

    return _orch._SPORTS_DATA_TYPE_TO_PIPELINE_MODE[data_type.upper()]


def _canonical_league_id(lid_raw: object) -> str:
    """Normalize a league_id at write time to canonical form.

    Used at every per-league GCS partition write so future rows are born
    with canonical league_ids (CF-7 write-path). Two passes:

    Pass 1 — numeric resolution: all-digits string → look up via UAC
    ``get_league_by_api_football_id`` (e.g. "39" → "EPL"). Unknown
    numerics fall through unchanged.

    Pass 2 — provider-suffix strip: delegate to UAC
    ``canonicalize_league_id`` which conservatively strips a trailing
    ``_<digits>`` segment *only* when the suffix is provably a registered
    provider ID for the base key (e.g. "EPL_39" → "EPL",
    "SCOTTISH_LEAGUE_CUP_185" → "SCOTTISH_LEAGUE_CUP"). Unresolved keys
    pass through unchanged — non-lossy by design (CF-7 spec).

    - already-canonical (e.g. "EPL") → pass through both passes unchanged
    - provider-suffixed (e.g. "EPL_39") → Pass 2 strips suffix → "EPL"
    - numeric (e.g. "39") → Pass 1 resolves → "EPL" → Pass 2 no-op
    - unknown numeric (e.g. "9999") → Pass 1 no-op → Pass 2 no-op → "9999"
    """
    s = str(lid_raw).strip()
    # Pass 1: numeric → canonical via api_football id lookup
    if s and s.isdigit():
        league = _orch.get_league_by_api_football_id(int(s))
        s = league.league_id if league is not None else s
    # Pass 2: strip provider-id suffix via UAC canonicalizer (CF-7 write-path)
    return _orch._uac_canonicalize_league_id(s)


# Cached set of canonical league_ids we actually track for api_football — the
# WRITE-UNIVERSE gate. The date-wide adapter calls (get_injuries(date), the
# fixtures roll-up, standings) return data for the ENTIRE api_football universe
# (~1200 leagues); without this gate the per-league capture loops write a row for
# every returned league — including the ~1100 leagues OUTSIDE our canonical set,
# which (having no canonical mapping) stay NUMERIC-keyed forever and pollute the
# manifest with two schemas (incident 2026-06-24: 1.68M of 4.6M sports _index rows
# were out-of-universe). The honest-absence emit (emit_empty_gaps_for_entity)
# already scopes to this same set; the CAPTURE path must too. SSOT:
# unified_api_contracts.sports.get_expected_leagues_for_source.
@_functools.lru_cache(maxsize=1)
def _expected_af_league_ids() -> frozenset[str]:
    """Memoised set of canonical league_ids we track for api_football."""
    return frozenset(lg.league_id for lg in _orch.get_expected_leagues_for_source("api_football"))


def _is_in_canonical_write_universe(canonical_league_id: str) -> bool:
    """True if ``canonical_league_id`` is a league we track for api_football.

    Used to GATE per-league captured writes so out-of-universe (and therefore
    un-canonicalisable / numeric-keyed) leagues from the date-wide adapter calls
    are never written as captured rows — keeping the manifest single-schema. The
    expected-league set is cached (the adapter call fans out per (date, league),
    so this is hit thousands of times per run). An empty/unknown id is treated as
    NOT in-universe (the bare-path fallback already drops blank-league rows).
    """
    return bool(canonical_league_id) and canonical_league_id in _expected_af_league_ids()


def _af_id_from_canonical(obj: object) -> int | None:
    """Extract API-Football numeric ID from a CanonicalLeague / CanonicalTeam / CanonicalVenue.

    Tries ``api_football_id`` attribute first, falls back to ``logo_url``
    pattern parsing (legacy normalizer output).
    """
    af_id = getattr(obj, "api_football_id", None)
    if isinstance(af_id, int):
        return af_id
    if af_id is not None:
        try:
            return int(af_id)
        except (TypeError, ValueError):
            pass
    logo_url = getattr(obj, "logo_url", None)
    if isinstance(logo_url, str):
        match = _orch._AF_LOGO_RE.search(logo_url)
        if match is not None:
            return int(match.group(1))
    return None


def _empty_lifecycle_columns() -> dict[str, object]:
    """Return the Q5/Q6 columns all defaulted — used when no af_response given.

    ``went_to_*`` default False (not None) to match the
    ``CanonicalFixtureOutcomes`` defaults + the contract (a regulation match
    definitively did NOT go to ET/pens). Every other Q5/Q6 column defaults None.
    """
    cols: dict[str, object] = dict.fromkeys(_orch._Q5_SCHEDULE_COLUMNS)
    cols.update(dict.fromkeys(_orch._Q6_OUTCOME_COLUMNS))
    cols["went_to_extra_time"] = False
    cols["went_to_penalties"] = False
    return cols


def _lifecycle_columns_from_af_response(af_response: dict[str, object]) -> dict[str, object]:
    """Build the Q5/Q6 columns from a raw api-football fixture response dict.

    Delegates ALL parsing to the UTL SSOT
    ``unified_trading_library.fixtures.extract_match_lifecycle`` (no re-derivation
    here) and maps the typed ``MatchLifecycle`` onto the flat SPORTS_FIXTURES
    column names. Regulation matches yield ET/PEN columns = None; ET/PEN matches
    populate the score-distinction + ``went_to_*`` columns (ET/PEN *timestamps*
    stay None — the AF fixtures endpoint does not emit them).

    Defensive: a malformed af_response (missing fixture id / kickoff) raises
    inside ``extract_match_lifecycle``; the caller (flatten) swallows it back to
    the empty defaults so one bad fixture never fails the whole shard.
    """
    lifecycle = _orch.extract_match_lifecycle(af_response)
    schedule = lifecycle.schedule
    outcomes = lifecycle.outcomes
    return {
        # Q5 — phase timestamps.
        "halftime_start_time": schedule.halftime_start_time,
        "halftime_end_time": schedule.halftime_end_time,
        "extra_time_first_half_start_time": schedule.extra_time_first_half_start_time,
        "extra_time_first_half_end_time": schedule.extra_time_first_half_end_time,
        "extra_time_second_half_start_time": schedule.extra_time_second_half_start_time,
        "extra_time_second_half_end_time": schedule.extra_time_second_half_end_time,
        "penalty_shootout_start_time": schedule.penalty_shootout_start_time,
        "penalty_shootout_end_time": schedule.penalty_shootout_end_time,
        "whistle_full_time_at": schedule.whistle_full_time_at,
        # Q6 — score distinction.
        "home_score_regulation": outcomes.home_score_regulation,
        "away_score_regulation": outcomes.away_score_regulation,
        "home_score_after_extra_time": outcomes.home_score_after_extra_time,
        "away_score_after_extra_time": outcomes.away_score_after_extra_time,
        "home_score_after_penalty_shootout": outcomes.home_score_after_penalty_shootout,
        "away_score_after_penalty_shootout": outcomes.away_score_after_penalty_shootout,
        "home_penalty_shootout_score": outcomes.home_penalty_shootout_score,
        "away_penalty_shootout_score": outcomes.away_penalty_shootout_score,
        "went_to_extra_time": outcomes.went_to_extra_time,
        "went_to_penalties": outcomes.went_to_penalties,
        # ``match_result`` is a closed-set StrEnum — persist its string value
        # (or None when not yet decided / not finished).
        "match_result": outcomes.match_result.value if outcomes.match_result is not None else None,
    }


def _round_from_af_response(af_response: dict[str, object] | None) -> str:
    """Extract API-Football's ``league.round`` from the raw fixture response.

    ``round`` is the competition-phase label ("Regular Season - 30", "Relegation
    Round", "Quarter-finals") — the SOLE input to
    ``adapters/sports/competition_phase.classify_competition_phase``, which the
    ML pipeline uses to separate relegation/playoff/knockout dynamics from
    ordinary league games. The CanonicalFixture model does NOT carry it (nor
    does the ``ApiFootballLeague`` schema), so the previous
    ``getattr(fx, "round", "")`` defaulted every row to "" — leaving
    ``competition_phase`` UNKNOWN and ``is_promotion_relegation`` a false-not-null
    everywhere (issue: sports_fixture_round_not_captured_competition_phase_
    unknown_2026_07_17). But the value is present all along in the RAW response
    dict ``af_response`` (which the writer already threads for the Q5/Q6 overlay
    via ``_lifecycle_columns_from_af_response``): api-football nests it at
    ``item["league"]["round"]``. Read it there — same source, same pattern, no
    new fetch. Empty string on any absence (no raw / no league block / no round
    key) — honest blank, never a fabricated placeholder."""
    if not af_response:
        return ""
    league = af_response.get("league")
    if not isinstance(league, dict):
        return ""
    rnd = league.get("round")
    return str(rnd).strip() if rnd is not None else ""


def _flatten_canonical_fixture_for_disk(
    fx: object,
    day: str,
    af_response: dict[str, object] | None = None,
) -> dict[str, object]:
    """Flatten a CanonicalFixture into a dict matching SPORTS_FIXTURES SchemaContract.

    Returns the full flat schema (43 columns). ``round`` comes from the raw
    ``af_response`` (``league.round``) via :func:`_round_from_af_response` — the
    CanonicalFixture model does not carry it; ``status_long`` is still defaulted
    (it is genuinely absent from both the model and the raw fixture block).

    Q5/Q6 lifecycle columns (HT/ET/PEN phase timestamps + score distinction +
    ``went_to_*`` + ``match_result``) are populated from ``af_response`` when the
    raw api-football fixture dict is supplied (the writer threads it through from
    the adapter), via the UTL ``extract_match_lifecycle`` SSOT. When
    ``af_response`` is None (legacy callers / no raw available) those columns take
    their honest defaults — None for every score / timestamp and False for
    ``went_to_extra_time`` / ``went_to_penalties``. This is purely ADDITIVE: the
    legacy ``home_score_extratime`` / ``home_score_penalty`` / ``periods_*``
    columns are unchanged (still None unless a sibling writer fills them).

    A malformed ``af_response`` (e.g. missing fixture id) does NOT fail the shard
    — the overlay falls back to the empty defaults and logs a warning.
    """
    home_team = getattr(fx, "home_team", None)
    away_team = getattr(fx, "away_team", None)
    league = getattr(fx, "league", None)
    venue = getattr(fx, "venue", None)
    referee = getattr(fx, "referee", None)
    kickoff = getattr(fx, "kickoff_utc", None)
    home_goals = getattr(fx, "home_goals", None)
    away_goals = getattr(fx, "away_goals", None)
    af_home_id = _orch._af_id_from_canonical(home_team) if home_team is not None else None
    af_away_id = _orch._af_id_from_canonical(away_team) if away_team is not None else None
    af_winner_id: int | None = None
    if home_goals is not None and away_goals is not None and home_goals != away_goals:
        af_winner_id = af_home_id if home_goals > away_goals else af_away_id

    raw_fid = getattr(fx, "source_fixture_id", None) or getattr(fx, "fixture_id", None)
    try:
        af_fixture_id = int(raw_fid) if raw_fid is not None else None
    except (TypeError, ValueError):
        af_fixture_id = None

    season_raw = getattr(fx, "season", None)
    try:
        season_int = int(str(season_raw).split("-")[0]) if season_raw is not None else None
    except (TypeError, ValueError):
        season_int = None

    # Q5/Q6 lifecycle overlay — populated from the raw api-football response when
    # available, else honest defaults. An empty dict (the base adapter's default
    # for non-api-football sources) is treated as "no raw" → silent defaults.
    # Wrapped so one malformed fixture can never fail the shard (CLAUDE.md
    # shard-level failure isolation).
    if af_response:
        try:
            lifecycle_cols = _orch._lifecycle_columns_from_af_response(af_response)
        except Exception as _lc_exc:
            _orch.logger.warning(
                "extract_match_lifecycle overlay failed for fixture %s — defaulting Q5/Q6 columns: %s",
                af_fixture_id,
                _lc_exc,
            )
            lifecycle_cols = _orch._empty_lifecycle_columns()
    else:
        lifecycle_cols = _orch._empty_lifecycle_columns()

    row: dict[str, object] = {
        "af_fixture_id": af_fixture_id,
        "referee_name": getattr(referee, "name", None) if referee is not None else None,
        "date": kickoff.date().isoformat() if kickoff is not None else day,
        "timestamp": kickoff.isoformat() if kickoff is not None else None,
        "periods_first": None,
        "periods_second": None,
        "venue_id": _orch._af_id_from_canonical(venue) if venue is not None else None,
        "venue_name": getattr(venue, "name", None) if venue is not None else None,
        "venue_city": getattr(venue, "city", None) if venue is not None else None,
        "status_long": getattr(fx, "status", None) or "Unknown",
        "status_short": getattr(fx, "status", None) or "NS",
        "status_elapsed_time": None,
        "af_league_id": _orch._af_id_from_canonical(league) if league is not None else None,
        "season": season_int,
        # From the raw af_response's league.round (see _round_from_af_response) —
        # NOT getattr(fx, "round") (CanonicalFixture has no such field, so that
        # was "" on every row). Falls back to any fx.round for non-af sources.
        "round": _round_from_af_response(af_response) or (getattr(fx, "round", "") or ""),
        "af_home_id": af_home_id,
        "af_away_id": af_away_id,
        "af_winner_id": af_winner_id,
        "af_home_name": getattr(home_team, "name", "") if home_team is not None else "",
        "af_away_name": getattr(away_team, "name", "") if away_team is not None else "",
        "home_score": home_goals,
        "away_score": away_goals,
        "home_score_halftime": getattr(fx, "home_goals_halftime", None),
        "away_score_halftime": getattr(fx, "away_goals_halftime", None),
        "home_score_fulltime": home_goals,
        "away_score_fulltime": away_goals,
        "home_score_extratime": None,
        "away_score_extratime": None,
        "home_score_penalty": None,
        "away_score_penalty": None,
        "day": day,
        "available_at": None,  # caller post-fills with kickoff_utc - 7 days
        "match_end_time": getattr(fx, "match_end_time", None),
        "announced_at": getattr(fx, "announced_at", None),
        "report_time": getattr(fx, "report_time", None),
    }
    row.update(lifecycle_cols)
    return row


def _set_cached_leagues(df: _orch.pd.DataFrame) -> None:
    _orch._cached_leagues_df = df


def _set_cached_teams(df: _orch.pd.DataFrame, league_ids: list[int]) -> None:
    _orch._cached_teams_df = df
    _orch._cached_prediction_league_ids = league_ids


def _set_cached_standings(df: _orch.pd.DataFrame) -> None:
    _orch._cached_standings_df = df


def _should_skip_date_for_per_league(
    manifest: _orch.ManifestWriter,
    *,
    date: str,
    data_type: str,
    expected_canonical_leagues: list[str],
    force: bool,
) -> bool:
    """Return True only when every expected canonical league is already
    captured / empty_confirmed for this date.

    The plain ``_should_skip_shard`` matches on ``(date, data_type)`` only
    and returns True if ANY league has a row for this date — that's wrong
    for per-league entities: if EPL was captured for date X but LA_LIGA
    wasn't, the orchestrator would still skip and never re-fetch LA_LIGA.

    Pre-fix incident (2026-05-05): MATCHES capped at 18% UI coverage even
    after fs-backfill ran for hours, because most dates had at least one
    league captured early on and the date-level skip prevented backfilling
    the rest. PREDICTIONS / ODDS share the same pattern; ODDS happened to
    look healthy because of dense empty_confirmed writes (the bulk
    /todays-matches endpoint returns odds for many leagues; matches not
    necessarily). Same pattern, same fix.

    ``force`` bypasses the skip entirely.

    MEMORY (2026-06-22 OOM fix): this reads the consolidated availability
    index **ONCE** and resolves every expected league in-memory, instead of
    calling ``manifest.lookup()`` once per league. ``lookup()`` re-reads the
    whole index per call (``read_availability_index``), and the in-process
    cache has a 60s TTL that expires mid-date under the throttled per-league
    fan-out — so the per-league loop re-decompressed the 80 MB sports index
    into a ~6.5 GB pandas frame (4.1M rows x 41 cols) up to 93x per date,
    with multiple multi-GB frames live at once -> OOM (exit 137) within ~2
    dates even on 32 GB. The single-read membership check resolves all
    leagues from one frame load. SSOT:
    ``plans/active/sports_reference_backfill_oom_2026_06_22.md``.
    """
    if force or not expected_canonical_leagues:
        return False

    bucket = manifest.catalogue_bucket
    if not bucket:
        # No bucket = no index = nothing captured yet; never skip.
        return False

    # ONE index read (TTL-cached) — NOT one per league.
    index = _orch.read_availability_index(bucket)
    if index.empty:
        return False

    # Scope to this service + this (date, data_type) shard, exactly as
    # ``ManifestWriter.lookup`` would (service_name filter + exact-match on
    # the specified row-key dims). Build a per-league status map; last row
    # wins (matches ``lookup``'s ``iloc[-1]`` last-write-wins semantics).
    _df = index
    if "service_name" in _df.columns:
        _df = _df[_df["service_name"] == manifest.service_name]
    for _col, _want in (("date", date), ("data_type", data_type)):
        if _col not in _df.columns:
            # Legacy parquet missing a dim we filter on → cannot prove
            # coverage; do not skip.
            return False
        _df = _df[_df[_col].fillna("").astype(str) == _want]
    if _df.empty or "league_id" not in _df.columns:
        return False

    _captured = _orch.CaptureStatus.CAPTURED.value
    _empty = _orch.CaptureStatus.EMPTY_CONFIRMED.value
    _expected_unattempted = _orch.CaptureStatus.EXPECTED_UNATTEMPTED.value
    _status_by_league: dict[str, tuple[str, str]] = {}
    for _lid_val, _status_val, _reason_val in zip(
        _df["league_id"].fillna("").astype(str),
        _df["capture_status"].fillna("").astype(str) if "capture_status" in _df.columns else [""] * len(_df),
        _df["error_reason"].fillna("").astype(str) if "error_reason" in _df.columns else [""] * len(_df),
        strict=False,
    ):
        # last write wins (dict assignment overwrites earlier rows)
        _status_by_league[_lid_val] = (_status_val, _reason_val)

    for lid in expected_canonical_leagues:
        prev = _status_by_league.get(lid)
        if prev is None:
            return False
        _status, _reason = prev
        if _status in (_captured, _empty):
            continue
        # expected_unattempted + EXPECTED_* reason = known-empty → treat as covered
        if _status == _expected_unattempted and _reason.startswith("EXPECTED_"):
            continue
        return False
    # All expected canonical leagues are already captured/empty_confirmed for this date — the
    # per-league preflight short-circuits. Emit PREFLIGHT_SKIPPED (STEP 5.64 observability parity):
    # without it a silent skip is indistinguishable from a silent success in the event stream.
    _orch.emit_preflight_skip(
        _orch.PreflightSkipReason.SHARD_ALREADY_FRESH,
        asset_group="sports",
        feature_group=data_type,
        date=date,
        shard="all_expected_canonical_leagues",
        message=f"per-league preflight: all {len(expected_canonical_leagues)} expected leagues already captured",
    )
    return True
