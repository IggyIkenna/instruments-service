"""Prediction (Polymarket / Kalshi) SOCCER fixture-id match — resolver + side-table.

Threads the canonical API-Football fixture identity onto prediction soccer
markets as ADDITIVE, honest-absence attributes, mirroring the odds-tick
``af_fixture_id`` join MTDS already ships
(``market-tick-data-service/.../adapters/sports/fixture_id_resolver.py`` +
``instruments-service/docs/SPORTS_INSTRUMENTS.md`` "odds↔instruments row-level
join-key gap"). Plan: ``prediction_consolidated_closeout_2026_07_18.md`` A4 /
Phase-E Leg-1.

Prediction-specific by construction — NOTHING here touches the shared UAC
``InstrumentRecord`` schema or the shared IS serialisation path. The six
resolved attributes (``af_league_id``, ``home_team_canonical_id``,
``away_team_canonical_id``, ``fixture_date``, ``af_fixture_id``,
``af_fixture_match_status``) are stamped into a package-level side-table keyed
by ``instrument_key`` — the SAME carrier pattern the polymarket package already
uses for ``clob_token_ids``
(``polymarket/markets.py::_CLOB_TOKEN_IDS_BY_CONDITION_ID``, joined into the
parquet by ``engine/orchestrator/process_write.py::_records_to_dataframe``).
Materialising these as parquet columns is that same downstream join and is
DEFERRED here: ``_records_to_dataframe`` is shared IS orchestrator infra, out of
this increment's prediction-specific scope.

Reuse, not reinvention — no new GCS walk or team-matching heuristic:

* ``unified_api_contracts.sports.candidate_parquet_paths()`` — the SPORTS GCS
  path SSOT (pipeline_mode-aware canonical path first, legacy fallbacks after),
  the SAME probe order MTDS's ``FixtureIdResolver`` uses.
* ``validate_team_resolution()`` — the SAME provider-agnostic universal team
  alias index the MTDS odds side canonicalises through, so a Polymarket-derived
  and an API-Football-derived canonical team id for the same real club are the
  same string (that shared namespace is what makes the join key line up).

``af_fixture_id`` stays a genuine nullable int (no sentinel); the companion
``FixtureMatchStatus`` closed-set string makes an attempt-with-no-match
distinguishable from a genuine null.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date

from unified_api_contracts import PipelineMode
from unified_api_contracts.sports import candidate_parquet_paths, get_league

logger = logging.getLogger(__name__)

__all__ = [
    "FixtureMatchAttributes",
    "FixtureMatchStatus",
    "PredictionFixtureResolver",
    "fixture_match_for_instrument_key",
    "football_league_for_sports_underlying",
    "parse_kalshi_soccer_participants",
    "register_fixture_match",
    "reset_fixture_match_registry",
]


class FixtureMatchStatus:
    """Closed set of outcomes for a prediction-market → ``af_fixture_id`` join.

    Identical taxonomy to MTDS's ``fixture_id_resolver.FixtureMatchStatus`` so
    prediction rows and odds rows classify the same real outcome the same way.
    """

    MATCHED = "MATCHED"
    UNRESOLVED_TEAM_NAME = "UNRESOLVED_TEAM_NAME"
    NO_FIXTURE_DATA = "NO_FIXTURE_DATA"


@dataclass(frozen=True)
class FixtureMatchAttributes:
    """The six additive fixture-match attributes stamped onto a soccer market.

    All fields are honest-absence: ``None`` when the source can't resolve one
    (no sentinel). ``af_fixture_match_status`` is the closed-set companion that
    distinguishes an attempt-with-no-match from a genuine null.
    """

    af_league_id: int | None
    home_team_canonical_id: str | None
    away_team_canonical_id: str | None
    fixture_date: str | None
    af_fixture_id: int | None
    af_fixture_match_status: str

    @classmethod
    def team_unresolved(
        cls,
        *,
        af_league_id: int | None,
        fixture_date: str | None,
        home_team_canonical_id: str | None = None,
        away_team_canonical_id: str | None = None,
    ) -> FixtureMatchAttributes:
        """Honest-absence record: the league + day are known but the team names
        did not canonicalise to the shared alias namespace (``af_fixture_id``
        cannot be attempted). This is the honest Kalshi-soccer status today —
        Kalshi carries city-level titles with no team registry (gated on the
        E2 team registry), so its home/away never resolve.
        """
        return cls(
            af_league_id=af_league_id,
            home_team_canonical_id=home_team_canonical_id,
            away_team_canonical_id=away_team_canonical_id,
            fixture_date=fixture_date,
            af_fixture_id=None,
            af_fixture_match_status=FixtureMatchStatus.UNRESOLVED_TEAM_NAME,
        )


def _af_league_id_for(canonical_league_id: str) -> int | None:
    """Best-effort canonical league_id → API-Football numeric league id (or None)."""
    league_def = get_league(canonical_league_id)
    if league_def is None:
        return None
    return league_def.api_football_id


_SPORTS_UNDERLYING_PREFIX = "SPORTS_"


def football_league_for_sports_underlying(sports_underlying: str) -> tuple[str, int | None] | None:
    """Map a prediction ``SPORTS_*`` underlying (e.g. ``"SPORTS_EPL"`` from
    ``underlying_for_group``) to ``(canonical_league_id, af_league_id)`` when it
    names a FOOTBALL (soccer) league in the registry, else ``None``.

    Facade-only (``get_league``); lets a caller that has already computed the
    canonical-question-group underlying (Kalshi ``_parse_market`` does) decide
    "is this a soccer market" + get its league ids without a deep import or a
    hardcoded football-group set that would drift as leagues are added.
    """
    if not sports_underlying.startswith(_SPORTS_UNDERLYING_PREFIX):
        return None
    league_token = sports_underlying[len(_SPORTS_UNDERLYING_PREFIX) :]
    league_def = get_league(league_token)
    if league_def is None or league_def.sport != "FOOTBALL":
        return None
    return league_def.league_id, league_def.api_football_id


# ── Kalshi soccer title → (home, away) participant parser ────────────────────
# Kalshi encodes a soccer GAME market's two clubs in the human ``title``, NOT in
# a team registry — the market ticker's team segment is a concatenation of
# variable-width 2-4 char codes (``LFCBRE`` = Liverpool+Brentford) that is NOT
# reliably splittable (measured live 2026-07-18 vs
# api.elections.kalshi.com/.../markets?series_ticker=KX{LEAGUE}GAME). The title
# is the deterministic participant source, but it carries a market-type suffix
# the club names must be stripped of before they can canonicalise through the
# SAME ``validate_team_resolution`` alias index the Polymarket path uses:
#   * ``"Liverpool vs Brentford Winner?"``            (EPL/BUN/LALIGA/SERIEA/LIGUE1 GAME)
#   * ``"Egnatia Rrogozhine vs NK Celje: Regulation Time Moneyline"`` (colon-suffixed)
# Splitting on " vs " and stripping that suffix turns Kalshi soccer titles from
# 0% team-matching into the alias-index namespace (E2 / Phase-E Leg-2).
_KALSHI_VS_SPLIT: re.Pattern[str] = re.compile(r"\s+vs\.?\s+|\s+v\.?\s+", re.IGNORECASE)
_KALSHI_MARKET_TYPE_SUFFIX: re.Pattern[str] = re.compile(
    r"\s*(?:winner|moneyline|regulation\s+time(?:\s+moneyline)?|result|to\s+win|"
    r"match|draw\s+no\s+bet|both\s+teams\s+to\s+score|btts)\s*$",
    re.IGNORECASE,
)


def _strip_kalshi_market_suffix(name: str) -> str:
    """Trim a Kalshi market-type qualifier (``Winner?`` / ``Moneyline`` / ``?``)
    off a title-derived club name so only the club token remains.
    """
    trimmed = name.strip().rstrip("?").strip()
    # Strip once, drop a possible bare ``?`` the suffix left, then strip again to
    # cover ``"… Winner?"`` (word before the ``?``) in a single pass-pair.
    trimmed = _KALSHI_MARKET_TYPE_SUFFIX.sub("", trimmed).strip().rstrip("?").strip()
    return _KALSHI_MARKET_TYPE_SUFFIX.sub("", trimmed).strip()


def parse_kalshi_soccer_participants(title: str | None) -> tuple[str, str] | None:
    """Extract the two clubs ``(home, away)`` from a Kalshi soccer market title.

    Returns ``None`` (→ caller keeps honest-absence ``UNRESOLVED_TEAM_NAME``) when
    the title is blank, carries no ``" vs "`` participant split, or either side is
    empty after the market-type suffix is stripped. The returned names are the raw
    venue renderings ("Liverpool", "Brentford") — canonicalisation to the shared
    alias namespace is the resolver's job (``validate_team_resolution``), so a
    Kalshi rendering that isn't in the alias index stays honestly unresolved
    rather than being force-slugged.

    Home/away order follows the title's left-to-right order (Kalshi soccer GAME
    titles render "Home vs Away"); the resolver's fixture lookup is keyed on that
    order, mirroring the Polymarket path.
    """
    if not title:
        return None
    # Drop a trailing ``": Regulation Time Moneyline"`` clause, then split on vs.
    cleaned = title.split(":", 1)[0].strip()
    parts = _KALSHI_VS_SPLIT.split(cleaned, maxsplit=1)
    if len(parts) != 2:
        return None
    home = _strip_kalshi_market_suffix(parts[0])
    away = _strip_kalshi_market_suffix(parts[1])
    if not home or not away:
        return None
    return home, away


class PredictionFixtureResolver:
    """Resolve (canonical_league_id, home, away, day) → ``af_fixture_id`` for a
    prediction soccer market, reading instruments-service's FIXTURES parquet.

    One instance is reused across an adapter run (the polymarket adapter builds
    it lazily). The per-(league, day) fixtures lookup is downloaded and
    canonicalised once, then reused for every market on that day — N markets on
    the same fixture day cost one GCS read, not N (same caching contract as
    MTDS's ``FixtureIdResolver``).

    ``resolve()`` NEVER raises — every transport / parse error degrades to an
    honest-absence status so it can run inside the hot adapter parse path
    without ever crashing capture.
    """

    def __init__(self, storage_client: object | None = None, bucket: str | None = None) -> None:
        self._client: object | None = storage_client
        self._bucket: str | None = bucket
        self._client_resolved: bool = storage_client is not None
        self._cache: dict[tuple[str, str], dict[tuple[str, str], int]] = {}

    def _ensure_client_and_bucket(self) -> tuple[object, str] | None:
        """Lazily acquire a UTL storage client + the sports instruments bucket.

        Returns ``None`` (→ honest NO_FIXTURE_DATA) when a client / bucket can't
        be obtained (e.g. no cloud creds in a unit-test context) rather than
        raising into the parse path.
        """
        if self._client is not None and self._bucket is not None:
            return self._client, self._bucket
        if self._client_resolved and self._client is None:
            return None
        self._client_resolved = True
        try:
            from unified_trading_library import (  # noqa: imports-inside-functions
                get_storage_client,
                is_gcp,
                resolve_bucket_name,
            )

            self._client = get_storage_client()
            self._bucket = resolve_bucket_name(
                cloud="gcp" if is_gcp() else "aws",
                kind="instruments-store",
                asset_group="sports",
            )
        except Exception as exc:  # degrade to honest-absence, never crash the parse path
            logger.debug("PredictionFixtureResolver: no storage client/bucket available: %s", exc)
            self._client = None
            self._bucket = None
            return None
        if self._client is None or self._bucket is None:
            return None
        return self._client, self._bucket

    def resolve(
        self,
        canonical_league_id: str,
        home_name: str,
        away_name: str,
        fixture_date: date,
    ) -> FixtureMatchAttributes:
        """Return the six fixture-match attributes for one soccer market.

        ``af_fixture_id`` is populated only on ``MATCHED``. Team names are
        canonicalised through the SAME ``validate_team_resolution`` universal
        alias index MTDS uses; a name that doesn't resolve yields
        ``UNRESOLVED_TEAM_NAME`` (no false match via a fallback slug).
        """
        af_league_id = _af_league_id_for(canonical_league_id)
        day_str = fixture_date.isoformat()

        from unified_api_contracts.external.api_football.team_mappings import (  # noqa: imports-inside-functions,qg-deep-import
            TeamResolutionError,
            validate_team_resolution,
        )

        try:
            home_cid = validate_team_resolution(home_name, provider="polymarket")
            away_cid = validate_team_resolution(away_name, provider="polymarket")
        except TeamResolutionError:
            # Neither side canonicalised — attempting the join with a fallback slug
            # could only ever collide by accident, never signal a real match.
            return FixtureMatchAttributes.team_unresolved(af_league_id=af_league_id, fixture_date=day_str)

        lookup = self._fixture_lookup(canonical_league_id, day_str)
        if not lookup:
            return FixtureMatchAttributes(
                af_league_id=af_league_id,
                home_team_canonical_id=home_cid,
                away_team_canonical_id=away_cid,
                fixture_date=day_str,
                af_fixture_id=None,
                af_fixture_match_status=FixtureMatchStatus.NO_FIXTURE_DATA,
            )

        fixture_id = lookup.get((home_cid, away_cid))
        status = FixtureMatchStatus.MATCHED if fixture_id is not None else FixtureMatchStatus.UNRESOLVED_TEAM_NAME
        return FixtureMatchAttributes(
            af_league_id=af_league_id,
            home_team_canonical_id=home_cid,
            away_team_canonical_id=away_cid,
            fixture_date=day_str,
            af_fixture_id=fixture_id,
            af_fixture_match_status=status,
        )

    def _fixture_lookup(self, canonical_league_id: str, day_str: str) -> dict[tuple[str, str], int]:
        """Return (and memoise) the {(home_cid, away_cid): af_fixture_id} lookup
        for one (league, day), downloading the FIXTURES parquet at most once.
        """
        cache_key = (canonical_league_id, day_str)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached
        lookup = self._load_fixture_lookup(canonical_league_id, day_str)
        self._cache[cache_key] = lookup
        return lookup

    def _load_fixture_lookup(self, canonical_league_id: str, day_str: str) -> dict[tuple[str, str], int]:
        """Download + parse instruments-service's FIXTURES parquet for (league, day).

        Probes every ``candidate_parquet_paths`` candidate in order (canonical
        pipeline_mode path first, legacy fallbacks after) — the SAME fallback
        chain MTDS's resolver + the reconciliation/audit tooling use, so this
        stays correct across the FIXTURES pipeline_mode migration.
        """
        acquired = self._ensure_client_and_bucket()
        if acquired is None:
            return {}
        client, bucket = acquired

        from io import BytesIO  # noqa: imports-inside-functions

        import pandas as pd  # noqa: imports-inside-functions

        for path in candidate_parquet_paths(
            "FIXTURES",
            day_str,
            canonical_league_id,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        ):
            try:
                raw = client.download_bytes(bucket, path)  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
            except Exception as exc:  # any transport error = "not this candidate"
                logger.debug("PredictionFixtureResolver: %s/%s unavailable: %s", bucket, path, exc)
                continue
            if not raw:
                continue
            try:
                df: pd.DataFrame = pd.read_parquet(BytesIO(raw))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            except Exception as exc:  # unparseable parquet → try the next candidate
                logger.warning("PredictionFixtureResolver: failed to parse %s/%s: %s", bucket, path, exc)
                continue
            if df.empty or "af_fixture_id" not in df.columns:  # pyright: ignore[reportUnknownMemberType]
                continue
            return _build_fixture_lookup(df)
        return {}


def _build_fixture_lookup(fixtures_df: object) -> dict[tuple[str, str], int]:
    """Project a FIXTURES DataFrame into {(home_cid, away_cid): af_fixture_id}.

    Canonicalises both fixture team names through
    ``validate_team_resolution(provider="api_football")`` — the SAME universal
    alias index the query (prediction-market) side resolves through — so a row
    is added only when BOTH names resolve; a fixture whose own team names don't
    resolve can never produce a false match (it is simply absent, read
    downstream as ``UNRESOLVED_TEAM_NAME``, an honest outcome). Byte-identical
    to MTDS's ``fixture_id_resolver._build_fixture_lookup``.
    """
    import pandas as pd  # noqa: imports-inside-functions
    from unified_api_contracts.external.api_football.team_mappings import (  # noqa: imports-inside-functions,qg-deep-import
        TeamResolutionError,
        validate_team_resolution,
    )

    df: pd.DataFrame = pd.DataFrame(fixtures_df)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    lookup: dict[tuple[str, str], int] = {}
    for _, row in df.iterrows():  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
        fid_raw: object = row.get("af_fixture_id")  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        if fid_raw is None or (isinstance(fid_raw, float) and pd.isna(fid_raw)):  # pyright: ignore[reportUnknownArgumentType]
            continue
        home_name: object = row.get("af_home_name")  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        away_name: object = row.get("af_away_name")  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        if not home_name or not away_name:
            continue
        try:
            home_cid = validate_team_resolution(str(home_name), provider="api_football")
            away_cid = validate_team_resolution(str(away_name), provider="api_football")
        except TeamResolutionError:
            continue
        try:
            lookup[(home_cid, away_cid)] = int(fid_raw)  # pyright: ignore[reportArgumentType]
        except (TypeError, ValueError):
            continue
    return lookup


# ── Per-instrument side-table (join carrier into the availability parquet) ───────
# Keyed by ``instrument_key`` (the wrapped condition_id / ticker == what
# ``_records_to_dataframe`` joins by), the SAME lifetime/bound class as
# ``_CLOB_TOKEN_IDS_BY_CONDITION_ID``. The (deferred) ``_records_to_dataframe``
# join reads this via ``fixture_match_for_instrument_key`` to materialise the six
# columns — exactly as it already does for ``clob_token_ids``.
_FIXTURE_MATCH_BY_INSTRUMENT_KEY: dict[str, FixtureMatchAttributes] = {}


def register_fixture_match(instrument_key: str, attrs: FixtureMatchAttributes) -> None:
    """Record the fixture-match attributes for a soccer market by ``instrument_key``.

    No-op for a blank key. A later registration for the same key overwrites (the
    latest parse wins — same convention as the clob token-id side-table).
    """
    if instrument_key:
        _FIXTURE_MATCH_BY_INSTRUMENT_KEY[instrument_key] = attrs


def fixture_match_for_instrument_key(instrument_key: str) -> FixtureMatchAttributes | None:
    """Return the registered fixture-match attributes for ``instrument_key`` (or None)."""
    return _FIXTURE_MATCH_BY_INSTRUMENT_KEY.get(instrument_key)


def reset_fixture_match_registry() -> None:
    """Clear the side-table (test hygiene / new-run reset)."""
    _FIXTURE_MATCH_BY_INSTRUMENT_KEY.clear()
