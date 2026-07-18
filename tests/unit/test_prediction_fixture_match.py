"""Unit tests for the prediction soccer fixture-match resolver + honest-absence stamping.

Covers Phase-E Leg-1 (``prediction_consolidated_closeout_2026_07_18.md`` A4):

* Polymarket ``PredictionFixtureResolver.resolve`` — MATCHED (af_fixture_id found
  in the injected FIXTURES parquet) + UNRESOLVED_TEAM_NAME (team resolves but is
  not in the lookup) + NO_FIXTURE_DATA (no parquet).
* Kalshi soccer resolution (Phase-E Leg-2 / E2) — the participant pair is parsed
  off the Kalshi title and fed into the SAME alias index + FIXTURES resolver: a
  club whose rendering is in the alias index resolves (MATCHED when the fixture
  parquet is present), a genuinely-absent rendering stays honest
  ``UNRESOLVED_TEAM_NAME`` with ``af_fixture_id=None`` (never a fake value).

All GCS reads are hermetic: the resolver is constructed with an in-memory fake
storage client, never touching the network.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from io import BytesIO

import pandas as pd

from instruments_service.reference_data.adapters.prediction.fixture_match import (
    FixtureMatchStatus,
    PredictionFixtureResolver,
    fixture_match_for_instrument_key,
    football_league_for_sports_underlying,
    parse_kalshi_soccer_participants,
    reset_fixture_match_registry,
)
from instruments_service.reference_data.adapters.prediction.kalshi import KalshiReferenceDataAdapter


class _FakeStorageClient:
    """Storage-client stub: ``download_bytes`` returns preset parquet bytes (or None)."""

    def __init__(self, payload: bytes | None) -> None:
        self._payload = payload
        self.calls: list[tuple[str, str]] = []

    def download_bytes(self, bucket: str, path: str) -> bytes | None:
        self.calls.append((bucket, path))
        return self._payload


def _fixtures_parquet_bytes(home: str, away: str, fixture_id: int) -> bytes:
    """One-row FIXTURES parquet with the columns the resolver joins on."""
    frame = pd.DataFrame([{"af_fixture_id": fixture_id, "af_home_name": home, "af_away_name": away}])
    buffer = BytesIO()
    frame.to_parquet(buffer)
    return buffer.getvalue()


def _resolver_with(payload: bytes | None) -> PredictionFixtureResolver:
    return PredictionFixtureResolver(storage_client=_FakeStorageClient(payload), bucket="test-bucket")


# ── Polymarket resolve ──────────────────────────────────────────────────────


def test_polymarket_resolve_matched() -> None:
    """A market whose (home, away) is in the fixtures parquet → MATCHED + real af_fixture_id."""
    resolver = _resolver_with(_fixtures_parquet_bytes("Arsenal", "Chelsea", 12345))

    attrs = resolver.resolve("EPL", "Arsenal", "Chelsea", date(2026, 3, 22))

    assert attrs.af_fixture_match_status == FixtureMatchStatus.MATCHED
    assert attrs.af_fixture_id == 12345
    assert attrs.af_league_id == 39  # EPL API-Football id
    assert attrs.home_team_canonical_id == "ARSENAL"
    assert attrs.away_team_canonical_id == "CHELSEA"
    assert attrs.fixture_date == "2026-03-22"


def test_polymarket_resolve_unresolved_not_in_lookup() -> None:
    """Team names canonicalise but the fixture is not in the parquet → UNRESOLVED_TEAM_NAME, null id."""
    resolver = _resolver_with(_fixtures_parquet_bytes("Arsenal", "Chelsea", 12345))

    attrs = resolver.resolve("EPL", "Liverpool", "Everton", date(2026, 3, 22))

    assert attrs.af_fixture_match_status == FixtureMatchStatus.UNRESOLVED_TEAM_NAME
    assert attrs.af_fixture_id is None
    # The market's own teams still canonicalise + are stamped (honest partial record).
    assert attrs.home_team_canonical_id == "LIVERPOOL"
    assert attrs.away_team_canonical_id == "EVERTON"


def test_polymarket_resolve_no_fixture_data() -> None:
    """No FIXTURES parquet on disk → NO_FIXTURE_DATA, null id (never a fake value)."""
    resolver = _resolver_with(None)

    attrs = resolver.resolve("EPL", "Arsenal", "Chelsea", date(2026, 3, 22))

    assert attrs.af_fixture_match_status == FixtureMatchStatus.NO_FIXTURE_DATA
    assert attrs.af_fixture_id is None
    assert attrs.af_league_id == 39


def test_resolver_caches_per_league_day() -> None:
    """The fixtures parquet is downloaded once per (league, day), reused for every market."""
    client = _FakeStorageClient(_fixtures_parquet_bytes("Arsenal", "Chelsea", 12345))
    resolver = PredictionFixtureResolver(storage_client=client, bucket="test-bucket")

    resolver.resolve("EPL", "Arsenal", "Chelsea", date(2026, 3, 22))
    calls_after_first = len(client.calls)
    resolver.resolve("EPL", "Liverpool", "Everton", date(2026, 3, 22))

    # Second resolve on the same (league, day) triggered no further downloads.
    assert len(client.calls) == calls_after_first
    assert calls_after_first >= 1


# ── Kalshi soccer honest-absence detection + stamping ───────────────────────


def test_football_league_for_sports_underlying() -> None:
    """Only FOOTBALL underlyings resolve to a (league, af_id) pair."""
    assert football_league_for_sports_underlying("SPORTS_EPL") == ("EPL", 39)
    assert football_league_for_sports_underlying("SPORTS_NBA") is None  # basketball, not football
    assert football_league_for_sports_underlying("BTC") is None  # not a sports underlying at all


def test_parse_kalshi_soccer_participants() -> None:
    """The title parser extracts (home, away), stripping the market-type suffix."""
    # EPL/BUN/LALIGA/SERIEA/LIGUE1 GAME titles: "{Home} vs {Away} Winner?"
    assert parse_kalshi_soccer_participants("Liverpool vs Brentford Winner?") == ("Liverpool", "Brentford")
    assert parse_kalshi_soccer_participants("Manchester City vs Aston Villa Winner?") == (
        "Manchester City",
        "Aston Villa",
    )
    # Colon-suffixed variant ("… : Regulation Time Moneyline").
    assert parse_kalshi_soccer_participants("Real Madrid vs Bilbao: Regulation Time Moneyline") == (
        "Real Madrid",
        "Bilbao",
    )
    # Bare "X vs Y" with no suffix still works.
    assert parse_kalshi_soccer_participants("Arsenal vs Chelsea") == ("Arsenal", "Chelsea")
    # Non-fixture / non-parseable titles yield None (→ caller honest-absence).
    assert parse_kalshi_soccer_participants("Bitcoin above 95000?") is None
    assert parse_kalshi_soccer_participants(None) is None
    assert parse_kalshi_soccer_participants("") is None


def _kalshi_soccer_raw(title: str) -> dict[str, object]:
    """A minimal Kalshi EPL GAME market payload with the given title."""
    return {
        "ticker": "KXEPLGAME-26MAY24LFCBRE-LFC",
        "event_ticker": "KXEPLGAME-26MAY24LFCBRE",
        "series_ticker": "KXEPLGAME",
        "title": title,
        "status": "active",
        "close_time": "2026-05-24T18:00:00Z",
        "open_time": "2026-05-01T00:00:00Z",
    }


def test_kalshi_soccer_resolves_when_alias_present() -> None:
    """A Kalshi EPL market whose clubs ARE in the alias index resolves to a real
    af_fixture_id when the FIXTURES parquet carries the fixture — MATCHED, no fake.
    """
    reset_fixture_match_registry()
    adapter = KalshiReferenceDataAdapter()
    # Inject the resolver with an in-memory FIXTURES parquet (hermetic — no GCS).
    adapter._fixture_resolver_instance = _resolver_with(  # pyright: ignore[reportPrivateUsage]
        _fixtures_parquet_bytes("Arsenal", "Chelsea", 55555)
    )

    record = adapter._parse_market(  # pyright: ignore[reportPrivateUsage]
        _kalshi_soccer_raw("Arsenal vs Chelsea Winner?"), datetime.now(UTC)
    )

    assert record is not None
    attrs = fixture_match_for_instrument_key(record.instrument_key)
    assert attrs is not None
    assert attrs.af_fixture_match_status == FixtureMatchStatus.MATCHED
    assert attrs.af_fixture_id == 55555
    assert attrs.af_league_id == 39  # EPL
    assert attrs.home_team_canonical_id == "ARSENAL"
    assert attrs.away_team_canonical_id == "CHELSEA"
    assert attrs.fixture_date == "2026-05-24"  # close date keys the day lookup


def test_kalshi_soccer_honest_absence_when_alias_missing() -> None:
    """A Kalshi soccer market whose rendering is NOT in the alias index stays
    honest UNRESOLVED_TEAM_NAME with af_fixture_id=None + null canonical ids.

    "Bilbao" / "Vallecano" are Kalshi short-form renderings genuinely absent from
    the shared alias index (worklist for the deferred team_mappings addition), so
    the honest outcome is 'league + date known, teams + fixture id unresolved'.
    """
    reset_fixture_match_registry()
    adapter = KalshiReferenceDataAdapter()
    # A fixtures parquet is present, but the team names never canonicalise, so the
    # resolver short-circuits to UNRESOLVED_TEAM_NAME before the lookup.
    adapter._fixture_resolver_instance = _resolver_with(  # pyright: ignore[reportPrivateUsage]
        _fixtures_parquet_bytes("Athletic Club", "Rayo Vallecano", 77777)
    )

    record = adapter._parse_market(  # pyright: ignore[reportPrivateUsage]
        _kalshi_soccer_raw("Bilbao vs Vallecano Winner?"), datetime.now(UTC)
    )

    assert record is not None
    attrs = fixture_match_for_instrument_key(record.instrument_key)
    assert attrs is not None
    assert attrs.af_fixture_match_status == FixtureMatchStatus.UNRESOLVED_TEAM_NAME
    assert attrs.af_fixture_id is None
    assert attrs.af_league_id == 39  # EPL — resolvable from the ticker
    assert attrs.home_team_canonical_id is None  # "Bilbao" not in alias index
    assert attrs.away_team_canonical_id is None  # "Vallecano" not in alias index
    assert attrs.fixture_date == "2026-05-24"  # close date is honestly resolvable


def test_kalshi_non_soccer_market_not_stamped() -> None:
    """A non-soccer Kalshi market (e.g. crypto) is not given a fixture-match record."""
    reset_fixture_match_registry()
    adapter = KalshiReferenceDataAdapter()

    record = adapter._parse_market(  # pyright: ignore[reportPrivateUsage]
        {
            "ticker": "KXBTCD-25AUG16-T95000",
            "event_ticker": "KXBTCD-25AUG16",
            "series_ticker": "KXBTCD",
            "title": "Bitcoin above 95000?",
            "status": "active",
            "close_time": "2026-08-16T18:00:00Z",
            "open_time": "2026-08-01T00:00:00Z",
        },
        datetime.now(UTC),
    )

    assert record is not None
    assert fixture_match_for_instrument_key(record.instrument_key) is None
