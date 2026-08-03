"""Regression tests: writer-side de-dup + schema-conformance gates for the
per-fixture entity write path (``sports_reference_fixtures.py``).

Context: issues/canonical_player_stats_fixture_events_quality_2026_07_16.md
found two pre-existing canonical data-correctness defects — player_stats
within-object exact-duplicate rows on (fixture_id, player_id) (~26% of rows),
and fixture_events schema heterogeneity (~30% of a sampled population carried
a degenerate 5-col stub instead of the canonical 13-col shape). Both were
already remediated once via one-off rewrite scripts; these tests guard the
writer-side gates that stop the defects from re-accruing on every future
fetch:

  1. player_stats rows get de-duped on (fixture_id, player_id) in
     ``_prepare_fixture_entity_df``, before the per-league split.
  2. fixture_events rows get reindexed to the canonical UAC 13-col contract in
     ``_write_fixture_entity_per_league``, AFTER the per-league join/split but
     right before ``_gated_sink_write`` (missing columns added as null,
     non-canonical columns dropped) — it must run post-split so it never
     strips the join key the league-mapping split depends on.
"""

from __future__ import annotations

import pandas as pd

from instruments_service.engine.orchestrator.sports_reference_fixture_entity_gates import (
    _dedupe_player_stats_df,
    _normalize_fixture_events_schema,
)
from instruments_service.engine.orchestrator.sports_reference_fixtures_write import _prepare_fixture_entity_df

# ---------------------------------------------------------------------------
# 1. player_stats writer-side de-dup
# ---------------------------------------------------------------------------


def test_player_stats_dedup_drops_duplicate_fixture_player_rows() -> None:
    """Exact (fixture_id, player_id) duplicates within the same fetch must be
    dropped before write — keeping the first occurrence, mirroring the
    canonical remediation script's own methodology."""
    df = pd.DataFrame(
        [
            {"fixture_id": "1001", "player_id": "55", "team_id": "10", "rating": 7.1},
            {"fixture_id": "1001", "player_id": "55", "team_id": "10", "rating": 7.1},  # exact dup
            {"fixture_id": "1001", "player_id": "56", "team_id": "10", "rating": 6.4},
        ]
    )
    result = _dedupe_player_stats_df(df)

    assert len(result) == 2, "Duplicate (fixture_id, player_id) row must be dropped."
    keys = list(zip(result["fixture_id"], result["player_id"], strict=True))
    assert keys == [("1001", "55"), ("1001", "56")], "Surviving rows must preserve first-seen order."


def test_player_stats_dedup_noop_when_already_clean() -> None:
    """No duplicates present → de-dup is a safe no-op (same rows, same order)."""
    df = pd.DataFrame(
        [
            {"fixture_id": "1001", "player_id": "55", "rating": 7.1},
            {"fixture_id": "1001", "player_id": "56", "rating": 6.4},
        ]
    )
    result = _dedupe_player_stats_df(df)
    assert len(result) == 2
    assert result["player_id"].tolist() == ["55", "56"]


def test_player_stats_dedup_noop_when_key_columns_absent() -> None:
    """The nested player_stats schema variant ([team, players, fixture_id,
    available_at], where ``players`` is a list-of-dicts) lacks a flat
    ``player_id`` column — the gate must not guess a dedup key it wasn't
    built for and must pass the frame through unchanged."""
    df = pd.DataFrame(
        [
            {"team": "Arsenal", "players": [{"id": 55}], "fixture_id": "1001"},
        ]
    )
    result = _dedupe_player_stats_df(df)
    assert len(result) == 1
    assert list(result.columns) == list(df.columns)


# ---------------------------------------------------------------------------
# 2. fixture_events writer-side schema-conformance gate
# ---------------------------------------------------------------------------

_FIXTURE_EVENTS_CANONICAL_13 = [
    "fixture_id",
    "time_elapsed",
    "time_extra",
    "team_id",
    "team_name",
    "player_id",
    "player_name",
    "assist_id",
    "assist_name",
    "event_type",
    "event_detail",
    "comments",
    "available_at",
]


def test_fixture_events_normalize_passthrough_when_already_canonical() -> None:
    """A properly-shaped canonical row must survive normalization unchanged
    (values untouched, no columns added/dropped) — ``available_at`` is
    included in the reindex so an already-stamped value round-trips too."""
    df = pd.DataFrame(
        [
            {
                "fixture_id": "1001",
                "time_elapsed": 45,
                "time_extra": None,
                "team_id": 10,
                "team_name": "Arsenal",
                "player_id": 55,
                "player_name": "Player A",
                "assist_id": None,
                "assist_name": None,
                "event_type": "Goal",
                "event_detail": "Normal Goal",
                "comments": None,
                "available_at": pd.Timestamp("2026-07-16T17:00:00Z"),
            }
        ]
    )
    result = _normalize_fixture_events_schema(df)

    assert set(result.columns) == set(_FIXTURE_EVENTS_CANONICAL_13)
    assert result.loc[0, "event_type"] == "Goal"
    assert result.loc[0, "player_id"] == 55
    assert result.loc[0, "available_at"] == pd.Timestamp("2026-07-16T17:00:00Z")


def test_fixture_events_normalize_fills_missing_and_drops_degenerate_stub_columns() -> None:
    """The degenerate 5-col stub (issue doc Finding 2: ``available_at``,
    ``comments``, ``detail``, ``fixture_id``, ``type`` — no attribution)
    must come out reindexed to the canonical shape: every canonical column
    present (missing ones null, never fabricated), and the stub's
    non-canonical ``type``/``detail`` columns dropped rather than written
    through under the wrong names."""
    df = pd.DataFrame(
        [
            {
                "fixture_id": "1001",
                "comments": "VAR review",
                "detail": "Normal Goal",  # non-canonical name for event_detail
                "type": "Goal",  # non-canonical name for event_type
            }
        ]
    )
    result = _normalize_fixture_events_schema(df)

    assert set(result.columns) == set(_FIXTURE_EVENTS_CANONICAL_13), "Result must carry exactly the 13 canonical cols."
    assert "type" not in result.columns, "Non-canonical 'type' column must not survive the gate."
    assert "detail" not in result.columns, "Non-canonical 'detail' column must not survive the gate."
    # event_type/event_detail were never provided under their canonical names —
    # must be null, never fabricated from the stub's differently-named fields.
    assert pd.isna(result.loc[0, "event_type"])
    assert pd.isna(result.loc[0, "event_detail"])
    # fixture_id WAS provided under its canonical name — preserved.
    assert result.loc[0, "fixture_id"] == "1001"


# ---------------------------------------------------------------------------
# 3. End-to-end wiring into the real write path
# ---------------------------------------------------------------------------


def test_prepare_fixture_entity_df_applies_player_stats_dedup_gate() -> None:
    counts: dict[str, int] = {}
    rows = [
        {"fixture_id": "1001", "player_id": "55", "rating": 7.1},
        {"fixture_id": "1001", "player_id": "55", "rating": 7.1},
    ]
    result = _prepare_fixture_entity_df("player_stats", rows, "2026-07-16", counts)
    assert len(result) == 1
    assert counts["player_stats"] == 1


def test_write_fixture_entity_per_league_normalizes_degenerate_fixture_events_before_write() -> None:
    """End-to-end: a degenerate-shaped fixture_events row reaching the real
    per-league write path (``_write_per_fixture_entities`` ->
    ``_write_fixture_entity_per_league``) must be normalized to the canonical
    13-col contract in the object actually handed to ``_gated_sink_write`` —
    proving the gate is wired into the live write path, not just the helper.

    Uses ``fixture_id`` (not ``af_fixture_id``) as the join key, matching every
    real API-Football normalizer's output shape (fixture_events/player_stats/
    fixture_stats/lineups all key on ``fixture_id`` — ``af_fixture_id`` is a
    defensive fallback in ``_fid_col`` selection, not the real data shape)."""
    from datetime import UTC, datetime
    from unittest.mock import MagicMock, patch

    from instruments_service.engine.orchestrator.sports_reference_core import _AfManifestHooks
    from instruments_service.engine.orchestrator.sports_reference_fixtures_write import _write_per_fixture_entities

    entity_rows: dict[str, list[dict[str, object]]] = {
        "fixture_events": [
            # Degenerate shape: fixture_id present (canonical), but event
            # type/detail under non-canonical names, no attribution columns.
            {"fixture_id": 111, "type": "Goal", "detail": "Normal Goal", "comments": None},
        ]
    }
    af_fid_to_league = {"111": "ENG_PREMIER_LEAGUE"}

    manifest = MagicMock()
    hooks = _AfManifestHooks(
        date="2026-07-16",
        manifest=manifest,
        attempt_ts=datetime(2026, 7, 16, tzinfo=UTC),
        bucket="",
    )
    hooks.emit_empty_gaps_for_entity = MagicMock()  # type: ignore[method-assign]

    captured_write_df: dict[str, object] = {}

    def _capture_write(*_args: object, **kwargs: object) -> None:
        captured_write_df["data"] = kwargs["data"]

    with (
        patch("instruments_service.engine.orchestrator._is_in_canonical_write_universe", return_value=True),
        patch("instruments_service.engine.orchestrator._gated_sink_write", side_effect=_capture_write),
        patch("instruments_service.engine.orchestrator._sports_ref_sink_for"),
        patch("instruments_service.engine.orchestrator._canonical_league_id", side_effect=lambda lid: str(lid)),
    ):
        _write_per_fixture_entities(
            date="2026-07-16",
            bucket="bucket",
            hooks=hooks,
            counts={},
            entity_names=["fixture_events"],
            entity_rows=entity_rows,
            entity_failures={},
            pre_captured_leagues={"fixture_events": set()},
            af_fid_to_league=af_fid_to_league,
            recovery_fixture_ids=None,
        )

    assert manifest.record_captured.called, "mapped in-universe row should still be captured"
    written_df = captured_write_df["data"]
    assert set(written_df.columns) == set(_FIXTURE_EVENTS_CANONICAL_13), (
        "The object actually handed to _gated_sink_write must be the normalized 13-col shape, "
        "not the degenerate raw response."
    )
    assert "type" not in written_df.columns
    assert "detail" not in written_df.columns
    assert written_df.loc[0, "fixture_id"] == 111


def test_prepare_fixture_entity_df_untouched_for_other_entities() -> None:
    """fixture_stats / fixture_lineups are NOT in scope for either gate — the
    dataframe must pass through with only the pre-existing nested-column-drop
    + available_at-stamp behaviour, unaffected by the new gates."""
    counts: dict[str, int] = {}
    rows = [
        {"fixture_id": "1001", "team_id": "10", "shots_total": 12},
        {"fixture_id": "1001", "team_id": "10", "shots_total": 12},  # would-be dup, must survive
    ]
    result = _prepare_fixture_entity_df("fixture_stats", rows, "2026-07-16", counts)
    assert len(result) == 2, "fixture_stats rows must not be de-duped — that gate is player_stats-only."
