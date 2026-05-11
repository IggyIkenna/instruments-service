"""Honest-coverage Phase B tests for instruments-service Polymarket-class paths.

Plan: ``unified-trading-pm/plans/active/honest_coverage_metrics_2026_04_19.plan``

Covers the orchestrator helpers ``_should_skip_shard`` /
``_classify_adapter_failure`` and the per-shard wrap pattern
(record_empty / record_failed / lookup pre-flight / --force) that adapters
adopt to lift attempt-coverage without inflating capture-coverage.

Tests are pure unit — they mock ``ManifestWriter`` at the helper boundary so
no GCS round-trip is needed.  The fetcher functions themselves are exercised
indirectly: we mock the underlying adapter API call and assert that the
correct manifest method is called (record_empty / record_failed) with the
expected attempt timestamp + classified error code.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast
from unittest.mock import MagicMock

import pytest
from unified_trading_library import CaptureStatus, ManifestWriter

from instruments_service.engine.orchestrator import (
    _classify_adapter_failure,
    _should_skip_shard,
)

# ---------------------------------------------------------------------------
# _should_skip_shard
# ---------------------------------------------------------------------------


def _make_lookup_row(status: str) -> MagicMock:
    """Build a stand-in ``ManifestRow``-ish object for lookup() to return."""
    row = MagicMock()
    row.capture_status = status
    return row


def test_should_skip_shard_returns_false_when_no_prior_row() -> None:
    """No manifest row → must attempt (don't skip)."""
    manifest = MagicMock(spec=ManifestWriter)
    manifest.lookup.return_value = None
    row_key = {"date": "2026-04-19", "venue": "POLYMARKET", "data_type": "BTC"}

    assert _should_skip_shard(manifest, row_key=row_key, force=False) is False
    manifest.lookup.assert_called_once_with(row_key)


def test_should_skip_shard_skips_when_prior_captured() -> None:
    """Prior CAPTURED row → skip (we trust the previous capture)."""
    manifest = MagicMock(spec=ManifestWriter)
    manifest.lookup.return_value = _make_lookup_row(CaptureStatus.CAPTURED.value)

    assert (
        _should_skip_shard(
            manifest,
            row_key={"date": "2026-04-19", "venue": "POLYMARKET", "data_type": "BTC"},
            force=False,
        )
        is True
    )


def test_should_skip_shard_skips_when_prior_empty_confirmed() -> None:
    """Prior EMPTY_CONFIRMED row → skip (legitimate zero-data outcome trusted)."""
    manifest = MagicMock(spec=ManifestWriter)
    manifest.lookup.return_value = _make_lookup_row(CaptureStatus.EMPTY_CONFIRMED.value)

    assert (
        _should_skip_shard(
            manifest,
            row_key={"date": "2026-04-19", "venue": "POLYMARKET", "data_type": "BTC"},
            force=False,
        )
        is True
    )


def test_should_skip_shard_does_not_skip_when_prior_failed() -> None:
    """Prior ATTEMPTED_FAILED row → retry by default (operator can override)."""
    manifest = MagicMock(spec=ManifestWriter)
    manifest.lookup.return_value = _make_lookup_row(CaptureStatus.ATTEMPTED_FAILED.value)

    assert (
        _should_skip_shard(
            manifest,
            row_key={"date": "2026-04-19", "venue": "POLYMARKET", "data_type": "BTC"},
            force=False,
        )
        is False
    )


def test_should_skip_shard_force_true_bypasses_lookup_entirely() -> None:
    """``force=True`` MUST short-circuit before the lookup call."""
    manifest = MagicMock(spec=ManifestWriter)

    assert (
        _should_skip_shard(
            manifest,
            row_key={"date": "2026-04-19", "venue": "POLYMARKET", "data_type": "BTC"},
            force=True,
        )
        is False
    )
    manifest.lookup.assert_not_called()


def test_should_skip_shard_force_overrides_captured_prior() -> None:
    """Even with a captured prior, force=True must NOT skip."""
    manifest = MagicMock(spec=ManifestWriter)
    manifest.lookup.return_value = _make_lookup_row(CaptureStatus.CAPTURED.value)

    assert (
        _should_skip_shard(
            manifest,
            row_key={"date": "2026-04-19", "venue": "POLYMARKET", "data_type": "BTC"},
            force=True,
        )
        is False
    )
    manifest.lookup.assert_not_called()


# ---------------------------------------------------------------------------
# _classify_adapter_failure
# ---------------------------------------------------------------------------


class _FakeRateLimitError(Exception):
    """Stand-in for a venue-specific exception class."""


def test_classify_adapter_failure_falls_back_to_exception_class_name() -> None:
    """Unknown venue/code pair returns the exception class name verbatim."""
    exc = _FakeRateLimitError("rate limit hit")
    code = _classify_adapter_failure(exc, venue="footystats")
    # No FootyStats classification for ``_FakeRateLimitError`` exists → fall back.
    assert code == "_FakeRateLimitError"


def test_classify_adapter_failure_returns_string() -> None:
    """Always returns a non-empty str so record_failed never raises."""
    code = _classify_adapter_failure(ValueError("boom"), venue="polymarket")
    assert isinstance(code, str)
    assert code  # non-empty


# ---------------------------------------------------------------------------
# Integration: mocked adapter wrapping pattern
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_understat_xg_calls_record_empty_on_zero_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Adapter returns empty fixture list → record_empty at date + per-league."""
    from instruments_service.engine import orchestrator as orch

    captured: dict[str, object] = {"record_empty_calls": []}

    class _MWStub:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            captured["instantiated"] = True

        def lookup(self, _row_key: object) -> None:
            return None

        def record_empty(self, *, row_key: object, attempted_at: datetime, **_kwargs: object) -> None:
            cast(list[dict[str, object]], captured["record_empty_calls"]).append(
                {"row_key": row_key, "attempted_at": attempted_at},
            )

        def record_failed(self, **_kwargs: object) -> None:
            captured["record_failed_called"] = True

        def add(self, **_kwargs: object) -> None:
            captured["add_called"] = True

        def write(self) -> None:
            captured["write_called"] = True

    monkeypatch.setattr(orch, "ManifestWriter", _MWStub)

    class _SinkStub:
        def write(self, **_kwargs: object) -> None:
            return None

    monkeypatch.setattr(orch, "get_data_sink", lambda **_kw: _SinkStub())

    class _AdapterStub:
        async def get_fixtures(self, _date: str) -> list[object]:
            return []

    monkeypatch.setattr(orch, "create_sports_reference_adapter", lambda _name: _AdapterStub())

    result = await orch._fetch_understat_xg(date="2026-04-19", bucket="test-bucket")

    assert result == {}
    assert captured.get("record_failed_called") is None
    calls = cast(list[dict[str, object]], captured["record_empty_calls"])
    assert len(calls) >= 1, "record_empty must be called on legitimate empty"
    row_keys = [cast(dict[str, str], c["row_key"]) for c in calls]
    # Phase 2 of sports_manifest_shard_migration_cleanup_2026_04_21 dropped
    # the date-aggregate (``{"date", "data_type"}`` without ``league_id``)
    # emission — only per-league rows are now written.
    assert {"date": "2026-04-19", "data_type": "XG"} not in row_keys
    # All 5 expected PREDICTION leagues must have per-league record_empty rows.
    _expected_leagues = {"EPL", "LA_LIGA", "BUNDESLIGA", "SERIE_A", "LIGUE_1"}
    _seen_leagues = {cast(str, rk.get("league_id")) for rk in row_keys if "league_id" in rk}
    assert _seen_leagues == _expected_leagues, (
        f"Per-league record_empty must cover all expected leagues; got {_seen_leagues}"
    )
    # Every emitted row carries a league_id (no unsharded rows).
    assert all("league_id" in rk and rk["league_id"] for rk in row_keys), (
        f"All record_empty rows must be per-league; got {row_keys}"
    )
    assert captured.get("write_called") is True


@pytest.mark.asyncio
async def test_understat_xg_skips_when_already_captured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-flight: prior CAPTURED row + force=False → adapter NOT called."""
    from instruments_service.engine import orchestrator as orch

    fetch_called = False

    class _AdapterStub:
        async def get_fixtures(self, _date: str) -> list[object]:
            nonlocal fetch_called
            fetch_called = True
            return []

    monkeypatch.setattr(orch, "create_sports_reference_adapter", lambda _name: _AdapterStub())

    class _MWStub:
        def __init__(self, *_a: object, **_kw: object) -> None:
            pass

        def lookup(self, _row_key: object) -> object:
            row = MagicMock()
            row.capture_status = CaptureStatus.CAPTURED.value
            return row

        def record_empty(self, **_kw: object) -> None:
            raise AssertionError("record_empty must not be called when shard is skipped")

        def record_failed(self, **_kw: object) -> None:
            raise AssertionError("record_failed must not be called when shard is skipped")

        def add(self, **_kw: object) -> None:
            raise AssertionError("add must not be called when shard is skipped")

        def write(self) -> None:
            raise AssertionError("write must not be called when shard is skipped")

    monkeypatch.setattr(orch, "ManifestWriter", _MWStub)

    result = await orch._fetch_understat_xg(date="2026-04-19", bucket="test-bucket", force=False)
    assert result == {}
    assert fetch_called is False, "Adapter must not be called when shard is already captured"


@pytest.mark.asyncio
async def test_understat_xg_force_bypasses_captured_pre_flight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``force=True`` causes adapter to run even when prior row is CAPTURED."""
    from instruments_service.engine import orchestrator as orch

    fetch_called = False

    class _AdapterStub:
        async def get_fixtures(self, _date: str) -> list[object]:
            nonlocal fetch_called
            fetch_called = True
            return []

    monkeypatch.setattr(orch, "create_sports_reference_adapter", lambda _name: _AdapterStub())

    class _SinkStub:
        def write(self, **_kw: object) -> None:
            return None

    monkeypatch.setattr(orch, "get_data_sink", lambda **_kw: _SinkStub())

    class _MWStub:
        def __init__(self, *_a: object, **_kw: object) -> None:
            pass

        def lookup(self, _row_key: object) -> None:
            raise AssertionError("force=True must short-circuit before lookup()")

        def record_empty(self, **_kw: object) -> None:
            return None

        def record_failed(self, **_kw: object) -> None:
            return None

        def add(self, **_kw: object) -> None:
            return None

        def write(self) -> None:
            return None

    monkeypatch.setattr(orch, "ManifestWriter", _MWStub)

    await orch._fetch_understat_xg(date="2026-04-19", bucket="test-bucket", force=True)
    assert fetch_called is True


@pytest.mark.asyncio
async def test_understat_xg_calls_record_failed_on_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Adapter raises → record_failed per-league only; no re-raise; no unsharded row."""
    from instruments_service.engine import orchestrator as orch

    captured: dict[str, object] = {"record_failed_calls": []}

    class _MWStub:
        def __init__(self, *_a: object, **_kw: object) -> None:
            pass

        def lookup(self, _row_key: object) -> None:
            return None

        def record_empty(self, **_kw: object) -> None:
            captured["record_empty_called"] = True

        def record_failed(
            self,
            *,
            row_key: object,
            error: str,
            attempted_at: datetime,
            **_kwargs: object,
        ) -> None:
            cast(list[dict[str, object]], captured["record_failed_calls"]).append(
                {"row_key": row_key, "error": error, "attempted_at": attempted_at},
            )

        def add(self, **_kw: object) -> None:
            return None

        def write(self) -> None:
            captured["write_called"] = True

    monkeypatch.setattr(orch, "ManifestWriter", _MWStub)

    class _AdapterStub:
        async def get_fixtures(self, _date: str) -> list[object]:
            raise RuntimeError("network exploded")

    monkeypatch.setattr(orch, "create_sports_reference_adapter", lambda _name: _AdapterStub())

    # Must NOT raise out of the per-shard call (shard isolation).
    result = await orch._fetch_understat_xg(date="2026-04-19", bucket="test-bucket")

    assert result == {}
    calls = cast(list[dict[str, object]], captured["record_failed_calls"])
    assert len(calls) >= 1, "record_failed must be called on adapter exception"
    row_keys = [cast(dict[str, str], c["row_key"]) for c in calls]
    # Phase 2 of sports_manifest_shard_migration_cleanup_2026_04_21 dropped the
    # date-aggregate (``{"date", "data_type"}`` without ``league_id``) emission.
    assert {"date": "2026-04-19", "data_type": "XG"} not in row_keys
    # Per-league failure rows for every expected league (5 PREDICTION leagues).
    _expected_leagues = {"EPL", "LA_LIGA", "BUNDESLIGA", "SERIE_A", "LIGUE_1"}
    _seen_leagues = {cast(str, rk.get("league_id")) for rk in row_keys if "league_id" in rk}
    assert _seen_leagues == _expected_leagues, (
        f"Per-league record_failed must cover all expected leagues; got {_seen_leagues}"
    )
    # Every failure call must carry the classified error code + a league_id.
    for c in calls:
        assert c["error"] == "RuntimeError"  # classify falls back to class name
        assert isinstance(c["attempted_at"], datetime)
        assert cast(datetime, c["attempted_at"]).tzinfo == UTC
        rk = cast(dict[str, str], c["row_key"])
        assert rk.get("league_id"), f"All record_failed rows must be per-league; got {rk}"
    assert captured.get("record_empty_called") is None
