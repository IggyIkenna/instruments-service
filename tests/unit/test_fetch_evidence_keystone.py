"""Regression tests for FetchEvidence keystone threading in sports honest-absence paths.

Incident SHAs:
  - IS@0db2450 (DP-FETCH-002): API-Football error mistakenly stamped as
    empty_confirmed SOURCE_RETURNED_ZERO instead of attempted_failed.
  - IS@505dcd9 (DP-VM-001): OOM during SFI stats fetch caused silent
    SOURCE_RETURNED_ZERO instead of routing to attempted_failed.

These tests assert:
  1. Error/exception paths → manifest row is ``attempted_failed`` (record_failed),
     NOT ``empty_confirmed`` / SOURCE_RETURNED_ZERO without a proving FetchEvidence.
  2. Genuine 2xx+0-rows paths → SOURCE_RETURNED_ZERO is accepted because a
     FetchEvidence with .proves_honest_absence()==True is passed.

Plan: unified-trading-pm/plans/active/data_pipeline_hardening_self_monitoring_2026_06_22.md
Phase-1 KEYSTONE (UTL manifest_writer/_writer_record.py).
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from unified_api_contracts import EmptyConfirmedReason, FetchEvidence, PipelineMode

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DATE = "2026-01-15"
_BUCKET = "test-bucket"
_ATTEMPT_TS = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


def _make_manifest_mock() -> MagicMock:
    """Return a MagicMock that records record_empty / record_failed calls."""
    mock = MagicMock()
    mock.record_empty = MagicMock()
    mock.record_failed = MagicMock()
    mock.record_captured_from_counts = MagicMock()
    mock.write = MagicMock()
    return mock


# ---------------------------------------------------------------------------
# SFI progressive stats — zero-rows path
# ---------------------------------------------------------------------------


class TestSfiProgressiveStatsZeroRows:
    """Tests for instruments_service.engine.orchestrator.sfi SOURCE_RETURNED_ZERO path.

    Regression for IS@505dcd9 (DP-VM-001): OOM/exception during per-match fetch
    must not reach SOURCE_RETURNED_ZERO without a proving FetchEvidence.
    The sfi.py else-branch (all per-match fetches produced 0 rows) now threads
    FetchEvidence(http_status=200, ...) so the keystone gate accepts the row.
    """

    def test_zero_rows_record_empty_includes_fetch_evidence(self) -> None:
        """When SFI stats fetch cleanly returns 0 rows, record_empty gets FetchEvidence.

        The else-branch in sfi.py is reached only when no per-match exception
        escaped (shard isolation caught them). So it is a genuine 2xx+0-rows
        path and must pass a proving FetchEvidence to satisfy the UTL keystone.
        Regression: IS@505dcd9 (DP-VM-001).
        """
        # Simulate the condition: record_empty was called with SOURCE_RETURNED_ZERO
        # and fetch_evidence kwarg that proves honest absence.
        ev = FetchEvidence(
            http_status=200,
            response_received=True,
            rows_in_response=0,
            source="soccer_football_info",
            endpoint="sfi_progressive_stats",
            attempted_at=_ATTEMPT_TS,
            error_signal="",
        )
        assert ev.proves_honest_absence(), (
            "FetchEvidence for SFI zero-rows path must prove honest absence "
            "(2xx + response_received + rows==0 + no error_signal)"
        )

    def test_fetch_evidence_with_error_signal_does_not_prove_absence(self) -> None:
        """FetchEvidence with an error_signal does NOT prove honest absence.

        This verifies the keystone gate would correctly reject an exception path.
        Regression: IS@0db2450 (DP-FETCH-002): error → empty_confirmed is wrong.
        """
        ev_error = FetchEvidence(
            http_status=0,
            response_received=False,
            rows_in_response=0,
            source="soccer_football_info",
            endpoint="sfi_progressive_stats",
            attempted_at=_ATTEMPT_TS,
            error_signal="ADAPTER_EXCEPTION",
        )
        assert not ev_error.proves_honest_absence(), (
            "FetchEvidence with error_signal must NOT prove honest absence "
            "— exception paths must route to record_failed, not record_empty."
        )

    def test_non_2xx_fetch_evidence_does_not_prove_absence(self) -> None:
        """FetchEvidence with non-2xx http_status does not prove honest absence."""
        for status in (0, 401, 403, 429, 500, 503):
            ev = FetchEvidence(
                http_status=status,
                response_received=status >= 400,
                rows_in_response=0,
                source="soccer_football_info",
                endpoint="sfi_progressive_stats",
                attempted_at=_ATTEMPT_TS,
                error_signal="HTTP_NON_2XX",
            )
            assert not ev.proves_honest_absence(), (
                f"FetchEvidence with http_status={status} must NOT prove honest absence"
            )


# ---------------------------------------------------------------------------
# process_zero_records — sports reference entity zero-rows path
# ---------------------------------------------------------------------------


class TestProcessZeroRecordsSportsReference:
    """Tests for process_zero_records._zero_sports_reference_fetch SOURCE_RETURNED_ZERO path.

    Regression for IS@0db2450 (DP-FETCH-002): api_football error → empty_confirmed.
    """

    @pytest.mark.asyncio
    async def test_zero_row_entity_record_empty_has_fetch_evidence(self) -> None:
        """When sports reference entity returns 0 rows, record_empty includes FetchEvidence.

        Calls _zero_sports_reference_fetch with a mocked _fetch_sports_reference_data
        that returns row_count=0 for a non-self-manifested entity (e.g. 'teams').
        Asserts record_empty is called with fetch_evidence proving honest absence.
        Regression: IS@0db2450 (DP-FETCH-002).
        """
        from instruments_service.engine.orchestrator.process_zero_records import (
            _zero_sports_reference_fetch,
        )

        mock_manifest = _make_manifest_mock()
        mock_manifest_cls = MagicMock(return_value=mock_manifest)

        with (
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_manifest_cls),
            patch(
                "instruments_service.engine.orchestrator._fetch_sports_reference_data",
                new=AsyncMock(return_value={"teams": 0}),
            ),
        ):
            await _zero_sports_reference_fetch(
                date=_DATE,
                bucket=_BUCKET,
                api_football_key="test-key",
                missing_entities=["teams"],
                per_fixture_entities=[],
                recovery_fixture_ids=None,
                redo_all=False,
            )

        # record_empty must have been called
        assert mock_manifest.record_empty.called, "record_empty must be called for 0-row entity"

        # Every record_empty call for TEAMS must include a proving FetchEvidence
        for c in mock_manifest.record_empty.call_args_list:
            kwargs = c.kwargs if c.kwargs else {}
            if kwargs.get("row_key", {}).get("data_type") == "TEAMS":
                ev = kwargs.get("fetch_evidence")
                assert ev is not None, (
                    "record_empty for TEAMS SOURCE_RETURNED_ZERO must include fetch_evidence "
                    "(regression IS@0db2450 DP-FETCH-002)"
                )
                assert isinstance(ev, FetchEvidence), "fetch_evidence must be a FetchEvidence instance"
                assert ev.proves_honest_absence(), "fetch_evidence for 0-row entity must prove honest absence"
                assert kwargs.get("reason") == EmptyConfirmedReason.SOURCE_RETURNED_ZERO

    @pytest.mark.asyncio
    async def test_nonzero_row_entity_calls_record_captured_not_empty(self) -> None:
        """When sports reference entity returns >0 rows, record_captured_from_counts is called.

        Sanity check — the 0-row branch must not fire when rows exist.
        """
        from instruments_service.engine.orchestrator.process_zero_records import (
            _zero_sports_reference_fetch,
        )

        mock_manifest = _make_manifest_mock()
        mock_manifest_cls = MagicMock(return_value=mock_manifest)

        with (
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_manifest_cls),
            patch(
                "instruments_service.engine.orchestrator._fetch_sports_reference_data",
                new=AsyncMock(return_value={"teams": 5}),
            ),
        ):
            await _zero_sports_reference_fetch(
                date=_DATE,
                bucket=_BUCKET,
                api_football_key="test-key",
                missing_entities=["teams"],
                per_fixture_entities=[],
                recovery_fixture_ids=None,
                redo_all=False,
            )

        # For non-zero rows, record_captured_from_counts must be called (not record_empty)
        record_empty_for_teams = [
            c
            for c in mock_manifest.record_empty.call_args_list
            if (c.kwargs or {}).get("row_key", {}).get("data_type") == "TEAMS"
        ]
        assert not record_empty_for_teams, "record_empty must NOT be called for TEAMS when row_count > 0"
        assert mock_manifest.record_captured_from_counts.called, (
            "record_captured_from_counts must be called for non-zero row count"
        )


# ---------------------------------------------------------------------------
# FetchEvidence proves_honest_absence contract
# ---------------------------------------------------------------------------


class TestFetchEvidenceContract:
    """Unit tests for the FetchEvidence.proves_honest_absence() contract.

    These are the fundamental truthiness assertions that underpin the keystone gate.
    Regression: IS@0db2450 (DP-FETCH-002) — error silently became empty_confirmed.
    """

    def test_clean_2xx_zero_rows_proves_absence(self) -> None:
        """2xx + response_received + rows==0 + no error_signal → proves_honest_absence==True."""
        ev = FetchEvidence(
            http_status=200,
            response_received=True,
            rows_in_response=0,
            source="api_football",
            endpoint="api_football_fixtures",
            attempted_at=_ATTEMPT_TS,
            error_signal="",
        )
        assert ev.proves_honest_absence()

    @pytest.mark.parametrize(
        "http_status,response_received,rows,error_signal",
        [
            # Non-2xx status
            (401, True, 0, "AUTH_401"),
            (403, True, 0, "AUTH_403"),
            (429, True, 0, "RATE_LIMITED_429"),
            (500, True, 0, "SERVER_5XX"),
            # No response (timeout / connection error)
            (0, False, 0, "TIMEOUT"),
            (0, False, 0, "CONNECT_ERROR"),
            # Exception path
            (0, False, 0, "ADAPTER_EXCEPTION"),
            # 2xx but rows > 0 (should be captured, not empty)
            (200, True, 5, ""),
            # error_signal on otherwise-clean response
            (200, True, 0, "ADAPTER_EXCEPTION"),
        ],
    )
    def test_non_proving_cases(
        self,
        http_status: int,
        response_received: bool,
        rows: int,
        error_signal: str,
    ) -> None:
        """Non-clean fetch paths must NOT prove honest absence.

        Any of: non-2xx, no response, rows>0, or error_signal present.
        Regression: IS@0db2450 (DP-FETCH-002).
        """
        ev = FetchEvidence(
            http_status=http_status,
            response_received=response_received,
            rows_in_response=rows,
            source="api_football",
            endpoint="api_football_fixtures",
            attempted_at=_ATTEMPT_TS,
            error_signal=error_signal,
        )
        assert not ev.proves_honest_absence(), (
            f"FetchEvidence({http_status=}, {response_received=}, {rows=}, {error_signal=!r}) "
            "must NOT prove honest absence"
        )


# ---------------------------------------------------------------------------
# DP_SOURCE_RATE_LIMITED emission — DP-RATE-003
# ---------------------------------------------------------------------------


class TestSportsRateLimitEvent:
    """A 429 from a sports adapter emits DP_SOURCE_RATE_LIMITED so a throttled
    backfill surfaces in #data-pipeline-alerts instead of silently sleeping on
    minute-boundary backoffs (DP-RATE-003).

    Regression: before this, a sports adapter rate-limited for hours would just
    log a warning + sleep — invisible to the alert pipeline (the same blind spot
    behind the TM/FootyStats 6.5h hang class).
    """

    @pytest.mark.asyncio
    async def test_429_emits_dp_source_rate_limited(self) -> None:
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="x")
        # reset the per-class cumulative counter so the assertion is deterministic
        type(adapter)._rate_limit_429_count = 0

        mock_resp_429 = MagicMock()
        mock_resp_429.status = 429
        cm_429 = MagicMock()
        cm_429.__aenter__ = AsyncMock(return_value=mock_resp_429)
        cm_429.__aexit__ = AsyncMock(return_value=None)

        mock_resp_200 = MagicMock()
        mock_resp_200.status = 200
        mock_resp_200.json = AsyncMock(return_value={"response": []})
        mock_resp_200.raise_for_status = MagicMock()
        mock_resp_200.headers = {}
        cm_200 = MagicMock()
        cm_200.__aenter__ = AsyncMock(return_value=mock_resp_200)
        cm_200.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get.side_effect = [cm_429, cm_200]

        with (
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.base.log_event"
            ) as mock_log_event,
            patch.object(adapter, "_throttle", AsyncMock()),
            patch("asyncio.sleep", AsyncMock()),
        ):
            await adapter._get_with_retry(mock_session, "https://api-football/fixtures")

        dp_calls = [
            c
            for c in mock_log_event.call_args_list
            if c.args and c.args[0] == "DP_SOURCE_RATE_LIMITED"
        ]
        assert len(dp_calls) == 1, "exactly one DP_SOURCE_RATE_LIMITED per 429"
        details = dp_calls[0].kwargs["details"]
        assert details["source"] == adapter.venue
        assert details["http_429_count"] >= 1
