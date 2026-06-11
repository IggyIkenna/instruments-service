"""Coverage boost for TransfermarktAdapter._run_apify_actor and get_teams paths.

Targets uncovered lines in transfermarkt.py:
- 91-140: _run_apify_actor (happy path + error branches)
- 172, 175: _fetch_clubs_via_rapidapi non-dict entry + no club_id
- 196-197: exception in club-profile fetch
- 231-236: get_teams Apify path
- 244-247: get_teams exception handler
- 255: squad is None
- 258-264: normalization exception in get_teams loop
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Session mock helpers
# ---------------------------------------------------------------------------


def _make_cm(response_mock: MagicMock) -> MagicMock:
    """Wrap a response mock in an async context manager."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response_mock)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


def _make_response(status: int = 200, json_data: object = None, text_data: str = "") -> MagicMock:
    """Build a mock aiohttp response."""
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data)
    resp.text = AsyncMock(return_value=text_data)
    return resp


def _apify_session(
    start_status: int = 201,
    run_id: str = "run123",
    dataset_id: str | None = "ds456",
    poll_statuses: list[str] | None = None,
    dataset_items: list[object] | None = None,
    poll_http_status: int = 200,
    dataset_http_status: int = 200,
) -> MagicMock:
    """Build a mock for TransfermarktAdapter._make_session() returning an Apify-style session.

    Args:
        start_status: HTTP status code for the POST (actor start).
        run_id: The run ID in the start response.
        dataset_id: The dataset ID in the start response (None → omit field).
        poll_statuses: Actor status values returned by successive GET polls.
        dataset_items: The JSON body for the dataset GET.
        poll_http_status: HTTP status for poll responses.
        dataset_http_status: HTTP status for the dataset response.
    """
    if poll_statuses is None:
        poll_statuses = ["SUCCEEDED"]
    if dataset_items is None:
        dataset_items = []

    # POST response
    start_body: dict[str, object] = {"data": {"id": run_id}}
    if dataset_id is not None:
        start_body["data"]["defaultDatasetId"] = dataset_id  # type: ignore[index]
    start_resp = _make_response(start_status, start_body)
    start_cm = _make_cm(start_resp)

    # GET poll responses (one per poll_statuses entry)
    poll_cms = []
    for s in poll_statuses:
        pr = _make_response(
            poll_http_status,
            {"data": {"status": s, "statusMessage": ""}},
        )
        poll_cms.append(_make_cm(pr))

    # GET dataset response
    dataset_resp = _make_response(dataset_http_status, dataset_items)
    dataset_cm = _make_cm(dataset_resp)

    # All GET calls in order: polls then dataset
    get_side_effect = [*poll_cms, dataset_cm]

    session = MagicMock()
    session.post = MagicMock(return_value=start_cm)
    session.get = MagicMock(side_effect=get_side_effect)

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    return session_cm


# ---------------------------------------------------------------------------
# _run_apify_actor — happy path + error branches
# ---------------------------------------------------------------------------


class TestRunApifyActor:
    """Lines 91-140: _run_apify_actor."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    def _adapter(self):
        from instruments_service.reference_data.adapters.sports.adapters.transfermarkt import (
            TransfermarktAdapter,
        )

        return TransfermarktAdapter(api_key="apify_api_test_key")

    @pytest.mark.asyncio
    async def test_success_returns_items(self) -> None:
        """Lines 91-140 happy path: actor starts → polls SUCCEEDED → returns items."""
        adapter = self._adapter()
        items = [{"player": "Salah", "clubId": 1}]
        session_cm = _apify_session(dataset_items=items)

        with patch.object(adapter, "_make_session", return_value=session_cm):
            result = await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

        assert result == items

    @pytest.mark.asyncio
    async def test_start_non_201_raises(self) -> None:
        """Lines 103-105: non-201 start response raises RuntimeError."""
        adapter = self._adapter()
        session_cm = _apify_session(start_status=403)

        with (
            patch.object(adapter, "_make_session", return_value=session_cm),
            pytest.raises(RuntimeError, match="Apify actor start failed"),
        ):
            await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

    @pytest.mark.asyncio
    async def test_no_run_id_raises(self) -> None:
        """Lines 111-112: start response missing run ID raises RuntimeError."""
        adapter = self._adapter()
        session_cm = _apify_session(run_id="")

        with (
            patch.object(adapter, "_make_session", return_value=session_cm),
            pytest.raises(RuntimeError, match="no run ID"),
        ):
            await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

    @pytest.mark.asyncio
    async def test_poll_non_200_continues(self) -> None:
        """Lines 120-121: poll status != 200 continues to next poll, then succeeds."""
        adapter = self._adapter()
        # First poll returns 404 (continue), second returns 200 SUCCEEDED
        items = [{"x": 1}]
        start_body: dict[str, object] = {"data": {"id": "run1", "defaultDatasetId": "ds1"}}
        start_resp = _make_response(201, start_body)
        start_cm = _make_cm(start_resp)

        poll_fail = _make_response(404, {})
        poll_fail_cm = _make_cm(poll_fail)
        poll_ok = _make_response(200, {"data": {"status": "SUCCEEDED", "statusMessage": ""}})
        poll_ok_cm = _make_cm(poll_ok)
        dataset_resp = _make_response(200, items)
        dataset_cm = _make_cm(dataset_resp)

        session = MagicMock()
        session.post = MagicMock(return_value=start_cm)
        session.get = MagicMock(side_effect=[poll_fail_cm, poll_ok_cm, dataset_cm])

        session_outer = MagicMock()
        session_outer.__aenter__ = AsyncMock(return_value=session)
        session_outer.__aexit__ = AsyncMock(return_value=None)

        with patch.object(adapter, "_make_session", return_value=session_outer):
            result = await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

        assert result == items

    @pytest.mark.asyncio
    async def test_poll_failed_status_raises(self) -> None:
        """Lines 126-128: actor FAILED status raises RuntimeError."""
        adapter = self._adapter()
        session_cm = _apify_session(poll_statuses=["FAILED"])

        with (
            patch.object(adapter, "_make_session", return_value=session_cm),
            pytest.raises(RuntimeError, match="Apify actor FAILED"),
        ):
            await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

    @pytest.mark.asyncio
    async def test_poll_aborted_status_raises(self) -> None:
        """Lines 126-128: actor ABORTED status raises RuntimeError."""
        adapter = self._adapter()
        session_cm = _apify_session(poll_statuses=["ABORTED"])

        with (
            patch.object(adapter, "_make_session", return_value=session_cm),
            pytest.raises(RuntimeError, match="Apify actor ABORTED"),
        ):
            await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

    @pytest.mark.asyncio
    async def test_poll_timeout_raises(self) -> None:
        """Lines 129-130: all poll attempts exhausted raises RuntimeError."""
        adapter = self._adapter()
        # With timeout_secs=10, poll_interval=5 → max_polls=2; both return RUNNING
        session_cm = _apify_session(poll_statuses=["RUNNING", "RUNNING"])

        with (
            patch.object(adapter, "_make_session", return_value=session_cm),
            pytest.raises(RuntimeError, match="timed out"),
        ):
            await adapter._run_apify_actor(
                "https://transfermarkt.com/clubs/5",
                timeout_secs=10,
                poll_interval=5,
            )

    @pytest.mark.asyncio
    async def test_no_dataset_id_returns_empty(self) -> None:
        """Lines 133-134: missing defaultDatasetId returns []."""
        adapter = self._adapter()
        session_cm = _apify_session(dataset_id=None)

        with patch.object(adapter, "_make_session", return_value=session_cm):
            result = await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

        assert result == []

    @pytest.mark.asyncio
    async def test_dataset_non_200_raises(self) -> None:
        """Lines 137-138: dataset HTTP != 200 raises RuntimeError."""
        adapter = self._adapter()
        session_cm = _apify_session(dataset_http_status=500)

        with (
            patch.object(adapter, "_make_session", return_value=session_cm),
            pytest.raises(RuntimeError, match="Apify dataset fetch failed"),
        ):
            await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

    @pytest.mark.asyncio
    async def test_dataset_non_list_returns_empty(self) -> None:
        """Line 140: dataset body is not a list → returns []."""
        adapter = self._adapter()
        session_cm = _apify_session(dataset_items={"data": "not-a-list"})  # type: ignore[arg-type]

        with patch.object(adapter, "_make_session", return_value=session_cm):
            result = await adapter._run_apify_actor("https://transfermarkt.com/clubs/5")

        assert result == []


# ---------------------------------------------------------------------------
# get_teams — Apify path + exception paths
# ---------------------------------------------------------------------------


class TestGetTeamsApifyPath:
    """Lines 231-264: get_teams using Apify backend."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    def _adapter(self):
        from instruments_service.reference_data.adapters.sports.adapters.transfermarkt import (
            TransfermarktAdapter,
        )

        return TransfermarktAdapter(api_key="apify_api_test_key")

    @pytest.mark.asyncio
    async def test_apify_path_success(self) -> None:
        """Lines 231-257: Apify path groups players into clubs and normalizes teams."""
        adapter = self._adapter()

        from unittest.mock import MagicMock

        mock_team = MagicMock()
        mock_team.team_id = "1"
        mock_team.name = "Arsenal"

        with (
            patch.object(adapter, "_run_apify_actor", new_callable=AsyncMock, return_value=[]),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._group_apify_players_into_clubs",
                return_value={"clubs": [{"id": "1", "name": "Arsenal"}]},
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._extract_clubs",
                return_value=[{"id": "1", "name": "Arsenal"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._parse_squad",
                return_value=MagicMock(),
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt.normalize_transfermarkt_team_from_squad",
                return_value=mock_team,
            ),
        ):
            result = await adapter.get_teams("GB1")

        assert len(result) == 1
        assert result[0].name == "Arsenal"

    @pytest.mark.asyncio
    async def test_apify_path_exception_reraises(self) -> None:
        """Lines 244-247: Apify exception is classified and re-raised."""
        adapter = self._adapter()

        with (
            patch.object(
                adapter,
                "_run_apify_actor",
                new_callable=AsyncMock,
                side_effect=RuntimeError("actor failed"),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError, match="actor failed"),
        ):
            await adapter.get_teams("GB1")

    @pytest.mark.asyncio
    async def test_squad_none_skipped(self) -> None:
        """Line 255: _parse_squad returning None skips the club."""
        adapter = self._adapter()

        with (
            patch.object(adapter, "_run_apify_actor", new_callable=AsyncMock, return_value=[]),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._group_apify_players_into_clubs",
                return_value={"clubs": [{"id": "1"}]},
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._extract_clubs",
                return_value=[{"id": "1"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._parse_squad",
                return_value=None,
            ),
        ):
            result = await adapter.get_teams("GB1")

        assert result == []

    @pytest.mark.asyncio
    async def test_normalize_exception_continues(self) -> None:
        """Lines 258-264: normalization exception logs warning and continues."""
        adapter = self._adapter()

        with (
            patch.object(adapter, "_run_apify_actor", new_callable=AsyncMock, return_value=[]),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._group_apify_players_into_clubs",
                return_value={"clubs": [{"id": "1"}]},
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._extract_clubs",
                return_value=[{"id": "1"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt._parse_squad",
                return_value=MagicMock(),
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.transfermarkt.normalize_transfermarkt_team_from_squad",
                side_effect=ValueError("bad squad"),
            ),
        ):
            result = await adapter.get_teams("GB1")

        assert result == []
