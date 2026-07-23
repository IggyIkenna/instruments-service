"""Unit tests for instruments-service ``scripts/smoke_matrix.py``.

Verifies the 3-step assertion contract, shard-level isolation, dry-run
behaviour, cell-filter flags, and the Phase 3 api-football DependencyError
skip path.

Tests use monkeypatch for subprocess + storage_client so no real CLIs run
and no real GCS calls are made.
"""

from __future__ import annotations

import importlib.util
import sys
from io import BytesIO
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SMOKE_PATH = _REPO_ROOT / "scripts" / "smoke_matrix.py"


def _load_smoke_module() -> ModuleType:
    """Import scripts/smoke_matrix.py as a module (scripts/ isn't packaged)."""
    spec = importlib.util.spec_from_file_location("instruments_smoke_matrix", _SMOKE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["instruments_smoke_matrix"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def smoke() -> ModuleType:
    return _load_smoke_module()


class _FakeBlob:
    def __init__(self, name: str, exists_flag: bool = True, payload: bytes = b"") -> None:
        self.name = name
        self._exists = exists_flag
        self._payload = payload

    def exists(self) -> bool:
        return self._exists

    def download_as_bytes(self) -> bytes:
        return self._payload


class _FakeBucket:
    def __init__(self, manifest_blob: _FakeBlob | None = None) -> None:
        self._manifest_blob = manifest_blob

    def blob(self, path: str) -> _FakeBlob:
        if self._manifest_blob is not None and path.endswith("availability_index.parquet"):
            return self._manifest_blob
        return _FakeBlob(path, exists_flag=False)


class _FakeStorageClient:
    def __init__(
        self,
        parquet_blobs: list[_FakeBlob] | None = None,
        manifest_blob: _FakeBlob | None = None,
    ) -> None:
        self._parquet_blobs = parquet_blobs or []
        self._manifest_blob = manifest_blob

    def list_blobs(self, bucket: str, prefix: str) -> list[_FakeBlob]:
        return self._parquet_blobs

    def bucket(self, name: str) -> _FakeBucket:
        return _FakeBucket(self._manifest_blob)


def _make_manifest_bytes(
    date: str,
    category: str,
    venue: str,
    data_type: str,
    capture_status: str = "captured",
) -> bytes:
    df = pd.DataFrame(
        [
            {
                "date": date,
                "category": category,
                "venue": venue,
                "data_type": data_type,
                "capture_status": capture_status,
            }
        ]
    )
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class _FakeCompleted:
    def __init__(self, rc: int = 0, stdout: bytes = b"", stderr: bytes = b"") -> None:
        self.returncode = rc
        self.stdout = stdout
        self.stderr = stderr


# ---------------------------------------------------------------------------
# Enumeration tests
# ---------------------------------------------------------------------------


def test_enumerate_cells_produces_multiple_categories(smoke: ModuleType) -> None:
    cells = smoke.enumerate_cells()
    asset_groups = {c.asset_group for c in cells}
    # Every major category should produce at least one cell.
    assert {"CEFI", "TRADFI", "DEFI", "SPORTS", "PREDICTION"}.issubset(asset_groups)
    assert len(cells) > 20, f"expected >20 cells from UAC SSOT, got {len(cells)}"


def test_enumerate_cells_category_filter(smoke: ModuleType) -> None:
    cells = smoke.enumerate_cells(asset_group_filter="CEFI")
    assert cells
    assert {c.asset_group for c in cells} == {"CEFI"}


def test_enumerate_cells_sports_emits_api_football_first(smoke: ModuleType) -> None:
    cells = smoke.enumerate_cells(asset_group_filter="SPORTS")
    assert cells
    # T0 (API_FOOTBALL) MUST appear before any T1 enrichment provider.
    providers_in_order = [c.sports_provider for c in cells if c.sports_provider]
    assert providers_in_order, "sports enumeration produced no provider cells"
    assert providers_in_order[0] == "API_FOOTBALL", f"expected API_FOOTBALL first, got: {providers_in_order[:3]}"


def test_enumerate_cells_sports_includes_venue_routed_betfair(smoke: ModuleType) -> None:
    """Bare BETFAIR is a real, credential-gated instruments-service adapter (unlike
    ODDS_API/PINNACLE/etc, which are MTDS-owned NO_ADAPTER_YET) — it must be
    enumerated as a venue-routed cell (sports_provider=None), not silently omitted."""
    cells = smoke.enumerate_cells(asset_group_filter="SPORTS")
    betfair_cells = [c for c in cells if c.venue == "BETFAIR"]
    assert betfair_cells, "expected a venue-routed BETFAIR cell in SPORTS enumeration"
    assert all(c.sports_provider is None for c in betfair_cells)


def test_enumerate_cells_sports_venue_filter_selects_only_betfair(smoke: ModuleType) -> None:
    cells = smoke.enumerate_cells(asset_group_filter="SPORTS", venue_filter="BETFAIR")
    assert cells
    assert all(c.venue == "BETFAIR" and c.sports_provider is None for c in cells)


def test_enumerate_cells_sports_mtds_only_venue_returns_zero_cells(smoke: ModuleType) -> None:
    """ODDS_API/PINNACLE/etc are MTDS-owned (NO_ADAPTER_YET in instruments-service's
    own venue_adapter_keys.py, registry-consolidation Decision C 2026-06-29) — zero
    cells here is the honest, correct answer, not a bug to route around."""
    for venue in ("ODDS_API", "PINNACLE", "DRAFTKINGS", "FANDUEL"):
        cells = smoke.enumerate_cells(asset_group_filter="SPORTS", venue_filter=venue)
        assert cells == [], f"expected 0 cells for MTDS-only venue={venue}, got {cells}"


# ---------------------------------------------------------------------------
# Bucket / path resolution
# ---------------------------------------------------------------------------


def test_resolve_test_bucket_honours_category(smoke: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    assert smoke.resolve_test_bucket("CEFI") == "instruments-store-cefi-test-test-project"


def test_resolve_test_bucket_prediction_uses_flat_kind(smoke: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    """Prediction MUST resolve the dedicated flat kind (``instruments-store-prediction``
    -> abbreviated ``instruments-store-pred-{env}-{pid}``) via ``resolve_bucket_name`` with
    ``deployment_env="test"`` — NOT the generic ``get_bucket_name``+string-mangle path,
    which produced the non-existent long ``instruments-store-prediction-test-{pid}`` (404).
    """
    calls: list[dict[str, object]] = []

    def _fake_resolve(**kwargs: object) -> str:
        calls.append(kwargs)
        return "instruments-store-pred-test-central-element-323112"

    monkeypatch.setattr(smoke, "resolve_bucket_name", _fake_resolve)
    # get_bucket_name must NOT be reached for prediction — make it explode if it is.
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda *a, **k: pytest.fail("prediction must not fall through to get_bucket_name string-mangle"),
    )
    out = smoke.resolve_test_bucket("prediction", "central-element-323112")
    assert out == "instruments-store-pred-test-central-element-323112"
    assert "-prediction-test-" not in out  # the old 404 long form
    assert calls == [{"cloud": "gcp", "kind": "instruments-store-prediction", "deployment_env": "test"}]


def test_expected_write_prefix_sports_uses_sports_reference(smoke: ModuleType) -> None:
    # Provider-routed SPORTS cell (as actually emitted by _enumerate_sports_cells —
    # sports_provider is always set alongside venue for these) -> sports_reference/.
    cell = smoke.SmokeCell(asset_group="SPORTS", venue="API_FOOTBALL", data_type="odds", sports_provider="API_FOOTBALL")
    prefix = smoke.expected_write_prefix(cell, "2026-04-20")
    assert prefix.startswith("sports_reference/by_date/day=2026-04-20/")


def test_expected_write_prefix_non_sports_uses_instrument_availability(smoke: ModuleType) -> None:
    """Full canonical hive (operator HARD RULE R2, 2026-07-21 —
    instrument_availability_hive_canonicalisation_2026_07_21.md): day/pipeline_mode/
    asset_group/venue, matching writers.py::_instrument_availability_sink_for exactly.
    Verified against live GCS (2026-07-23): a real --test-run wrote parquets at exactly
    this path shape; the old day/venue-only prefix this test asserted was stale and
    caused every non-sports/non-prediction Phase-D smoke cell to false-fail."""
    cell = smoke.SmokeCell(asset_group="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    prefix = smoke.expected_write_prefix(cell, "2026-04-20")
    assert prefix == (
        "instrument_availability/by_date/day=2026-04-20/"
        "pipeline_mode=batch_instruments_service/asset_group=cefi/venue=BINANCE-FUTURES/"
    )


def test_expected_write_prefix_venue_routed_sports_uses_instrument_availability(smoke: ModuleType) -> None:
    """Bare BETFAIR (sports_provider=None) writes through the generic per-venue
    instrument-catalog path, NOT sports_reference/ (which is provider-only). Same
    hive-canonicalisation fix as the CEFI case above."""
    cell = smoke.SmokeCell(asset_group="SPORTS", venue="BETFAIR", data_type="instruments")
    prefix = smoke.expected_write_prefix(cell, "2026-04-20")
    assert prefix == (
        "instrument_availability/by_date/day=2026-04-20/"
        "pipeline_mode=batch_instruments_service/asset_group=sports/venue=BETFAIR/"
    )


def test_expected_write_prefix_prediction_uses_cqg_base_list_prefix(smoke: ModuleType) -> None:
    """PREDICTION writes the CQG-FIRST layout (``canonical_question_group=`` precedes
    ``day=``/``venue=``), so no day-first literal prefix can match — ``expected_write_prefix``
    returns the coarse base ``by_date/`` tree that day+venue substring-scoping lists under."""
    cell = smoke.SmokeCell(asset_group="PREDICTION", venue="POLYMARKET", data_type="prediction")
    prefix = smoke.expected_write_prefix(cell, "2026-07-15")
    assert prefix == "instrument_availability/by_date/"
    assert "day=" not in prefix and "venue=" not in prefix


def test_verify_prediction_parquet_written_scoped_by_day_venue(smoke: ModuleType) -> None:
    """The prediction write-verify counts ONLY the CQG-first ``instruments.parquet`` objects
    for THIS day+venue, and skips other-day / other-venue / non-instruments objects. It also
    proves the day-first prefix the other asset_groups use would match ZERO of them."""
    day, venue = "2026-07-15", "POLYMARKET"
    base = "instrument_availability/by_date"
    hit_a = f"{base}/canonical_question_group=BTC_UP_DOWN_DAILY/day={day}/venue={venue}/instruments.parquet"
    hit_b = f"{base}/canonical_question_group=ETH_UP_DOWN_DAILY/day={day}/venue={venue}/instruments.parquet"
    other_day = f"{base}/canonical_question_group=BTC_UP_DOWN_DAILY/day=2026-07-14/venue={venue}/instruments.parquet"
    other_venue = f"{base}/canonical_question_group=BTC_UP_DOWN_DAILY/day={day}/venue=KALSHI/instruments.parquet"
    not_instruments = (
        f"{base}/canonical_question_group=BTC_UP_DOWN_DAILY/day={day}/venue={venue}/market_lifecycle.parquet"
    )
    client = _FakeStorageClient(
        parquet_blobs=[
            _FakeBlob(hit_a),
            _FakeBlob(hit_b),
            _FakeBlob(other_day),
            _FakeBlob(other_venue),
            _FakeBlob(not_instruments),
        ]
    )
    ok, n = smoke.verify_prediction_parquet_written("bkt", day, venue, client)
    assert ok is True
    assert n == 2  # only the two THIS-day THIS-venue instruments.parquet objects

    # A GCS prefix-listing under the day-first prefix (what verify_parquet_written uses for
    # cefi/tradfi/defi/sports) would return zero of these CQG-first objects — the exact bug.
    day_first_prefix = f"instrument_availability/by_date/day={day}/venue={venue}/"
    assert not any(name.startswith(day_first_prefix) for name in (hit_a, hit_b))


def test_run_cell_prediction_passes_via_cqg_first_verify(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end run_cell for a prediction venue cell: the CQG-first write-verify + manifest
    row both resolve, so the cell passes (before the fix, the day-first prefix found 0)."""
    monkeypatch.setattr(smoke, "get_project_id", lambda: "central-element-323112")
    monkeypatch.setattr(
        smoke,
        "resolve_bucket_name",
        lambda **kwargs: "instruments-store-pred-test-central-element-323112",
    )
    cell = smoke.SmokeCell(asset_group="PREDICTION", venue="POLYMARKET", data_type="prediction")
    smoke_date = "2026-07-15"
    cqg_obj = (
        "instrument_availability/by_date/canonical_question_group=BTC_UP_DOWN_DAILY/"
        f"day={smoke_date}/venue=POLYMARKET/instruments.parquet"
    )
    client = _FakeStorageClient(
        parquet_blobs=[_FakeBlob(cqg_obj)],
        manifest_blob=_FakeBlob(
            "_index/availability_index.parquet",
            exists_flag=True,
            payload=_make_manifest_bytes(smoke_date, "PREDICTION", "POLYMARKET", "prediction"),
        ),
    )
    result = smoke.run_cell(
        cell=cell,
        smoke_date=smoke_date,
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=0),
        storage_client=client,
    )
    assert result.status == "passed", f"expected passed, got {result.status} ({result.reason})"
    assert result.parquet_count == 1
    assert result.manifest_status == "captured"


def test_build_cli_args_venue_routed_sports_uses_venues_flag(smoke: ModuleType) -> None:
    """A sports_provider=None SPORTS cell (bare BETFAIR) must build --venues BETFAIR,
    not silently omit any venue selector (which would run the full default SPORTS set)."""
    cell = smoke.SmokeCell(asset_group="SPORTS", venue="BETFAIR", data_type="instruments")
    argv = smoke.build_cli_args(cell, "2026-04-20")
    assert "--venues" in argv
    assert argv[argv.index("--venues") + 1] == "BETFAIR"
    assert "--sports-provider" not in argv


def test_build_cli_args_provider_routed_sports_uses_sports_provider_flag(smoke: ModuleType) -> None:
    cell = smoke.SmokeCell(asset_group="SPORTS", venue="API_FOOTBALL", data_type="odds", sports_provider="API_FOOTBALL")
    argv = smoke.build_cli_args(cell, "2026-04-20")
    assert "--sports-provider" in argv
    assert argv[argv.index("--sports-provider") + 1] == "API_FOOTBALL"
    assert "--venues" not in argv


# ---------------------------------------------------------------------------
# 3-step contract — pass case
# ---------------------------------------------------------------------------


def test_run_cell_passes_when_all_three_steps_succeed(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )

    cell = smoke.SmokeCell(asset_group="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    smoke_date = "2026-04-20"

    def fake_runner(argv: list[str], **kwargs: Any) -> _FakeCompleted:
        assert "IS_TEST_RUN" in kwargs["env"]
        assert kwargs["env"]["IS_TEST_RUN"] == "true"
        return _FakeCompleted(rc=0, stdout=b"ok", stderr=b"")

    client = _FakeStorageClient(
        parquet_blobs=[
            _FakeBlob("instrument_availability/by_date/day=2026-04-20/venue=BINANCE-FUTURES/instruments.parquet")
        ],
        manifest_blob=_FakeBlob(
            "_index/availability_index.parquet",
            exists_flag=True,
            payload=_make_manifest_bytes(smoke_date, "CEFI", "BINANCE-FUTURES", "trades"),
        ),
    )

    result = smoke.run_cell(
        cell=cell,
        smoke_date=smoke_date,
        subprocess_runner=fake_runner,
        storage_client=client,
    )

    assert result.status == "passed", f"expected passed, got {result.status} ({result.reason})"
    assert result.parquet_count == 1
    assert result.manifest_status == "captured"


def test_run_cell_empty_confirmed_is_pass(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )

    cell = smoke.SmokeCell(asset_group="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    smoke_date = "2026-04-20"

    client = _FakeStorageClient(
        parquet_blobs=[
            _FakeBlob("instrument_availability/by_date/day=2026-04-20/venue=BINANCE-FUTURES/instruments.parquet")
        ],
        manifest_blob=_FakeBlob(
            "_index/availability_index.parquet",
            exists_flag=True,
            payload=_make_manifest_bytes(
                smoke_date, "CEFI", "BINANCE-FUTURES", "trades", capture_status="empty_confirmed"
            ),
        ),
    )

    result = smoke.run_cell(
        cell=cell,
        smoke_date=smoke_date,
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=0),
        storage_client=client,
    )
    assert result.status == "passed"
    assert result.manifest_status == "empty_confirmed"


# ---------------------------------------------------------------------------
# 3-step contract — failure modes
# ---------------------------------------------------------------------------


def test_run_cell_fails_on_nonzero_rc(smoke: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    cell = smoke.SmokeCell(asset_group="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    result = smoke.run_cell(
        cell=cell,
        smoke_date="2026-04-20",
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=1, stderr=b"boom"),
        storage_client=_FakeStorageClient(),
    )
    assert result.status == "failed"
    assert "cli_nonzero_rc=1" in result.reason


def test_run_cell_fails_when_no_parquet(smoke: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    cell = smoke.SmokeCell(asset_group="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    result = smoke.run_cell(
        cell=cell,
        smoke_date="2026-04-20",
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=0),
        storage_client=_FakeStorageClient(parquet_blobs=[]),  # no parquet
    )
    assert result.status == "failed"
    assert result.reason.startswith("no_parquet_at:")


def test_run_cell_fails_when_manifest_row_missing(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    cell = smoke.SmokeCell(asset_group="CEFI", venue="BINANCE-FUTURES", data_type="trades")
    # Manifest has row for a DIFFERENT date — no matching row for our smoke_date.
    bad_manifest = _make_manifest_bytes("2000-01-01", "CEFI", "BINANCE-FUTURES", "trades")
    client = _FakeStorageClient(
        parquet_blobs=[_FakeBlob("x.parquet")],
        manifest_blob=_FakeBlob("_index/availability_index.parquet", exists_flag=True, payload=bad_manifest),
    )
    result = smoke.run_cell(
        cell=cell,
        smoke_date="2026-04-20",
        subprocess_runner=lambda *a, **k: _FakeCompleted(rc=0),
        storage_client=client,
    )
    assert result.status == "failed"
    assert "no_matching_row" in result.reason or "manifest_status_invalid" in result.reason


def test_verify_manifest_row_venue_routed_sports_filters_by_venue(smoke: ModuleType) -> None:
    """A BETFAIR-style (sports_provider=None) SPORTS cell must filter on venue —
    unlike a provider-routed cell, its manifest rows carry a real venue column."""
    cell = smoke.SmokeCell(asset_group="SPORTS", venue="BETFAIR", data_type="instruments")
    # Manifest has a row for a DIFFERENT SPORTS venue on the same date — must NOT
    # false-match just because asset_group+date agree (that was the pre-fix bug:
    # SPORTS cells universally skipped the venue filter).
    other_venue_manifest = _make_manifest_bytes("2026-04-20", "SPORTS", "SOME_OTHER_VENUE", "instruments")
    client = _FakeStorageClient(
        parquet_blobs=[_FakeBlob("x.parquet")],
        manifest_blob=_FakeBlob("_index/availability_index.parquet", exists_flag=True, payload=other_venue_manifest),
    )
    ok, status = smoke.verify_manifest_row(bucket="ignored", cell=cell, smoke_date="2026-04-20", storage_client=client)
    assert not ok
    assert status == "no_matching_row"


def test_run_cell_skips_on_api_football_dependency_error(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    cell = smoke.SmokeCell(
        asset_group="SPORTS",
        venue="FOOTYSTATS",
        data_type="odds",
        sports_provider="FOOTYSTATS",
    )
    result = smoke.run_cell(
        cell=cell,
        smoke_date="2026-04-20",
        subprocess_runner=lambda *a, **k: _FakeCompleted(
            rc=1,
            stderr=b"DependencyError: api-football reference data missing for date 2026-04-20",
        ),
        storage_client=_FakeStorageClient(),
    )
    assert result.status == "skipped"
    assert result.reason == "api_football_missing"


# ---------------------------------------------------------------------------
# Shard-level isolation
# ---------------------------------------------------------------------------


def test_matrix_continues_after_single_cell_failure(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One failed cell must NOT abort subsequent cells."""
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )

    cells = [
        smoke.SmokeCell(asset_group="CEFI", venue="BINANCE-FUTURES", data_type="trades"),
        smoke.SmokeCell(asset_group="CEFI", venue="BYBIT", data_type="trades"),
    ]

    call_count = {"n": 0}

    def fake_runner(argv: list[str], **kwargs: Any) -> _FakeCompleted:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _FakeCompleted(rc=1, stderr=b"first cell dies")
        return _FakeCompleted(rc=0)

    # Fake storage succeeds for the second cell only.
    manifest_bytes = _make_manifest_bytes("2026-04-20", "CEFI", "BYBIT", "trades")
    client = _FakeStorageClient(
        parquet_blobs=[_FakeBlob("x.parquet")],
        manifest_blob=_FakeBlob("_index/availability_index.parquet", exists_flag=True, payload=manifest_bytes),
    )

    # Patch run_cell's storage default via monkeypatching get_storage_client inside smoke module.
    monkeypatch.setattr(smoke, "get_storage_client", lambda: client)

    # Drive run_matrix directly but route via our fake subprocess.runner.
    # Note: run_matrix doesn't accept a runner injection by design — we inline it here.
    results = []
    for c in cells:
        results.append(
            smoke.run_cell(cell=c, smoke_date="2026-04-20", subprocess_runner=fake_runner, storage_client=client)
        )

    assert call_count["n"] == 2, "second cell must still have run after the first failed"
    assert results[0].status == "failed"
    assert results[1].status == "passed"


# ---------------------------------------------------------------------------
# Dry-run + CLI smoke
# ---------------------------------------------------------------------------


def test_dry_run_enumerates_without_invoking_subprocess(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )

    called = {"n": 0}

    def fail_if_called(*a: Any, **k: Any) -> Any:
        called["n"] += 1
        raise AssertionError("subprocess must NOT run during --dry-run")

    monkeypatch.setattr(smoke.subprocess, "run", fail_if_called)

    cells = smoke.enumerate_cells(asset_group_filter="PREDICTION")
    assert cells, "PREDICTION should enumerate at least one cell"
    report = smoke.run_matrix(cells=cells, smoke_date="2026-04-20", execute=False)

    assert called["n"] == 0
    assert report.skipped == len(cells)
    assert report.failed == 0
    assert report.passed == 0
    assert all(r.reason == "dry_run" for r in report.results)


def test_main_returns_zero_on_dry_run(
    smoke: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(smoke, "get_project_id", lambda: "test-project")
    monkeypatch.setattr(
        smoke,
        "get_bucket_name",
        lambda domain, category, project_id: f"instruments-store-{category.lower()}-{project_id}",
    )
    rc = smoke.main(["--asset-group", "PREDICTION"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "smoke matrix" in out
