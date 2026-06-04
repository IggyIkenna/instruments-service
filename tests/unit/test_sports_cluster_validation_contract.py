"""SP-10 regression guard for the bundled-shard cluster-validation contract.

Context (``unified-trading-pm/plans/epics/sports_master.md`` SP-10): the
sports / prediction manifest writes in
``instruments_service.engine.orchestrator`` call
``ManifestWriter.record_captured_from_counts`` with
``expected_root_clusters={}``. An empty dict makes the 4th write-gate pillar
(cluster coverage) a deliberate NO-OP — which is correct for those callsites
because each is a *single-entity* shard (one ``(date, data_type[, league_id])``
row per logical entity), with no authoritative per-root-cluster contract to
assert against. Inventing an expectation there would FALSE-FAIL genuinely
captured data.

These tests do NOT assert that the orchestrator passes a non-empty dict. They
pin the *mechanism* itself: when a real ``expected_root_clusters`` IS supplied
(the shape a future genuinely-bundled caller would use), the gate
(a) PASSES → ``captured`` when every expected cluster meets its minimum, and
(b) FLAGS → ``attempted_failed`` (``ClusterCoverageError``) when a cluster is
missing — i.e. it is not a silent pass. If a future refactor regresses the gate
to a no-op, case ``missing_cluster`` flips to ``captured`` and this test fails,
surfacing the silent-incompleteness bug SP-10 guards against.

Credential-free: ``record_captured_from_counts`` only appends to the writer's
in-memory ``_records`` buffer; no GCS round-trip happens until ``.write()``,
which is never called here.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
from unified_api_contracts import PipelineMode
from unified_trading_library import CaptureStatus, ManifestWriter

_AVAILABLE_AT = pd.Timestamp(datetime(2026, 6, 4, 17, 0, tzinfo=UTC))


def _writer() -> ManifestWriter:
    # catalogue_bucket is only touched at .write()-time; the in-memory record
    # staging exercised here needs no real bucket / credentials.
    return ManifestWriter(service_name="instruments-service", catalogue_bucket="test-bucket")


class TestBundledClusterValidationContract:
    """The cluster gate is the 4th write-time pillar; prove it is not a no-op
    when a real ``expected_root_clusters`` is supplied."""

    def test_all_expected_clusters_present_records_captured(self) -> None:
        """All expected clusters meet their minimum → row stages as CAPTURED."""
        writer = _writer()
        writer.record_captured_from_counts(
            row_key={"date": "2026-06-04", "data_type": "FIXTURE_STATS", "league_id": "EPL"},
            total_rows=20,
            expected_root_clusters={"fixture_100": 1, "fixture_101": 1},
            observed_clusters={"fixture_100": 12, "fixture_101": 8},
            available_at_envelope=_AVAILABLE_AT,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        )
        assert len(writer._records) == 1
        rec = writer._records[-1]
        assert rec.capture_status == CaptureStatus.CAPTURED.value
        assert rec.error_reason == ""

    def test_missing_expected_cluster_records_attempted_failed(self) -> None:
        """A missing expected cluster → the gate flips the row to
        ATTEMPTED_FAILED with a ClusterCoverageError reason (NOT a silent
        captured-with-partial-bundle)."""
        writer = _writer()
        writer.record_captured_from_counts(
            row_key={"date": "2026-06-04", "data_type": "FIXTURE_STATS", "league_id": "EPL"},
            total_rows=12,
            expected_root_clusters={"fixture_100": 1, "fixture_101": 1},
            observed_clusters={"fixture_100": 12},  # fixture_101 absent
            available_at_envelope=_AVAILABLE_AT,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        )
        assert len(writer._records) == 1
        rec = writer._records[-1]
        assert rec.capture_status == CaptureStatus.ATTEMPTED_FAILED.value
        # The missing cluster id must surface in the error reason for triage.
        assert "fixture_101" in rec.error_reason
        assert "cluster_coverage_under_min" in rec.error_reason

    def test_under_min_cluster_records_attempted_failed(self) -> None:
        """A cluster present but BELOW its declared minimum is also a failure —
        catches the silent-partial-bundle case (some clusters truncated)."""
        writer = _writer()
        writer.record_captured_from_counts(
            row_key={"date": "2026-06-04", "data_type": "FIXTURE_STATS", "league_id": "EPL"},
            total_rows=5,
            expected_root_clusters={"fixture_100": 10, "fixture_101": 10},
            observed_clusters={"fixture_100": 10, "fixture_101": 3},  # 101 under min
            available_at_envelope=_AVAILABLE_AT,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        )
        rec = writer._records[-1]
        assert rec.capture_status == CaptureStatus.ATTEMPTED_FAILED.value
        assert "fixture_101" in rec.error_reason

    def test_empty_expected_clusters_is_intentional_no_op(self) -> None:
        """The current sports/prediction single-entity callsites pass
        ``expected_root_clusters={}`` — documenting that this is a deliberate
        no-op (single blank-key ``observed`` cluster stages as CAPTURED). This
        pins the SP-10 decision so a reviewer/refactor sees it is intentional,
        not an oversight."""
        writer = _writer()
        writer.record_captured_from_counts(
            row_key={"date": "2026-06-04", "data_type": "TEAMS"},
            total_rows=5,
            expected_root_clusters={},
            observed_clusters={"": 5},
            available_at_envelope=_AVAILABLE_AT,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        )
        rec = writer._records[-1]
        assert rec.capture_status == CaptureStatus.CAPTURED.value
        assert rec.error_reason == ""
