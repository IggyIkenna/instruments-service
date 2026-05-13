"""Unit tests for catalog_snapshot emission policy wiring (Phase 6.8 PART B).

Covers _check_emission_policy() PARTIAL_OK routing for the four completeness
scenarios: full, partial, zero, and smoke-test (function returns EmissionDecision).

UAC seed: ("instruments-service", "catalog_snapshot"): ServiceEmissionPolicy.PARTIAL_OK
Plan: writegate_honest_coverage_endtoend_2026_05_06.md Phase 6.8 PART B.
"""

from __future__ import annotations

import pytest
from unified_trading_library import EmissionDecision

from instruments_service.engine.orchestrator import _check_emission_policy


class TestCheckEmissionPolicyReturnType:
    """Smoke-test: _check_emission_policy returns an EmissionDecision."""

    def test_returns_emission_decision(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """_check_emission_policy must return an EmissionDecision instance."""
        # No monkeypatching needed — publish_with_policy works credential-free in
        # CLOUD_MOCK_MODE=true (test infrastructure per cicd_mock_hardening plan).
        decision = _check_emission_policy(date="2026-05-13", completeness_fraction=1.0)
        assert isinstance(decision, EmissionDecision)


class TestCheckEmissionPolicyPartialOK:
    """Routing matrix for PARTIAL_OK policy at the catalog_snapshot boundary.

    PARTIAL_OK semantics (UAC Phase 6.8 PART B):
    - completeness == 1.0  → PUBLISHED_OK  + should_publish_row=True  + should_alert=False
    - completeness < 1.0   → PUBLISHED_DEGRADED + should_publish_row=True  + should_alert=False
    - completeness == 0.0  → PUBLISHED_DEGRADED + should_publish_row=True  + should_alert=False
      (PARTIAL_OK never suppresses — only STRICT_FAIL / BLOCK_CRITICAL suppress)
    """

    def test_full_completeness_yields_published_ok(self) -> None:
        """completeness=1.0 → PUBLISHED_OK + should_publish_row=True."""
        decision = _check_emission_policy(date="2026-05-13", completeness_fraction=1.0)
        assert decision.should_publish_row is True
        assert decision.should_alert is False
        assert decision.service_emission_state == "PUBLISHED_OK"
        assert decision.completeness_fraction == 1.0

    def test_partial_completeness_yields_published_degraded(self) -> None:
        """completeness=0.5 → PUBLISHED_DEGRADED + should_publish_row=True (PARTIAL_OK allows)."""
        decision = _check_emission_policy(date="2026-05-13", completeness_fraction=0.5)
        assert decision.should_publish_row is True
        assert decision.should_alert is False
        assert decision.service_emission_state == "PUBLISHED_DEGRADED"
        assert decision.completeness_fraction == pytest.approx(0.5)

    def test_zero_completeness_still_publishes(self) -> None:
        """completeness=0.0 → PUBLISHED_DEGRADED + should_publish_row=True.

        PARTIAL_OK never suppresses, even at zero completeness.
        Only STRICT_FAIL / BLOCK_CRITICAL would suppress.
        """
        decision = _check_emission_policy(date="2026-05-13", completeness_fraction=0.0)
        assert decision.should_publish_row is True
        assert decision.should_alert is False
        assert decision.service_emission_state == "PUBLISHED_DEGRADED"
        assert decision.completeness_fraction == pytest.approx(0.0)
