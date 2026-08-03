"""Regression: phantom reconciler exempts manifest-only bundle atoms.

Guards the 2026-07-10 incident (issue:
``prediction_phantom_reconciler_wipes_bundle_atom_2026_07_10.md``) where
``scripts/reconcile_phantom_manifest_rows_all.py`` demoted ~15,769 legitimately
``captured`` prediction bundle-atom cells to ``attempted_failed`` because the
v9 prediction bundle atom (``data_type=prediction_canonical_question_group``)
has a SYNTHETIC ``canonical_question_group`` ``instrument_id`` with NO on-disk
``data_type={dt}/`` folder — the object-existence probe can never match it.

Rule-11 (this reconciler runs against ALL asset groups): the exemption MUST NOT
weaken per-object phantom detection. Both are asserted against ``_audit_generic``:

* (a) a bundle-atom ``captured`` row (``prediction_canonical_question_group``,
  synthetic cqg ``instrument_id``) is classified REAL (survives) even when the
  bucket lists NO objects — its existence is proven by write-time cluster
  validation, not an object at a canonical path.
* (b) a genuinely-phantom PER-OBJECT ``captured`` row (real path-segment
  ``instrument_id``, no backing object) is STILL classified phantom.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import NamedTuple

import pandas as pd
from unified_trading_library import MANIFEST_ONLY_BUNDLE_DATA_TYPES


def _load_reconciler_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "reconcile_phantom_manifest_rows_all.py"
    spec = importlib.util.spec_from_file_location("reconcile_phantom_manifest_rows_all", script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class _Blob(NamedTuple):
    name: str


class _FakeClient:
    """Storage client stub — lists a fixed blob set filtered by prefix."""

    def __init__(self, blobs: tuple[_Blob, ...] = ()) -> None:
        self._blobs = blobs

    def list_blobs(self, _bucket: str, prefix: str = "") -> list[_Blob]:
        return [b for b in self._blobs if b.name.startswith(prefix)]


def _prediction_manifest() -> pd.DataFrame:
    """Two captured prediction rows: a bundle atom + a genuine per-object phantom."""
    return pd.DataFrame(
        {
            "date": ["2026-06-15", "2026-06-15"],
            "venue": ["POLYMARKET", "POLYMARKET"],
            "chain": ["", ""],
            "instrument_type": ["prediction", "prediction_market"],
            # Row 0: manifest-only bundle atom (synthetic cqg id, no on-disk folder).
            # Row 1: genuine per-object shard (real conditionId, no backing object).
            "data_type": ["prediction_canonical_question_group", "trades"],
            "instrument_id": ["BTC_UP_DOWN_DAILY", "0xdeadbeef_condition_id"],
            "capture_status": ["captured", "captured"],
        }
    )


class TestBundleAtomExemption:
    def test_manifest_only_registry_has_prediction_bundle(self) -> None:
        assert "prediction_canonical_question_group" in MANIFEST_ONLY_BUNDLE_DATA_TYPES

    def test_bundle_atom_survives_while_per_object_phantom_flagged(self) -> None:
        mod = _load_reconciler_module()
        df = _prediction_manifest()
        captured_idx = df.index  # both rows are captured
        # Bucket lists NOTHING — no object at any canonical path for either row.
        client = _FakeClient(blobs=())

        result = mod._audit_generic(
            "prediction",
            client,
            "market-data-tick-prediction-test-project",
            df,
            captured_idx,
            workers=2,
        )

        # (a) bundle atom (idx 0) is REAL despite zero objects — exempt, survives.
        assert result[0] is True
        # (b) genuine per-object phantom (idx 1) is still PHANTOM (not real).
        assert result[1] is False
