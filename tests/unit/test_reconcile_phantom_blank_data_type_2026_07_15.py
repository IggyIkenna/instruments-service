"""Regression: blank-data_type captured rows are no longer unconditionally phantom.

Guards the corrected root cause documented in
``plans/active/issues/phantom_captures_cefi_2026_06_28.md`` (2026-07-15
investigation section). ``_audit_generic`` builds the on-disk match needle as
``data_type={data_type}/`` — for a blank ``data_type`` this needle is
literally ``"data_type=/"``, a path segment that can NEVER exist on disk. So
ANY ``captured`` row with a blank/corrupt ``data_type`` was unconditionally,
permanently flagged phantom by the forward pass regardless of whether real
data existed elsewhere for that (date, venue) — confirmed live 2026-07-15:
9,658/9,757 (99.0%) of the CeFi blank-data_type population already had a
separate, correctly-typed ``captured`` row for the same (date, venue).

The prior guard (``schema_version==4`` only, added 2026-05-04) was too
narrow: the undocumented 2026-06-28T03:12:34Z apply run flipped a
byte-identical 9,757-row population anyway (live-verified: schema_version==4
for 100% of that population, so the narrow scoping should have applied —
the incident is exactly why the fix here is widened to catch ANY blank
data_type regardless of schema_version, since the underlying blind spot in
``_audit_generic`` is structural, not schema-version-specific).

Two assertions, both against the real production code path
(``_build_forward_captured_idx`` -> ``_audit_generic``, exactly as ``main()``
drives them):

* (a) a blank-data_type ``captured`` row with a real, correctly-typed sibling
  ``captured`` row at the same (date, venue) is dropped from the forward
  audit's captured-row scope entirely (never reaches the audit, never gets
  flipped to ``attempted_failed``) — regardless of its ``schema_version``.
* (b) a genuinely-phantom row (non-blank ``data_type``, no backing parquet
  anywhere) is still correctly flagged phantom — the widened filter must not
  weaken real phantom detection.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import NamedTuple

import pandas as pd


def _load_reconciler_module() -> ModuleType:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "reconcile_phantom_manifest_rows_all.py"
    spec = importlib.util.spec_from_file_location("reconcile_phantom_manifest_rows_all_blank_dt_test", script_path)
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


def _default_args() -> argparse.Namespace:
    return argparse.Namespace(venues="", data_types="", start_date="", end_date="")


def _cefi_manifest() -> pd.DataFrame:
    """Three captured CeFi rows mirroring the 2026-06-28 incident shape.

    Row 0: blank data_type, schema_version=8 (deliberately NOT 4 — proves the
    filter is no longer schema_version-scoped), sibling data captured under
    the correct data_type at the same (date, venue).
    Row 1: the real, correctly-typed sibling for row 0's (date, venue) — backed
    by a real object on disk.
    Row 2: a genuinely-phantom row — non-blank data_type, no backing object
    anywhere — must still be flagged phantom.
    """
    return pd.DataFrame(
        {
            "date": ["2026-04-10", "2026-04-10", "2026-04-11"],
            "venue": ["BYBIT", "BYBIT", "OKX-SPOT"],
            "instrument_type": ["", "", ""],
            "data_type": ["", "trades", "trades"],
            "capture_status": ["captured", "captured", "captured"],
            "schema_version": [8, 9, 9],
        }
    )


class TestBlankDataTypeNoLongerUnconditionalPhantom:
    def test_blank_data_type_dropped_from_forward_audit_scope(self) -> None:
        mod = _load_reconciler_module()
        df = _cefi_manifest()
        captured_idx = mod._build_forward_captured_idx(df, _default_args())

        # (a) idx 0 (blank data_type, schema_version=8) is excluded from the
        # forward-audit candidate set entirely — it can never be flipped to
        # attempted_failed, regardless of schema_version.
        assert 0 not in captured_idx
        # The real sibling + the genuine phantom stay in scope.
        assert 1 in captured_idx
        assert 2 in captured_idx

    def test_genuine_phantom_still_flagged_after_widened_filter(self) -> None:
        mod = _load_reconciler_module()
        df = _cefi_manifest()
        captured_idx = mod._build_forward_captured_idx(df, _default_args())

        # Only row 1 (BYBIT/2026-04-10/trades) has a backing object; row 2
        # (OKX-SPOT/2026-04-11/trades) has none anywhere in the fake bucket.
        real_blob = _Blob(
            name=(
                "raw_tick_data/by_date/day=2026-04-10/pipeline_mode=batch_tardis/"
                "asset_group=cefi/venue=BYBIT/instrument_type=spot/data_type=trades/part-0.parquet"
            )
        )
        client = _FakeClient(blobs=(real_blob,))

        result = mod._audit_generic(
            "cefi",
            client,
            "market-data-tick-cefi-test-project",
            df,
            captured_idx,
            workers=2,
        )

        # (b) The real sibling (idx 1) is classified real; the genuine
        # per-object phantom (idx 2) is still classified phantom. The
        # blank-data_type row (idx 0) never entered captured_idx, so it has
        # no verdict at all — it simply never gets touched by the forward
        # pass (the "skip from the phantom-candidate set entirely" fix).
        assert 0 not in result
        assert result[1] is True
        assert result[2] is False
