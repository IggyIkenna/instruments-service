#!/usr/bin/env python3
"""beta_manifest_writer.py — projected v9 ``_index`` preview writer (CF-20 / V5).

The operator's "dump the dry-run somewhere and look at it" idea: a migrator/rebuild
``--dry-run`` already computes the would-be post-migration manifest; this helper writes
that projected v9 ``_index`` to a THROWAWAY dev prefix so the EXISTING (env-configurable)
data-status / deployment-api / deployment-ui stack can render the post-migration
goalposts — coverage %, the 4-state breakdown, the could-exist denominator, the
drilldowns — BEFORE ``--apply`` touches prod. No GCS object is moved.

Naming: it is **"v9 projected"**, NOT a new schema version — ``schema_version`` stays 9.
It is a preview RENDER of the v9 manifest, not a v8.5/v9-beta schema downstream must
special-case.

Wiring: a per-AG migrator's dry-run calls :func:`write_projected_index(projected_df,
"--beta-manifest-out gs://<dev-bucket>/_index/availability_index.parquet")`. The
operator then drops it in the **dev** bucket, runs ``restart-deployment-stack.sh --api``
with ``DEPLOYMENT_ENV_SHORT=dev``, eyeballs the goalposts, signs off, and deletes the
dev ``_index``.

Safety: :func:`assert_dev_target` HARD-REFUSES any non-dev / prod ``_index`` URI so a
projected preview can NEVER overwrite a live manifest.

Plan ref: ``plans/active/migration_verification_orphan_safety_2026_06_10.md`` § V5/CF-20.
"""

from __future__ import annotations

import io
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# The canonical v9 _index columns the projection must carry (the 8→9 additions:
# source / asset_group / pipeline_mode, on top of schema_version=9). A projected index
# missing any of these would not render the v9 drilldowns.
V9_REQUIRED_COLUMNS: tuple[str, ...] = ("asset_group", "source", "pipeline_mode", "schema_version", "capture_status")
V9_SCHEMA_VERSION: int = 9

# A dev/throwaway target MUST be one of these (never a prod/staging _index). The guard
# is intentionally strict: a preview overwriting a live manifest is the worst outcome.
_DEV_TARGET_MARKERS: tuple[str, ...] = ("-dev-", "/dev/", "_dev/", "beta", "projected", "/_index/audit/")


class NonDevManifestTargetError(RuntimeError):
    """Raised when a projected ``_index`` write targets a non-dev / prod URI."""


def assert_dev_target(dev_uri: str) -> None:
    """HARD-REFUSE a projected-index write to anything but a dev/throwaway target.

    A preview must never overwrite a prod/staging manifest. The URI must be a ``gs://``
    path AND carry a recognised dev marker (``-dev-`` bucket tier, a ``/dev/`` or
    ``beta``/``projected`` prefix, or an ``_index/audit/`` sandbox)."""
    if not dev_uri.startswith("gs://"):
        raise NonDevManifestTargetError(f"projected-index target must be a gs:// URI, got {dev_uri!r}")
    if not any(marker in dev_uri for marker in _DEV_TARGET_MARKERS):
        raise NonDevManifestTargetError(
            f"refusing to write a projected _index to a non-dev target {dev_uri!r} — "
            f"the target must carry a dev marker ({_DEV_TARGET_MARKERS}); a preview must "
            f"NEVER overwrite a prod/staging manifest"
        )


def project_v9_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``df`` shaped as the projected v9 ``_index``: ``schema_version``
    stamped to 9 and the required v9 columns present (filled blank where absent). Does
    NOT invent capture states — it only re-shapes the COLUMNS so data-status can read it;
    the 4-state values are whatever the migrator's dry-run computed."""
    out = df.copy()
    out["schema_version"] = V9_SCHEMA_VERSION
    for col in V9_REQUIRED_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


def write_projected_index(projected_df: pd.DataFrame, dev_uri: str) -> int:
    """Write the projected v9 ``_index`` to a dev ``gs://`` URI (no objects moved).

    Returns the row count written. Refuses a non-dev target (:func:`assert_dev_target`).
    Idempotent overwrite (a preview is regenerated freely)."""
    assert_dev_target(dev_uri)
    from unified_trading_library import get_storage_client

    shaped = project_v9_columns(projected_df)
    missing = [c for c in V9_REQUIRED_COLUMNS if c not in shaped.columns]
    if missing:  # defensive — project_v9_columns guarantees these, but never write a malformed preview
        raise ValueError(f"projected _index missing required v9 columns: {missing}")
    buf = io.BytesIO()
    shaped.to_parquet(buf, index=False)
    buf.seek(0)
    bucket, _sep, blob_path = dev_uri[len("gs://") :].partition("/")
    get_storage_client().upload_from_file_obj(bucket, blob_path, buf)  # type: ignore[attr-defined]
    logger.info(
        "wrote projected v9 _index preview (%d rows, schema_version=9) to %s — render via "
        "restart-deployment-stack.sh --api DEPLOYMENT_ENV_SHORT=dev; delete after sign-off",
        len(shaped),
        dev_uri,
    )
    return len(shaped)
