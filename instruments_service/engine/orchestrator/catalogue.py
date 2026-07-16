"""Instruments store/bucket resolution, emission-policy check, catalogue record writes, catalogue refresh.

Cohesion module of the ``engine.orchestrator`` package (split from the former
monolithic ``engine/orchestrator.py``; plan:
``unified-trading-pm/plans/active/codex_violations_ratchet_to_five_2026_06_10.md``).

Shared collaborators, constants and mutable module state resolve through
``_orch`` — the live ``instruments_service.engine.orchestrator`` package
namespace — so the package keeps the original module's single-namespace
semantics: ``unittest.mock.patch("instruments_service.engine.orchestrator.<name>")``
targets and cross-module monkeypatching behave exactly as they did before the
split, and mutable caches remain package-level attributes.
"""

# Package-internal access: the orchestrator package is ONE logical namespace
# split across cohesion modules; underscore symbols are package-internal.
# pyright: reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from instruments_service.engine import orchestrator as _orch
else:  # pragma: no cover - runtime namespace indirection
    from instruments_service.engine.orchestrator._pkg_ref import orch_namespace as _orch

__all__ = [
    "_check_emission_policy",
    "_get_instruments_bucket",
    "_write_catalogue_record",
    "resolve_instruments_store_kind",
]


def resolve_instruments_store_kind(asset_group: str | None) -> tuple[str, str | None]:
    """Map an asset_group to the ``(kind, asset_group)`` pair for ``resolve_bucket_name``.

    The bucket-name SSOT (``cloud-providers.yaml``) models the prediction
    instruments store as a **dedicated flat kind** ``instruments-store-prediction``
    (→ ``instruments-store-pred-{env}-{pid}``), NOT as a ``PREDICTION`` entry in the
    per-asset_group ``instruments-store`` dict. So ``resolve_bucket_name(kind=
    "instruments-store", asset_group="prediction")`` raises ``BucketNamingError``
    ("no entry for asset_group='prediction'"). Every other consumer of the prediction
    store — the MTDS reader ``_load_market_lifecycle_for_date`` and all the prediction
    scripts — already resolves the flat kind, so the orchestrator MUST too or its
    prediction writes (instruments.parquet + market_lifecycle.parquet) crash at bucket
    resolution and emit ZERO objects. Flat kinds ignore the ``asset_group`` argument.
    """
    if asset_group == "prediction":
        return "instruments-store-prediction", None
    return "instruments-store", asset_group


def _get_instruments_bucket(asset_group: str | None = None) -> str:
    """Resolve the instruments write bucket for the given asset group.

    Delegates to ``resolve_bucket_name`` (bucket-name SSOT) which returns
    env-tiered buckets: instruments-store-{ag}-{DEPLOYMENT_ENV_SHORT}-{pid}.
    DEPLOYMENT_ENV=prod → instruments-store-{ag}-prd-{pid}.
    DEPLOYMENT_ENV=dev  → instruments-store-{ag}-dev-{pid}.
    IS_TEST_RUN=true    → DEPLOYMENT_ENV=test → instruments-store-{ag}-test-{pid}.

    Prediction resolves via the dedicated flat kind ``instruments-store-prediction``
    (→ instruments-store-pred-{env}-{pid}) — see :func:`_resolve_instruments_store_kind`.
    """
    cfg = _orch.get_config()
    # resolve_bucket_name is strict-lowercase (canonical asset_group keys: cefi/defi/
    # prediction/sports/tradfi) since the Option B bucket-SSOT migration (library@6c8a1175,
    # 2026-05-30). Must lowercase here — uppercasing raised BucketNamingError("Unknown
    # asset_group 'SPORTS'") on every sports/footystats short-circuit run. Matches the
    # handler's _get_instruments_bucket_for_asset_group which already lowercases.
    ag: str | None = asset_group.lower() if asset_group else None
    kind, kind_ag = _orch.resolve_instruments_store_kind(ag)
    # IS_TEST_RUN → resolve the ``test`` bucket tier via the explicit
    # ``deployment_env`` param (library@bucket_naming) — NOT by mutating the
    # process env (the banned config-env write the QG flags).
    deployment_env = "test" if cfg.is_test_run else None
    return _orch.resolve_bucket_name(cloud="gcp", kind=kind, asset_group=kind_ag, deployment_env=deployment_env)


def _check_emission_policy(
    *,
    date: str,
    completeness_fraction: float,
    correlation_id: str | None = None,
) -> _orch.EmissionDecision:
    """Return publish decision for catalog_snapshot emission boundary.

    PARTIAL_OK — catalog_snapshot is a best-effort union of multiple source feeds;
    partial coverage is normal (some venues may lag or be unavailable on any given day).
    completeness < 1.0 → emits PUBLISHED_DEGRADED but still writes.
    completeness == 0.0 → still writes (PARTIAL_OK never suppresses).
    Only STRICT_FAIL / BLOCK_CRITICAL would suppress a write — not applicable here.

    UAC seed: ("instruments-service", "catalog_snapshot"): ServiceEmissionPolicy.PARTIAL_OK
    Plan: writegate_honest_coverage_endtoend_2026_05_06.md Phase 6.8 PART B.
    """
    return _orch.publish_with_policy(
        service=_orch._SERVICE_NAME,
        output_data_type="catalog_snapshot",
        row_key={"date": date},
        completeness_fraction=completeness_fraction,
        correlation_id=correlation_id,
    )


def _write_catalogue_record(bucket: str, path: str, date: str, record_count: int) -> None:
    """Update the consolidated availability index in the instruments bucket.

    Merges this venue/date record into:
      gs://{bucket}/_index/availability_index.parquet

    Downstream services call read_availability_index(bucket) to check completeness
    without listing thousands of GCS blobs.

    v4: Extracts chain (DeFi), data_type (prediction markets), league_id from path.
    """
    try:
        venue_match = _orch.re.search(r"venue=([^/]+)", path)
        venue_str = venue_match.group(1) if venue_match else ""
        date_match = _orch.re.search(r"day[=-](\d{4}-\d{2}-\d{2})", path)
        date_str = date_match.group(1) if date_match else date
        parsed = _orch.date_type.fromisoformat(date_str)

        # v4: Extract shard dimensions from path/venue
        manifest_venue = venue_str
        manifest_chain = ""
        manifest_data_type = ""
        manifest_league_id = ""

        # Sports: venue → data_type (API_FOOTBALL_INJURIES → data_type=INJURIES, venue="")
        _sports_prefixes = ("API_FOOTBALL", "TRANSFERMARKT", "FOOTYSTATS", "SFI", "UNDERSTAT", "WEATHER")
        if venue_str.startswith(_sports_prefixes):
            if venue_str == "API_FOOTBALL":
                manifest_data_type = "FIXTURES"
            else:
                for pfx in _sports_prefixes:
                    if venue_str.startswith(pfx + "_"):
                        manifest_data_type = venue_str[len(pfx) + 1 :]
                        break
                else:
                    manifest_data_type = venue_str
            manifest_venue = ""
        # DeFi: split AAVE_V3-ETHEREUM → venue=AAVE_V3, chain=ETHEREUM
        elif "-" in venue_str:
            # Canonical value is "instruments" (operator decision 2026-07-16, P9 Q2) — matches
            # REFERENCE_DATA_TYPE in migrate_instruments_store_v9.py and the live batched-writer
            # path (_write_venue's manifest-not-None branch, writers.py). This legacy per-venue
            # path is unreachable from the current orchestrator (_write_all_venues always passes
            # a ManifestWriter), kept in sync for correctness if ever revived.
            manifest_data_type = "instruments"
            try:
                from unified_api_contracts.registry.capability_declarations._defi import (
                    KNOWN_CHAINS,
                    parse_defi_venue,
                )

                protocol, chain = parse_defi_venue(venue_str)
                if chain in KNOWN_CHAINS:
                    manifest_venue = protocol.upper()
                    manifest_chain = chain
            except (ImportError, ValueError):
                pass
        # Prediction: split POLYMARKET:BTC → venue=POLYMARKET, data_type=BTC
        elif ":" in venue_str:
            parts = venue_str.split(":", 1)
            manifest_venue = parts[0]
            manifest_data_type = parts[1]

        # Sports: extract league from path
        league_match = _orch.re.search(r"league=([^/]+)", path)
        if league_match:
            manifest_league_id = league_match.group(1)

        writer = _orch.ManifestWriter(
            service_name="instruments-service",
            catalogue_bucket=bucket,
        )
        writer.record_captured_from_counts(  # QG-allow: emission-policy-not-applicable
            row_key={
                "date": str(parsed),
                "data_type": manifest_data_type,
                "venue": manifest_venue,
                "chain": manifest_chain,
                "league_id": manifest_league_id,
            },
            total_rows=record_count,
            # SP-10: generic parquet-path recovery writer — the manifest row is derived from a
            # single parquet path's (date, data_type, venue, chain, league_id) and an aggregate
            # record_count. There is no per-root-cluster decomposition available from a path, so
            # the gate is a deliberate no-op; an unfounded expectation would false-fail real data.
            expected_root_clusters={},
            observed_clusters={"": record_count},
            available_at_envelope=_orch.pd.Timestamp(_orch.datetime.now(_orch.UTC)),
            pipeline_mode=_orch.PipelineMode.BATCH_INSTRUMENTS_SERVICE,
            service_emission_state=None,
        )
        writer.write()
    except Exception as exc:
        _orch.classify_and_emit_error(
            exc,
            service_name="instruments-service",
            operation="manifest_writer",
            shard=path,
        )
