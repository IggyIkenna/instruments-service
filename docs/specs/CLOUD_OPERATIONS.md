<!-- POST_PLAN_BANNER_2026_05_06_FINAL -->

> **Post-2026-05-06** — read [`../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md`](../../unified-trading-pm/codex/POST_PLAN_REALITY_2026_05_06.md) before code/doc changes informed by this doc. The post-plan-reality doc summarizes the 10 cross-cutting principles codified in workspace `CLAUDE.md` (live=batch, no double SSOT, three-category empty-output decision A/B/C, cluster validation MANDATORY at `record_captured`, `available_at` per-row write-time, prediction lifecycle, temporary state must have named successor, per-VM shard isolation, multi-axis shard-vs-display distinction) plus the active plans (`writegate_honest_coverage_endtoend_2026_05_06.md`, `predictions_canonical_question_group_polymarket_migration_2026_05_06.md`, `data_status_multi_axis_shard_propagation_2026_05_06.md`). If this doc disagrees with the active plans, the plans win. Flag conflicts to user — don't decide unilaterally.

# Cloud Operations - instruments-service

## GCS Storage Pattern

All GCS operations use CloudTarget from unified-trading-services:

```python
from unified_trading_services.domain import CloudTarget, StandardizedDomainCloudService
from instruments_service.config import instruments_config

config = instruments_config

# Create CloudTarget with ALL required parameters
target = CloudTarget(
    project_id=config.gcp_project_id,
    gcs_bucket=config.get_bucket_for_category("cefi"),
    bigquery_dataset=config.bigquery_dataset,  # Required!
    bigquery_location=config.bigquery_location,
)

# Use StandardizedDomainCloudService for operations
service = StandardizedDomainCloudService(
    domain="instruments",
    cloud_target=target,
)

# Upload data
service.upload_to_gcs(df, "path/to/file.parquet")

# Download data
df = service.download_from_gcs("path/to/file.parquet")
```

## Why BigQuery Dataset is Required

Even for GCS-only operations, `bigquery_dataset` is required because:

- CloudTarget is a unified config object for all cloud resources
- Some operations may need BigQuery metadata or fallback
- Ensures consistent naming across GCS and BigQuery
- Prevents partial configuration errors

**Bottom line:** Always provide ALL CloudTarget parameters.

## Config Anti-Patterns to Avoid

```python
# WRONG: Using get_config with defaults
from unified_trading_services import get_config
project_id = get_config("GCP_PROJECT_ID", instruments_config.gcp_project_id)

# CORRECT: Use config attributes directly
from instruments_service.config import instruments_config
project_id = instruments_config.gcp_project_id
```
