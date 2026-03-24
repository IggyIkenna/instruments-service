#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-codex/06-coding-standards/quality-gates-service-template.sh
#
# Instructions for a new service:
#   1. Copy this to scripts/quality-gates.sh in your repo (rollout-quality-gates-unified.py does this)
#   2. SERVICE_NAME, SOURCE_DIR, and MIN_COVERAGE are set automatically by rollout (floor=70)
#   3. Set RUN_INTEGRATION=true only if your repo has integration tests
#   4. Add LOCAL_DEPS entries if your service has local editable deps (e.g. unified-events-interface)
SERVICE_NAME="instruments-service"
SOURCE_DIR="instruments_service"
MIN_COVERAGE=70
RUN_INTEGRATION=true  # integration tests are library contract tests — no credentials needed
PYTEST_WORKERS=${PYTEST_WORKERS:-}  # default: max(1, cpu_count//4) computed by base script
LOCAL_DEPS=()
MAX_DURATION=300
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"

# STEP 5.11/5.12 bypass: _write_catalogue_record in orchestrator.py calls
# ManifestWriter.write(gcs_bucket=...) which uses gcs_bucket as a UCI schema
# field name — not a raw GCS SDK import. Documented in QUALITY_GATE_BYPASS_AUDIT.md.
HARDCODED_PROTO_EXCLUDE_GLOBS=("--glob=!**/engine/orchestrator.py")

source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# NOTE: STARTED/STOPPED/FAILED lifecycle events are emitted by UTL ServiceBootstrap.run().
# Services that use ServiceBootstrap do NOT need to emit these manually.
# The QG check for these events would always warn for UTL-based services, so it is removed.
