# AGENTS.md

## Setup

```bash
uv sync --extra dev
source .venv/bin/activate
```

## Quality Gates

```bash
bash scripts/quality-gates.sh
```

## Type Checking

```bash
timeout 120 basedpyright instruments_service/
```

## Key Entry Points

- `instruments_service/cli/main.py` — CLI entry point

## Notes

- Initialize events with `from unified_events_interface import setup_events`
- Required env vars: `GCP_PROJECT_ID` — see `docs/CONFIGURATION.md`
- Requires GCP credentials: `gcloud auth application-default login`
- Absorbs corporate actions (formerly a separate service, now operational mode within instruments-service)
- Sports reference data was merged into this service (2026-03-01)
