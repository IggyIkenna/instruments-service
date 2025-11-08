# Scripts Directory

Utility scripts for instruments-service.

## Available Scripts

### `clickup_import.py`

**Purpose**: Import STATUS.md data into ClickUp via API

**Features**:
- ✅ Parses STATUS.md automatically
- ✅ Creates milestone tasks
- ✅ Creates subtasks with proper hierarchy
- ✅ Sets custom fields (Coverage %, Test Coverage %, DRY Compliance %, Week, Strategy)
- ✅ Applies tags automatically
- ✅ Handles rate limiting (100 requests/minute)
- ✅ Dry run mode for testing

**Usage**:
```bash
# Dry run first (recommended)
python scripts/clickup_import.py \
    --api-token "pk_YOUR_TOKEN" \
    --list-id "YOUR_LIST_ID" \
    --dry-run

# Actual import
python scripts/clickup_import.py \
    --api-token "pk_YOUR_TOKEN" \
    --list-id "YOUR_LIST_ID"
```

**Setup**: See `scripts/CLICKUP_GUIDE.md` for complete setup and usage guide.

**Requirements**: 
- ClickUp API token (get from https://app.clickup.com/settings/apps)
- ClickUp List ID (from URL)
- `requests` library (already in requirements.txt)

---

### `run_quality_gates.py`

**Purpose**: Run quality gates (tests, coverage, linting)

**Usage**:
```bash
python scripts/run_quality_gates.py
```

---

## Adding New Scripts

When adding new scripts:

1. **Add to this README**: Document purpose, usage, and requirements
2. **Make executable**: `chmod +x scripts/your_script.py`
3. **Add shebang**: `#!/usr/bin/env python3` at top
4. **Add docstring**: Explain purpose and usage
5. **Handle errors**: Use proper error handling and exit codes

