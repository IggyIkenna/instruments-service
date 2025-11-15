# Branch Protection Setup

To ensure that pushes to `main` are blocked when quality gates fail, you need to configure branch protection rules in GitHub.

## Steps to Enable Branch Protection

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Branches**
3. Under **Branch protection rules**, click **Add rule** or edit the existing rule for `main`
4. Configure the following:

### Required Settings

- **Branch name pattern**: `main`
- **Require a pull request before merging**: ✅ (optional but recommended)
- **Require status checks to pass before merging**: ✅ **ENABLE THIS**
  - **Require branches to be up to date before merging**: ✅ (recommended)
  - Under **Status checks that are required**, add:
    - `quality-gates` (this is the job name from the workflow)

### Optional but Recommended

- **Require conversation resolution before merging**: ✅
- **Require linear history**: ✅
- **Do not allow bypassing the above settings**: ✅ (for admins too)

## How It Works

1. When you push to `main` or create a PR targeting `main`, the `quality-gates` workflow runs
2. If the workflow fails (exit code 1), GitHub will:
   - Show a ❌ status check on the commit/PR
   - **Block the merge** if branch protection is enabled
   - Prevent direct pushes to `main` if "Require status checks" is enabled

## Testing

After setting up branch protection:

1. Make a commit that would fail quality gates (e.g., reduce test coverage)
2. Push to `main` or create a PR
3. The workflow should run and fail
4. GitHub should block the merge/push

## Troubleshooting

- **Workflow not showing as required check**: Make sure the workflow file is in `.github/workflows/` and has been run at least once
- **Can still push despite protection**: Check that "Do not allow bypassing" is enabled
- **Workflow passes but shouldn't**: Check the coverage threshold in `.github/workflows/quality-gates.yml` (currently 65%)
