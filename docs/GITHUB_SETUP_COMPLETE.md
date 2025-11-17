# GitHub Setup Status

## ✅ Completed

1. **GCP_SERVICE_ACCOUNT_JSON secret** - ✅ Set successfully
   - Secret name: `GCP_SERVICE_ACCOUNT_JSON`
   - Contains: GCP service account credentials
   - Set on: 2025-11-13

2. **GitHub Packages Publishing Workflow** - ✅ Created
   - Location: `unified-cloud-services/.github/workflows/publish-package.yml`
   - Publishes `unified-cloud-services` to GitHub Packages on release

3. **Quality Gates Workflow** - ✅ Created
   - Location: `instruments-service/.github/workflows/quality-gates.yml`
   - Runs on pushes to `main` branch

## ⚠️ Action Required

### Set GH_PAT Secret

**Note:** GitHub doesn't allow secret names starting with `GITHUB_`, so we use `GH_PAT` instead.

You need to create a GitHub Personal Access Token (PAT) and add it as a secret:

**Option 1: Using GitHub CLI (Recommended)**
```bash
cd instruments-service
./scripts/setup_github_secrets.sh
```

**Option 2: Manual Setup**

1. **Create a PAT:**
   - Go to: https://github.com/settings/tokens/new
   - Name: `instruments-service-ci`
   - Expiration: Choose 90 days or No expiration
   - Scopes needed:
     - ✅ `repo` (Full control of private repositories)
     - ✅ `read:packages` (Download packages from GitHub Package Registry)
   - Click **Generate token**
   - **Copy the token immediately** (you won't see it again!)

2. **Set the secret:**
   ```bash
   cd instruments-service
   echo "YOUR_TOKEN_HERE" | gh secret set GH_PAT --repo IggyIkenna/instruments-service
   ```

   Or manually:
   - Go to: https://github.com/IggyIkenna/instruments-service/settings/secrets/actions
   - Click **New repository secret**
   - Name: `GH_PAT` (note: cannot start with GITHUB_)
   - Value: Paste your token
   - Click **Add secret**

## 📦 Publishing unified-cloud-services to GitHub Packages

To publish `unified-cloud-services` to GitHub Packages:

1. **First time setup:**
   - The workflow is already created at `unified-cloud-services/.github/workflows/publish-package.yml`
   - It will automatically publish when you create a release

2. **Publish a version:**
   ```bash
   cd unified-cloud-services

   # Option A: Create a GitHub release (triggers workflow automatically)
   gh release create v1.0.0 --title "v1.0.0" --notes "Initial release"

   # Option B: Manual trigger
   gh workflow run publish-package.yml -f version=1.0.0
   ```

3. **After publishing, install from GitHub Packages:**
   ```bash
   pip install unified-cloud-services \
     --extra-index-url https://__token__:$GH_PAT@pypi.pkg.github.com/iggyikenna/simple
   ```

## 🧪 Testing the Quality Gates

Once `GH_PAT` is set:

1. Make a small change to any file
2. Commit and push:
   ```bash
   git add .
   git commit -m "Test quality gates"
   git push origin main
   ```
3. Check the Actions tab: https://github.com/IggyIkenna/instruments-service/actions

## 📋 Summary

- ✅ GCP credentials secret configured
- ⚠️ Need to set `GH_PAT` secret (see instructions above)
- ✅ Workflows created and ready
- ✅ Publishing workflow ready for `unified-cloud-services`

Once `GH_PAT` is set, the quality gates will run automatically on every push to `main`!
