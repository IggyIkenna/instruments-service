# Setting Up GitHub Secrets for Quality Gates

This guide explains how to add the required secrets to your GitHub repository so the quality gates workflow can run successfully.

## Required Secrets

### 1. GCP_SERVICE_ACCOUNT_JSON (REQUIRED)

This is the service account JSON file content needed for GCP authentication.

**Steps:**
1. Go to your `instruments-service` repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `GCP_SERVICE_ACCOUNT_JSON`
5. Value: Copy the **entire contents** of `central-element-323112-e35fb0ddafe2.json` file
   - Open the file locally
   - Select all (Cmd+A / Ctrl+A)
   - Copy (Cmd+C / Ctrl+C)
   - Paste into the secret value field
6. Click **Add secret**

**Note:** The file should look like:
```json
{
  "type": "service_account",
  "project_id": "central-element-323112",
  "private_key_id": "...",
  "private_key": "...",
  ...
}
```

### 2. GH_PAT (REQUIRED for private unified-cloud-services)

**Note:** GitHub doesn't allow secret names starting with `GITHUB_`, so we use `GH_PAT` instead.

A GitHub Personal Access Token (PAT) with access to the private `unified-cloud-services` repository.

**Steps:**
1. Go to GitHub.com → **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**
2. Click **Generate new token** → **Generate new token (classic)**
3. Give it a name: `instruments-service-ci`
4. Select expiration (recommend: 90 days or No expiration for automation)
5. Check these scopes:
   - ✅ `repo` (Full control of private repositories)
   - ✅ `read:packages` (Download packages from GitHub Package Registry)
6. Click **Generate token**
7. **Copy the token immediately** (you won't see it again!)
8. Go back to your `instruments-service` repository
9. Navigate to **Settings** → **Secrets and variables** → **Actions**
10. Click **New repository secret**
11. Name: `GH_PAT` (note: cannot start with GITHUB_)
12. Value: Paste the token you copied
13. Click **Add secret**

## Optional Secrets (with defaults)

These secrets have defaults built into the workflow, but you can override them if needed:

- `GCP_PROJECT_ID` (default: `central-element-323112`)
- `GCS_REGION` (default: `asia-northeast1-c`)
- `GCS_LOCATION` (default: `asia-northeast1`)
- `INSTRUMENTS_GCS_BUCKET` (default: `instruments-store-central-element-323112`)
- `INSTRUMENTS_GCS_BUCKET_TEST` (default: `instruments-store-test-central-element-323112`)
- `INSTRUMENTS_GCS_BUCKET_CEFI` (default: `instruments-store-cefi-central-element-323112`)
- `INSTRUMENTS_GCS_BUCKET_TRADFI` (default: `instruments-store-tradfi-central-element-323112`)
- `INSTRUMENTS_GCS_BUCKET_DEFI` (default: `instruments-store-defi-central-element-323112`)
- `INSTRUMENTS_BIGQUERY_DATASET` (default: `instruments`)
- `BIGQUERY_LOCATION` (default: `asia-northeast1`)
- `TARDIS_SECRET_NAME` (default: `tardis-api-key`)
- `DATABENTO_SECRET_NAME` (default: `databento-api-key`)
- `AAVESCAN_SECRET_NAME` (default: `aavescan-api-key`)
- `ALCHEMY_SECRET_NAME` (default: `alchemy-api-key`)

## Verifying Secrets Are Set

After adding secrets, you can verify they're set by:
1. Going to **Settings** → **Secrets and variables** → **Actions**
2. You should see your secrets listed (values are hidden for security)

## Testing the Workflow

Once secrets are set:
1. Make a small change to any file in `instruments-service`
2. Commit and push to `main` branch
3. Go to **Actions** tab in your repository
4. You should see the "Quality Gates" workflow running
5. Check the logs to verify secrets are being read correctly

## Troubleshooting

### "GCP_SERVICE_ACCOUNT_JSON secret is not set"
- Make sure you added the secret with the exact name `GCP_SERVICE_ACCOUNT_JSON`
- Verify the secret value contains valid JSON (check for typos)

### "Failed to install unified-cloud-services"
- Make sure `GH_PAT` is set and has `repo` scope
- Verify the token hasn't expired
- Check that the `unified-cloud-services` repository exists and is accessible

### "Permission denied" errors
- Ensure the GCP service account has the necessary permissions
- Check that the service account JSON is valid and not corrupted
