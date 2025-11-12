# Add Graph API Key to Secret Manager

The Graph API key needs to be added to Google Secret Manager.

## API Key
```
3c6f3ec90154a9928c442f2d71335b67
```

## Command to Add Secret

Run one of these commands:

### Option 1: Using gcloud CLI (if available)
```bash
echo "3c6f3ec90154a9928c442f2d71335b67" | gcloud secrets create graph-api-key \
  --project=central-element-323112 \
  --data-file=-
```

If the secret already exists, add a new version:
```bash
echo "3c6f3ec90154a9928c442f2d71335b67" | gcloud secrets versions add graph-api-key \
  --project=central-element-323112 \
  --data-file=-
```

### Option 2: Using Google Cloud Console
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to **Secret Manager** in the left menu
3. Select project: `central-element-323112`
4. Click **CREATE SECRET**
5. Name: `graph-api-key`
6. Secret value: `3c6f3ec90154a9928c442f2d71335b67`
7. Click **CREATE SECRET**

### Option 3: Using Python (if you have Secret Manager Admin permissions)
```python
from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()
project_id = 'central-element-323112'
secret_id = 'graph-api-key'
secret_value = '3c6f3ec90154a9928c442f2d71335b67'

parent = f"projects/{project_id}"

# Create secret (if doesn't exist)
try:
    secret = client.create_secret(
        request={
            "parent": parent,
            "secret_id": secret_id,
            "secret": {"replication": {"automatic": {}}},
        }
    )
except Exception:
    pass  # Secret may already exist

# Add version
parent_secret = f"{parent}/secrets/{secret_id}"
version = client.add_secret_version(
    request={"parent": parent_secret, "payload": {"data": secret_value.encode("UTF-8")}}
)
print(f"✅ Added secret version: {version.name}")
```

## Verify Secret Was Added

After adding the secret, verify it works:
```bash
python3 -c "
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../central-element-323112-e35fb0ddafe2.json'
os.environ['GCP_PROJECT_ID'] = 'central-element-323112'
from unified_cloud_services import get_secret_with_fallback
api_key = get_secret_with_fallback(
    project_id='central-element-323112',
    secret_name='graph-api-key',
    fallback_env_var='THE_GRAPH_API_KEY',
)
print(f'✅ Retrieved: {api_key[:10]}...' if api_key else '❌ Not found')
"
```

## Temporary Testing (Environment Variable)

For immediate testing without Secret Manager, you can temporarily set:
```bash
export THE_GRAPH_API_KEY=3c6f3ec90154a9928c442f2d71335b67
```

This will work but is not recommended for production.


