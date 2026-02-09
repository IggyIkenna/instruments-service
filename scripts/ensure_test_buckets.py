import os

from unified_cloud_services import get_storage_client


def create_bucket_if_not_exists(bucket_name, location="asia-northeast1"):
    """Create bucket using cloud-agnostic storage client."""
    try:
        storage_client = get_storage_client()
        # Check if bucket exists using cloud-agnostic method
        if storage_client.blob_exists(bucket=bucket_name, blob_path=".bucket-exists-check"):
            print(f"✅ Bucket {bucket_name} already exists")
        else:
            # Cloud-agnostic client may not support bucket creation
            print(f"⚠️  Bucket {bucket_name} may not exist")
            print("   Please create manually or use cloud console")
    except Exception as e:
        print(f"❌ Error checking/creating {bucket_name}: {e}")


if __name__ == "__main__":
    print("Ensuring test buckets exist...")

    # Get project ID from environment (supports both GCP and AWS)
    project_id = (
        os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("AWS_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("AWS_ACCOUNT_ID")
    )

    if not project_id:
        print("❌ Error: PROJECT_ID not found. Set GCP_PROJECT_ID or AWS_PROJECT_ID environment variable.")
        exit(1)

    # List of buckets to ensure exist
    # We include both suffix and infix patterns to cover configuration drift
    buckets = [
        f"instruments-store-test-{project_id}",
        # Suffix style (what failing code is currently trying to use)
        f"instruments-store-cefi-{project_id}-test",
        f"instruments-store-tradfi-{project_id}-test",
        f"instruments-store-defi-{project_id}-test",
        # Infix style (what is defined in .env)
        f"instruments-store-test-cefi-{project_id}",
        f"instruments-store-test-tradfi-{project_id}",
        f"instruments-store-test-defi-{project_id}",
    ]

    for b in buckets:
        create_bucket_if_not_exists(b)

    print("Done.")
