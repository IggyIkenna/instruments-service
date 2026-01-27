from google.cloud import storage
from google.api_core.exceptions import Forbidden

def create_bucket_if_not_exists(bucket_name, location="asia-northeast1"):
    try:
        storage_client = storage.Client()
        try:
            bucket = storage_client.bucket(bucket_name)
            if not bucket.exists():
                print(f"Creating bucket {bucket_name} in {location}...")
                bucket.create(location=location)
                print(f"✅ Created {bucket_name}")
            else:
                print(f"✅ Bucket {bucket_name} already exists")
        except Forbidden:
            print(f"❌ Access denied for bucket {bucket_name}. Check credentials.")
        except Exception as e:
            print(f"❌ Error checking/creating {bucket_name}: {e}")

    except Exception as e:
        print(f"❌ Failed to initialize storage client: {e}")

if __name__ == "__main__":
    print("Ensuring test buckets exist...")

    # List of buckets to ensure exist
    # We include both suffix and infix patterns to cover configuration drift
    buckets = [
        "instruments-store-test-central-element-323112",

        # Suffix style (what failing code is currently trying to use)
        "instruments-store-cefi-central-element-323112-test",
        "instruments-store-tradfi-central-element-323112-test",
        "instruments-store-defi-central-element-323112-test",

        # Infix style (what is defined in .env)
        "instruments-store-test-cefi-central-element-323112",
        "instruments-store-test-tradfi-central-element-323112",
        "instruments-store-test-defi-central-element-323112",
    ]

    for b in buckets:
        create_bucket_if_not_exists(b)

    print("Done.")

























