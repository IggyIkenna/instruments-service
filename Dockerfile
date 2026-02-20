# Dockerfile for instruments-service
#
# Uses unified-cloud-services base image from Artifact Registry.
# No GitHub token (GH_PAT) required.
#
# Build:
#   docker build --build-arg PROJECT_ID=central-element-323112 -t instruments-service .
#
# Run:
#   docker run -v /path/to/credentials.json:/app/credentials.json \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
#     -e GCP_PROJECT_ID=central-element-323112 \
#     instruments-service --mode instruments --start-date 2024-01-01 --CEFI

ARG PROJECT_ID
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-cloud-services/unified-cloud-services:latest

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user
RUN useradd --create-home --shell /bin/bash appuser

# Set working directory
WORKDIR /app/instruments-service

# Copy pip.conf for Artifact Registry access
COPY pip.conf /etc/pip.conf

# Copy instruments-service source code
COPY . .

# Install service with dev dependencies (needed for quality gates in Cloud Build)
# Use pip (not uv) for Artifact Registry authentication via keyring
RUN pip install --no-cache-dir -e ".[dev]"

# Create data directories
RUN mkdir -p /app/instruments-service/data/samples /app/instruments-service/logs

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Default environment variables
# ENABLE_CSV_SAMPLING=false prevents disk filling on ephemeral VMs
# UCS_SKIP_GCSFUSE_CHECK=1 skips GCSFUSE check (not used in Cloud Run/VMs)
ENV ENVIRONMENT=production \
    ENABLE_CSV_SAMPLING=false \
    UCS_SKIP_GCSFUSE_CHECK=1 \
    GCS_REGION=asia-northeast1-c \
    GCS_LOCATION=asia-northeast1 \
    INSTRUMENTS_GCS_BUCKET_CEFI=instruments-store-cefi-central-element-323112 \
    INSTRUMENTS_GCS_BUCKET_TRADFI=instruments-store-tradfi-central-element-323112 \
    INSTRUMENTS_GCS_BUCKET_DEFI=instruments-store-defi-central-element-323112

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import instruments_service; print('healthy')" || exit 1

# Default entrypoint - runs the CLI
ENTRYPOINT ["python", "-m", "instruments_service"]

# Default command (can be overridden)
CMD ["--help"]
