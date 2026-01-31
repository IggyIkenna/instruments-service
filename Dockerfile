# Dockerfile for instruments-service
#
# This Dockerfile is designed to build the instruments-service container.
# It requires unified-cloud-services to be available during build.
#
# Option 1: Build with GitHub PAT (for CI/CD)
#   docker build --build-arg GH_PAT=your_github_token -t instruments-service .
#
# Option 2: Build with local unified-cloud-services (for development)
#   # First, clone unified-cloud-services next to instruments-service:
#   # cd .. && git clone https://github.com/IggyIkenna/unified-cloud-services.git
#   docker build --build-arg USE_LOCAL_UCS=true -t instruments-service .
#
# Run:
#   docker run -v /path/to/credentials.json:/app/credentials.json \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
#     -e GCP_PROJECT_ID=central-element-323112 \
#     instruments-service --mode instruments --start-date 2024-01-01 --CEFI

FROM python:3.13-slim AS base

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash appuser

# Set working directory
WORKDIR /app

# ============================================================
# Stage: Install unified-cloud-services from GitHub
# ============================================================
FROM base AS with-github-ucs

ARG GH_PAT

# Clone and install unified-cloud-services from GitHub
RUN if [ -n "$GH_PAT" ]; then \
        git clone https://${GH_PAT}@github.com/IggyIkenna/unified-cloud-services.git /app/unified-cloud-services && \
        pip install /app/unified-cloud-services && \
        rm -rf /app/unified-cloud-services/.git; \
    else \
        echo "ERROR: GH_PAT is required to install unified-cloud-services" && exit 1; \
    fi

# ============================================================
# Stage: Final image
# ============================================================
FROM base AS final

ARG GH_PAT

# Copy unified-cloud-services from previous stage if GH_PAT was provided
COPY --from=with-github-ucs /usr/local/lib/python3.13/site-packages/ /usr/local/lib/python3.13/site-packages/

# Copy instruments-service source code
COPY . /app/instruments-service

# Install instruments-service (skip unified-cloud-services, already installed)
# Configure git to use GH_PAT for any remaining git dependencies
WORKDIR /app/instruments-service
RUN git config --global url."https://${GH_PAT}@github.com/".insteadOf "https://github.com/" && \
    pip install -e . && \
    git config --global --unset url."https://${GH_PAT}@github.com/".insteadOf

# Create data directories
RUN mkdir -p /app/data/samples /app/logs

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set working directory
WORKDIR /app/instruments-service

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
