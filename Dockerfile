# Dockerfile for instruments-service
#
# Uses unified-trading-library base image from Artifact Registry.
# No GitHub token (GH_PAT) required.
#
# Build:
#   docker build --build-arg PROJECT_ID=your-gcp-project-id -t instruments-service .
#
# Run:
#   docker run -v /path/to/credentials.json:/app/credentials.json \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
#     -e GCP_PROJECT_ID=your-gcp-project-id \
#     instruments-service --operation instruments --mode batch --start-date 2024-01-01 --CEFI

ARG PROJECT_ID
# Digest-pinned UTL base image (QG STEP 5.79 -- reproducible builds + UTL/UAC provenance).
# Refreshed by the dependency-update fan-out (update-dependency-version.yml) on base-image
# republish; cloudbuild may override at build time: --build-arg BASE_IMAGE_DIGEST=sha256:...
#
# Rebuild trigger 2026-07-15 22:45Z: pulls the UTL base image bundling the new UAC
# (YAHOO_FINANCE phantom-venue REMOVED uac@fec3f110 + CBOE ohlcv_24h treasury capability
# uac@2ace1fca). This operationalizes the YAHOO_FINANCE removal in the expected-universe
# enumeration jobs (they enumerate VENUES_BY_ASSET_GROUP["tradfi"] via UAC, so the new UAC
# stops them seeding phantom YAHOO_FINANCE expected-coverage rows into the MTDS tradfi tick
# manifest).
# CORRECTION: the prior pin sha256:b7c57243 (UTL base cut 2026-07-15 17:54:46Z) had YAHOO
# removed but PREDATED uac@7754661a (2026-07-15 18:14:29Z, "add venue_data_type_has_batch_source"),
# so the enum crashed at runtime with `ImportError: cannot import name
# 'venue_data_type_has_batch_source' from 'unified_api_contracts'` (enumerate_expected_universe.py
# now imports that symbol). Bumped to the newer UTL 0.55.0/latest base (cut 2026-07-15 23:27:01Z)
# which bundles all of {YAHOO removed, CBOE ohlcv_24h, venue_data_type_has_batch_source} — verified
# in-image (cloudbuild=70dbc75f-c8db-4245-b3bb-fd175829f6b3, SUCCESS).
# Digest sha256:be51b33f... = UTL AR 0.55.0/latest.
# Issue: tradfi_unreachable_databento_data_types_mbp10_ohlcv_coarse_calendar_2026_07_15.md.
ARG BASE_IMAGE_DIGEST=sha256:be51b33ff0f399d13f0e81628c16fefda60c385c3ff8452141b5cb784718f2c3
ARG BASE_IMAGE=asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library@${BASE_IMAGE_DIGEST}
FROM --platform=linux/amd64 ${BASE_IMAGE}

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user
RUN useradd --create-home --shell /bin/bash appuser

# Set working directory
WORKDIR /app/instruments-service

# Install uv package manager (bootstrap with pip - acceptable exception per quality gate)
RUN pip install --no-cache-dir uv  # uv bootstrap — acceptable QG exception (bootstraps uv before uv is available)

# Install keyring FIRST (before pip.conf) to avoid auth loop
# keyring must be installed from PyPI, not Artifact Registry
RUN uv pip install --system --no-cache-dir keyrings.google-artifactregistry-auth

# NOW copy pip.conf - keyring is ready to handle Artifact Registry auth
COPY pip.conf /etc/pip.conf

# Copy service source code and lockfile
COPY . .

# WS-L (2026-06-28): hatch-vcs (source = "vcs") can't read git tags inside the docker build context
# (.git is .dockerignore'd + COPY . . excludes it), so the package's OWN version must be injected.
# cloudbuild passes the git-tag-derived version as --build-arg SETUPTOOLS_SCM_PRETEND_VERSION; export it
# for setuptools-scm/hatch-vcs BEFORE the editable install, else `uv pip install -e .` fails with
# "setuptools-scm was unable to detect version for /workspace".
ARG SETUPTOOLS_SCM_PRETEND_VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

# Install service dependencies (base image already has UTL + UAC pre-installed)
RUN uv pip install --system --no-sources -e .

# Create data directories
RUN mkdir -p /app/instruments-service/data/samples /app/instruments-service/logs

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Default environment variables (bucket names use PROJECT_ID from build-arg)
# ENABLE_CSV_SAMPLING=false prevents disk filling on ephemeral VMs
# UCS_SKIP_GCSFUSE_CHECK=1 skips GCSFUSE check (not used in Cloud Run/VMs)
ARG PROJECT_ID
ENV ENVIRONMENT=production \
    ENABLE_CSV_SAMPLING=false \
    UCS_SKIP_GCSFUSE_CHECK=1 \
    GCS_REGION=asia-northeast1-c \
    GCS_LOCATION=asia-northeast1 \
    GCP_PROJECT_ID=${PROJECT_ID} \
    INSTRUMENTS_GCS_BUCKET_CEFI=instruments-store-cefi-${PROJECT_ID} \
    INSTRUMENTS_GCS_BUCKET_TRADFI=instruments-store-tradfi-${PROJECT_ID} \
    INSTRUMENTS_GCS_BUCKET_DEFI=instruments-store-defi-${PROJECT_ID}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import instruments_service; print('healthy')" || exit 1

# Default entrypoint - runs the CLI
ENTRYPOINT ["python", "-m", "instruments_service"]

# Default command (can be overridden)
CMD ["--help"]
