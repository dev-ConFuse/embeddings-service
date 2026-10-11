# ==============================================================================
# Embeddings Service - Dockerfile
# ==============================================================================
# Multi-stage build for Rust embedding service
# Port: 3011
# ==============================================================================

# Stage 1: Rust builder
FROM debian:bookworm-slim AS rust-builder

ARG RUST_VERSION=stable

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    zlib1g-dev \
    build-essential \
    cmake \
    libsasl2-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust via rustup to guarantee latest stable compiler
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Copy Rust files
COPY Cargo.toml Cargo.lock* ./

# Create dummy src for dependency caching
RUN mkdir -p src/generators src/models src/infra && \
    echo 'fn main() {}' > src/main.rs && \
    printf 'pub mod config;\npub mod error;\npub mod generators;\npub mod models;\npub mod infra;\n' > src/lib.rs && \
    touch src/config.rs src/error.rs src/generators/mod.rs src/models/mod.rs src/infra/mod.rs

# Build dependencies (cached)
RUN cargo build --release 2>/dev/null || true

# Remove dummy source
RUN rm -rf src/*

# Copy actual source
COPY src/ ./src/

# Update file modification times to force Cargo to rebuild instead of using the cached dummy build
RUN touch src/main.rs src/lib.rs 2>/dev/null || true
RUN find src -type f -exec touch {} +

# Build the application
RUN cargo build --release

# Stage 2: Final runtime image
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libpq5 \
    libssl3 \
    dumb-init \
    librdkafka1 \
    libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
WORKDIR /app
RUN groupadd -r appuser && useradd -r -g appuser -d /app appuser

# Copy Rust binary
COPY --from=rust-builder --chown=appuser:appuser /app/target/release/embeddings-service-bin /usr/local/bin/embeddings-service

# Verify the binary exists and is executable
RUN ls -la /usr/local/bin/embeddings-service && \
    chmod +x /usr/local/bin/embeddings-service

# Ensure correct permissions and create logs directory
RUN chown -R appuser:appuser /app && \
    mkdir -p /app/logs && \
    chown -R appuser:appuser /app/logs
USER appuser

# PORT is supplied at runtime via .map.env / .secret.env (see .cline/rules/env-policy.md)

# Health check optimized for Render
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Expose port
EXPOSE 3011

# Use dumb-init as PID 1 for proper signal handling
ENTRYPOINT ["dumb-init", "--"]

# Start the Rust service
CMD ["embeddings-service"]
