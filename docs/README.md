# Embeddings Service

**Port**: 3011 · **Role**: Kafka-based embedding generation

> This is the canonical README for the service; **all documentation lives in
> [`docs/`](./)** (this folder). See [index.md](index.md) for the docs index.

## Overview

High-performance Kafka-based embedding generation service for the ConFuse
platform. Consumes chunk events from Kafka, generates embeddings using
NVIDIA NIM / Gemini API, and publishes results back to Kafka for the unified
processors to store in FalkorDB.

- **Kafka-Based Architecture**: Event-driven processing via Apache Kafka
- **NVIDIA NIM / Gemini API Embeddings**: Configurable model provider
- **Batch Processing**: Efficient batch embedding generation
- **Health Check Endpoint**: Simple HTTP health check for deployments
- **Auto-Retry**: Resilient Kafka publishing with retry logic and DLQ fallback
- **Keep-Alive**: Self-ping mechanism to prevent cloud provider spin-down

## Event Flow

1. **Input**: consumes `SimplifiedChunkRawEvent` from `chunk-raw-events`
2. **Processing**: generates embeddings per chunk (batched)
3. **Output**: publishes `SimplifiedEmbeddingGeneratedEvent` to
   `embedding-generated-events`
4. **Storage**: unified processors consume output events and store in FalkorDB

## Key API surface

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check (also verifies Kafka connectivity) |
| `/api/v1/embed` | POST | Text embedding (used by the Arc lookup fallback) |
| `/api/v1/embeddings/generate` | POST | Single-text embedding |
| `/api/v1/embeddings/batch` | POST | Batch embedding |

Full schemas: [api-reference.md](api-reference.md).

## How to run the microservice

### Prerequisites

- Rust 1.70+ · Apache Kafka · NVIDIA NIM or Gemini API key

### Installation & run

```bash
cargo build --release
cargo run            # dev
cargo run --release  # prod
```

### Configuration

```bash
export HOST="0.0.0.0"
export PORT="3011"
export DEFAULT_EMBEDDING_MODEL="nvidia/embeddings/003"
export MAX_BATCH_SIZE="32"
export MODEL_TIMEOUT_SECS="30"
export NVIDIA_NIM_BASE_URL="https://integrate.api.nvidia.com/v1"
export NVIDIA_NIM_API_KEY="your-api-key"      # or GEMINI_API_KEY / GEMINI_BASE_URL

export KAFKA_BOOTSTRAP_SERVERS="your-kafka-bootstrap-servers"
export EMBEDDINGS_SERVICE_KAFKA_GROUP_ID="embeddings-service-group"
export KAFKA_INPUT_TOPIC="chunk-raw-events"
export KAFKA_OUTPUT_TOPIC="embedding-generated-events"
export KAFKA_ENABLED="true"
```

## Testing

Tests for this service live in the central test module (this service contains
no tests):

```bash
cd ../ConFuse-test-module
pip install -r requirements.txt
pytest embeddings_service kafka_events -v
```

## Documentation

| Document | Contents |
|----------|----------|
| [index.md](index.md) | Overview & docs index |
| [api-reference.md](api-reference.md) | Full endpoint reference |
| [architecture.md](architecture.md) | Internal architecture |
| [setup.md](setup.md) | Local development setup |
