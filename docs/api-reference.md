# embeddings-service API Reference

## Overview
This document outlines the API endpoints and interfaces exposed by `embeddings-service`.

## HTTP Endpoints

### `GET /health`
Returns the health status of the service. This endpoint is used by deployment platforms (Render, Docker) for health checks.
- **Response**: `200 OK` with body "OK"
- **Purpose**: Health check for deployment monitoring

## Kafka Events

### Input Event: SimplifiedChunkRawEvent
Consumed from the Kafka input topic (configured via `KAFKA_CHUNKS_RAW_TOPIC`).

**Fields:**
- `headers`: Event metadata (event_id, correlation_id, timestamp)
- `metadata`: Additional metadata about the source
- `source_id`: Identifier for the source document/repo
- `repo_name`: Name of the repository
- `chunks`: Array of chunks to process
  - `id`: Chunk identifier
  - `file_id`: File identifier
  - `content`: Text content to embed
  - `chunk_type`: Type of chunk
  - `language`: Language of the content

### Output Event: SimplifiedEmbeddingGeneratedEvent
Published to the Kafka output topic (configured via `KAFKA_EMBEDDING_GENERATED_TOPIC`).

**Fields:**
- `headers`: Event metadata (event_id, correlation_id, timestamp)
- `metadata`: Additional metadata about the source
- `source_id`: Identifier for the source document/repo
- `repo_name`: Name of the repository
- `chunks`: Array of generated embeddings
  - `id`: Chunk identifier
  - `file_id`: File identifier
  - `chunk_type`: Type of chunk
  - `language`: Language of the content
  - `embedding`: Vector representation (array of floats)
  - `model`: Model name used for generation
  - `dimension`: Dimension of the embedding vector
- `model`: Model name used for generation
- `timestamp`: When the embeddings were generated

## Environment Variables

### Server Configuration
- `HOST`: Server host address (default: "0.0.0.0")
- `PORT`: Server port (default: "3011")
- `WORKERS`: Number of worker threads (default: "1")

### Model Configuration
- `DEFAULT_EMBEDDING_MODEL`: Default model to use (required)
- `MAX_BATCH_SIZE`: Maximum batch size for processing (default: "32")
- `MODEL_TIMEOUT_SECS`: Model timeout in seconds (default: "30")
- `NVIDIA_NIM_BASE_URL`: NVIDIA NIM API base URL (required)
- `NVIDIA_NIM_API_KEY`: NVIDIA NIM API key (required)

### Kafka Configuration
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers
- `EMBEDDINGS_SERVICE_KAFKA_GROUP_ID`: Kafka consumer group ID
- `KAFKA_CHUNKS_RAW_TOPIC`: Input topic for chunk events
- `KAFKA_EMBEDDING_GENERATED_TOPIC`: Output topic for embedding events
- `KAFKA_ENABLED`: Enable/disable Kafka processing (default: "true")
