# embeddings-service Setup and Configuration

## Prerequisites
- Rust 1.70+
- Apache Kafka cluster (Aiven or self-hosted)
- NVIDIA NIM API key or Gemini API key
- Access to Kafka bootstrap servers

## Environment Variables

### Server Configuration
| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `HOST` | Server host address | No | "0.0.0.0" |
| `PORT` | Service port | No | "3011" |
| `WORKERS` | Number of worker threads | No | "1" |

### Model Configuration
| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DEFAULT_EMBEDDING_MODEL` | Default embedding model | Yes | - |
| `MAX_BATCH_SIZE` | Maximum batch size | No | "32" |
| `MODEL_TIMEOUT_SECS` | Model timeout in seconds | No | "30" |
| `NVIDIA_NIM_BASE_URL` | NVIDIA NIM API base URL | Yes* | - |
| `NVIDIA_NIM_API_KEY` | NVIDIA NIM API key | Yes* | - |
| `GEMINI_BASE_URL` | Gemini API base URL | Yes* | - |
| `GEMINI_API_KEY` | Gemini API key | Yes* | - |

*Either NVIDIA NIM or Gemini credentials are required

### Kafka Configuration
| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | Yes | - |
| `EMBEDDINGS_SERVICE_KAFKA_GROUP_ID` | Consumer group ID | Yes | - |
| `KAFKA_INPUT_TOPIC` | Input topic for chunk events | Yes | - |
| `KAFKA_OUTPUT_TOPIC` | Output topic for embedding events | Yes | - |
| `KAFKA_ENABLED` | Enable Kafka processing | No | "true" |

## Local Development

### Building the Service
```bash
# Development build
cargo build

# Release build
cargo build --release
```

### Running the Service
```bash
# Development mode
cargo run

# Production mode
cargo run --release
```

### Testing
```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture
```

## Environment File Setup

Create a `.env.local` file for local development:

```bash
# Server
HOST=0.0.0.0
PORT=3011
WORKERS=1

# Model
DEFAULT_EMBEDDING_MODEL=nvidia/embeddings/003
MAX_BATCH_SIZE=32
MODEL_TIMEOUT_SECS=30
NVIDIA_NIM_BASE_URL=https://integrate.api.nvidia.com/v1
NVIDIA_NIM_API_KEY=your-api-key-here

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
EMBEDDINGS_SERVICE_KAFKA_GROUP_ID=embeddings-service-local
KAFKA_INPUT_TOPIC=chunk-raw-events
KAFKA_OUTPUT_TOPIC=embedding-generated-events
KAFKA_ENABLED=true
```

## Docker Deployment

The service includes a Dockerfile for containerized deployment:

```bash
# Build Docker image
docker build -t embeddings-service .

# Run container
docker run -p 3011:3011 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e NVIDIA_NIM_API_KEY=your-key \
  embeddings-service
```

## Container Deployment (Docker / AKS)

Deployment is Dockerfile-only:
1. Build the image from `Dockerfile`
2. Supply all required environment variables at runtime (`.map.env` → AKS
   ConfigMap, `.secret.env` → AKS Secret — see `.cline/rules/env-policy.md`)
3. The health check endpoint (`/health`) is used for container probes
4. Kafka connectivity will be verified before the worker starts
