# embeddings-service Documentation

Welcome to the documentation for `embeddings-service`, a Kafka-based embedding generation service for the ConFuse platform.

## Contents

- [Architecture](./architecture.md) - System design and component overview
- [API Reference](./api-reference.md) - HTTP endpoints and Kafka event schemas
- [Setup & Configuration](./setup.md) - Installation, environment variables, and deployment

## Quick Start

The embeddings service is a Rust-based microservice that:
1. Consumes chunk events from Kafka
2. Generates embeddings using NVIDIA NIM
3. Publishes embedding events back to Kafka for storage

See the [Setup & Configuration](./setup.md) guide for deployment instructions.
