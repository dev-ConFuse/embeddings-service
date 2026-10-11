//! Kafka Topic Definitions for ConFuse Platform
//!
//! Topic names are configuration, not code (env-only — see
//! `.cline/rules/env-policy.md`); values come from `.map.env`.

/// Kafka topic names used across the ConFuse platform
pub struct Topics;

impl Topics {
    // Chunk Processing Topics (unified-processor → embeddings-service)
    pub fn chunks_raw() -> String {
        std::env::var("KAFKA_CHUNKS_RAW_TOPIC")
            .expect("Missing required environment variable: KAFKA_CHUNKS_RAW_TOPIC")
    }

    // Embedding Topics (embeddings-service → unified-processor)
    pub fn embedding_generated() -> String {
        std::env::var("KAFKA_EMBEDDING_GENERATED_TOPIC")
            .expect("Missing required environment variable: KAFKA_EMBEDDING_GENERATED_TOPIC")
    }
}

/// Get all active topic names for configuration
pub fn get_all_topics() -> Vec<String> {
    vec![
        Topics::chunks_raw(),
        Topics::embedding_generated(),
    ]
}
