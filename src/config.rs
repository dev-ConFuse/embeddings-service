//! Configuration for the embeddings service (generate-only)

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub models: ModelConfig,
    pub kafka: KafkaConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub input_topic: String,
    pub output_topic: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub workers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub default_model: String,
    pub max_batch_size: usize,
    pub timeout: Duration,
    pub gemini_base_url: String,
    pub gemini_api_key: String,
}

/// All configuration is env-only (`.map.env` / `.secret.env`, see
/// `.cline/rules/env-policy.md`) — required vars fail fast at boot, optional
/// pairs (NVIDIA/GEMINI) fall back to the sibling env var, never a literal.
impl Config {
    fn required(name: &str) -> crate::Result<String> {
        std::env::var(name).map_err(|_| {
            crate::error::EmbeddingError::ConfigError(format!(
                "Missing required environment variable: {name}"
            ))
        })
    }

    fn required_parse<T: std::str::FromStr>(name: &str) -> crate::Result<T> {
        Self::required(name)?
            .parse::<T>()
            .map_err(|_| crate::error::EmbeddingError::ConfigError(format!("Invalid value for environment variable: {name}")))
    }

    fn required_bool(name: &str) -> crate::Result<bool> {
        match Self::required(name)?.to_lowercase().as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            other => Err(crate::error::EmbeddingError::ConfigError(format!(
                "Invalid boolean for environment variable {name}: {other}"
            ))),
        }
    }

    /// First present env var of the given names (both are env — no literal fallback).
    fn required_any(names: &[&str]) -> crate::Result<String> {
        for name in names {
            if let Ok(v) = std::env::var(name) {
                return Ok(v);
            }
        }
        Err(crate::error::EmbeddingError::ConfigError(format!(
            "Missing required environment variable (one of): {}",
            names.join(", ")
        )))
    }

    pub fn from_env() -> crate::Result<Self> {
        Ok(Self {
            server: ServerConfig {
                host: Self::required("HOST")?,
                port: Self::required_any(&["PORT", "EMBEDDINGS_SERVICE_PORT"])?
                    .parse()
                    .map_err(|_| crate::error::EmbeddingError::ConfigError("Invalid PORT or EMBEDDINGS_SERVICE_PORT".to_string()))?,
                workers: Self::required_parse("WORKERS")?,
            },
            models: ModelConfig {
                default_model: Self::required("DEFAULT_EMBEDDING_MODEL")?,
                max_batch_size: Self::required_parse("MAX_BATCH_SIZE")?,
                timeout: Duration::from_secs(Self::required_parse("MODEL_TIMEOUT_SECS")?),
                gemini_base_url: Self::required_any(&["NVIDIA_NIM_BASE_URL", "GEMINI_BASE_URL"])?,
                gemini_api_key: Self::required_any(&["NVIDIA_NIM_API_KEY", "GEMINI_API_KEY"])?,
            },
            kafka: KafkaConfig {
                bootstrap_servers: Self::required("KAFKA_BOOTSTRAP_SERVERS")?,
                group_id: Self::required("EMBEDDINGS_SERVICE_KAFKA_GROUP_ID")?,
                input_topic: Self::required("KAFKA_INPUT_TOPIC")?,
                output_topic: Self::required("KAFKA_OUTPUT_TOPIC")?,
                enabled: Self::required_bool("KAFKA_ENABLED")?,
            },
        })
    }
}
