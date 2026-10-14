//! Main entry point for the embeddings service.
//! Provides REST endpoints for embedding generation via Kafka communication with unified-processor.


use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use embeddings_service::{
    Config,
    models::ModelManager,
};


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup panic handler to catch and log panics
    std::panic::set_hook(Box::new(|panic_info| {
        tracing::error!("Panic occurred: {}", panic_info);
    }));

    // Load environment variables (env policy: .map.env → .secret.env → .env.local)
    dotenvy::from_filename_override(".map.env").ok();
    dotenvy::from_filename_override(".secret.env").ok();
    dotenvy::from_filename_override(".env.local").ok();

    // Initialize structured tracing (stdout + daily JSON file sink).
    // Level/filter and stdout format come from LOG_LEVEL / LOG_FORMAT —
    // no baked-in fallback (see .cline/rules/env-policy.md).
    let filter = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("LOG_LEVEL"))
        .unwrap_or_else(|_| {
            panic!("Missing required environment variable: LOG_LEVEL (or RUST_LOG)")
        });
    let format = std::env::var("LOG_FORMAT")
        .unwrap_or_else(|_| panic!("Missing required environment variable: LOG_FORMAT"));
    let file_appender = tracing_appender::rolling::daily("logs", "embeddings-service.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    if format == "json" {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(filter.clone()))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stdout)
                    .json(),
            )
            .with(tracing_subscriber::fmt::layer().with_writer(non_blocking).json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new(filter))
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stdout))
            .with(tracing_subscriber::fmt::layer().with_writer(non_blocking).json())
            .init();
    }

    tracing::info!(service = "embeddings-service", "Starting service");

    // Load configuration
    let config = Config::from_env()?;
    tracing::info!("Starting embeddings service on {}:{}", config.server.host, config.server.port);

    // Start a lightweight health check server so Render and Docker healthchecks pass
    // We start this BEFORE checking Kafka so that Render doesn't kill the deployment while we wait
    let port = config.server.port;
    let bind_host = config.server.host.clone();
    tokio::spawn(async move {
        let addr = format!("{}:{}", bind_host, port);
        if let Ok(listener) = tokio::net::TcpListener::bind(&addr).await {
            tracing::info!("Health check server listening on {}", addr);
            loop {
                if let Ok((mut socket, _)) = listener.accept().await {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0; 1024];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let path = request.split_whitespace().nth(1).unwrap_or("");
                    // Only the health surface answers 200; everything else is 404.
                    let response = if path == "/health" || path == "/" {
                        // JSON status payload — matches the platform health contract
                        // (ConFuse-test-module assert_health_payload expects a status-ish key).
                        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 20\r\n\r\n{\"status\":\"healthy\"}"
                    } else {
                        "HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: 32\r\n\r\n{\"error\":\"not found\",\"path\":\"?\"}"
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                }
            }
        } else {
            tracing::error!("Failed to bind health check server to {}", addr);
        }
    });

    // Check Kafka health before starting (Kafka is required)
    tracing::info!("Checking Kafka connectivity with retry loop...");
    use embeddings_service::infra::events::EventProducer;
    let mut attempts = 0;
    let max_attempts = 18; // 18 * 5s = 90 seconds timeout
    
    loop {
        match EventProducer::new(&config.kafka.bootstrap_servers) {
            Ok(_) => {
                tracing::info!("Kafka health check passed");
                tracing::info!("Kafka on Aiven is initialized");
                break;
            }
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    anyhow::bail!("Kafka is required but not available after {} attempts: {}", max_attempts, e);
                }
                tracing::warn!("Kafka health check failed, retrying in 5s... ({}/{})", attempts, max_attempts);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }

    // Initialize service components
    let model_manager = Arc::new(ModelManager::new(config.clone()));
    

    // Ensure default model is loaded at startup
    if let Err(e) = model_manager.ensure_model_loaded(&config.models.default_model).await {
        tracing::warn!("Failed to load default embedding model at startup: {}", e);
    }



    // Initialize Kafka worker
    let kafka_worker = embeddings_service::infra::kafka_worker::KafkaWorker::new(
        config.clone(),
        model_manager.clone(),
    )?;

    // Start Kafka worker directly
    tracing::info!("Starting Kafka worker...");
    kafka_worker.start().await?;

    Ok(())
}

