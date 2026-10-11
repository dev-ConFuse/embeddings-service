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

    // Load environment variables from .env
    tracing::info!("Loading environment variables...");
    dotenvy::from_filename_override(".map.env").ok();
    dotenvy::from_filename_override(".secret.env").ok();
    dotenvy::from_filename_override(".env.local").ok();
    tracing::info!("Environment variables loaded");
    // Initialize tracing with file appender
    let file_appender = tracing_appender::rolling::daily("logs", "embeddings-service.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stdout))
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking).json())
        .init();

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
                    let _ = socket.read(&mut buf).await;
                    let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK").await;
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

