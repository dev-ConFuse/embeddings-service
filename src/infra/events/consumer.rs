//! Event Consumer for ConFuse Platform

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::message::{Message, BorrowedMessage};
use serde::de::DeserializeOwned;
use futures::StreamExt;
use anyhow::Result;
use std::sync::Arc;
use tracing::{info, error, warn};

/// Kafka event consumer for background message processing
pub struct EventConsumer {
    consumer: StreamConsumer,
    group_id: String,
}

impl EventConsumer {
    /// Create a new event consumer with specified consumer group
    pub fn new(bootstrap_servers: &str, group_id: &str) -> Result<Self> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("client.id", format!("{}-consumer", group_id))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.offset.store", "false")
            // Session / heartbeat were 10000/3000 — far too tight for the
            // WSL -> VPN -> Aiven path. Heartbeats routinely took >10s in flight
            // ("Timed out HeartbeatRequest in flight (after 10508ms)"), so the
            // broker evicted the consumer and it re-subscribed in a loop without
            // ever finishing `chunks.raw`. That is why `embedding.generated` was
            // never produced and chunks ended up without vectors. These values
            // match the ones proven stable in doc-uni-proc/src/graph/consumer.rs.
            .set("session.timeout.ms", "45000")
            .set("heartbeat.interval.ms", "15000")
            .set("max.poll.interval.ms", "1800000")
            .set("auto.commit.interval.ms", "5000")
            // Keep the TCP connection alive so Aiven doesn't drop idle
            // connections (this is what caused BrokerTransportFailure after
            // quiet periods).
            .set("socket.keepalive.enable", "true")
            .set("socket.timeout.ms", "120000")
            // TCP+TLS setup budget. Explicit rather than relying on the library default.
            // NOTE: librdkafka has no socket.connection.setup.timeout.max.ms (that is a
            // Java-client knob) — setting it aborts client creation with
            // "No such configuration property".
            .set("socket.connection.setup.timeout.ms", "120000")
            // Reconnect with exponential backoff: a struggling broker needs breathing room,
            // not a reconnect storm.
            .set("reconnect.backoff.ms", "1000")
            .set("reconnect.backoff.max.ms", "30000")
            .set("retry.backoff.ms", "500")
            // Return promptly when topics are idle instead of holding fetches open.
            .set("fetch.wait.max.ms", "500");

        if let Ok(protocol) = std::env::var("KAFKA_SECURITY_PROTOCOL") {
            config.set("security.protocol", protocol);
        }
        if let Ok(mechanism) = std::env::var("KAFKA_SASL_MECHANISM") {
            config.set("sasl.mechanism", mechanism);
        }
        if let Ok(username) = std::env::var("KAFKA_SASL_USERNAME").or_else(|_| std::env::var("CONFLUENT_API_KEY")) {
            config.set("sasl.username", username);
        }
        if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD").or_else(|_| std::env::var("CONFLUENT_API_SECRET")) {
            config.set("sasl.password", password);
        }
        if let Ok(ca_location) = std::env::var("KAFKA_SSL_CA_LOCATION") {
            config.set("ssl.ca.location", ca_location);
        }
        if let Ok(ca_pem) = std::env::var("KAFKA_SSL_CA_PEM") {
            config.set("ssl.ca.pem", ca_pem.replace("\\n", "\n"));
        }

        let consumer: StreamConsumer = config.create()?;

        info!("Kafka event consumer initialized with group: {}", group_id);

        Ok(Self {
            consumer,
            group_id: group_id.to_string(),
        })
    }

    /// Subscribe to topics and start consuming messages
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        self.consumer.subscribe(topics)?;
        info!("Subscribed to topics: {:?}", topics);
        Ok(())
    }

    /// Start consuming messages with async handler
    pub async fn consume<F, T>(&self, handler: Arc<F>) -> Result<()>
    where
        F: Fn(T) -> futures::future::BoxFuture<'static, Result<()>> + Send + Sync + 'static,
        T: DeserializeOwned + Send + 'static,
    {
        info!("Starting message consumption for group: {}", self.group_id);

        let mut message_stream = self.consumer.stream();
        
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(message) => {
                    let payload = match message.payload() {
                        Some(p) => p,
                        None => {
                            warn!("Received message with empty payload");
                            continue;
                        }
                    };

                    // Deserialize message
                    match serde_json::from_slice::<T>(payload) {
                        Ok(event) => {
                            let handler = handler.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handler(event).await {
                                    error!("Error processing message: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to deserialize message: {}", e);
                            // Continue processing other messages
                        }
                    }
                }
                Err(e) => {
                    error!("Kafka consumer error: {}", e);
                    // Continue processing
                }
            }
        }

        Ok(())
    }

    /// Get consumer group ID
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Commit offsets manually (if auto-commit is disabled)
    pub async fn commit_message(&self, message: &BorrowedMessage<'_>) -> Result<()> {
        self.consumer.commit_message(message, rdkafka::consumer::CommitMode::Async)?;
        Ok(())
    }
}

/// Trait for event handlers
pub trait EventHandler<T>: Send + Sync {
    fn handle(&self, event: T) -> futures::future::BoxFuture<'static, Result<()>>;
}

/// Convenience function to create a boxed future handler
pub fn boxed_handler<F, Fut, T>(f: F) -> impl Fn(T) -> futures::future::BoxFuture<'static, Result<()>>
where
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: futures::Future<Output = Result<()>> + Send + 'static,
    T: Send + 'static,
{
    move |event| Box::pin(f(event))
}
