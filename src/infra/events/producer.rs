//! Event Producer for ConFuse Platform


// EventProducer requires the kafka feature (rdkafka → librdkafka → OpenSSL).
// Gate the entire implementation so services that don't need Kafka can compile
// without a system OpenSSL / CMake installation.

mod kafka_impl {
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::ClientConfig;
    use serde::Serialize;
    use anyhow::Result;

    /// Kafka event producer
    pub struct EventProducer {
        producer: FutureProducer,
    }

    impl EventProducer {
        pub fn new(bootstrap_servers: &str) -> Result<Self> {
            let enable_idempotence = std::env::var("KAFKA_ENABLE_IDEMPOTENCE")
                .expect("Missing required environment variable: KAFKA_ENABLE_IDEMPOTENCE")
                .to_lowercase() == "true";

            let mut config = ClientConfig::new();
            config
                .set("bootstrap.servers", bootstrap_servers)
                .set("message.max.bytes", "10485760") // 10MB
                .set("delivery.timeout.ms", "300000") // 5 minutes
                .set("request.timeout.ms", "120000")
                // Slow/lossy link tolerance: give TCP+TLS setup a generous budget and back
                // off exponentially so a struggling broker is not hammered with reconnects.
                // NOTE: librdkafka has no socket.connection.setup.timeout.max.ms (Java-only
                // knob) — it aborts client creation as an unknown property.
                .set("socket.connection.setup.timeout.ms", "120000")
                .set("reconnect.backoff.ms", "1000")
                .set("reconnect.backoff.max.ms", "30000")
                .set("retry.backoff.ms", "500")
                .set("batch.size", "1048576") // 1MB batches
                .set("linger.ms", "50") // wait 50ms for more messages to batch
                .set("queue.buffering.max.messages", "100000")
                .set("queue.buffering.max.kbytes", "1048576")
                .set("enable.idempotence", enable_idempotence.to_string());

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

            let producer: FutureProducer = config.create()?;

            // Verify connection by fetching metadata
            use rdkafka::producer::Producer;
            producer.client().fetch_metadata(None, std::time::Duration::from_secs(10))?;
            tracing::info!("Successfully connected and verified Aiven Kafka at {}", bootstrap_servers);

            Ok(Self { producer })
        }
        pub async fn publish<T: Serialize>(&self, topic: &str, event: &T) -> Result<()> {
            let payload = serde_json::to_string(event)?;
            let record = FutureRecord::to(topic)
                .payload(&payload)
                .key("event");

            match self.producer.send(record, std::time::Duration::from_secs(0)).await {
                Ok(_delivery) => {
                    tracing::debug!("Event sent successfully to topic: {}", topic);
                    Ok(())
                }
                Err((e, _)) => {
                    tracing::error!("Failed to send event: {}", e);
                    Err(anyhow::anyhow!("Failed to send event: {}", e))
                }
            }
        }

        /// Publish with retries and optional DLQ fallback.
        pub async fn publish_with_retry<T: Serialize + std::fmt::Debug>(
            &self,
            topic: &str,
            event: &T,
            retries: usize,
            dlq_topic: Option<&str>,
        ) -> Result<()> {
            use tokio::time::{sleep, Duration};

            let mut last_err: Option<anyhow::Error> = None;

            for attempt in 0..retries {
                match self.publish(topic, event).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        tracing::warn!("Publish attempt {} failed for topic {}: {}", attempt + 1, topic, e);
                        last_err = Some(e);
                        let delay = Duration::from_millis((2u64.pow(attempt as u32)) * 500);
                        sleep(delay).await;
                    }
                }
            }

            tracing::error!("Failed to publish after {} attempts", retries);

            if let Some(dlq) = dlq_topic {
                // Build failure envelope
                let envelope = serde_json::json!({
                    "failedTopic": topic,
                    "failedAt": chrono::Utc::now().timestamp_millis(),
                    "error": format!("{:?}", last_err),
                    "event": format!("{:?}", event),
                });
                if let Err(e) = self.publish(dlq, &envelope).await {
                    tracing::error!("Failed to publish failure envelope to DLQ {}: {}", dlq, e);
                } else {
                    tracing::info!("Published failure envelope to DLQ {}", dlq);
                }
            }

            Err(last_err.unwrap_or_else(|| anyhow::anyhow!("publish failed without error")))
        }
    }
}


pub use kafka_impl::EventProducer;
