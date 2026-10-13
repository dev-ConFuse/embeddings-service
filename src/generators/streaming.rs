//! Streaming embedding generation for large datasets

use crate::Result;
use crate::models::ModelManager;
use futures::stream::{self, Stream, StreamExt};

pub struct StreamingGenerator {
    model_manager: ModelManager,
}

impl StreamingGenerator {
    pub fn new(model_manager: ModelManager) -> Self {
        Self { model_manager }
    }

    pub async fn generate_stream(
        &self,
        texts: Vec<String>,
        model_name: String,
        batch_size: usize,
    ) -> Result<impl Stream<Item = Result<Vec<Vec<f32>>>>> {
        let model = self.model_manager.ensure_model_loaded(&model_name).await?;
        
        let stream = stream::iter(texts)
            .chunks(batch_size)
            .map(move |chunk| {
                let model = model.clone();
                async move {
                    model.generate(chunk).await
                }
            })
            .buffered(2); // Process 2 batches concurrently

        Ok(stream)
    }

    pub async fn generate_unbounded(
        &self,
        texts: impl Stream<Item = String> + Send + 'static,
        model_name: String,
        batch_size: usize,
    ) -> impl Stream<Item = Result<Vec<Vec<f32>>>> {
        let model = self.model_manager.ensure_model_loaded(&model_name).await.expect("model must load before generation");
        
        texts
            .chunks(batch_size)
            .map(move |chunk| {
                let model = model.clone();
                async move {
                    model.generate(chunk).await
                }
            })
            .buffered(2)
    }
}
