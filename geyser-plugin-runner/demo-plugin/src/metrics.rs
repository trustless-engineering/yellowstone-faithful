// src/metrics.rs

use prometheus::{Encoder, TextEncoder};
use warp::Filter;

/// Struct to handle Prometheus metrics collection and exposition.
pub struct Metrics;

impl Metrics {
    /// Creates a new instance of Metrics.
    pub fn new() -> Self {
        Metrics
    }

    /// Serves the metrics endpoint on the specified port.
    pub fn serve(&self, port: u16) {
        let metrics_route = warp::path("metrics").map(move || {
            let encoder = TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();
            String::from_utf8(buffer).unwrap()
        });

        // Spawn a new thread for the metrics server
        std::thread::spawn(move || {
            // Create a new Tokio runtime
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to create Tokio runtime for metrics server");

            // Run the Warp server within the Tokio runtime
            rt.block_on(async {
                warp::serve(metrics_route).run(([0, 0, 0, 0], port)).await;
            });
        });
    }
}
