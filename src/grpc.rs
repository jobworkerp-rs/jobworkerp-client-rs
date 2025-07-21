use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};
use tokio::sync::{RwLock, RwLockReadGuard};
use tonic::transport::{ClientTlsConfig, Endpoint};

#[derive(Debug, Clone)]
pub struct GrpcConnection {
    endpoint: tonic::transport::Endpoint,
    channel: Arc<RwLock<tonic::transport::Channel>>,
}

impl GrpcConnection {
    pub async fn new(
        addr: String,
        request_timeout: Option<Duration>,
        use_tls: bool,
    ) -> Result<Self> {
        let endpoint = if let Some(timeout) = request_timeout {
            Endpoint::try_from(addr.clone())?.timeout(timeout)
        } else {
            Endpoint::try_from(addr.clone())?
        };

        // Add HTTP/2 configuration to help with stability and large headers
        let endpoint = endpoint
            // .http2_keep_alive_interval(Duration::from_secs(60))  // Increased interval
            // .keep_alive_timeout(Duration::from_secs(10))         // Increased timeout
            // .keep_alive_while_idle(true)
            // .http2_adaptive_window(false)  // Disable adaptive flow control for stability
            // .initial_stream_window_size(Some(65536))       // Conservative 64KB window
            .initial_connection_window_size(Some(1024 * 1024)) // 1MB connection window
            .tcp_keepalive(Some(Duration::from_secs(600))) // TCP keepalive
            .http2_max_header_list_size(16 * 1024 * 1024 - 1); // 16MB max header list size for detailed error messages

        // // Debug note about HTTP/2 stability issues
        // let endpoint = if std::env::var("HTTP2_DISABLE").is_ok() {
        //     println!("NOTE: HTTP2_DISABLE is set, but HTTP/2 is required for gRPC");
        //     println!("      Consider server-side fixes instead of disabling HTTP/2");
        //     endpoint
        // } else {
        //     endpoint
        // };

        // // Debug option: Note about HTTP/2 configuration
        // if std::env::var("HTTP2_DEBUG").is_ok() {
        //     if std::env::var("HTTP2_DISABLE").is_ok() {
        //         println!("HTTP/1.1 mode enabled");
        //     } else {
        //         println!("HTTP/2 configuration: keep_alive_interval=60s, timeout=10s, adaptive_window=false");
        //     }
        // }
        let endpoint = if use_tls {
            // https://github.com/rustls/rustls/issues/1938
            let _ = rustls::crypto::ring::default_provider().install_default();
            endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())?
        } else {
            endpoint
        };

        let channel =
            Arc::new(RwLock::new(endpoint.connect().await.context(format!(
                "Failed to connect to gRPC server at {}",
                &addr
            ))?));
        Ok(Self { endpoint, channel })
    }

    pub async fn reconnect(&self) -> Result<()> {
        let c = self.channel.clone();
        let mut m = c.write().await;
        *m = self.endpoint.connect().await?;
        Ok(())
    }

    pub async fn read_channel(&self) -> RwLockReadGuard<tonic::transport::Channel> {
        self.channel.read().await
    }
}
