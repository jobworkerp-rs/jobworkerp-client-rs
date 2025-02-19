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
