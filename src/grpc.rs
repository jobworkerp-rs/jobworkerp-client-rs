use anyhow::Result;
use std::{sync::Arc, time::Duration};
use tokio::sync::{RwLock, RwLockReadGuard};
use tonic::transport::Endpoint;

pub struct GrpcConnection {
    endpoint: tonic::transport::Endpoint,
    channel: Arc<RwLock<tonic::transport::Channel>>,
}

impl GrpcConnection {
    pub async fn new(addr: String, timeout: Duration) -> Result<Self> {
        let endpoint = Endpoint::try_from(addr)?.timeout(timeout);
        let channel = Arc::new(RwLock::new(endpoint.connect().await?));
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
