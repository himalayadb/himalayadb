use async_trait::async_trait;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;

pub mod etcd;
pub use etcd::*;

use crate::server::himalaya_internal::NodeMetadata as ProtoNodeMetadata;

#[derive(Clone, Debug, PartialEq)]
pub struct NodeMetadata {
    pub identifier: String,
    pub token: i64,
}

impl From<ProtoNodeMetadata> for NodeMetadata {
    fn from(nm: ProtoNodeMetadata) -> Self {
        NodeMetadata{
            identifier: nm.identifier,
            token: nm.token,
        }
    }
}
#[async_trait]
pub trait MetadataProvider {
    type MetaWatcher: NodeWatcher;

    async fn node_register(
        &self,
        r: &NodeMetadata,
    ) -> Result<(), Box<dyn std::error::Error>>;

    async fn subscribe(
        &self,
    ) -> Result<Subscription<Self::MetaWatcher>, Box<dyn std::error::Error>>;

    async fn node_list_all(&self) -> Result<Vec<NodeMetadata>, Box<dyn std::error::Error>>;
}

#[derive(Debug, PartialEq)]
pub enum NodeWatchEvent {
    LeftCluster(NodeMetadata),
    JoinedCluster(NodeMetadata),
}

#[derive(Debug)]
pub struct NodeRegisterResponse {
    pub node_metadata: NodeMetadata,
}

#[async_trait]
pub trait NodeWatcher:
    Stream<Item = Result<Vec<NodeWatchEvent>, Box<dyn std::error::Error>>> + Unpin
{
    async fn unsubscribe(&mut self) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct Subscription<T: NodeWatcher> {
    inner: T,
}

impl<T: NodeWatcher> Stream for Subscription<T> {
    type Item = Result<Vec<NodeWatchEvent>, Box<dyn std::error::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

impl<T: NodeWatcher> Subscription<T> {
    pub async fn unsubscribe(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.unsubscribe().await
    }
}
