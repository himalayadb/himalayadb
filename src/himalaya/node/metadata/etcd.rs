use crate::node::metadata::{
    MetadataProvider, NodeMetadata, NodeWatchEvent, NodeWatcher, Subscription,
};
use prost::Message;

use crate::server::himalaya_internal::NodeMetadata as ProtoNodeMetadata;

use async_trait::async_trait;
use etcd_client::{Client, EventType, GetOptions, WatchOptions, WatchStream, Watcher};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;

#[derive(Clone)]
pub struct EtcdMetadataProvider {
    client: Client,
}

pub struct EtcdMetadataProviderConfig {
    pub hosts: Vec<String>,
}

impl EtcdMetadataProvider {
    pub async fn new(
        config: EtcdMetadataProviderConfig,
    ) -> Result<EtcdMetadataProvider, Box<dyn std::error::Error>> {
        let client = Client::connect(config.hosts, None).await?;

        Ok(EtcdMetadataProvider { client })
    }
}

#[async_trait]
impl MetadataProvider for EtcdMetadataProvider {
    type MetaWatcher = EtcdWatcher;

    async fn node_register(&self, r: &NodeMetadata) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = Vec::new();

        let nm = ProtoNodeMetadata {
            identifier: r.identifier.clone(),
            token: r.token,
        };

        nm.encode(&mut buf)?;

        let _worker = self
            .client
            .clone()
            .put(format!("members/{}", r.identifier), buf, None)
            .await?;

        Ok(())
    }

    async fn subscribe(&self) -> Result<Subscription<EtcdWatcher>, Box<dyn std::error::Error>> {
        let (watcher, stream) = self
            .client
            .clone()
            .watch("members/", Some(WatchOptions::new().with_prefix()))
            .await?;

        Ok(Subscription {
            inner: EtcdWatcher { watcher, stream },
        })
    }

    async fn node_list_all(&self) -> Result<Vec<NodeMetadata>, Box<dyn std::error::Error>> {
        self.client
            .clone()
            .get("members/", Some(GetOptions::new().with_prefix()))
            .await?
            .kvs()
            .iter()
            .map(|x| {
                let nm = ProtoNodeMetadata::decode(x.value())?;
                Ok(nm.into())
            })
            .collect()
    }
}

pub struct EtcdWatcher {
    watcher: Watcher,
    stream: WatchStream,
}

#[async_trait]
impl NodeWatcher for EtcdWatcher {
    async fn unsubscribe(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.watcher.cancel().await?;
        Ok(())
    }
}

impl Stream for EtcdWatcher {
    type Item = Result<Vec<NodeWatchEvent>, Box<dyn std::error::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().stream)
            .poll_next(cx)
            .map(|t| match t {
                Some(Ok(response)) => {
                    if response.canceled() {
                        return None;
                    }

                    let mut results = Vec::with_capacity(response.events().len());
                    for event in response.events() {
                        if let Some(kv) = event.kv() {
                            if let Ok(pnm) = ProtoNodeMetadata::decode(kv.value()) {
                                match event.event_type() {
                                    EventType::Delete => results.push(NodeWatchEvent::LeftCluster(pnm.into())),
                                    EventType::Put => results.push(NodeWatchEvent::JoinedCluster(pnm.into())),
                                }
                            } 
                        }
                    }
                    Some(Ok(results))
                }
                Some(Err(e)) => Some(Err(From::from(e))),
                None => None,
            })
    }
}
