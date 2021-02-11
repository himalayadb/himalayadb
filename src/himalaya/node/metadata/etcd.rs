use crate::node::metadata::{
    MetadataProvider, NodeMetadata, NodeRegisterRequest, NodeRegisterResponse, NodeWatchEvent,
    NodeWatcher, Subscription,
};
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

    async fn node_register(
        &self,
        r: &NodeRegisterRequest,
    ) -> Result<NodeRegisterResponse, Box<dyn std::error::Error>> {
        let _worker = self
            .client
            .clone()
            .put(format!("members/{}", r.identifier), vec![], None)
            .await?;

        Ok(NodeRegisterResponse {
            node_metadata: NodeMetadata {
                identifier: r.identifier.clone(),
                token: -1,
            },
        })
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
                Ok(NodeMetadata {
                    identifier: x.key_str()?.to_owned(),
                    token: -1,
                })
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
                            if let Ok(key) = kv.key_str() {
                                let identifier = key.to_owned();
                                match event.event_type() {
                                    EventType::Delete => {
                                        results.push(NodeWatchEvent::LeftCluster(NodeMetadata {
                                            identifier,
                                            token: -1,
                                        }))
                                    }
                                    EventType::Put => {
                                        results.push(NodeWatchEvent::JoinedCluster(NodeMetadata {
                                            identifier,
                                            token: -1,
                                        }))
                                    }
                                }
                            } else {
                                return Some(Err(From::from("invalid identifier found")));
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

#[cfg(test)]
mod test {
    use super::*;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_can_register_with_etcd() {
        let provider = EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
            hosts: vec!["localhost:2379".to_owned()],
        })
        .await
        .expect("failed to create provider");

        let _registration = provider
            .node_register(&NodeRegisterRequest {
                identifier: "node_1".to_owned(),
            })
            .await
            .expect("failed to obtain registration");

        let nodes = provider
            .node_list_all()
            .await
            .expect("failed to list all nodes");

        assert_eq!(1, nodes.len());
        assert_eq!("members/node_1", nodes[0].identifier);
    }

    #[tokio::test]
    async fn test_can_watch_nodes() {
        let provider = EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
            hosts: vec!["localhost:2379".to_owned()],
        })
        .await
        .expect("failed to create provider");

        let subscriber = provider
            .subscribe()
            .await
            .expect("failed to obtain watcher");

        tokio::spawn(async move {
            let _registration = provider
                .node_register(&NodeRegisterRequest {
                    identifier: "node_3".to_owned(),
                })
                .await
                .expect("failed to obtain registration");
        });

        let messages = subscriber
            .take(1)
            .next()
            .await
            .expect("failed to get events")
            .expect("failed to get events");
        assert_eq!(1, messages.len());
        assert_eq!(
            NodeWatchEvent::JoinedCluster(NodeMetadata {
                identifier: "members/node_3".to_string(),
                token: -1
            }),
            messages[0]
        )
    }
}
