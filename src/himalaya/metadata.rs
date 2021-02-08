use crate::core::NodeMetadata;
use async_trait::async_trait;
use etcd_client::{Client, GetOptions, WatchOptions};
use tokio::sync::watch::{self, Receiver};

#[async_trait]
trait MetadataProvider {
    async fn node_register(
        &mut self,
        r: &NodeRegisterRequest,
    ) -> Result<NodeRegisterResponse, Box<dyn std::error::Error>>;
    async fn node_watch(&mut self) -> Result<Receiver<NodeWatchEvent>, Box<dyn std::error::Error>>;
    async fn node_list_all(&mut self) -> Result<Vec<NodeMetadata>, Box<dyn std::error::Error>>;
}

#[derive(Debug)]
enum NodeWatchEvent {
    Empty {},
    JoinedCluster { node_metadata: NodeMetadata },
    WatchExpired {},
}

#[derive(Debug)]
struct NodeRegisterRequest {
    identifier: String,
}

#[derive(Debug)]
struct NodeRegisterResponse {
    node_metadata: NodeMetadata,
}

struct EtcdMetadataProvider {
    client: Client,
}

struct EtcdMetadataProviderConfig {
    hosts: Vec<String>,
}

impl EtcdMetadataProvider {
    async fn new(
        config: EtcdMetadataProviderConfig,
    ) -> Result<EtcdMetadataProvider, Box<dyn std::error::Error>> {
        let client = Client::connect(config.hosts, None).await?;

        Ok(EtcdMetadataProvider { client })
    }
}

#[async_trait]
impl MetadataProvider for EtcdMetadataProvider {
    async fn node_register(
        &mut self,
        r: &NodeRegisterRequest,
    ) -> Result<NodeRegisterResponse, Box<dyn std::error::Error>> {
        let _worker = self.client.put("members/Spencer", vec![], None).await?;

        Ok(NodeRegisterResponse {
            node_metadata: NodeMetadata {
                identifier: "Spencer".to_owned(),
                token: -1,
            },
        })
    }

    async fn node_watch(&mut self) -> Result<Receiver<NodeWatchEvent>, Box<dyn std::error::Error>> {
        let (mut watcher, mut stream) = self
            .client
            .watch("members/", Some(WatchOptions::new().with_prefix()))
            .await?;
        let (tx, mut rx) = watch::channel(NodeWatchEvent::Empty {});

        tokio::spawn(async move {
            loop {
                if let Ok(res) = stream.message().await {
                    match res {
                        Some(watch_res) => {
                            for event in watch_res.events() {
                                if let Some(kv) = event.kv() {
                                    tx.send(NodeWatchEvent::JoinedCluster {
                                        node_metadata: NodeMetadata {
                                            identifier: "ha".to_owned(),
                                            token: -1,
                                        },
                                    });
                                }
                            }
                        }
                        None => {
                            tx.send(NodeWatchEvent::WatchExpired {});
                        }
                    }
                } else {
                    tx.send(NodeWatchEvent::WatchExpired {});
                    break;
                }
            }
        });

        Ok(rx)
    }

    async fn node_list_all(&mut self) -> Result<Vec<NodeMetadata>, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .get("members/", Some(GetOptions::new().with_prefix()))
            .await?;
        Ok(resp
            .kvs()
            .iter()
            .map(|x| NodeMetadata {
                identifier: x.key_str().unwrap_or("").to_owned(),
                token: -1,
            })
            .collect())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn test_can_register_with_etcd() {
        let mut provider = EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
            hosts: vec!["localhost:2379".to_owned()],
        })
        .await
        .expect("failed to create provider");

        let registration = provider
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
        assert_eq!("members/Spencer", nodes[0].identifier);
    }

    #[tokio::test]
    async fn test_can_watch_nodes() {
        let mut provider = EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
            hosts: vec!["localhost:2379".to_owned()],
        })
        .await
        .expect("failed to create provider");

        let mut watcher = provider
            .node_watch()
            .await
            .expect("failed to obtain watcher");

        let registration = provider
            .node_register(&NodeRegisterRequest {
                identifier: "node_1".to_owned(),
            })
            .await
            .expect("failed to obtain registration");

        while watcher.changed().await.is_ok() {
            println!("received = {:?}", *watcher.borrow());
        }
    }
}
