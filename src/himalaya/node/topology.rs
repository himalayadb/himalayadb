use crate::node::Node;
use crate::node::metadata::{MetadataProvider, NodeWatcher, NodeWatchEvent};
use tokio_stream::StreamExt;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::future::Future;

pub struct Topology<W: NodeWatcher> {
    pub(crate) nodes: Arc<RwLock<HashMap<String, Node>>>,
    pub(crate) provider: Box<dyn MetadataProvider<MetaWatcher = W>>,
}

impl <W: NodeWatcher> Topology<W> {
    pub fn new(n: HashMap<String, Node>, p: Box<dyn MetadataProvider<MetaWatcher = W>>) -> Self {
        Topology {
            nodes: Arc::new(RwLock::new(n)),
            provider: p,
        
        }
    }

    pub async fn start(&self, shutdown: impl Future) -> Result<(), Box<dyn std::error::Error>> {
        tokio::select! {
            resp = self.watch() => {
                resp
            }
            _ = shutdown => {
                Ok(())
            }
        }
    }

    async fn watch(&self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let mut sub = self.provider.subscribe().await?;
            while let Some(resp) = sub.next().await {
                if let Ok(events) = resp {
                    for e in events {
                        match e {
                            NodeWatchEvent::LeftCluster(nm) => {
                                self.remove_node(&nm.identifier);
                            }
                            NodeWatchEvent::JoinedCluster(nm) => {
                                self.add_node(Node::new(nm));
                            }
                        }
                    }
                }
            }
            sub.unsubscribe().await?;
        }
    }
    
    pub fn get_node(&self, identifier: &str) -> Option<Node> {
        let map = self.nodes.try_read().ok()?;
        if let Some(nm) = map.get(identifier) {
            Some((*nm).clone())
        } else {
            None
        }
    }

    pub fn add_node(&self, node: Node) -> Option<Node> {
        let mut map = self.nodes.try_write().ok()?;
        map.insert(node.metadata.identifier.clone(), node)
    }

    pub fn remove_node(&self, identifier: &str) -> Option<Node> {
        let mut map = self.nodes.try_write().ok()?;
        map.remove(identifier)
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::node::metadata::{EtcdMetadataProvider, EtcdMetadataProviderConfig, NodeMetadata};
    use tokio::sync::oneshot;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_can_populate_topology() {
        let provider = Box::new(
            EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
                hosts: vec!["localhost:2379".to_owned()],
            })
            .await
            .expect("failed to create etcd provider"),
        );

        
        let topology = Topology::new(HashMap::new(), provider);

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let provider = Box::new(
                EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
                    hosts: vec!["localhost:2379".to_owned()],
                })
                .await
                .expect("failed to create etcd provider"),
            );

            for n in 1..5 {
                let _registration = provider
                    .node_register(&NodeMetadata {
                        token: n,
                        identifier: format!("node_{:?}", n),
                    })
                    .await
                    .expect("failed to obtain registration");
            }
            tx.send(()).expect("failed to send shutdown");
        });

        topology.start(rx).await.expect("node failed");

        for token in 1..5 {
            let identifier = format!("members/node_{:?}", token);

            let expected = topology
                .get_node(&identifier)
                .expect(&format!("{:?} not found", identifier));
            assert_eq!(
                Node::new(NodeMetadata{
                    identifier,
                    token: -1,
                }),
                expected
            )
        }
    }
}
