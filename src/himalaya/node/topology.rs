use crate::node::metadata::{MetadataProvider, NodeWatchEvent};
use crate::node::partitioner::{Murmur3, Partitioner};
use crate::node::Node;
use tokio_stream::StreamExt;

use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

pub struct Topology<MetaProvider> {
    nodes: Arc<RwLock<HashMap<String, Rc<Node>>>>,
    provider: MetaProvider,
    partitioner: Partitioner,
}

impl<Provider: MetadataProvider> Topology<Provider> {
    pub fn new(
        nodes: HashMap<String, Rc<Node>>,
        provider: Provider,
        partitioner: Partitioner,
    ) -> Self {
        Topology {
            nodes: Arc::new(RwLock::new(nodes)),
            provider,
            partitioner,
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

    pub fn find_coordinator(&self, key: &[u8]) -> Option<Rc<Node>> {
        let tk = self.partitioner.partition(key);
        let map = self.nodes.read().ok()?;

        map.iter().take(1).next().map(|(_, v)| Rc::clone(v))
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

    pub fn get_node(&self, identifier: &str) -> Option<Rc<Node>> {
        let map = self.nodes.try_read().ok()?;
        map.get(identifier).map(|n| Rc::clone(n))
    }

    fn add_node(&self, node: Node) -> Option<Rc<Node>> {
        let mut map = self.nodes.try_write().ok()?;
        map.insert(node.metadata.identifier.clone(), Rc::new(node))
    }

    fn remove_node(&self, identifier: &str) -> Option<Rc<Node>> {
        let mut map = self.nodes.try_write().ok()?;
        map.remove(identifier)
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Borrow;

    use super::*;
    use crate::node::metadata::{EtcdMetadataProvider, EtcdMetadataProviderConfig, NodeMetadata};
    use tokio::sync::oneshot;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_can_find_coordinator() {
        let provider = EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
            hosts: vec!["localhost:2379".to_owned()],
        })
        .await
        .expect("failed to create etcd provider");

        let partitioner = Partitioner::Murmur3(Murmur3 {});
        let existing_nodes = provider
            .node_list_all()
            .await
            .expect("failed to list all nodes");

        let topology = Topology::new(
            existing_nodes
                .into_iter()
                .map(|x| (x.identifier.clone(), Rc::new(Node::new(x))))
                .collect(),
            provider,
            partitioner,
        );
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let provider = Box::new(
                EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
                    hosts: vec!["localhost:2379".to_owned()],
                })
                .await
                .expect("failed to create etcd provider"),
            );

            let _registration = provider
                .node_register(&NodeMetadata {
                    token: 1,
                    identifier: format!("node_{:?}", 1),
                })
                .await
                .expect("failed to obtain registration");
            tx.send(()).expect("failed to send shutdown");
        });

        topology.start(rx).await.expect("node failed");

        let (k, _) = ("hello", "world");
        let _ = topology
            .find_coordinator(k.as_bytes())
            .expect("could not find coordinator");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_can_populate_topology() {
        let provider = EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
            hosts: vec!["localhost:2379".to_owned()],
        })
        .await
        .expect("failed to create etcd provider");

        let partitioner = Partitioner::Murmur3(Murmur3 {});

        let topology = Topology::new(HashMap::new(), provider, partitioner);

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let provider = Box::new(
                EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
                    hosts: vec!["localhost:2379".to_owned()],
                })
                .await
                .expect("failed to create etcd provider"),
            );

            for n in 1..10 {
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

        for token in 1..10 {
            let identifier = format!("node_{:?}", token);

            let expected = topology
                .get_node(&identifier)
                .expect(&format!("{:?} not found", identifier));
            assert_eq!(
                &Node::new(NodeMetadata { identifier, token }),
                expected.borrow()
            )
        }
    }
}
