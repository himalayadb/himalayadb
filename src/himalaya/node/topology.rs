use crate::node::metadata::{MetadataProvider, NodeWatchEvent};
use crate::node::partitioner::Partitioner;
use crate::node::Node;
use tokio_stream::StreamExt;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};

pub struct Topology<MetaProvider> {
    nodes: Arc<RwLock<HashMap<String, Arc<Node>>>>,
    nodes_list: Arc<RwLock<Vec<Arc<Node>>>>,
    provider: MetaProvider,
    partitioner: Partitioner,
}

impl<Provider: MetadataProvider> Topology<Provider> {
    pub fn new(
        nodes: HashMap<String, Arc<Node>>,
        provider: Provider,
        partitioner: Partitioner,
    ) -> Self {
        let mut n = nodes
            .iter()
            .map(|(_, v)| Arc::clone(v))
            .collect::<Vec<Arc<Node>>>();
        n.sort();

        Topology {
            nodes: Arc::new(RwLock::new(nodes)),
            nodes_list: Arc::new(RwLock::new(n)),
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

    pub fn find_coordinator_and_replicas(
        &self,
        key: &[u8],
        num_replicas: usize,
    ) -> Option<(Arc<Node>, Vec<Arc<Node>>)> {
        let tk = self.partitioner.partition(key);
        let list = self.nodes_list.try_read().ok()?;
        if num_replicas > list.len() {
            None
        } else {
            Some(Topology::<Provider>::get_coordinator_and_replicas(
                num_replicas as usize,
                tk,
                &list,
            ))
        }
    }

    fn get_coordinator_and_replicas(
        num_replicas: usize,
        token: i64,
        nodes: &[Arc<Node>],
    ) -> (Arc<Node>, Vec<Arc<Node>>) {
        let num_nodes = nodes.len();
        assert!(num_replicas < num_nodes);

        let mut found = num_nodes;
        for (i, x) in nodes.iter().enumerate() {
            if token > x.metadata.token {
                continue;
            }
            found = i;
            break;
        }

        let mut coordinator = found;
        // did not find coordinator
        if coordinator == num_nodes {
            coordinator = 0;
        }

        let replica_start = coordinator + 1;
        let mut replica_nodes = Vec::new();
        for n in replica_start..(num_replicas + replica_start) {
            if n >= num_nodes {
                replica_nodes.push(nodes[n % num_nodes].clone());
            } else {
                replica_nodes.push(nodes[n].clone());
            }
        }

        (nodes[coordinator].clone(), replica_nodes)
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

    pub fn get_node(&self, identifier: &str) -> Option<Arc<Node>> {
        let map = self.nodes.try_read().ok()?;
        map.get(identifier).map(|n| n.clone())
    }

    fn add_node(&self, node: Node) -> Option<Arc<Node>> {
        let mut list = self.nodes_list.try_write().ok()?;
        let nrc = Arc::new(node);
        list.push(nrc.clone());
        list.sort();

        let mut map = self.nodes.try_write().ok()?;
        map.insert(nrc.metadata.identifier.clone(), nrc)
    }

    fn remove_node(&self, identifier: &str) -> Option<Arc<Node>> {
        let mut list = self.nodes_list.try_write().ok()?;
        if let Some(pos) = list
            .iter()
            .position(|x| x.metadata.identifier == identifier)
        {
            list.remove(pos);
        }

        let mut map = self.nodes.try_write().ok()?;
        map.remove(identifier)
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Borrow;

    use super::*;
    use crate::node::metadata::{EtcdMetadataProvider, EtcdMetadataProviderConfig, NodeMetadata};
    use crate::node::partitioner::Murmur3;
    use tokio::sync::oneshot;

    #[test]
    fn test_find_coordinator() {
        let tests = vec![
            (4, 1, 5, vec![10]),
            (12, 4, 15, vec![23, 0, 5, 10]),
            (18, 2, 23, vec![0, 5]),
            (26, 2, 0, vec![5, 10]),
        ];

        let mut nodes = Vec::new();
        for i in vec![0, 5, 10, 15, 23] {
            nodes.push(Arc::new(Node::new(NodeMetadata {
                identifier: "test".to_string(),
                token: i,
                host: "127.0.0.1:50051".to_owned(),
            })));
        }

        for (token, replicas, expected_coordinator, expected_replica_ids) in tests {
            let (coordinator, replicas) =
                Topology::<EtcdMetadataProvider>::get_coordinator_and_replicas(
                    replicas, token, &nodes,
                );
            assert_eq!(
                Node::new(NodeMetadata {
                    host: "127.0.0.1:50051".to_string(),
                    identifier: "test".to_string(),
                    token: expected_coordinator
                }),
                *coordinator
            );

            assert_eq!(
                expected_replica_ids,
                replicas
                    .iter()
                    .map(|x| x.metadata.token)
                    .collect::<Vec<i64>>()
            );
        }
    }

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
                .map(|x| (x.identifier.clone(), Arc::new(Node::new(x))))
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
                    host: "test".to_string(),
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
            .find_coordinator_and_replicas(k.as_bytes(), 2)
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
                        host: "test".to_string(),

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
                &Node::new(NodeMetadata {
                    host: "test".to_string(),
                    identifier,
                    token
                }),
                expected.borrow()
            )
        }
    }
}
