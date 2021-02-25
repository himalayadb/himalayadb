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

    #[tracing::instrument(name = "Finding Coordinator", skip(self, key))]
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

    #[tracing::instrument(name = "Getting Replicas for token", skip(nodes))]
    fn get_coordinator_and_replicas(
        num_replicas: usize,
        token: i64,
        nodes: &[Arc<Node>],
    ) -> (Arc<Node>, Vec<Arc<Node>>) {
        let num_nodes = nodes.len();
        assert!(num_replicas < num_nodes);

        let found = nodes.binary_search_by(|x| x.metadata.token.cmp(&token));
        let coordinator = match found {
            Ok(index) => index,
            Err(index) => {
                if index == num_nodes {
                    0 // need to insert at the end, so wrap around
                } else {
                    index
                }
            }
        };

        let replica_start = coordinator + 1;
        let mut replica_nodes = Vec::with_capacity(num_replicas);
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
                                tracing::info!(identifier=%nm.identifier.clone(), "Node left cluster.");
                                self.remove_node(&nm.identifier);
                            }
                            NodeWatchEvent::JoinedCluster(nm) => {
                                tracing::info!(identifier=%nm.identifier.clone(), "Node joined cluster.");
                                if let Err(e) = self.add_node(Node::new(nm)) {
                                    tracing::error!(error = %e, "failed to add node to topology");
                                }
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

    fn add_node(&self, node: Node) -> Result<(), Box<dyn std::error::Error + '_>> {
        let mut map = self.nodes.try_write()?;
        let mut list = self.nodes_list.try_write()?;
        let nrc = Arc::new(node);

        match map.insert(nrc.metadata.identifier.clone(), nrc.clone()) {
            None => {
                // did not exist in map
                list.push(nrc);
                list.sort();
            }
            Some(_) => {
                //overwrite node
                if let Ok(i) =
                    list.binary_search_by(|n| n.metadata.identifier.cmp(&nrc.metadata.identifier))
                {
                    list[i] = nrc;
                }
            }
        }
        Ok(())
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
mod tests {
    use super::*;
    use crate::node::metadata::{EtcdMetadataProvider, NodeMetadata};
    use test::Bencher;

    fn test_nodes() -> Vec<Arc<Node>> {
        let mut nodes = Vec::new();
        for i in vec![0, 5, 10, 15, 23] {
            nodes.push(Arc::new(Node::new(NodeMetadata {
                identifier: "test".to_string(),
                token: i,
                host: "127.0.0.1:50051".to_owned(),
            })));
        }

        nodes
    }

    #[bench]
    fn bench_find_coordinator(b: &mut Bencher) {
        let nodes = test_nodes();

        b.iter(|| Topology::<EtcdMetadataProvider>::get_coordinator_and_replicas(1, 4, &nodes));
    }

    #[test]
    fn test_find_coordinator() {
        let tests = vec![
            (4, 1, 5, vec![10]),
            (12, 4, 15, vec![23, 0, 5, 10]),
            (18, 2, 23, vec![0, 5]),
            (26, 2, 0, vec![5, 10]),
            (26, 3, 0, vec![5, 10, 15]),
        ];

        let nodes = test_nodes();

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
}
