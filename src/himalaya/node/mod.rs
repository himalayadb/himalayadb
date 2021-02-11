use crate::node::metadata::{
    MetadataProvider, NodeMetadata, NodeRegisterRequest, NodeWatchEvent, NodeWatcher,
};
use crate::node::topology::Topology;
use std::collections::HashMap;
use std::future::Future;
use tokio_stream::StreamExt;

pub mod metadata;
mod topology;

pub struct Node<W: NodeWatcher> {
    pub metadata: NodeMetadata,
    pub provider: Box<dyn MetadataProvider<MetaWatcher = W> + Send + Sync>,
    pub topology: Topology,
}

impl<W: NodeWatcher + Send> Node<W> {
    pub async fn create(
        identifier: &str,
        provider: Box<dyn MetadataProvider<MetaWatcher = W> + Send + Sync>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let nodes: HashMap<String, NodeMetadata> = provider
            .node_list_all()
            .await?
            .into_iter()
            .map(|nm| (nm.identifier.clone(), nm))
            .collect();

        let resp = provider
            .node_register(&NodeRegisterRequest {
                identifier: identifier.to_owned(),
            })
            .await?;

        Ok(Node {
            metadata: resp.node_metadata,
            topology: Topology::new(nodes),
            provider,
        })
    }

    pub async fn start(&self, shutdown: impl Future) -> Result<(), Box<dyn std::error::Error>> {
        tokio::select! {
            resp = self.monitor_topology() => {
                resp
            }
            _ = shutdown => {
                Ok(())
            }
        }
    }

    async fn monitor_topology(&self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let mut sub = self.provider.subscribe().await?;
            while let Some(resp) = sub.next().await {
                if let Ok(events) = resp {
                    for e in events {
                        match e {
                            NodeWatchEvent::LeftCluster(nm) => {
                                self.topology.remove_node(&nm.identifier);
                            }
                            NodeWatchEvent::JoinedCluster(nm) => {
                                self.topology.add_node(nm);
                            }
                        }
                    }
                }
            }
            sub.unsubscribe().await?;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::node::metadata::{
        EtcdMetadataProvider, EtcdMetadataProviderConfig, NodeRegisterRequest,
    };
    use tokio::sync::oneshot;
    use tokio::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_node() {
        let provider = Box::new(
            EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
                hosts: vec!["localhost:2379".to_owned()],
            })
            .await
            .expect("failed to create etcd provider"),
        );

        let mut node = Node::create(
            NodeMetadata {
                identifier: "test".to_owned(),
                token: -1,
            },
            provider,
        )
        .await
        .expect("failed to create node");

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
                    .node_register(&NodeRegisterRequest {
                        identifier: format!("node_{:?}", n),
                    })
                    .await
                    .expect("failed to obtain registration");
            }
            tx.send(()).expect("failed to send shutdown");
        });

        node.start(rx).await.expect("node failed");

        for n in 1..5 {
            let identifier = format!("members/node_{:?}", n);

            let expected = node
                .topology
                .get_node(&identifier)
                .expect(&format!("{:?} not found", identifier));
            assert_eq!(
                NodeMetadata {
                    identifier,
                    token: -1
                },
                expected
            )
        }

        let mut t = node.topology.clone();
        tokio::spawn(async move {
            for n in 1..5 {
                let identifier = format!("members/node_{:?}", n);

                let expected = t
                    .get_node(&identifier)
                    .expect(&format!("{:?} not found", identifier));
                assert_eq!(
                    NodeMetadata {
                        identifier,
                        token: -1
                    },
                    expected
                )
            }

            tokio::time::sleep(Duration::from_micros(1)).await;

            t.add_node(NodeMetadata {
                identifier: "spencer".to_string(),
                token: 0,
            })
        })
        .await;

        let metadata = node
            .topology
            .get_node("spencer")
            .expect("spencer doesnt exist");
        assert_eq!(
            NodeMetadata {
                identifier: "spencer".to_string(),
                token: 0
            },
            metadata
        );
    }
}
