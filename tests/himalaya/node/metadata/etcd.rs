use crate::helpers;
use claim::assert_ok;
use himalaya::node::metadata::{MetadataProvider, NodeMetadata, NodeWatchEvent};
use std::sync::Arc;
use tokio_stream::StreamExt;

#[tokio::test]
async fn register_nodes() {
    let (provider, _) = helpers::etcd_provider(None)
        .await
        .expect("Failed to create etcd provider");

    let provider = Arc::new(provider);

    let nodes = provider
        .node_list_all()
        .await
        .expect("Failed to list nodes");

    assert_eq!(0, nodes.len());

    let meta = NodeMetadata {
        host: "127.0.0.1".to_owned(),
        token: 1,
        identifier: "node_1".to_owned(),
    };

    assert_ok!(provider.node_register(&meta).await);

    let nodes = provider
        .node_list_all()
        .await
        .expect("Failed to list nodes");

    assert_eq!(1, nodes.len());
    assert_eq!(&meta, &nodes[0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_watching_joined_nodes() {
    let (provider, _) = helpers::etcd_provider(None)
        .await
        .expect("Failed to create etcd provider");

    let provider = Arc::new(provider);

    let mut sub = provider.subscribe().await.expect("Failed to subscribe");
    let t2 = tokio::spawn(async move {
        let mut joined = Vec::with_capacity(10);
        loop {
            if let Some(resp) = sub.next().await {
                if let Ok(events) = resp {
                    for e in events {
                        match e {
                            NodeWatchEvent::LeftCluster(_) => {
                                panic!("no nodes left cluster");
                            }
                            NodeWatchEvent::JoinedCluster(nm) => {
                                joined.push(nm);
                            }
                        }
                    }
                }
            }

            if joined.len() >= 10 {
                assert_ok!(sub.unsubscribe().await);
                for n in 0..10 {
                    assert_eq!(
                        NodeMetadata {
                            host: "127.0.0.1".to_owned(),
                            token: n,
                            identifier: format!("node_{:?}", n),
                        },
                        joined[n as usize]
                    )
                }

                return;
            }
        }
    });

    for n in 0..10 {
        assert_ok!(
            provider
                .node_register(&NodeMetadata {
                    host: "127.0.0.1".to_owned(),
                    token: n,
                    identifier: format!("node_{:?}", n),
                })
                .await
        );
    }

    assert_ok!(t2.await);
}
