use claim::assert_ok;
use himalaya::node::metadata::{
    EtcdMetadataProvider, EtcdMetadataProviderConfig, MetadataProvider, NodeMetadata,
    NodeWatchEvent,
};
use std::sync::Arc;
use tokio_stream::StreamExt;
use uuid::Uuid;

pub async fn etcd_provider() -> Result<(EtcdMetadataProvider, String), Box<dyn std::error::Error>> {
    let prefix = Uuid::new_v4().to_string();
    let etcd_config = EtcdMetadataProviderConfig {
        hosts: vec!["localhost:2379".to_owned()],
        prefix: prefix.clone(),
        lease_ttl: 5,
        ttl_refresh_interval: 3000,
    };
    let provider = etcd_provider_with_settings(etcd_config).await?;
    Ok((provider, prefix))
}

pub async fn etcd_provider_with_prefix(
    prefix: String,
) -> Result<EtcdMetadataProvider, Box<dyn std::error::Error>> {
    let etcd_config = EtcdMetadataProviderConfig {
        hosts: vec!["localhost:2379".to_owned()],
        prefix,
        lease_ttl: 5,
        ttl_refresh_interval: 3000,
    };
    let provider = etcd_provider_with_settings(etcd_config).await?;
    Ok(provider)
}

pub async fn etcd_provider_with_settings(
    configuration: EtcdMetadataProviderConfig,
) -> Result<EtcdMetadataProvider, Box<dyn std::error::Error>> {
    let provider = EtcdMetadataProvider::new(configuration).await?;
    Ok(provider)
}

#[tokio::test]
async fn register_nodes() {
    let (provider, _) = etcd_provider()
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
    let (provider, _) = etcd_provider()
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
