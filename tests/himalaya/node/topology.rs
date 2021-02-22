use crate::node::metadata::etcd::{etcd_provider, etcd_provider_with_prefix};
use himalaya::node::metadata::{EtcdMetadataProvider, MetadataProvider, NodeMetadata};
use himalaya::node::partitioner::{Murmur3, Partitioner};
use himalaya::node::topology::Topology;
use himalaya::node::Node;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;
use test::Bencher;
use tokio::sync::oneshot;

#[tokio::test(flavor = "multi_thread")]
async fn test_can_find_coordinator() {
    let (provider, prefix) = etcd_provider()
        .await
        .expect("failed to create etcd provider");

    let partitioner = Partitioner::Murmur3(Murmur3);
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
        let provider = etcd_provider_with_prefix(prefix)
            .await
            .expect("failed to create etcd provider");

        provider
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
        .find_coordinator_and_replicas(k.as_bytes(), 0)
        .expect("could not find coordinator");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_can_populate_topology() {
    let (provider, prefix) = etcd_provider()
        .await
        .expect("failed to create etcd provider");

    let partitioner = Partitioner::Murmur3(Murmur3);
    let topology = Topology::new(HashMap::new(), provider, partitioner);

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let provider = etcd_provider_with_prefix(prefix)
            .await
            .expect("failed to create etcd provider");

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
//
// #[bench]
// fn bench_find_coordinator(b: &mut Bencher) {
//     b.iter(|| {
//         Topology::<EtcdMetadataProvider>::get_coordinator_and_replicas(replicas, token, &nodes);
//     });
// }
