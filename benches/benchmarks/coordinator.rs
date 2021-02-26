use criterion::{criterion_group, Criterion};
use himalaya::node::metadata::{EtcdMetadataProvider, NodeMetadata};
use himalaya::node::topology::Topology;
use himalaya::node::Node;
use std::sync::Arc;

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

fn find_coordinator(c: &mut Criterion) {
    let nodes = test_nodes();

    c.bench_function("Topology Find Coordinator", |b| {
        b.iter(|| Topology::<EtcdMetadataProvider>::get_coordinator_and_replicas(1, 4, &nodes));
    });
}

criterion_group!(coorindator, find_coordinator,);
