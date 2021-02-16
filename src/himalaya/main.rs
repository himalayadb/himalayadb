use himalaya::node::metadata::{
    EtcdMetadataProvider, EtcdMetadataProviderConfig, MetadataProvider, NodeMetadata,
};
use himalaya::node::partitioner::{Murmur3, Partitioner};
use himalaya::node::topology::Topology;
use himalaya::node::Node;
use himalaya::proto::himalaya::himalaya_server::HimalayaServer as HimalayaGRPCServer;
use himalaya::proto::himalaya_internal::himalaya_internal_server::HimalayaInternalServer;
use himalaya::server::{HimalayaServer, InternalHimalayaServer};
use himalaya::storage::rocksbd::RocksDb as RocksClient;
use himalaya::storage::PersistentStore;
use himalaya::storage::PersistentStore::RocksDb;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::subscriber::set_global_default;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info".to_owned()));

    let formatting_layer = BunyanFormattingLayer::new("himalayadb".to_owned(), std::io::stdout);
    let subscriber = Registry::default()
        .with(env_filter)
        .with(JsonStorageLayer)
        .with(formatting_layer);
    LogTracer::init().expect("Failed to set log tracer");

    set_global_default(subscriber).expect("Failed to set subscriber");

    let addr = "[::1]:50051".parse()?;

    tracing::info!(%addr, "Starting server.");

    let provider = EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
        hosts: vec!["localhost:2379".to_owned()],
    })
    .await?;

    let node = Node::new(NodeMetadata {
        identifier: "test".to_owned(),
        token: 1,
        host: "127.0.0.1:50051".to_owned(),
    });

    provider.node_register(&node.metadata).await?;

    let nodes = provider.node_list_all().await?;

    let topology = Topology::new(
        nodes
            .into_iter()
            .map(|x| (x.identifier.clone(), Arc::new(Node::new(x))))
            .collect(),
        provider,
        Partitioner::Murmur3(Murmur3 {}),
    );

    let storage = Arc::new(PersistentStore::RocksDb(RocksClient::create("./test")?));
    let external_server = HimalayaServer::new(node, topology, storage.clone());
    let internal_server = InternalHimalayaServer::new(storage);

    Server::builder()
        .trace_fn(|headers| {
            let user_agent = headers
                .get("User-Agent")
                .map(|h| h.to_str().unwrap_or(""))
                .unwrap_or("");
            tracing::info_span!(
            "Request",
            user_agent = %user_agent,
            request_id = %Uuid::new_v4(),
            )
        })
        .add_service(HimalayaGRPCServer::new(external_server))
        .add_service(HimalayaInternalServer::new(internal_server))
        .serve(addr)
        .await?;

    Ok(())
}
