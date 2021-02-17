use clap::{load_yaml, App, AppSettings};
use himalaya::coordinator::Coordinator;
use himalaya::external_server::HimalayaServer;
use himalaya::internal_server::InternalHimalayaServer;
use himalaya::node::metadata::{
    EtcdMetadataProvider, EtcdMetadataProviderConfig, MetadataProvider, NodeMetadata,
};
use himalaya::node::partitioner::{Murmur3, Partitioner};
use himalaya::node::topology::Topology;
use himalaya::node::Node;
use himalaya::proto::himalaya::himalaya_server::HimalayaServer as HimalayaGRPCServer;
use himalaya::proto::himalaya_internal::himalaya_internal_server::HimalayaInternalServer;
use himalaya::storage::rocksbd::RocksDb as RocksClient;
use himalaya::storage::PersistentStore;
use std::sync::Arc;
use tokio::sync::oneshot;
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

    // parse given arguments
    let yaml = load_yaml!("options.yaml");
    let matches = App::from(yaml)
        .setting(AppSettings::AllowNegativeNumbers)
        .get_matches();

    let mut bind_port = 50051;
    if let Some(p) = matches.value_of("port") {
        bind_port = p.parse::<u32>().unwrap();
    }

    let mut replicas = 0;
    if let Some(r) = matches.value_of("replicas") {
        replicas = r.parse::<usize>().unwrap();
    }

    let mut etcd_host = "localhost";
    if let Some(e) = matches.value_of("etcd_host") {
        etcd_host = e
    }

    let mut etcd_port = 2379;
    if let Some(e) = matches.value_of("etcd_port") {
        etcd_port = e.parse::<u32>().unwrap();
    }

    let token = matches
        .value_of("token")
        .expect("You must provide an initial token.")
        .parse::<i64>()
        .unwrap();
    let identifier = matches
        .value_of("identifier")
        .expect("You must provide an identifier for this node.");
    let rocksdb_path = matches
        .value_of("rocksdb_path")
        .expect("You must provide a rocksdb path.");

    let addr = format!("[::1]:{}", bind_port);

    tracing::info!(%addr, "Starting server.");
    tracing::info!(%etcd_host, %etcd_port, "Configuring etcd connection.");
    tracing::info!(%replicas, "Setting replica count.");

    let provider = EtcdMetadataProvider::new(EtcdMetadataProviderConfig {
        hosts: vec![format!("{}:{}", etcd_host, etcd_port)],
    })
    .await?;

    let node = Node::new(NodeMetadata {
        identifier: identifier.to_string(),
        token,
        host: addr.to_string(),
    });

    provider.node_register(&node.metadata).await?;

    let nodes = provider.node_list_all().await?;

    let topology = Arc::new(Topology::new(
        nodes
            .into_iter()
            .map(|x| (x.identifier.clone(), Arc::new(Node::new(x))))
            .collect(),
        provider,
        Partitioner::Murmur3(Murmur3 {}),
    ));

    let (_tx, rx) = oneshot::channel::<()>();
    let t = topology.clone();
    tokio::spawn(async move {
        tracing::info!("Monitoring topology.");
        if let Err(e) = t.start(rx).await {
            tracing::error!(error = %e, "stopped monitoring topology");
        };
    });

    let coordinator = Arc::new(Coordinator::new(vec![node], topology, replicas));
    let storage = Arc::new(PersistentStore::RocksDb(RocksClient::create(rocksdb_path)?));
    let external_server = HimalayaServer::new(coordinator.clone(), storage.clone());
    let internal_server = InternalHimalayaServer::new(coordinator.clone(), storage.clone());

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
        .serve(addr.parse()?)
        .await?;

    Ok(())
}
