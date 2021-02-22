use crate::configuration::Settings;
use crate::coordinator::Coordinator;
use crate::node::metadata::{MetadataProvider, NodeMetadata};
use crate::node::partitioner::{Murmur3, Partitioner};
use crate::node::topology::Topology;
use crate::node::Node;
use crate::proto::himalaya::himalaya_server::HimalayaServer as HimalayaGRPCServer;
use crate::proto::himalaya_internal::himalaya_internal_server::HimalayaInternalServer;
use crate::server::external::HimalayaServer;
use crate::server::internal::InternalHimalayaServer;
use crate::storage::rocksbd::RocksClient;
use crate::storage::PersistentStore;
use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tonic::transport::{Error, Server as TonicServer};
use uuid::Uuid;

pub struct Server<MetaProvider> {
    port: u16,
    listener: TcpListener,
    coordinator: Arc<Coordinator<MetaProvider>>,
    storage: Arc<PersistentStore>,
    tx: Sender<()>,
}

impl<MetaProvider: MetadataProvider + Send + Sync + 'static> Server<MetaProvider> {
    pub async fn build(
        configuration: Settings,
        provider: MetaProvider,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        <MetaProvider as MetadataProvider>::MetaWatcher: Send,
    {
        let addr = format!("{}:{}", configuration.bind_address, configuration.bind_port);
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let port = local_addr.port();

        tracing::info!(addr = %local_addr, "Starting server.");
        tracing::info!(%configuration.replicas, "Setting replica count.");

        let node = Node::new(NodeMetadata {
            identifier: configuration.identifier,
            token: configuration.token,
            host: local_addr.to_string(),
        });

        provider.node_register(&node.metadata).await?;

        let nodes = provider
            .node_list_all()
            .await?
            .into_iter()
            .map(|x| (x.identifier.clone(), Arc::new(Node::new(x))))
            .collect();

        let topology = Arc::new(Topology::new(
            nodes,
            provider,
            Partitioner::Murmur3(Murmur3),
        ));

        let (tx, rx) = oneshot::channel::<()>();
        let t = topology.clone();
        tokio::spawn(async move {
            tracing::info!("Monitoring topology.");
            if let Err(e) = t.start(rx).await {
                tracing::error!(error = %e, "stopped monitoring topology");
            };
        });

        let coordinator = Arc::new(Coordinator::new(
            vec![node],
            topology,
            configuration.replicas,
            configuration.consistency,
        ));
        let storage = Arc::new(PersistentStore::RocksDb(RocksClient::create(
            configuration.rocks.path,
        )?));

        Ok(Server {
            port,
            listener,
            coordinator,
            storage,
            tx,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn run<F: Future<Output = ()>>(self, shutdown: F) -> Result<(), Error> {
        let external_server = HimalayaServer::new(self.coordinator.clone(), self.storage.clone());
        let internal_server =
            InternalHimalayaServer::new(self.coordinator.clone(), self.storage.clone());

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(self.listener);

        let res = TonicServer::builder()
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
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await;

        let _ = self.tx.send(());
        res?;

        Ok(())
    }
}
