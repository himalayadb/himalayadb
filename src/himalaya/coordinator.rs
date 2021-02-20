use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;
use tonic::transport::{Channel, Uri};

use tower::timeout::Timeout;
use tracing::field::debug;
use tracing::Span;

use crate::node::metadata::MetadataProvider;
use crate::node::topology::Topology;
use crate::node::Node;
use crate::proto::himalaya_internal::himalaya_internal_client::HimalayaInternalClient;
use crate::proto::himalaya_internal::{DeleteRequest, GetRequest, PutRequest};
use crate::storage::PersistentStore;

use chrono::Utc;

use bytes::Bytes;
use std::str::FromStr;
use tonic::codegen::http::uri::Authority;

pub struct Coordinator<MetaProvider> {
    nodes: Vec<Node>,
    topology: Arc<Topology<MetaProvider>>,
    num_replicas: usize,
    consistency: usize,
}

impl<MetaProvider: MetadataProvider> Coordinator<MetaProvider> {
    pub fn new(
        nodes: Vec<Node>,
        topology: Arc<Topology<MetaProvider>>,
        num_replicas: usize,
        consistency: usize,
    ) -> Self {
        Self {
            nodes,
            topology,
            num_replicas,
            consistency,
        }
    }

    #[tracing::instrument(
        name = "Get value with coordinator",
        skip(self, key, storage),
        fields(
            coordinator = tracing::field::Empty,
            replicas = tracing::field::Empty
        )
    )]
    pub async fn get(
        &self,
        key: &Bytes,
        storage: &PersistentStore,
    ) -> Result<Option<Bytes>, Box<dyn std::error::Error>> {
        if let Some((coordinator, mut replicas)) = self
            .topology
            .find_coordinator_and_replicas(key.as_ref(), self.num_replicas)
        {
            Span::current().record("coordinator", &debug(&coordinator));
            Span::current().record("replicas", &debug(&replicas));

            return if self.nodes.contains(&coordinator) {
                tracing::info!("coordinator processing get request");
                match storage.get(&key) {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        tracing::error!(error = %e, "failed to get from coordinator storage, attempting to fetch from replicas");
                        let replicas = replicas.iter().map(|r| r.metadata.host.clone()).collect();
                        self.get_from_replicas(&key, &replicas).await
                    }
                }
            } else {
                tracing::info!("forwarding get to replicas");
                replicas.push(coordinator);
                let replicas = replicas.iter().map(|r| r.metadata.host.clone()).collect();
                self.get_from_replicas(&key, &replicas).await
            };
        }

        Ok(None)
    }

    #[tracing::instrument(
        name = "Put value with coordinator",
        skip(self, key, value, storage),
        fields(
            coordinator = tracing::field::Empty,
            replicas = tracing::field::Empty
        )
    )]
    pub async fn put(
        &self,
        key: Bytes,
        value: Bytes,
        ts: i64,
        storage: &PersistentStore,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some((coordinator, replicas)) = self
            .topology
            .find_coordinator_and_replicas(key.as_ref(), self.num_replicas)
        {
            Span::current().record("coordinator", &debug(&coordinator));
            Span::current().record("replicas", &debug(&replicas));

            return if self.nodes.contains(&coordinator) {
                tracing::info!("coordinator processing put and replicating data");
                storage.put(&key, &value, ts)?;
                let replicas = replicas.iter().map(|r| r.metadata.host.clone()).collect();
                self.replicate_data(&key, &value, &replicas).await
            } else {
                tracing::info!("forwarding put request to coordinator");
                let mut client = HimalayaInternalClient::connect(format!(
                    "http://{}",
                    coordinator.metadata.host
                ))
                .await?;
                client
                    .put(PutRequest {
                        key,
                        value,
                        replicas: replicas.iter().map(|r| r.metadata.host.clone()).collect(),
                    })
                    .await?;
                Ok(())
            };
        } else {
            Ok(())
        }
    }

    #[tracing::instrument(
        name = "Delete value with coordinator",
        skip(self, key, storage),
        fields(
            coordinator = tracing::field::Empty,
            replicas = tracing::field::Empty
        )
    )]
    pub async fn delete(
        &self,
        key: &Bytes,
        storage: &PersistentStore,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some((coordinator, mut replicas)) = self
            .topology
            .find_coordinator_and_replicas(key.as_ref(), self.num_replicas)
        {
            Span::current().record("coordinator", &debug(&coordinator));
            Span::current().record("replicas", &debug(&replicas));

            return if self.nodes.contains(&coordinator) {
                tracing::info!("coordinator processing and replicating deletion");
                match storage.delete(&key) {
                    Ok(_) => self.delete_from_replicas(&key, &replicas).await,
                    Err(e) => Err(Box::from(e)),
                }
            } else {
                tracing::info!("forwarding deletion to replicas");
                replicas.push(coordinator);
                self.delete_from_replicas(&key, &replicas).await
            };
        }

        Ok(())
    }

    #[tracing::instrument(name = "Replicating data to node", skip(value), err)]
    async fn replicate_to_node(
        host: String,
        key: Bytes,
        value: Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let uri = Uri::builder()
            .scheme("http")
            .authority(Authority::from_str(&host)?)
            .path_and_query("")
            .build()?;
        let channel = Channel::builder(uri).connect().await?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(100));
        let mut client = HimalayaInternalClient::new(timeout_channel);

        client
            .put(PutRequest {
                replicas: Vec::new(),
                key,
                value,
            })
            .await?;

        Ok(())
    }

    #[tracing::instrument(name = "Replicating data", skip(self, value))]
    pub async fn replicate_data(
        &self,
        key: &Bytes,
        value: &Bytes,
        replicas: &Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if replicas.is_empty() {
            return Ok(());
        }

        let (tx, rx) = mpsc::channel(replicas.len());
        for replica in replicas {
            let k = key.clone();
            let v = value.clone();
            let tx = tx.clone();
            let host = replica.clone();

            tokio::spawn(async move {
                let result = Coordinator::<MetaProvider>::replicate_to_node(host, k, v).await;
                tx.send(result).await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            });
        }

        ConsistencyChecker::new(self.consistency, replicas.len(), rx).await?;
        Ok(())
    }

    #[tracing::instrument(name = "Deleting data from replicas", skip(self))]
    async fn delete_from_replicas(
        &self,
        key: &Bytes,
        replicas: &Vec<Arc<Node>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for ref replica in replicas {
            let key = key.clone();
            let host = replica.metadata.host.clone();

            let mut client = HimalayaInternalClient::connect(format!("http://{}", host)).await?;
            client.delete(DeleteRequest { key }).await?;
        }

        Ok(())
    }

    #[tracing::instrument(name = "Fetching value from replicas", skip(self))]
    async fn get_from_replicas(
        &self,
        key: &Bytes,
        replicas: &Vec<String>,
    ) -> Result<Option<Bytes>, Box<dyn std::error::Error>> {
        let (tx, mut rx) = mpsc::channel(1);

        for replica in replicas {
            let key = key.clone();
            let host = replica.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut client =
                    HimalayaInternalClient::connect(format!("http://{}", host)).await?;
                let response = client.get(GetRequest { key }).await?.into_inner();
                tx.send(response.value).await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            });
        }

        match timeout(Duration::from_millis(10000), rx.recv()).await {
            Ok(Some(v)) => {
                rx.close();
                Ok(Some(v))
            }
            Ok(None) => Err(Box::from("all replicas failed to get")),
            Err(_) => Err(Box::from("timeout occurred")),
        }
    }
}

struct ConsistencyChecker {
    replicated: usize,
    failed: usize,
    total: usize,
    consistency: usize,
    rx: mpsc::Receiver<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
}

impl ConsistencyChecker {
    fn new(
        consistency: usize,
        total: usize,
        rx: mpsc::Receiver<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    ) -> Self {
        Self {
            replicated: 0,
            failed: 0,
            total,
            consistency,
            rx,
        }
    }
}

impl Future for ConsistencyChecker {
    type Output = Result<(), Box<dyn std::error::Error>>;

    #[tracing::instrument(name = "Consistency Check", skip(self, cx))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(res) => {
                    match res {
                        Some(Ok(_)) => self.replicated += 1,
                        Some(Err(_e)) => self.failed += 1,
                        None => return Poll::Ready(Ok(())),
                    };

                    if self.replicated == self.consistency {
                        self.rx.close();
                        return Poll::Ready(Ok(()));
                    }

                    if self.replicated + self.failed == self.total {
                        self.rx.close();
                        return Poll::Ready(Err(Box::from("replication failed")));
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
