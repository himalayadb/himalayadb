use crate::node::metadata::MetadataProvider;
use crate::node::topology::Topology;
use crate::node::Node;
use crate::proto::himalaya::himalaya_server::Himalaya;
use crate::proto::himalaya::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse,
};
use crate::proto::himalaya_internal::himalaya_internal_client::HimalayaInternalClient;
use crate::proto::himalaya_internal::himalaya_internal_server::HimalayaInternal;
use crate::proto::himalaya_internal::{
    DeleteRequest as InternalDeleteRequest, DeleteResponse as InternalDeleteResponse,
    GetRequest as InternalGetRequest, GetResponse as InternalGetResponse,
    PutRequest as InternalPutRequest, PutResponse as InternalPutResponse,
};
use crate::storage::PersistentStore;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::field::debug;
use tracing::Span;

#[derive(Debug)]
pub struct Key(Vec<u8>);

impl Key {
    pub fn parse(v: Vec<u8>) -> Result<Key, String> {
        if v.len() > 0 {
            Span::current().record("key", &debug(&v));
            Ok(Self(v))
        } else {
            Err("empty key provided".to_owned())
        }
    }
}

impl AsRef<Vec<u8>> for Key {
    fn as_ref(&self) -> &Vec<u8> {
        &self.0
    }
}

pub struct HimalayaServer<MetaProvider> {
    topology: Topology<MetaProvider>,
    node: Node,
    storage: Arc<PersistentStore>,
}

impl<MetaProvider> HimalayaServer<MetaProvider> {
    pub fn new(
        node: Node,
        topology: Topology<MetaProvider>,
        storage: Arc<PersistentStore>,
    ) -> Self {
        Self {
            node,
            topology,
            storage,
        }
    }
}

#[tonic::async_trait]
impl<MetaProvider: MetadataProvider + Send + Sync + 'static> Himalaya
    for HimalayaServer<MetaProvider>
{
    #[tracing::instrument(
        name = "Get value",
        skip(self, request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let get = request.into_inner();
        let key = Key::parse(get.key).map_err(|e| Status::invalid_argument(e))?;
        match self.storage.get(&key.0) {
            Ok(Some(v)) => Ok(Response::new(GetResponse {
                key: key.0,
                value: v,
            })),
            Ok(None) => Ok(Response::new(GetResponse {
                key: key.0,
                value: vec![],
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    #[tracing::instrument(
        name = "Put value",
        skip(self, request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let put = request.into_inner();
        let key = Key::parse(put.key).map_err(|e| Status::invalid_argument(e))?;

        if let Some((coordinator, replicas)) =
            self.topology.find_coordinator_and_replicas(&key.0, 0)
        {
            if *coordinator == self.node {
                self.storage
                    .put(&key.0, &put.value)
                    .map_err(|e| Status::internal(e))?;

                for replica in replicas {
                    let mut client = HimalayaInternalClient::connect(replica.metadata.host.clone())
                        .await
                        .map_err(|e| Status::internal("failed to propagate request"))?;
                    client
                        .put(InternalPutRequest {
                            replicas: Vec::new(),
                            key: key.0.clone(),
                            value: put.value.clone(),
                        })
                        .await?;
                }
            } else {
                let mut client = HimalayaInternalClient::connect(coordinator.metadata.host.clone())
                    .await
                    .map_err(|e| Status::internal("failed to propagate request"))?;
                client
                    .put(InternalPutRequest {
                        replicas: replicas.iter().map(|n| n.metadata.host.clone()).collect(),
                        key: key.0,
                        value: put.value,
                    })
                    .await?;
            }
        }

        Ok(Response::new(PutResponse {}))
    }

    #[tracing::instrument(
        name = "Delete value",
        skip(self, request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let delete = request.into_inner();
        let _ = Key::parse(delete.key).map_err(|e| Status::invalid_argument(e))?;
        Ok(Response::new(DeleteResponse {}))
    }
}

pub struct InternalHimalayaServer {
    storage: Arc<PersistentStore>,
}

impl InternalHimalayaServer {
    pub fn new(storage: Arc<PersistentStore>) -> Self {
        Self { storage }
    }
}

#[tonic::async_trait]
impl HimalayaInternal for InternalHimalayaServer {
    async fn put(
        &self,
        request: Request<InternalPutRequest>,
    ) -> Result<Response<InternalPutResponse>, Status> {
        let put = request.into_inner();
        let key = Key::parse(put.key).map_err(|e| Status::invalid_argument(e))?;

        //Put in DB
        self.storage
            .put(&key.0, &put.value)
            .map_err(|e| Status::internal(e))?;

        // send to replicas
        for replica in put.replicas {
            let mut client = HimalayaInternalClient::connect(replica)
                .await
                .map_err(|e| Status::internal("failed to propagate request"))?;
            client
                .put(InternalPutRequest {
                    replicas: Vec::new(),
                    key: key.0.clone(),
                    value: put.value.clone(),
                })
                .await?;
        }
        Ok(Response::new(InternalPutResponse {}))
    }

    async fn get(
        &self,
        request: Request<InternalGetRequest>,
    ) -> Result<Response<InternalGetResponse>, Status> {
        unimplemented!()
    }

    async fn delete(
        &self,
        request: Request<InternalDeleteRequest>,
    ) -> Result<Response<InternalDeleteResponse>, Status> {
        unimplemented!()
    }
}
