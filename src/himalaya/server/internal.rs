use crate::coordinator::Coordinator;
use crate::node::metadata::MetadataProvider;
use crate::proto::himalaya_internal::himalaya_internal_server::HimalayaInternal;
use crate::proto::himalaya_internal::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse,
};
use crate::storage::PersistentStore;
use crate::Key;
use bytes::Bytes;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use chrono::Utc;

pub struct InternalHimalayaServer<MetaProvider> {
    coordinator: Arc<Coordinator<MetaProvider>>,
    storage: Arc<PersistentStore>,
}

impl<MetaProvider> InternalHimalayaServer<MetaProvider> {
    pub fn new(coordinator: Arc<Coordinator<MetaProvider>>, storage: Arc<PersistentStore>) -> Self {
        Self {
            coordinator,
            storage,
        }
    }
}

#[tonic::async_trait]
impl<MetaProvider: MetadataProvider + Send + Sync + 'static> HimalayaInternal
    for InternalHimalayaServer<MetaProvider>
{
    #[tracing::instrument(
        name = "Internal put value",
        skip(self, request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let put = request.into_inner();
        let key = Key::parse(put.key).map_err(|e| {
            tracing::error!(error = %e, "invalid key");
            Status::invalid_argument(e)
        })?;

        self.storage.put(&key, &put.value, Utc::now().timestamp_millis()).map_err(|e| {
            tracing::error!(error = %e, "failed to store value");
            Status::internal(e)
        })?;

        if !put.replicas.is_empty() {
            self.coordinator
                .replicate_data(&key.as_ref(), &put.value, &put.replicas)
                .await
                .map_err(|e| {
                    tracing::error!(error = %e, "replication failed");
                    Status::internal("replication failed")
                })?;
        }

        Ok(Response::new(PutResponse {}))
    }

    #[tracing::instrument(
        name = "Internal get value",
        skip(self, request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let get = request.into_inner();
        let key = Key::parse(get.key).map_err(|e| {
            tracing::error!(error = %e, "invalid key");
            Status::invalid_argument(e)
        })?;

        match self.storage.get(&key) {
            Ok(Some(value)) => Ok(Response::new(GetResponse { value })),
            Ok(None) => Ok(Response::new(GetResponse {
                value: Bytes::new(),
            })),
            Err(e) => {
                tracing::error!(error = %e, "failed to get key");
                Err(Status::internal(e))
            }
        }
    }

    #[tracing::instrument(
        name = "Internal delete value",
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
        let key = Key::parse(delete.key).map_err(|e| {
            tracing::error!(error = %e, "invalid key");
            Status::invalid_argument(e)
        })?;

        match self.storage.delete(&key) {
            Ok(()) => Ok(Response::new(DeleteResponse {})),
            Err(e) => {
                tracing::error!(error = %e, "failed to delete key");
                Err(Status::internal(e))
            }
        }
    }
}
