use crate::coordinator::Coordinator;
use crate::node::metadata::MetadataProvider;
use crate::proto::himalaya::himalaya_server::Himalaya;
use crate::proto::himalaya::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse,
};

use crate::storage::PersistentStore;
use crate::Key;
use bytes::Bytes;
use chrono::Utc;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct HimalayaServer<MetaProvider> {
    coordinator: Arc<Coordinator<MetaProvider>>,
    storage: Arc<PersistentStore>,
}

impl<MetaProvider> HimalayaServer<MetaProvider> {
    pub fn new(coordinator: Arc<Coordinator<MetaProvider>>, storage: Arc<PersistentStore>) -> Self {
        Self {
            coordinator,
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
        let key = Key::parse(get.key).map_err(|e| {
            tracing::error!(error = %e, "invalid key");
            Status::invalid_argument(e)
        })?;

        match self.coordinator.get(key.as_ref(), &self.storage).await {
            Ok(Some(v)) => Ok(Response::new(GetResponse {
                key: key.0,
                value: v,
            })),
            Ok(None) => Ok(Response::new(GetResponse {
                key: key.0,
                value: Bytes::new(),
            })),
            Err(e) => {
                tracing::error!(error = %e, "get failed");
                Err(Status::internal(format!("{:?}", e)))
            }
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
        let key = Key::parse(put.key).map_err(|e| {
            tracing::error!(error = %e, "invalid key");
            Status::invalid_argument(e)
        })?;

        match self
            .coordinator
            .put(
                key.0,
                put.value,
                Utc::now().timestamp_millis(),
                &self.storage,
            )
            .await
        {
            Ok(_) => Ok(Response::new(PutResponse {})),
            Err(e) => {
                tracing::error!(error = %e, "put failed");
                Err(Status::internal(format!("{:?}", e)))
            }
        }
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
        let key = Key::parse(delete.key).map_err(|e| {
            tracing::error!(error = %e, "invalid key");
            Status::invalid_argument(e)
        })?;

        match self.coordinator.delete(key.as_ref(), &self.storage).await {
            Ok(_) => Ok(Response::new(DeleteResponse {})),
            Err(e) => {
                tracing::error!(error = %e, "delete failed");
                Err(Status::internal(format!("{:?}", e)))
            }
        }
    }
}
