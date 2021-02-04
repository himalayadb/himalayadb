mod himalaya {
    tonic::include_proto!("himalaya");
}

use himalaya::himalaya_server::Himalaya;
pub use himalaya::himalaya_server::HimalayaServer as HimalayaGrpcServer;
use himalaya::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse};
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

#[derive(Debug, Default)]
pub struct HimalayaServer {}

#[tonic::async_trait]
impl Himalaya for HimalayaServer {
    #[tracing::instrument(
        name = "Put value",
        skip(request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let put = request.into_inner();
        let entry = put
            .entry
            .ok_or_else(|| Status::invalid_argument("entry was not provided"))?;
        let _ = Key::parse(entry.key).map_err(|e| Status::invalid_argument(e))?;

        Ok(Response::new(himalaya::PutResponse {}))
    }

    #[tracing::instrument(
        name = "Get value",
        skip(request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let get = request.into_inner();
        let _ = Key::parse(get.key).map_err(|e| Status::invalid_argument(e))?;
        Ok(Response::new(himalaya::GetResponse { entry: None }))
    }

    #[tracing::instrument(
        name = "Delete value",
        skip(request),
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
        Ok(Response::new(himalaya::DeleteResponse {}))
    }
}
