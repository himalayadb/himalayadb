mod himalaya {
    tonic::include_proto!("himalaya");
}

pub mod himalaya_internal {
    tonic::include_proto!("himalaya.internal");
}

pub use himalaya::himalaya_server::HimalayaServer as HimalayaGRPCServer;

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
impl himalaya::himalaya_server::Himalaya for HimalayaServer {
    #[tracing::instrument(
        name = "Get value",
        skip(request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn get(
        &self,
        request: Request<himalaya::GetRequest>,
    ) -> Result<Response<himalaya::GetResponse>, Status> {
        let get = request.into_inner();
        let _ = Key::parse(get.key).map_err(|e| Status::invalid_argument(e))?;
        Ok(Response::new(himalaya::GetResponse {
            key: vec![0, 1, 2, 3],
            value: vec![0, 1, 2, 3],
        }))
    }

    #[tracing::instrument(
        name = "Put value",
        skip(request),
        fields(
            key = tracing::field::Empty
        )
    )]
    async fn put(
        &self,
        request: Request<himalaya::PutRequest>,
    ) -> Result<Response<himalaya::PutResponse>, Status> {
        let put = request.into_inner();
        let _ = Key::parse(put.key).map_err(|e| Status::invalid_argument(e))?;

        Ok(Response::new(himalaya::PutResponse {}))
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
        request: Request<himalaya::DeleteRequest>,
    ) -> Result<Response<himalaya::DeleteResponse>, Status> {
        let delete = request.into_inner();
        let _ = Key::parse(delete.key).map_err(|e| Status::invalid_argument(e))?;
        Ok(Response::new(himalaya::DeleteResponse {}))
    }
}
