mod himalaya {
    tonic::include_proto!("himalaya");
}

use himalaya::himalaya_worker_server::HimalayaWorker;
use himalaya::{
    WorkerDeleteRequest, WorkerDeleteResponse, WorkerGetRequest, WorkerGetResponse,
    WorkerPutRequest, WorkerPutResponse,
};

#[derive(Debug, Default)]
pub struct HimalayaWorkerServer {}

#[tonic::async_trait]
impl HimalayaWorker for HimalayaWorkerServer {
    async fn put(
        &self,
        _request: tonic::Request<WorkerPutRequest>,
    ) -> Result<tonic::Response<WorkerPutResponse>, tonic::Status> {
        return Ok(tonic::Response::new(WorkerPutResponse {}));
    }

    async fn get(
        &self,
        _request: tonic::Request<WorkerGetRequest>,
    ) -> Result<tonic::Response<WorkerGetResponse>, tonic::Status> {
        return Ok(tonic::Response::new(WorkerGetResponse { value: vec![0] }));
    }

    async fn delete(
        &self,
        _request: tonic::Request<WorkerDeleteRequest>,
    ) -> Result<tonic::Response<WorkerDeleteResponse>, tonic::Status> {
        return Ok(tonic::Response::new(WorkerDeleteResponse {}));
    }
}
