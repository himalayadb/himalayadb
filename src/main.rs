use crate::server::{HimalayaGrpcServer, HimalayaServer};
use tonic::transport::Server;
use tracing::subscriber::set_global_default;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};
use uuid::Uuid;

mod server;

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
    let server = HimalayaServer::default();

    tracing::info!(%addr, "Starting server.");

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
        .add_service(HimalayaGrpcServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
