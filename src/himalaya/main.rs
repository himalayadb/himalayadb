use futures_util::future::FutureExt;
use himalaya::configuration::get_configuration;
use himalaya::node::metadata::{EtcdMetadataProvider, EtcdMetadataProviderConfig};
use himalaya::server::server::Server;
use tracing::subscriber::set_global_default;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};

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

    let configuration = get_configuration().expect("Failed to parse configuration");

    let etcd_config = EtcdMetadataProviderConfig {
        hosts: configuration.etcd.hosts(),
        prefix: configuration.etcd.prefix.clone(),
        lease_ttl: configuration.etcd.lease_ttl,
        ttl_refresh_interval: configuration.etcd.ttl_refresh_interval,
    };

    tracing::info!(
        hosts = &debug(&etcd_config.hosts),
        "Configuring etcd connection."
    );
    let provider = EtcdMetadataProvider::new(etcd_config).await?;

    let server = Server::build(configuration, provider).await?;
    let signal = tokio::signal::ctrl_c().map(drop);
    server.run(signal).await?;

    Ok(())
}
