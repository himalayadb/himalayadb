use futures_util::future::FutureExt;
use himalaya::configuration::get_configuration;
use himalaya::node::metadata::{EtcdMetadataProvider, EtcdMetadataProviderConfig};
use himalaya::server::server::Server;
use himalaya::telemetry::{get_subscriber, init_subscriber};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = get_subscriber("himalayadb", "info");
    init_subscriber(subscriber);

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
