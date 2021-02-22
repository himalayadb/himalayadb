use claim::{assert_ok, assert_some};
use futures_util::future::FutureExt;
use himalaya::configuration::{EtcdSettings, RocksDbSettings, Settings};
use himalaya::node::metadata::{EtcdMetadataProvider, EtcdMetadataProviderConfig, NodeMetadata};
use himalaya::server::server::Server;
use tempfile::tempdir;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

pub struct TestServer {
    pub port: u16,
    pub address: String,
    pub shutdown: Sender<()>,
}

pub async fn server<S>(
    token: i64,
    identifier: S,
    consistency: usize,
    replicas: usize,
) -> Result<TestServer, Box<dyn std::error::Error>>
where
    S: Into<String>,
{
    let dir = tempdir()?;
    let path = dir.path().to_str();
    assert_some!(path);

    let configuration = Settings {
        bind_address: "127.0.0.1".to_owned(),
        bind_port: 0,
        consistency,
        replicas,
        metadata: NodeMetadata {
            identifier: identifier.into(),
            token,
            host: "127.0.0.1".to_owned(),
        },
        rocks: RocksDbSettings {
            path: path.unwrap().to_owned(),
        },
        etcd: EtcdSettings {
            host: "".to_owned(),
            port: 0,
            prefix: "".to_owned(),
            lease_ttl: 5,
            ttl_refresh_interval: 3000,
        },
    };

    server_with_settings(configuration).await
}

pub async fn server_with_settings(
    configuration: Settings,
) -> Result<TestServer, Box<dyn std::error::Error>> {
    let (provider, _) = etcd_provider(None)
        .await
        .expect("Failed to create etcd provider.");

    let address = configuration.bind_address.clone();

    let srv = Server::build(configuration, provider).await?;
    let port = srv.port();
    let (shutdown, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        assert_ok!(srv.run(rx.map(drop)).await);
    });

    Ok(TestServer {
        address,
        port,
        shutdown,
    })
}

pub async fn etcd_provider(
    prefix: Option<String>,
) -> Result<(EtcdMetadataProvider, String), Box<dyn std::error::Error>> {
    let prefix = match prefix {
        Some(p) => p,
        None => Uuid::new_v4().to_string(),
    };

    let etcd_config = EtcdMetadataProviderConfig {
        hosts: vec!["localhost:2379".to_owned()],
        prefix: prefix.clone(),
        lease_ttl: 5,
        ttl_refresh_interval: 3000,
    };
    let provider = etcd_provider_with_settings(etcd_config).await?;
    Ok((provider, prefix))
}

pub async fn etcd_provider_with_settings(
    configuration: EtcdMetadataProviderConfig,
) -> Result<EtcdMetadataProvider, Box<dyn std::error::Error>> {
    let provider = EtcdMetadataProvider::new(configuration).await?;
    Ok(provider)
}
