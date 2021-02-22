use crate::node::metadata::etcd::etcd_provider;
use bytes::Bytes;
use claim::{assert_ok, assert_some};
use futures_util::FutureExt;
use himalaya::configuration::{EtcdSettings, RocksDbSettings, Settings};
use himalaya::node::metadata::NodeMetadata;
use himalaya::proto::himalaya::himalaya_client::HimalayaClient;
use himalaya::proto::himalaya::{DeleteRequest, GetRequest, PutRequest};
use himalaya::server::server::Server;
use tempfile::tempdir;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

struct TestServer {
    pub port: u16,
    pub address: String,
    pub shutdown: Sender<()>,
    rocks_path: String,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.rocks_path.clone());
    }
}

async fn server<S>(
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

async fn server_with_settings(
    configuration: Settings,
) -> Result<TestServer, Box<dyn std::error::Error>> {
    let (provider, _) = etcd_provider()
        .await
        .expect("Failed to create etcd provider.");

    let rocks_path = configuration.rocks.path.clone();
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
        rocks_path,
    })
}

#[tokio::test]
async fn put_key() {
    let srv = server(1, "test", 1, 0).await.expect("Failed to create db");

    let mut client = HimalayaClient::connect(format!("http://{}:{}", srv.address, srv.port))
        .await
        .expect("Failed to create client");

    let key = Bytes::copy_from_slice("test".as_bytes());
    let value = Bytes::copy_from_slice("value".as_bytes());

    let put_request = tonic::Request::new(PutRequest {
        key: key.clone(),
        value: value.clone(),
    });

    assert_ok!(client.put(put_request).await);

    let get_request = tonic::Request::new(GetRequest { key: key.clone() });

    let response = client.get(get_request).await;
    assert_ok!(&response);
    let get = response.unwrap().into_inner();
    assert_eq!(value, get.value);
    assert_eq!(key, get.key);
}

#[tokio::test]
async fn delete_key() {
    let srv = server(1, "test", 1, 0).await.expect("Failed to create db");
    let mut client = HimalayaClient::connect(format!("http://{}:{}", srv.address, srv.port))
        .await
        .expect("Failed to create client");

    let key = Bytes::copy_from_slice("test".as_bytes());
    let value = Bytes::copy_from_slice("value".as_bytes());

    let put_request = tonic::Request::new(PutRequest {
        key: key.clone(),
        value: value.clone(),
    });
    assert_ok!(client.put(put_request).await);

    let delete_request = tonic::Request::new(DeleteRequest { key: key.clone() });
    assert_ok!(client.delete(delete_request).await);

    let get_request = tonic::Request::new(GetRequest { key: key.clone() });
    let response = client.get(get_request).await;
    assert_ok!(&response);
    let get = response.unwrap().into_inner();
    assert_eq!(0, get.value.len());
    assert_eq!(key, get.key);
}
