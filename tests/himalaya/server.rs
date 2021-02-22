use crate::helpers;
use bytes::Bytes;
use claim::assert_ok;
use himalaya::proto::himalaya::himalaya_client::HimalayaClient;
use himalaya::proto::himalaya::{DeleteRequest, GetRequest, PutRequest};

#[tokio::test]
async fn put_key() {
    let srv = helpers::server(1, "test", 1, 0)
        .await
        .expect("Failed to create db");

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
    let srv = helpers::server(1, "test", 1, 0)
        .await
        .expect("Failed to create db");
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
