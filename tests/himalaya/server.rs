use crate::helpers;
use bytes::Bytes;
use claim::assert_ok;
use fake::{Fake, Faker};
use himalaya::proto::himalaya::himalaya_client::HimalayaClient;
use himalaya::proto::himalaya::{DeleteRequest, GetRequest, PutRequest};
use uuid::Uuid;

#[tokio::test]
async fn put_key() {
    let srv = helpers::server(1, "test", 1, 0, None)
        .await
        .expect("Failed to create db");

    let mut client = HimalayaClient::connect(format!("http://{}:{}", srv.address, srv.port))
        .await
        .expect("Failed to create client");

    let key = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());
    let value = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());

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
    let srv = helpers::server(1, "test", 1, 0, None)
        .await
        .expect("Failed to create db");
    let mut client = HimalayaClient::connect(format!("http://{}:{}", srv.address, srv.port))
        .await
        .expect("Failed to create client");

    let key = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());
    let value = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());

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

#[tokio::test]
async fn replicate_keys() {
    let prefix = Uuid::new_v4().to_string();
    let srv1 = helpers::server(-3074457345618258603, "node_1", 2, 2, Some(prefix.clone()))
        .await
        .expect("Failed to create db");

    let srv2 = helpers::server(-9223372036854775808, "node_2", 2, 2, Some(prefix.clone()))
        .await
        .expect("Failed to create db");

    let srv3 = helpers::server(3074457345618258602, "node_3", 2, 2, Some(prefix.clone()))
        .await
        .expect("Failed to create db");

    let hosts = vec![
        format!("http://{}:{}", srv1.address, srv1.port),
        format!("http://{}:{}", srv2.address, srv2.port),
        format!("http://{}:{}", srv3.address, srv3.port),
    ];

    let kvs = vec![
        (
            Bytes::from(Faker.fake::<String>()),
            Bytes::from(Faker.fake::<String>()),
        ),
        (
            Bytes::from(Faker.fake::<String>()),
            Bytes::from(Faker.fake::<String>()),
        ),
        (
            Bytes::from(Faker.fake::<String>()),
            Bytes::from(Faker.fake::<String>()),
        ),
    ];

    for (ix, h) in hosts.iter().cloned().enumerate() {
        let mut client = HimalayaClient::connect(h)
            .await
            .expect("Failed to create client");

        let (key, value) = &kvs[ix];

        let put_request = tonic::Request::new(PutRequest {
            key: key.clone(),
            value: value.clone(),
        });

        assert_ok!(client.put(put_request).await);
    }

    for h in hosts.iter().cloned() {
        let mut client = HimalayaClient::connect(h)
            .await
            .expect("Failed to create client");

        for (key, value) in &kvs {
            let get_request = tonic::Request::new(GetRequest { key: key.clone() });

            let response = client.get(get_request).await;
            assert_ok!(&response);
            let get = response.unwrap().into_inner();
            assert_eq!(value, &get.value);
            assert_eq!(key, &get.key);
        }
    }
}
