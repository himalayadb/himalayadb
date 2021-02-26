use crate::benchmarks::helpers;
use crate::benchmarks::helpers::TestServer;
use bytes::Bytes;
use criterion::{black_box, criterion_group, BatchSize, Criterion};
use fake::{Fake, Faker};
use himalaya::proto::himalaya::himalaya_client::HimalayaClient;
use himalaya::proto::himalaya::{GetRequest, PutRequest};
use tokio::runtime::Runtime;
use uuid::Uuid;

fn rt() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .unwrap()
}

async fn setup_cluster() -> (TestServer, TestServer, TestServer) {
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

    (srv1, srv2, srv3)
}

pub fn get(c: &mut Criterion) {
    let key = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());
    let value = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());

    let rt = rt();

    let srv = rt
        .block_on(helpers::server(1, "test", 1, 0, None))
        .expect("Failed to create db");

    let addr = format!("http://{}:{}", srv.address, srv.port);
    rt.block_on(async {
        let mut client = HimalayaClient::connect(addr.clone())
            .await
            .expect("Failed to create client");

        let put_request = tonic::Request::new(PutRequest {
            key: key.clone(),
            value: value.clone(),
        });

        client.put(put_request).await
    })
    .expect("Failed to put value.");

    c.bench_function("Himalaya Get", move |b| {
        b.to_async(&rt).iter_batched(
            || (addr.clone(), key.clone()),
            |(addr, key)| async move {
                let mut client = HimalayaClient::connect(addr)
                    .await
                    .expect("Failed to create client");
                let get_request = tonic::Request::new(GetRequest { key: key.clone() });

                let response = client.get(get_request).await.expect("Failed to send get");
                black_box(response)
            },
            BatchSize::SmallInput,
        );
    });
}

pub fn replicate_get(c: &mut Criterion) {
    let key = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());
    let value = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());

    let rt = rt();

    let (s1, _s2, _s3) = rt.block_on(setup_cluster());
    let addr = format!("http://{}:{}", s1.address, s1.port);

    rt.block_on(async {
        let mut client = HimalayaClient::connect(addr.clone())
            .await
            .expect("Failed to create client");

        let put_request = tonic::Request::new(PutRequest {
            key: key.clone(),
            value: value.clone(),
        });

        client.put(put_request).await
    })
    .expect("Failed to put value.");

    c.bench_function("Himalaya Replicate Get", move |b| {
        b.to_async(&rt).iter_batched(
            || (addr.clone(), key.clone()),
            |(addr, key)| async move {
                let mut client = HimalayaClient::connect(addr)
                    .await
                    .expect("Failed to create client");
                let get_request = tonic::Request::new(GetRequest { key: key.clone() });

                let response = client.get(get_request).await.expect("Failed to send get");
                black_box(response)
            },
            BatchSize::PerIteration,
        );
    });
}

pub fn put(c: &mut Criterion) {
    let key = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());
    let value = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());

    let rt = rt();

    let srv = rt
        .block_on(helpers::server(1, "test", 1, 0, None))
        .expect("Failed to create db");

    let addr = format!("http://{}:{}", srv.address, srv.port);

    c.bench_function("Himalaya Put", move |b| {
        b.to_async(&rt).iter_batched(
            || (addr.clone(), key.clone(), value.clone()),
            |(addr, key, value)| async move {
                let mut client = HimalayaClient::connect(addr)
                    .await
                    .expect("Failed to create client");
                let put_request = tonic::Request::new(PutRequest {
                    key: key.clone(),
                    value: value.clone(),
                });

                let response = client.put(put_request).await.expect("Failed to send put");
                black_box(response)
            },
            BatchSize::SmallInput,
        );
    });
}

pub fn replicate_put(c: &mut Criterion) {
    let key = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());
    let value = Bytes::copy_from_slice(Faker.fake::<String>().as_bytes());

    let rt = rt();
    let (s1, _s2, _s3) = rt.block_on(setup_cluster());
    let addr = format!("http://{}:{}", s1.address, s1.port);

    c.bench_function("Himalaya Replicate Put", move |b| {
        b.to_async(&rt).iter_batched(
            || (addr.clone(), key.clone(), value.clone()),
            |(addr, key, value)| async move {
                let mut client = HimalayaClient::connect(addr)
                    .await
                    .expect("Failed to create client");
                let put_request = tonic::Request::new(PutRequest {
                    key: key.clone(),
                    value: value.clone(),
                });

                black_box(client.put(put_request).await)
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(himalaya, put, replicate_put, get, replicate_get);
