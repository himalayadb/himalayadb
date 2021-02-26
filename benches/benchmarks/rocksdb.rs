use criterion::{criterion_group, Criterion};
use fake::{Fake, Faker};
use himalaya::storage::rocksbd::RocksClient;

fn wrap_value(c: &mut Criterion) {
    let ts = Faker.fake::<i64>();
    let value = Faker.fake::<String>();

    c.bench_function("Rocks wrap_value", |b| {
        b.iter(|| RocksClient::wrap_value(value.as_bytes(), ts))
    });
}

criterion_group!(rocks, wrap_value);
