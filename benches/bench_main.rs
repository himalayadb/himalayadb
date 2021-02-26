use criterion::criterion_main;

mod benchmarks;

criterion_main!(
    benchmarks::rocksdb::rocks,
    benchmarks::coordinator::coorindator,
    benchmarks::himalaya::himalaya,
);
