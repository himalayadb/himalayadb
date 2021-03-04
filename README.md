# HimalayaDB


## Quickstart

```bash
cargo build
```

Then:

```bash

docker-compose up -d etcd

cargo run --bin himalayadb -- -i node1 -r 2 -t -3074457345618258603 --port 50051 --etcd_host localhost --etcd_port 2379 --rocksdb_path /tmp/node1

cargo run --bin himalayadb -- -i node2 -r 2 -t -9223372036854775808 --port 50052 --etcd_host localhost --etcd_port 2379 --rocksdb_path /tmp/node2

cargo run --bin himalayadb -- -i node3 -r 2 -t 3074457345618258602 --port 50053 --etcd_host localhost --etcd_port 2379 --rocksdb_path /tmp/node3

cargo run --bin himalaya-client -- --host localhost --port 50051 write -k k1 -v v1
```
