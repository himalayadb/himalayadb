use crate::node::metadata::NodeMetadata;
use clap::{load_yaml, App, AppSettings};

pub struct Settings {
    pub bind_address: String,
    pub bind_port: u32,
    pub consistency: usize,
    pub replicas: usize,
    pub metadata: NodeMetadata,

    pub rocks: RocksDbSettings,
    pub etcd: EtcdSettings,
}

pub struct RocksDbSettings {
    pub path: String,
}

pub struct EtcdSettings {
    pub host: String,
    pub port: u32,
    pub prefix: String,
    pub lease_ttl: i64,
    pub ttl_refresh_interval: u64,
}

impl EtcdSettings {
    pub fn hosts(&self) -> Vec<String> {
        vec![format!("{}:{}", self.host, self.port)]
    }
}

pub fn get_configuration() -> Result<Settings, Box<dyn std::error::Error>> {
    // parse given arguments
    let yaml = load_yaml!("options.yaml");
    let matches = App::from(yaml)
        .setting(AppSettings::AllowNegativeNumbers)
        .get_matches();

    let bind_address = matches.value_of_t("address").unwrap_or("[::1]".to_owned());
    let bind_port = matches.value_of_t("port").unwrap_or(50051u32);
    let consistency = matches.value_of_t("consistency").unwrap_or(1usize);
    let replicas = matches.value_of_t("replicas").unwrap_or(0usize);

    let etcd_host = matches.value_of("etcd_host").unwrap_or("localhost");
    let etcd_port = matches.value_of_t("etcd_port").unwrap_or(2379u32);
    let etcd_prefix = matches.value_of("etcd_prefix").unwrap_or("members");
    let etcd_lease_ttl = matches.value_of_t("etcd_lease_ttl").unwrap_or(5i64);
    let etcd_ttl_refresh_interval = matches
        .value_of_t("etcd_ttl_refresh_interval_ms")
        .unwrap_or(3000u64);

    let token = matches
        .value_of_t::<i64>("token")
        .expect("You must provide an initial token.");

    let identifier = matches
        .value_of("identifier")
        .expect("You must provide an identifier for this node.");

    let rocksdb_path = matches
        .value_of("rocksdb_path")
        .expect("You must provide a rocksdb path.");

    Ok(Settings {
        bind_address,
        bind_port,
        consistency,
        metadata: NodeMetadata {
            identifier: identifier.to_owned(),
            token,
            host: format!("[::1]:{}", bind_port),
        },
        replicas,
        rocks: RocksDbSettings {
            path: rocksdb_path.to_owned(),
        },
        etcd: EtcdSettings {
            host: etcd_host.to_owned(),
            port: etcd_port,
            prefix: etcd_prefix.to_owned(),
            lease_ttl: etcd_lease_ttl,
            ttl_refresh_interval: etcd_ttl_refresh_interval,
        },
    })
}
