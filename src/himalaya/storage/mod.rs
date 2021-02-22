use crate::storage::rocksbd::RocksClient;
use bytes::Bytes;
use std::fmt::Formatter;

pub mod rocksbd;

pub enum PersistentStore {
    RocksDb(RocksClient),
}

impl PersistentStore {
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: &K,
        value: &V,
        ts: i64,
    ) -> Result<(), Error> {
        match self {
            PersistentStore::RocksDb(r) => r.put(key.as_ref(), value.as_ref(), ts),
        }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: &K) -> Result<Option<Bytes>, Error> {
        match self {
            PersistentStore::RocksDb(r) => r.get(key.as_ref()),
        }
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: &K) -> Result<(), Error> {
        match self {
            PersistentStore::RocksDb(r) => r.delete(key.as_ref()),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Configuration(Box<dyn std::error::Error>),
    GetError(Box<dyn std::error::Error>),
    PutError(Box<dyn std::error::Error>),
    DeleteError(Box<dyn std::error::Error>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Configuration(e) => write!(f, "get error: {}", e),
            Error::GetError(e) => write!(f, "get error: {}", e),
            Error::PutError(e) => write!(f, "put error: {}", e),
            Error::DeleteError(e) => write!(f, "delete error: {}", e),
        }
    }
}

impl std::convert::From<Error> for String {
    fn from(e: Error) -> Self {
        format!("{}", e)
    }
}

impl std::error::Error for Error {}
unsafe impl Send for Error {}
unsafe impl Sync for Error {}
