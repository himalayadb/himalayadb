use rocksbd::RocksDb;
use std::fmt::Formatter;

pub mod rocksbd;

pub enum PersistentStore {
    RocksDb(RocksDb),
}

impl PersistentStore {
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: &K, value: &V) -> Result<(), Error> {
        match self {
            PersistentStore::RocksDb(r) => r.put(key.as_ref(), value.as_ref()),
        }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: &K) -> Result<Option<Vec<u8>>, Error> {
        match self {
            PersistentStore::RocksDb(r) => r.get(key.as_ref()),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    GetError(Box<dyn std::error::Error>),
    PutError(Box<dyn std::error::Error>),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::GetError(e) => write!(f, "get error: {}", e),
            Error::PutError(e) => write!(f, "put error: {}", e),
        }
    }
}
impl std::convert::From<Error> for String {
    fn from(e: Error) -> Self {
        format!("{}", e)
    }
}

impl std::error::Error for Error {}
