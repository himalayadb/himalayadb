use crate::storage::Error;
use crate::storage::Error::{Configuration, DeleteError, GetError, PutError};
use rocksdb::DB;
use std::path::Path;

pub struct RocksDb {
    rocks: DB,
}

impl RocksDb {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db = DB::open_default(path).map_err(|e| Configuration(From::from(e)))?;
        Ok(RocksDb { rocks: db })
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.rocks
            .put(key, value)
            .map_err(|e| PutError(From::from(e)))?;
        Ok(())
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self.rocks.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(GetError(From::from(e))),
        }
    }

    pub(crate) fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.rocks
            .delete(key)
            .map_err(|e| DeleteError(From::from(e)))
    }
}
