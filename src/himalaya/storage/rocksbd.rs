use crate::storage::Error;
use crate::storage::Error::{Configuration, DeleteError, GetError, PutError};
use bytes::BufMut;
use bytes::Bytes;
use rocksdb::{MergeOperands, Options, DB};
use std::cmp::Ordering;
use std::mem;
use std::path::Path;

pub struct RocksClient {
    rocks: DB,
}

impl RocksClient {
    fn ts_aware_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
    ) -> Option<Vec<u8>> {
        let latest_inc_val: Option<&[u8]> = operands.max_by(|v1, v2| v1[0..8].cmp(&v2[0..8]));

        match (existing_val, latest_inc_val) {
            (Some(e), Some(l)) => {
                if e[0..8].cmp(&l[0..8]) != Ordering::Greater {
                    Some(l.to_vec())
                } else {
                    Some(e.to_vec())
                }
            }
            (Some(e), None) => Some(e.to_vec()),
            (None, Some(l)) => Some(l.to_vec()),
            _ => None,
        }
    }

    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator("ts_aware_merge", Self::ts_aware_merge, None);

        let db = DB::open(&opts, path).map_err(|e| Configuration(From::from(e)))?;

        Ok(Self { rocks: db })
    }

    #[inline]
    fn wrap_value(value: &[u8], ts: i64) -> Vec<u8> {
        let mut wrapped_value = Vec::with_capacity(value.len() + mem::size_of::<i64>());
        wrapped_value.put_i64(ts);
        wrapped_value.put_slice(value);

        wrapped_value
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8], ts: i64) -> Result<(), Error> {
        let wrapped_value = Self::wrap_value(value, ts);
        self.rocks
            .merge(key, wrapped_value)
            .map_err(|e| PutError(From::from(e)))?;
        Ok(())
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        match self.rocks.get(key) {
            Ok(Some(v)) => Ok(Some(Bytes::copy_from_slice(&v[8..]))),
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

#[cfg(test)]
mod tests {
    use super::*;
    use claim::assert_ok;
    use tempfile::tempdir;
    use test::Bencher;

    #[test]
    fn test_can_merge_values() {
        let temp_res = tempdir();
        assert_ok!(&temp_res);
        let dir = temp_res.unwrap();
        let rocksdb = RocksClient::create(dir.path()).expect("failed to create rocksdb");

        rocksdb
            .put("key".as_bytes(), "value3".as_bytes(), 3)
            .expect("failed to put value");
        rocksdb
            .put("key".as_bytes(), "value2".as_bytes(), 2)
            .expect("failed to put value");
        rocksdb
            .put("key".as_bytes(), "value1".as_bytes(), 1)
            .expect("failed to put value");

        rocksdb
            .put("key".as_bytes(), "test".as_bytes(), 113123123123)
            .expect("failed to put value");

        let val = rocksdb
            .get("key".as_bytes())
            .expect("expected to receive key")
            .expect("expected to receive value");

        assert_eq!(val, "test".as_bytes());
    }

    #[bench]
    fn bench_value_new(b: &mut Bencher) {
        let ts = 123123i64;
        let value = "asdfasdfasdf".as_bytes();
        b.iter(|| {
            let wrapped_value = RocksClient::wrap_value(value, ts);

            wrapped_value
        });
    }
}
