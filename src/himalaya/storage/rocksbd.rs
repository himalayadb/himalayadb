use crate::storage::Error;
use crate::storage::Error::{Configuration, DeleteError, GetError, PutError};
use bytes::Bytes;
use rocksdb::{MergeOperands, Options, DB};
use std::cmp::Ordering;
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

    pub(crate) fn put(&self, key: &[u8], value: &[u8], ts: i64) -> Result<(), Error> {
        let mut wrapped_value = Vec::with_capacity(value.len() + 8);
        let ts_bytes = ts.to_be_bytes();

        wrapped_value.append(&mut ts_bytes.to_vec());
        for x in value {
            wrapped_value.push(*x)
        }

        self.rocks
            .merge(key, wrapped_value)
            .map_err(|e| PutError(From::from(e)))?;
        Ok(())
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Bytes>, Error> {
        match self.rocks.get(key) {
            Ok(Some(v)) => Ok(Some(Bytes::from(v[8..].to_vec()))),
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use test::Bencher;
//
//     #[test]
//     fn test_can_merge_values() {
//         let rocksdb = RocksDb::create("/tmp/rocks_test").expect("failed to create rocksdb");
//
//         rocksdb
//             .put("key".as_bytes(), "value3".as_bytes(), 3)
//             .expect("failed to put value");
//         rocksdb
//             .put("key".as_bytes(), "value2".as_bytes(), 2)
//             .expect("failed to put value");
//         rocksdb
//             .put("key".as_bytes(), "value1".as_bytes(), 1)
//             .expect("failed to put value");
//
//         rocksdb
//             .put("key".as_bytes(), "test".as_bytes(), 113123123123)
//             .expect("failed to put value");
//
//         let val = rocksdb
//             .get("key".as_bytes())
//             .expect("expected to receive key")
//             .expect("expected to receive value");
//
//         assert_eq!(val, "test".as_bytes());
//     }
//
//     #[bench]
//     fn bench_value(b: &mut Bencher) {
//         b.iter(|| {
//             let mut wrapped_value = Vec::with_capacity(value.len() + 8);
//             let ts_bytes = ts.to_be_bytes();
//
//             wrapped_value.append(&mut ts_bytes.to_vec());
//             for x in value {
//                 wrapped_value.push(*x)
//             }
//             wrapped_value
//         });
//     }
// }
