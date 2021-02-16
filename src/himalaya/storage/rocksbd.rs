use crate::storage::Error;
use crate::storage::Error::GetError;
use rocksdb::{Options, DB};
use std::path::Path;

pub struct RocksDb {
    rocks: DB,
}

impl RocksDb {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let db = DB::open_default(path)?;
        Ok(RocksDb { rocks: db })
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.rocks
            .put(key, value)
            .map_err(|e| Error::PutError(From::from(e)))?;
        Ok(())
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self.rocks.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(GetError(From::from(e))),
        }
    }
}
struct Apple {
    name: String,
}

impl AsRef<[u8]> for Apple {
    fn as_ref(&self) -> &[u8] {
        (&self.name).as_ref()
    }
}

//impl AsRef<[u8]> for &Apple {
//    fn as_ref(&self) -> &[u8] {
//        (&self.name).as_ref()
//    }
//}

#[cfg(test)]
mod test {
    use std::borrow::Borrow;

    use super::*;

    #[test]
    fn test_blah() {
        let a = Apple {
            name: "test".to_string(),
        };

        let db = RocksDb::create("./test").expect("t");
        //db.put(a.as_ref(), a).expect("test");
        //db.put(&a, &a).expect("test");
        //db.put(a, vec![b'1'].as_ref()).expect("test");
    }
}
