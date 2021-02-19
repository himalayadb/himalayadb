use bytes::Bytes;
use tracing::field::debug;
use tracing::Span;

pub mod coordinator;
pub mod external_server;
pub mod internal_server;
pub mod node;
pub mod proto;
pub mod storage;

#[derive(Debug)]
pub struct Key(Bytes);

impl Key {
    pub fn parse(k: Bytes) -> Result<Key, String> {
        if k.len() > 0 {
            Span::current().record("key", &debug(&k));
            Ok(Self(k))
        } else {
            Err("empty key provided".to_owned())
        }
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<Bytes> for Key {
    fn as_ref(&self) -> &Bytes {
        &self.0
    }
}
