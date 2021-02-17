use tracing::field::debug;
use tracing::Span;

pub mod coordinator;
pub mod external_server;
pub mod internal_server;
pub mod node;
pub mod proto;
pub mod storage;

#[derive(Debug)]
pub struct Key(Vec<u8>);

impl Key {
    pub fn parse(v: Vec<u8>) -> Result<Key, String> {
        if v.len() > 0 {
            Span::current().record("key", &debug(&v));
            Ok(Self(v))
        } else {
            Err("empty key provided".to_owned())
        }
    }
}

impl AsRef<Vec<u8>> for Key {
    fn as_ref(&self) -> &Vec<u8> {
        &self.0
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
