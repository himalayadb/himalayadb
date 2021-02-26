use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint, Uri};

#[derive(Debug)]
pub enum ConnError {
    Read,
    Write,
}

unsafe impl Send for ConnError {}
unsafe impl Sync for ConnError {}
impl std::error::Error for ConnError {}

impl std::fmt::Display for ConnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnError::Read => write!(f, "failed to get connection"),
            ConnError::Write => write!(f, "failed to insert connection"),
        }
    }
}

#[derive(Clone)]
pub struct ConnManager {
    channels: Arc<RwLock<HashMap<Uri, Channel>>>,
    timeout: Duration,
}

impl ConnManager {
    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn from_parts(src: Parts) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::default())),
            timeout: src.timeout,
        }
    }

    pub fn get(&self, uri: Uri) -> Result<Channel, ConnError> {
        {
            let reader = self.channels.read().map_err(|_| ConnError::Read)?;
            match reader.get(&uri) {
                Some(c) => return Ok(c.clone()),
                None => {}
            };
        }

        let mut writer = self.channels.write().map_err(|_| ConnError::Write)?;
        match writer.get(&uri) {
            Some(c) => return Ok(c.clone()),
            None => {}
        };

        let c = self.connect(uri.clone());
        writer.insert(uri, c.clone());

        Ok(c)
    }

    fn connect(&self, uri: Uri) -> Channel {
        let endpoint = Endpoint::from(uri).timeout(self.timeout);
        Channel::balance_list(vec![endpoint].into_iter())
    }
}

pub struct Builder {
    parts: Parts,
}

impl Builder {
    #[inline]
    pub fn new() -> Builder {
        Builder::default()
    }

    pub fn build(self) -> ConnManager {
        ConnManager::from_parts(self.parts)
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.parts.timeout = timeout;
        self
    }
}

impl Default for Builder {
    #[inline]
    fn default() -> Builder {
        Self {
            parts: Parts::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Parts {
    pub timeout: Duration,
}
