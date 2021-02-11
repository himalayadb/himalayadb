use crate::node::metadata::NodeMetadata;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub struct Topology(Arc<RwLock<HashMap<String, NodeMetadata>>>);

impl Topology {
    pub fn new(map: HashMap<String, NodeMetadata>) -> Self {
        Topology(Arc::new(RwLock::new(map)))
    }

    pub fn get_node(&self, identifier: &str) -> Option<NodeMetadata> {
        let map = self.0.try_read().ok()?;
        if let Some(nm) = map.get(identifier) {
            Some((*nm).clone())
        } else {
            None
        }
    }

    pub fn add_node(&self, meta: NodeMetadata) -> Option<NodeMetadata> {
        let mut map = self.0.try_write().ok()?;
        map.insert(meta.identifier.clone(), meta)
    }

    pub fn remove_node(&self, identifier: &str) -> Option<NodeMetadata> {
        let mut map = self.0.try_write().ok()?;
        map.remove(identifier)
    }
}
