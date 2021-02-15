use crate::node::metadata::NodeMetadata;
use std::cmp::Ordering;

pub mod metadata;
pub mod partitioner;
pub mod topology;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub metadata: NodeMetadata,
}

impl Node {
    pub fn new(m: NodeMetadata) -> Self {
        Node { metadata: m }
    }
}
impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.metadata.cmp(&other.metadata)
    }
}
