use crate::node::metadata::{NodeMetadata};

pub mod metadata;
pub mod topology;
pub mod partitioner;


#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub metadata: NodeMetadata,
}

impl Node {
    pub fn new(
        m: NodeMetadata
    ) -> Self {
        Node {
            metadata: m,
        }
    }
}

