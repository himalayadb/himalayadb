#[derive(Debug)]
struct Node {
    pub metadata: NodeMetadata
}

#[derive(Debug)]
pub struct NodeMetadata {
    pub identifier: String,
    pub token: i64
}