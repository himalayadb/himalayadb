pub enum Partitioner {
    Murmur3(Murmur3)
}


pub struct Murmur3 {}

impl Murmur3 {
    fn partition(&self, key: &[u8]) -> i64 {
        return 1
    }
}

impl Partitioner {
    pub fn partition(&self, key: &[u8]) -> i64 {
        match self {
            Partitioner::Murmur3(m) => m.partition(key)
        }
    }
}