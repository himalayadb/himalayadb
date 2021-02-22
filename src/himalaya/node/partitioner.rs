use murmur3;

pub enum Partitioner {
    Murmur3(Murmur3),
}

pub struct Murmur3;

impl Murmur3 {
    const SEED: u32 = 432144324;
    #[cfg(target_pointer_width = "32")]
    fn partition(&self, key: &[u8]) -> i64 {
        let mut k = key;
        return (murmur3::murmur3_x86_128(&mut k, Murmur3::SEED).unwrap() >> 64) as i64;
    }

    #[cfg(target_pointer_width = "64")]
    fn partition(&self, key: &[u8]) -> i64 {
        let mut k = key;
        return (murmur3::murmur3_x64_128(&mut k, Murmur3::SEED).unwrap() >> 64) as i64;
    }
}

impl Partitioner {
    pub fn partition(&self, key: &[u8]) -> i64 {
        match self {
            Partitioner::Murmur3(m) => m.partition(key),
        }
    }
}
