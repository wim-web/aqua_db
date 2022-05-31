use std::{
    collections::hash_map::DefaultHasher,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
};

pub type BucketLockRef<K, V> = Arc<RwLock<Bucket<K, V>>>;

#[derive(Debug)]
pub struct Bucket<K, V>
where
    K: Hash + PartialEq + Copy,
    V: Copy,
{
    items: Vec<(K, V)>,
}

impl<K, V> Bucket<K, V>
where
    K: Hash + PartialEq + Copy,
    V: Copy,
{
    fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn get(&self, key: K) -> Option<V> {
        self.items.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
    }

    pub fn put(&mut self, key: K, value: V) {
        match self.items.iter().enumerate().find(|&(_, (k, _))| *k == key) {
            Some((index, _)) => self.items[index] = (key, value),
            None => self.items.push((key, value)),
        }
    }

    pub fn remove(&mut self, key: K) {
        self.items.retain(|(k, _)| *k != key);
    }
}

pub struct HashTable<K, V>
where
    K: Hash + PartialEq + Copy,
    V: Copy,
{
    size: usize,
    pub buckets: Vec<BucketLockRef<K, V>>,
}

impl<K, V> HashTable<K, V>
where
    K: Hash + PartialEq + Copy + Debug,
    V: Copy,
{
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let mut buckets = Vec::with_capacity(size);
        (0..size)
            .into_iter()
            .for_each(|_| buckets.push(Arc::new(RwLock::new(Bucket::new()))));

        Self { size, buckets }
    }

    pub fn same_bucket(&mut self, key1: K, key2: K) -> bool {
        self.calculate_bucket(&key1) == self.calculate_bucket(&key2)
    }

    fn calculate_bucket(&mut self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.size
    }

    pub fn get_bucket_locker(&mut self, key: K) -> Option<BucketLockRef<K, V>> {
        let index = self.calculate_bucket(&key);
        self.buckets.get(index).map(Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn hash_table_0_size() {
        let _table = HashTable::<u8, u8>::new(0);
    }

    #[test]
    fn bucket_test() {
        let mut bucket = Bucket::new();

        let key = "test_key";

        assert!(bucket.get(key).is_none());

        let value = "test_value";
        bucket.put(key, value);

        assert_eq!(value, bucket.get(key).unwrap());

        let value = "test_value1";
        bucket.put(key, value);

        assert_eq!(value, bucket.get(key).unwrap());

        bucket.remove(key);

        assert!(bucket.get(key).is_none());
    }
    #[test]
    fn hash_table_1_size() {
        let mut table = HashTable::new(1);

        let key = "test_key";
        let value = "test_value";

        let bucket_locker = table.get_bucket_locker(key).unwrap();

        {
            let mut write_bucket = bucket_locker.write().unwrap();
            write_bucket.put(key, value);
        }
        {
            let read_bucket = bucket_locker.read().unwrap();
            assert_eq!(value, read_bucket.get(key).unwrap());
        }
    }
}
