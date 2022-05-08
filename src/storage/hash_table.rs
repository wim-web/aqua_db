use std::{
    collections::hash_map::DefaultHasher,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Mutex,
};

pub struct HashTable<K, V>
where
    K: Hash + PartialEq + Copy,
{
    size: usize,
    buckets: Vec<Mutex<Vec<(K, V)>>>,
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
            .for_each(|_| buckets.push(Mutex::new(Vec::new())));

        let hasher = DefaultHasher::new();

        Self { size, buckets }
    }

    fn calculate_bucket(&mut self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.size
    }

    pub fn get(&mut self, key: K) -> Option<V> {
        let index = self.calculate_bucket(&key);
        let bucket = self.buckets[index].lock().unwrap();

        bucket.iter().find(|&&(k, _)| k == key).map(|(k, v)| *v)
    }

    pub fn put(&mut self, key: K, value: V) {
        let index = self.calculate_bucket(&key);
        let mut bucket = self.buckets[index].lock().unwrap();

        match bucket.iter().enumerate().find(|&(_, (k, _))| *k == key) {
            Some((index, _)) => bucket[index] = (key, value),
            None => bucket.push((key, value)),
        }
    }
}

mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn hash_table_0_size() {
        let _table = HashTable::<u8, u8>::new(0);
    }

    #[test]
    fn hash_table_1_size() {
        let mut table = HashTable::new(1);

        table.put(1, "value_1");
        assert_eq!(table.get(1).unwrap(), "value_1");

        table.put(1, "value_2");
        assert_eq!(table.get(1).unwrap(), "value_2");

        assert_eq!(table.buckets[0].lock().unwrap().len(), 1);
    }

    #[test]
    fn hash_table_some_size() {
        let mut table = HashTable::new(10);

        table.put(2, "value_1");
        assert_eq!(table.get(2).unwrap(), "value_1");

        table.put(1, "value_2");
        assert_eq!(table.get(1).unwrap(), "value_2");

        table.put(3, "value_3");
        assert_eq!(table.get(3).unwrap(), "value_3");
    }
}
