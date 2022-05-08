use super::descriptors::DescriptorID;

pub trait Replacer {
    fn victim(&mut self) -> Option<DescriptorID>;
    fn pin(&mut self, descriptor_id: DescriptorID);
    fn unpin(&mut self, descriptor_id: DescriptorID);
}

pub struct LruReplacer {
    cache: lru::LruCache<DescriptorID, bool>,
}

impl LruReplacer {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        Self {
            cache: lru::LruCache::new(size),
        }
    }
}

impl Replacer for LruReplacer {
    fn victim(&mut self) -> Option<DescriptorID> {
        self.cache.pop_lru().map(|(id, _)| id)
    }

    fn pin(&mut self, descriptor_id: DescriptorID) {
        self.cache.pop(&descriptor_id);
    }

    fn unpin(&mut self, descriptor_id: DescriptorID) {
        self.cache.put(descriptor_id, true);
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::descriptors::DescriptorID;

    use super::{LruReplacer, Replacer};

    #[test]
    #[should_panic]
    fn lru_replacer_zero_size() {
        let _replacer = LruReplacer::new(0);
    }

    #[test]
    fn lru_replacer() {
        let mut replacer = LruReplacer::new(2);
        let id1 = DescriptorID(1);
        let id2 = DescriptorID(2);
        let id3 = DescriptorID(3);

        replacer.unpin(id1);
        replacer.unpin(id2);
        replacer.unpin(id3);

        assert_eq!(id2, replacer.victim().unwrap());
        assert_eq!(id3, replacer.victim().unwrap());
        assert!(replacer.victim().is_none());
    }
}
