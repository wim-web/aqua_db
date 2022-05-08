use std::ops::{Index, IndexMut};

use super::disk_manager::Page;

#[derive(Debug)]
pub struct BufferPool {
    cache: Vec<Page>,
}

impl BufferPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let mut cache = Vec::with_capacity(size);

        for _ in 0..size {
            cache.push(Page::default());
        }

        Self { cache }
    }
}

impl Index<BufferPoolID> for BufferPool {
    type Output = Page;

    fn index(&self, index: BufferPoolID) -> &Self::Output {
        &self.cache[index.value()]
    }
}

impl IndexMut<BufferPoolID> for BufferPool {
    fn index_mut(&mut self, index: BufferPoolID) -> &mut Self::Output {
        &mut self.cache[index.value()]
    }
}

#[derive(Copy, Clone)]
pub struct BufferPoolID(pub usize);

impl BufferPoolID {
    pub fn value(&self) -> usize {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::BufferPool;

    #[test]
    #[should_panic]
    fn new_no_size() {
        let _pool = BufferPool::new(0);
    }
}
