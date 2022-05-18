use std::sync::{Arc, RwLock};

use super::disk_manager::Page;

#[derive(Debug)]
pub struct Buffer {
    pub id: BufferPoolID,
    pub page: Page,
}

impl Buffer {
    pub fn new(id: BufferPoolID, page: Page) -> Self {
        Self { id, page }
    }

    pub fn write(&mut self, data: &[u8]) {
        let len = data.len();
        assert!(len <= self.page.size);

        self.page.data[..len].copy_from_slice(data);
    }
}

#[derive(Debug)]
pub struct BufferPool {
    cache: Vec<Arc<RwLock<Buffer>>>,
}

impl BufferPool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let mut cache = Vec::with_capacity(size);

        for n in 0..size {
            let buffer = Buffer::new(BufferPoolID(n), Page::default());
            cache.push(Arc::new(RwLock::new(buffer)));
        }

        Self { cache }
    }

    pub fn get(&self, id: BufferPoolID) -> Arc<RwLock<Buffer>> {
        Arc::clone(&self.cache[id.value()])
    }

    pub fn put(&mut self, id: BufferPoolID, page: Page) {
        let buffer = Buffer::new(id, page);
        self.cache[id.value()] = Arc::new(RwLock::new(buffer));
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct BufferPoolID(pub usize);

impl BufferPoolID {
    pub fn value(&self) -> usize {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::BorrowMut, sync::Arc};

    use crate::storage::disk_manager::{Page, PageID};

    use super::{Buffer, BufferPool, BufferPoolID};

    #[test]
    #[should_panic]
    fn buffer_pool_new_no_size() {
        let _pool = BufferPool::new(0);
    }

    #[test]
    fn buffer_pool_get_put() {
        let mut pool = BufferPool::new(1);
        let id = BufferPoolID(0);

        let page = Page {
            id: PageID(100),
            ..Default::default()
        };

        pool.put(id, page);

        let buffer_locked = pool.get(id);
        let buffer = buffer_locked.read().unwrap();

        assert_eq!(buffer.page.id, page.id);
    }

    #[test]
    fn buffer_write() {
        let mut buffer = Buffer::new(BufferPoolID(0), Page::default());

        buffer.write(b"test");

        assert_eq!(buffer.page.data[..4], b"test"[..]);
    }
}
