use std::{
    path::Path,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Ok};

use super::{
    buffer_pool::{Buffer, BufferPool, BufferPoolID},
    descriptors::{Descriptor, DescriptorID, Descriptors},
    disk_manager::{DiskManager, Page, PageID},
    hash_table,
    replacer::{LruReplacer, Replacer},
    StorageResult,
};

struct BufferPoolManager<R>
where
    R: Replacer,
{
    replacer: R,
    disk_manager: DiskManager,
    buffer_pool: BufferPool,
    page_table: hash_table::HashTable<PageID, DescriptorID>,
    descriptors: Descriptors,
}

impl BufferPoolManager<LruReplacer> {
    pub fn new(pool_size: usize, file_path: impl AsRef<Path>) -> Self {
        let mut replacer = LruReplacer::new(pool_size);
        let disk_manager = DiskManager::new(file_path);
        let buffer_pool = BufferPool::new(pool_size);
        let page_table = hash_table::HashTable::new(pool_size);
        let descriptors = Descriptors::new(pool_size);

        // 初期化時は全てのdescriptor_idをreplacerに登録しておく
        for d in &descriptors.items {
            replacer.unpin(d.read().unwrap().id);
        }

        Self {
            replacer,
            disk_manager,
            buffer_pool,
            page_table,
            descriptors,
        }
    }

    fn load_to_victim_buffer(&mut self, p_id: PageID) -> StorageResult<Arc<RwLock<Buffer>>> {
        let victim_descriptor_id = self
            .replacer
            .victim()
            .ok_or_else(|| anyhow!("not found victim descriptor id"))?;

        let descriptor_locker = self.descriptors.get(victim_descriptor_id);
        let (victim_page_id, buffer_pool_id) = {
            let mut descriptor = descriptor_locker.write().unwrap();
            let buffer_locker = self.buffer_pool.get(descriptor.buffer_pool_id);
            if descriptor.dirty {
                let page = &buffer_locker.write().unwrap().page;
                self.disk_manager.write(page)?;
                descriptor.reset();
            }
            let buffer = buffer_locker.read().unwrap();
            (buffer.page.id, descriptor.buffer_pool_id)
        };

        let buffer = if self.page_table.same_bucket(victim_page_id, p_id) {
            let bucket_locker = self
                .page_table
                .get_bucket_locker(victim_page_id)
                .ok_or_else(|| anyhow!("cant get bucket"))?;

            let mut bucket = bucket_locker.write().unwrap();

            bucket.put(p_id, victim_descriptor_id);
            bucket.remove(victim_page_id);

            let page = self.disk_manager.read(p_id)?;
            self.buffer_pool.put(buffer_pool_id, page);
            self.buffer_pool.get(buffer_pool_id)
        } else {
            let old_bucket_locker = self
                .page_table
                .get_bucket_locker(victim_page_id)
                .ok_or_else(|| anyhow!("cant get old bucket"))?;

            let mut old_bucket = old_bucket_locker.write().unwrap();

            let new_bucket_locker = self
                .page_table
                .get_bucket_locker(p_id)
                .ok_or_else(|| anyhow!("cant get new bucket"))?;

            let mut new_bucket = new_bucket_locker.write().unwrap();

            new_bucket.put(p_id, victim_descriptor_id);
            old_bucket.remove(victim_page_id);

            let page = self.disk_manager.read(p_id)?;
            self.buffer_pool.put(buffer_pool_id, page);
            self.buffer_pool.get(buffer_pool_id)
        };

        Ok(buffer)
    }

    pub fn mark_dirty(&mut self, buffer_pool_id: BufferPoolID) -> StorageResult<()> {
        let descriptor_id = DescriptorID::from_buf_pool_id(buffer_pool_id);
        let descriptor_arc = self.descriptors.get(descriptor_id);
        let mut descriptor = descriptor_arc.write().unwrap();
        descriptor.dirty = true;

        Ok(())
    }

    pub fn new_buffer(&mut self) -> StorageResult<Arc<RwLock<Buffer>>> {
        let new_page = self.disk_manager.allocate_page()?;

        let victim_descriptor_id = self
            .replacer
            .victim()
            .ok_or_else(|| anyhow!("not found victim descriptor id"))?;

        let descriptor_locker = self.descriptors.get(victim_descriptor_id);
        let (victim_page_id, buffer_pool_id) = {
            let mut descriptor = descriptor_locker.write().unwrap();
            let buffer_locker = self.buffer_pool.get(descriptor.buffer_pool_id);
            if descriptor.dirty {
                let page = &buffer_locker.write().unwrap().page;
                self.disk_manager.write(page)?;
            }
            descriptor.reset();
            descriptor.pin();
            let buffer = buffer_locker.read().unwrap();
            (buffer.page.id, descriptor.buffer_pool_id)
        };

        let buffer = if self.page_table.same_bucket(victim_page_id, new_page.id) {
            let bucket_locker = self
                .page_table
                .get_bucket_locker(victim_page_id)
                .ok_or_else(|| anyhow!("cant get bucket"))?;

            let mut bucket = bucket_locker.write().unwrap();

            bucket.remove(victim_page_id);
            bucket.put(new_page.id, victim_descriptor_id);

            self.buffer_pool.put(buffer_pool_id, new_page);
            self.buffer_pool.get(buffer_pool_id)
        } else {
            let old_bucket_locker = self
                .page_table
                .get_bucket_locker(victim_page_id)
                .ok_or_else(|| anyhow!("cant get old bucket"))?;

            let mut old_bucket = old_bucket_locker.write().unwrap();

            let new_bucket_locker = self
                .page_table
                .get_bucket_locker(new_page.id)
                .ok_or_else(|| anyhow!("cant get new bucket"))?;

            let mut new_bucket = new_bucket_locker.write().unwrap();

            old_bucket.remove(victim_page_id);
            new_bucket.put(new_page.id, victim_descriptor_id);

            self.buffer_pool.put(buffer_pool_id, new_page);
            self.buffer_pool.get(buffer_pool_id)
        };

        Ok(buffer)
    }

    pub fn fetch_buffer(&mut self, p_id: PageID) -> StorageResult<Arc<RwLock<Buffer>>> {
        let bucket_locker = self
            .page_table
            .get_bucket_locker(p_id)
            .ok_or_else(|| anyhow!("cant get bucket"))?;

        if let Some(d_id) = bucket_locker.read().unwrap().get(p_id) {
            let descriptor_arc = self.descriptors.get(d_id);
            let mut descriptor = descriptor_arc.write().unwrap();
            descriptor.pin();
            return Ok(self.buffer_pool.get(descriptor.buffer_pool_id));
        };

        self.load_to_victim_buffer(p_id)
    }

    pub fn unpin_buffer(&mut self, p_id: PageID) -> StorageResult<()> {
        let bucket_locker = self
            .page_table
            .get_bucket_locker(p_id)
            .ok_or_else(|| anyhow!("cant get bucket"))?;

        if let Some(descriptor_id) = bucket_locker.read().unwrap().get(p_id) {
            let descriptor_arc = self.descriptors.get(descriptor_id);
            let mut descriptor = descriptor_arc.write().unwrap();
            descriptor.unpin();
            if !descriptor.pinned() {
                self.replacer.unpin(descriptor_id);
            }
        }

        Ok(())
    }

    pub fn flush_buffer(&mut self, p_id: PageID) -> StorageResult<()> {
        let bucket_locker = self
            .page_table
            .get_bucket_locker(p_id)
            .ok_or_else(|| anyhow!("cant get bucket"))?;

        if let Some(descriptor_id) = bucket_locker.read().unwrap().get(p_id) {
            let descriptor_arc = self.descriptors.get(descriptor_id);
            let descriptor = descriptor_arc.write().unwrap();
            let buffer = self.buffer_pool.get(descriptor.buffer_pool_id);
            let page = &buffer.write().unwrap().page;
            self.disk_manager.write(page).unwrap();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use crate::storage::{
        disk_manager::{PageID, PAGE_SIZE},
        replacer::Replacer,
    };

    use super::BufferPoolManager;

    #[test]
    #[should_panic]
    fn buffer_pool_manager_new_test_no_size() {
        let _manager = BufferPoolManager::new(0, "dummy");
    }

    #[test]
    fn buffer_pool_manager_write_and_flush() {
        let (_, p) = NamedTempFile::new().unwrap().into_parts();
        let mut manager = BufferPoolManager::new(1, p);

        let page_id = {
            let buffer_locker = manager.new_buffer().unwrap();
            let mut buffer = buffer_locker.write().unwrap();
            buffer.write(b"test");
            manager.unpin_buffer(buffer.page.id).unwrap();
            buffer.page.id
        };

        manager.flush_buffer(page_id).unwrap();

        let buffer_locker = manager.fetch_buffer(page_id).unwrap();
        let buffer = buffer_locker.read().unwrap();

        assert_eq!(buffer.page.data[..4], b"test"[..]);
    }

    #[test]
    fn buffer_pool_manager_victim() {
        let (_, p) = NamedTempFile::new().unwrap().into_parts();
        let mut manager = BufferPoolManager::new(1, p);

        let page_id = {
            let buffer_locker = manager.new_buffer().unwrap();
            let mut buffer = buffer_locker.write().unwrap();
            buffer.write(b"test");
            manager.unpin_buffer(buffer.page.id).unwrap();
            manager.mark_dirty(buffer.id).unwrap();
            buffer.page.id
        };

        // 明示的にflushしなくても、new_buffer時のvictimでdiskにwriteされる
        {
            let buffer_locker = manager.new_buffer().unwrap();
            let buffer = buffer_locker.read().unwrap();
            manager.unpin_buffer(buffer.page.id).unwrap();
        }

        let buffer_locker = manager.fetch_buffer(page_id).unwrap();
        let buffer = buffer_locker.read().unwrap();

        assert_eq!(buffer.page.data[..4], b"test"[..]);
    }
}
