use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Ok};

use crate::catalog::Catalog;

use super::{
    buffer_pool::{Buffer, BufferPool, BufferPoolID},
    descriptors::{DescriptorID, Descriptors},
    disk_manager::DiskManager,
    hash_table,
    page::*,
    replacer::{LruReplacer, Replacer},
    StorageResult,
};

#[derive(Hash, PartialEq, Debug)]
struct Key {
    page_id: PageID,
    table_name: String,
}

impl Key {
    fn new(page_id: PageID, table_name: String) -> Self {
        Self {
            page_id,
            table_name,
        }
    }
}

pub struct BufferPoolManager<R>
where
    R: Replacer,
{
    replacer: R,
    disk_manager: DiskManager,
    buffer_pool: BufferPool,
    page_table: hash_table::HashTable<Key, DescriptorID>,
    descriptors: Descriptors,
}

impl BufferPoolManager<LruReplacer> {
    pub fn new(pool_size: usize, base_path: String, catalog: Catalog) -> Self {
        let mut replacer = LruReplacer::new(pool_size);
        let disk_manager = DiskManager::new(base_path, catalog);
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
}

impl<R: Replacer> BufferPoolManager<R> {
    fn victim_descriptor(
        &mut self,
        descriptor_id: DescriptorID,
        table_name: &str,
    ) -> StorageResult<Arc<RwLock<Buffer>>> {
        let descriptor_locker = self.descriptors.get(descriptor_id);
        let mut descriptor = descriptor_locker.write().unwrap();
        let buffer_locker = self.buffer_pool.get(descriptor.buffer_pool_id);

        if descriptor.dirty {
            let page = &buffer_locker.write().unwrap().page;
            self.disk_manager.write(page, table_name)?;
        }

        descriptor.reset();
        descriptor.pin();

        Ok(buffer_locker)
    }

    fn load_page_to_buffer_pool(
        &mut self,
        p_id: PageID,
        buffer_pool_id: BufferPoolID,
        table_name: &str,
    ) -> StorageResult<Arc<RwLock<Buffer>>> {
        let page = self.disk_manager.read(p_id, table_name)?;
        self.buffer_pool.put(buffer_pool_id, page);
        Ok(self.buffer_pool.get(buffer_pool_id))
    }

    fn load_page_from_storage_to_buffer_pool(
        &mut self,
        p_id: PageID,
        table_name: &str,
    ) -> StorageResult<Arc<RwLock<Buffer>>> {
        let victim_descriptor_id = self
            .replacer
            .victim()
            .ok_or_else(|| anyhow!("not found victim descriptor id"))?;

        let buffer_locker = self.victim_descriptor(victim_descriptor_id, table_name)?;
        let (victim_page_id, buffer_pool_id) = {
            let buffer = buffer_locker.read().unwrap();
            (buffer.page.id, buffer.id)
        };

        let victim_key = Key::new(victim_page_id, table_name.to_string());
        let target_key = Key::new(p_id, table_name.to_string());

        let buffer_locker = if self.page_table.same_bucket(&victim_key, &target_key) {
            let bucket_locker = self
                .page_table
                .get_bucket_locker(&victim_key)
                .ok_or_else(|| anyhow!("cant get bucket"))?;

            let mut bucket = bucket_locker.write().unwrap();

            bucket.remove(victim_key);
            bucket.put(target_key, victim_descriptor_id);

            self.load_page_to_buffer_pool(p_id, buffer_pool_id, table_name)?
        } else {
            let old_bucket_locker = self
                .page_table
                .get_bucket_locker(&victim_key)
                .ok_or_else(|| anyhow!("cant get old bucket"))?;

            let mut old_bucket = old_bucket_locker.write().unwrap();

            let new_bucket_locker = self
                .page_table
                .get_bucket_locker(&target_key)
                .ok_or_else(|| anyhow!("cant get new bucket"))?;

            let mut new_bucket = new_bucket_locker.write().unwrap();

            old_bucket.remove(victim_key);
            new_bucket.put(target_key, victim_descriptor_id);

            self.load_page_to_buffer_pool(p_id, buffer_pool_id, table_name)?
        };

        Ok(buffer_locker)
    }

    pub fn mark_dirty(&mut self, buffer_pool_id: BufferPoolID) -> StorageResult<()> {
        let descriptor_id = DescriptorID::from_buf_pool_id(buffer_pool_id);
        let descriptor_arc = self.descriptors.get(descriptor_id);
        let mut descriptor = descriptor_arc.write().unwrap();
        descriptor.dirty = true;

        Ok(())
    }

    pub fn new_buffer(&mut self, table_name: &str) -> StorageResult<Arc<RwLock<Buffer>>> {
        let new_page = self.disk_manager.allocate_page(table_name)?;
        self.load_page_from_storage_to_buffer_pool(new_page.id, table_name)
    }

    pub fn fetch_buffer(
        &mut self,
        p_id: PageID,
        table_name: &str,
    ) -> StorageResult<Arc<RwLock<Buffer>>> {
        let key = Key::new(p_id, table_name.to_string());
        let bucket_locker = self
            .page_table
            .get_bucket_locker(&key)
            .ok_or_else(|| anyhow!("cant get bucket"))?;

        if let Some(d_id) = bucket_locker.read().unwrap().get(key) {
            let descriptor_arc = self.descriptors.get(d_id);
            let mut descriptor = descriptor_arc.write().unwrap();
            descriptor.pin();
            return Ok(self.buffer_pool.get(descriptor.buffer_pool_id));
        };

        self.load_page_from_storage_to_buffer_pool(p_id, table_name)
    }

    pub fn unpin_buffer(&mut self, p_id: PageID, table_name: &str) -> StorageResult<()> {
        let key = Key::new(p_id, table_name.to_string());
        let bucket_locker = self
            .page_table
            .get_bucket_locker(&key)
            .ok_or_else(|| anyhow!("cant get bucket"))?;

        if let Some(descriptor_id) = bucket_locker.read().unwrap().get(key) {
            let descriptor_arc = self.descriptors.get(descriptor_id);
            let mut descriptor = descriptor_arc.write().unwrap();
            descriptor.unpin();
            if !descriptor.pinned() {
                self.replacer.unpin(descriptor_id);
            }
        }

        Ok(())
    }

    pub fn flush_buffer(&mut self, p_id: PageID, table_name: &str) -> StorageResult<()> {
        let key = Key::new(p_id, table_name.to_string());
        let bucket_locker = self
            .page_table
            .get_bucket_locker(&key)
            .ok_or_else(|| anyhow!("cant get bucket"))?;

        if let Some(descriptor_id) = bucket_locker.read().unwrap().get(key) {
            let descriptor_arc = self.descriptors.get(descriptor_id);
            let descriptor = descriptor_arc.write().unwrap();
            let buffer = self.buffer_pool.get(descriptor.buffer_pool_id);
            let page = &buffer.write().unwrap().page;
            self.disk_manager.write(page, table_name).unwrap();
        }

        Ok(())
    }

    pub fn last_page_id(&self, table_name: &str) -> StorageResult<Option<PageID>> {
        self.disk_manager.last_page_id(table_name)
    }

    pub fn dirty_buffers(&self) -> Vec<Arc<RwLock<Buffer>>> {
        let mut v = Vec::new();
        for d in &self.descriptors.items {
            let d_ = d.read().unwrap();
            if d_.dirty {
                let b = self.buffer_pool.get(d_.buffer_pool_id);
                v.push(Arc::clone(&b));
            }
        }

        v
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use crate::{catalog::Catalog, storage::tuple::Tuple};

    use super::BufferPoolManager;

    const JSON: &str = r#"{
        "schemas": [
            {
                "table": {
                    "name": "buffer_pool_test",
                    "columns": [
                        {
                            "types": "int",
                            "name": "column_int"
                        },
                        {
                            "types": "text",
                            "name": "column_text"
                        }
                    ]
                }
            }
        ]
    }"#;

    #[test]
    #[should_panic]
    fn buffer_pool_manager_new_test_no_size() {
        let c = Catalog::from_json("");
        let _manager = BufferPoolManager::new(0, "dummy".to_string(), c);
    }

    #[test]
    fn buffer_pool_manager_write_and_flush() {
        let temp_dir = temp_dir();
        let catalog = Catalog::from_json(JSON);
        let mut manager =
            BufferPoolManager::new(1, temp_dir.to_str().unwrap().to_string(), catalog);

        let table_name = "buffer_pool_test";

        let page_id = {
            let buffer_locker = manager.new_buffer(table_name).unwrap();
            let mut buffer = buffer_locker.write().unwrap();
            let mut tuple = Tuple::new();
            tuple.add_attribute("column_int", crate::catalog::AttributeType::Int(888));
            tuple.add_attribute(
                "column_text",
                crate::catalog::AttributeType::Text("test".to_string()),
            );
            buffer.page.add_tuple(tuple);
            manager.unpin_buffer(buffer.page.id, table_name).unwrap();
            buffer.page.id
        };

        manager.flush_buffer(page_id, table_name).unwrap();

        let buffer_locker = manager.fetch_buffer(page_id, table_name).unwrap();
        let buffer = buffer_locker.read().unwrap();

        assert_eq!(buffer.page.header.tuple_count, 1);
    }

    #[test]
    fn buffer_pool_manager_victim() {
        let temp_dir = temp_dir();
        let catalog = Catalog::from_json(JSON);
        let mut manager =
            BufferPoolManager::new(1, temp_dir.to_str().unwrap().to_string(), catalog);

        let table_name = "buffer_pool_test";

        let page_id = {
            let buffer_locker = manager.new_buffer(table_name).unwrap();
            let mut buffer = buffer_locker.write().unwrap();
            let mut tuple = Tuple::new();
            tuple.add_attribute("column_int", crate::catalog::AttributeType::Int(888));
            tuple.add_attribute(
                "column_text",
                crate::catalog::AttributeType::Text("test".to_string()),
            );
            buffer.page.add_tuple(tuple);
            manager.unpin_buffer(buffer.page.id, table_name).unwrap();
            manager.mark_dirty(buffer.id).unwrap();
            buffer.page.id
        };

        // 明示的にflushしなくても、new_buffer時のvictimでdiskにwriteされる
        {
            let buffer_locker = manager.new_buffer(table_name).unwrap();
            let buffer = buffer_locker.read().unwrap();
            manager.unpin_buffer(buffer.page.id, table_name).unwrap();
        }

        let buffer_locker = manager.fetch_buffer(page_id, table_name).unwrap();
        let buffer = buffer_locker.read().unwrap();

        assert_eq!(buffer.page.header.tuple_count, 1);
    }
}
