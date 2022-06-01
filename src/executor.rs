use crate::{
    catalog::AttributeType,
    storage::{
        buffer_pool::Buffer, buffer_pool_manager::BufferPoolManager, page::PageID,
        replacer::Replacer, tuple::Tuple,
    },
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

struct Executor<T>
where
    T: Replacer,
{
    buffer_pool_manager: BufferPoolManager<T>,
}

impl<T: Replacer> Executor<T> {
    pub fn new(buffer_pool_manager: BufferPoolManager<T>) -> Self {
        Self {
            buffer_pool_manager,
        }
    }

    fn find_writable_buffer(
        &mut self,
        table_name: &str,
    ) -> Result<Arc<RwLock<Buffer>>, anyhow::Error> {
        let b = match self.buffer_pool_manager.last_page_id(table_name)? {
            Some(p_id) => {
                let b = self.buffer_pool_manager.fetch_buffer(p_id, table_name)?;

                let can_add_tuple = {
                    let buf = b.read().unwrap();
                    buf.page.can_add_tuple()
                };

                if can_add_tuple {
                    b
                } else {
                    self.buffer_pool_manager.unpin_buffer(p_id)?;
                    self.buffer_pool_manager.new_buffer(table_name)?
                }
            }
            // まだテーブルにデータがないとき
            None => self.buffer_pool_manager.new_buffer(table_name)?,
        };

        Ok(Arc::clone(&b))
    }

    pub fn insert(
        &mut self,
        attributes: &HashMap<&str, AttributeType>,
        table_name: &str,
    ) -> Result<(), anyhow::Error> {
        let b = self.find_writable_buffer(table_name)?;

        {
            let mut b = b.write().unwrap();
            let mut t = Tuple::new();

            for (column, types) in attributes.iter() {
                t.add_attribute(column, types.clone());
            }

            b.page.add_tuple(t);
            self.buffer_pool_manager.unpin_buffer(b.page.id).unwrap();
        }

        Ok(())
    }

    pub fn scan(
        &mut self,
        table_name: &str,
        records: &mut Vec<HashMap<String, AttributeType>>,
    ) -> Result<(), anyhow::Error> {
        let last = match self.buffer_pool_manager.last_page_id(table_name)? {
            Some(PageID(n)) => n,
            None => return Ok(()),
        };

        for i in 0..=last {
            let b = self
                .buffer_pool_manager
                .fetch_buffer(PageID(i), table_name)?;

            let b = b.read().unwrap();
            for t in &b.page.body {
                records.push(t.body.attributes.clone());
            }
            self.buffer_pool_manager.unpin_buffer(b.page.id).unwrap();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env::temp_dir};

    use crate::catalog::Catalog;

    use super::*;

    const JSON: &str = r#"{
        "schemas": [
            {
                "table": {
                    "name": "executor_test",
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
    fn executor_insert_scan() {
        let temp_dir = temp_dir();
        let catalog = Catalog::from_json(JSON);
        let table_name = "executor_test";
        let b_manager = BufferPoolManager::new(1, temp_dir.to_str().unwrap().to_string(), catalog);
        let mut executor = Executor::new(b_manager);

        let mut attributes = HashMap::new();
        attributes.insert("column_int", AttributeType::Int(12));
        attributes.insert("column_text", AttributeType::Text("executor".to_string()));

        executor.insert(&attributes, table_name).unwrap();

        let mut records = Vec::new();

        executor.scan(table_name, &mut records).unwrap();

        assert_eq!(records.len(), 1);

        assert_eq!(records[0]["column_int"], AttributeType::Int(12));
        assert_eq!(
            records[0]["column_text"],
            AttributeType::Text("executor".to_string())
        );
    }
}
