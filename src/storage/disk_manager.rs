use anyhow::Ok;

use crate::catalog::Catalog;

use super::page::*;
use super::StorageResult;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
};

pub struct DiskManager {
    catalog: Catalog,
    base_path: String,
}

impl DiskManager {
    pub fn new(base_path: String, catalog: Catalog) -> Self {
        DiskManager { base_path, catalog }
    }

    fn open(&self, table_name: &str) -> StorageResult<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}/{}", self.base_path, table_name))?;

        Ok(file)
    }

    pub fn read(&mut self, page_id: PageID, table_name: &str) -> StorageResult<Page> {
        let mut file = self.open(table_name)?;

        let mut page = Page {
            id: page_id,
            ..Default::default()
        };

        let mut data = [0_u8; PAGE_SIZE];

        file.seek(SeekFrom::Start(page_id.offset() as u64))?;
        file.read_exact(&mut data)?;

        let schema = self
            .catalog
            .get_schema_by_table_name(table_name)
            .ok_or_else(|| anyhow::anyhow!(format!("{} not found in catalog", table_name)))?;

        page.fill(&data, schema);

        Ok(page)
    }

    pub fn write(&mut self, page: &Page, table_name: &str) -> StorageResult<()> {
        let mut file = self.open(table_name)?;

        let schema = self
            .catalog
            .get_schema_by_table_name(table_name)
            .ok_or_else(|| anyhow::anyhow!(format!("{} not found in catalog", table_name)))?;

        file.seek(SeekFrom::Start(page.id.offset() as u64))?;
        file.write_all(&page.raw(schema))?;

        Ok(())
    }

    pub fn allocate_page(&mut self, table_name: &str) -> StorageResult<Page> {
        let file = self.open(table_name)?;

        let offset = (file.metadata().unwrap().len() / PAGE_SIZE as u64) as usize;

        let page = Page {
            id: PageID(offset),
            ..Default::default()
        };

        self.write(&page, table_name)?;

        Ok(page)
    }

    pub fn last_page_id(&self, table_name: &str) -> StorageResult<Option<PageID>> {
        let file = self.open(table_name)?;
        let page_num = file.metadata()?.len() as usize / PAGE_SIZE;

        if page_num == 0 {
            Ok(None)
        } else {
            Ok(Some(PageID(page_num - 1)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use crate::{catalog::AttributeType, storage::tuple::Tuple};

    use super::*;

    const JSON: &str = r#"{
        "schemas": [
            {
                "table": {
                    "name": "table1",
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
    fn disk_read_write() {
        let temp_dir = temp_dir();
        let c = Catalog::from_json(JSON);

        let mut manager = DiskManager::new(temp_dir.to_str().unwrap().to_string(), c);

        let mut page = manager.allocate_page("table1").unwrap();
        let mut tuple = Tuple::new();
        tuple.add_attribute("column_int", AttributeType::Int(999));
        tuple.add_attribute("column_text", AttributeType::Text("text".to_string()));
        page.add_tuple(tuple);

        manager.write(&page, "table1").unwrap();

        let page = manager.read(page.id, "table1").unwrap();

        assert_eq!(1, page.header.tuple_count);
        let tuple = &page.body[0];

        match &tuple.body.attributes["column_int"] {
            AttributeType::Int(v) => assert_eq!(999, *v),
            _ => panic!("strange column_int"),
        }

        match &tuple.body.attributes["column_text"] {
            AttributeType::Text(v) => assert_eq!(v, "text"),
            _ => panic!("strange column_text"),
        }
    }
}
