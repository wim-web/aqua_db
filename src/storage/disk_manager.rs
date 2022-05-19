use super::StorageResult;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

pub const PAGE_SIZE: usize = 4096;

pub struct DiskManager {
    file: File,
    next_index: usize,
}

impl DiskManager {
    pub fn new(file_path: impl AsRef<Path>) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .unwrap();

        let next_index = (file.metadata().unwrap().len() / PAGE_SIZE as u64) as usize;

        DiskManager { file, next_index }
    }

    pub fn read(&mut self, page_id: PageID) -> StorageResult<Page> {
        let mut page = Page {
            id: page_id,
            ..Default::default()
        };

        self.file.seek(SeekFrom::Start(page_id.offset() as u64))?;
        self.file.read_exact(&mut page.data)?;

        Ok(page)
    }

    pub fn write(&mut self, page: &Page) -> StorageResult<()> {
        self.file.seek(SeekFrom::Start(page.id.offset() as u64))?;
        self.file.write_all(&page.data)?;

        Ok(())
    }

    pub fn allocate_page(&mut self) -> StorageResult<Page> {
        let next_index = self.next_index;
        self.next_index += 1;

        let page = Page {
            id: PageID(next_index),
            ..Default::default()
        };

        self.write(&page)?;

        Ok(page)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Page {
    pub id: PageID,
    pub data: [u8; PAGE_SIZE],
    pub size: usize,
}

impl Page {
    pub fn new(id: PageID, data: [u8; PAGE_SIZE]) -> Self {
        Self {
            id,
            data,
            size: PAGE_SIZE,
        }
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            id: PageID(0),
            data: [0_u8; PAGE_SIZE],
            size: PAGE_SIZE,
        }
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Debug, Copy)]
pub struct PageID(pub usize);

impl PageID {
    pub fn value(&self) -> usize {
        self.0
    }

    fn offset(&self) -> usize {
        PAGE_SIZE * self.0
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    #[test]
    fn read_write() {
        let (_, p) = NamedTempFile::new().unwrap().into_parts();

        let mut manager = DiskManager::new(p);

        // 1回目のwrite & read
        let mut write_page1 = manager.allocate_page().unwrap();
        write_page1.data[..5].copy_from_slice(b"test1");

        manager.write(&write_page1).unwrap();

        let read_page1 = manager.read(write_page1.id).unwrap();

        assert_eq!(write_page1.data, read_page1.data);

        // 2回目のwrite & read
        let mut write_page2 = manager.allocate_page().unwrap();
        write_page2.data[..5].copy_from_slice(b"test2");

        manager.write(&write_page2).unwrap();

        let read_page2 = manager.read(write_page2.id).unwrap();

        assert_eq!(write_page1.data, read_page1.data);
        assert_eq!(write_page2.data, read_page2.data);
    }
}
