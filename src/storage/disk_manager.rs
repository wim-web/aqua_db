use super::StorageResult;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

pub const PAGE_SIZE: usize = 4096;

pub struct DiskManager<T>
where
    T: AsRef<Path>,
{
    file_path: T,
    next_index: usize,
}

impl<T> DiskManager<T>
where
    T: AsRef<Path>,
{
    pub fn new(file_path: T) -> Self {
        let mut manager = DiskManager {
            file_path,
            next_index: 0,
        };

        let f = manager.open().unwrap();
        manager.next_index = (f.metadata().unwrap().len() / PAGE_SIZE as u64) as usize;

        manager
    }

    fn open(&self) -> StorageResult<File> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.file_path)?;

        Ok(f)
    }

    pub fn read(&self, page_id: PageID) -> StorageResult<Page> {
        let mut page = Page {
            id: page_id,
            ..Default::default()
        };
        let mut file = self.open()?;

        file.seek(SeekFrom::Start(page_id.offset() as u64))?;
        file.read_exact(&mut page.data)?;

        Ok(page)
    }

    pub fn write(&self, page_id: PageID, page: &Page) -> StorageResult<()> {
        let mut file = self.open()?;
        file.seek(SeekFrom::Start(page_id.offset() as u64))?;
        file.write_all(&page.data)?;

        Ok(())
    }

    pub fn allocate_page(&mut self) -> StorageResult<PageID> {
        let next_index = self.next_index;

        self.next_index += 1;

        Ok(PageID(next_index))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Page {
    pub id: PageID,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(id: PageID, data: [u8; PAGE_SIZE]) -> Self {
        Self { id, data }
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            id: PageID(0),
            data: [0_u8; PAGE_SIZE],
        }
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Debug, Copy)]
pub struct PageID(usize);

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
        let mut write_page1 = Page::default();
        write_page1.data[..5].copy_from_slice(b"test1");

        let id1 = manager.allocate_page().unwrap();
        manager.write(id1, &write_page1).unwrap();

        let read_page1 = manager.read(id1).unwrap();

        assert_eq!(write_page1.data, read_page1.data);

        // 2回目のwrite & read
        let mut write_page2 = Page::default();
        write_page2.data[..5].copy_from_slice(b"test2");

        let id2 = manager.allocate_page().unwrap();
        manager.write(id2, &write_page2).unwrap();

        let read_page2 = manager.read(id2).unwrap();

        assert_eq!(write_page1.data, read_page1.data);
        assert_eq!(write_page2.data, read_page2.data);
    }
}
