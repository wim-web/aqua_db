use std::result;

mod buffer_pool;
mod descriptors;
pub mod disk_manager;
mod hash_table;

pub type StorageResult<T> = result::Result<T, anyhow::Error>;
