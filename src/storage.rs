use std::result;

mod buffer_pool;
pub mod buffer_pool_manager;
mod descriptors;
pub mod disk_manager;
mod hash_table;
mod replacer;

pub type StorageResult<T> = result::Result<T, anyhow::Error>;
