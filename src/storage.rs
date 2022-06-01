use std::result;

pub mod buffer_pool;
pub mod buffer_pool_manager;
mod descriptors;
pub mod disk_manager;
mod hash_table;
pub mod page;
pub mod replacer;
pub mod tuple;

pub type StorageResult<T> = result::Result<T, anyhow::Error>;
