use std::result;

pub mod disk_manager;
mod hash_table;

pub type StorageResult<T> = result::Result<T, anyhow::Error>;
