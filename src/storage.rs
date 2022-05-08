use std::result;

pub mod disk_manager;

pub type StorageResult<T> = result::Result<T, anyhow::Error>;
