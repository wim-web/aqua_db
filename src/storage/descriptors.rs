use std::sync::{Arc, RwLock};

use super::buffer_pool::{Buffer, BufferPoolID};

type DescriptorLockRef = Arc<RwLock<Descriptor>>;

pub struct Descriptors {
    pub items: Vec<DescriptorLockRef>,
}

impl Descriptors {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let mut items = Vec::with_capacity(size);

        for n in 0..size {
            let buffer_pool_id = BufferPoolID(n);
            let id = DescriptorID::from_buf_pool_id(buffer_pool_id);

            items.push(Arc::new(RwLock::new(Descriptor::new(id, buffer_pool_id))));
        }

        Self { items }
    }

    pub fn get(&self, id: DescriptorID) -> DescriptorLockRef {
        Arc::clone(&self.items[id.value()])
    }
}

#[derive(Clone, Copy)]
pub struct Descriptor {
    pub id: DescriptorID,
    pub dirty: bool,
    pub buffer_pool_id: BufferPoolID,
    pin_count: usize,
}

impl Descriptor {
    pub fn new(id: DescriptorID, buffer_pool_id: BufferPoolID) -> Self {
        Self {
            id,
            dirty: false,
            buffer_pool_id,
            pin_count: 0,
        }
    }

    pub fn pin(&mut self) {
        self.pin_count += 1
    }

    pub fn unpin(&mut self) {
        self.pin_count -= 1
    }

    pub fn pinned(&self) -> bool {
        self.pin_count > 0
    }

    pub fn reset(&mut self) {
        self.dirty = false;
        self.pin_count = 0;
    }
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct DescriptorID(pub usize);

impl DescriptorID {
    fn value(&self) -> usize {
        self.0
    }

    pub fn from_buf_pool_id(buffer_pool_id: BufferPoolID) -> Self {
        Self(buffer_pool_id.value())
    }
}

mod tests {
    use crate::storage::buffer_pool::BufferPoolID;

    use super::{Descriptor, DescriptorID, Descriptors};

    #[test]
    #[should_panic]
    fn new_no_size() {
        let _descriptors = Descriptors::new(0);
    }

    #[test]
    fn pin_descriptor() {
        let mut d = Descriptor::new(DescriptorID(0), BufferPoolID(0));

        assert!(!d.pinned());

        d.pin();

        assert!(d.pinned());

        d.unpin();

        assert!(!d.pinned());
    }
}
