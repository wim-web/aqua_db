use std::{
    ops::{Index, IndexMut},
    sync::Mutex,
};

use super::buffer_pool::BufferPoolID;

pub struct Descriptors {
    pub items: Vec<Mutex<Descriptor>>,
}

impl Index<DescriptorID> for Descriptors {
    type Output = Mutex<Descriptor>;

    fn index(&self, index: DescriptorID) -> &Self::Output {
        &self.items[index.value()]
    }
}

impl IndexMut<DescriptorID> for Descriptors {
    fn index_mut(&mut self, index: DescriptorID) -> &mut Self::Output {
        &mut self.items[index.value()]
    }
}

impl Descriptors {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let mut items = Vec::with_capacity(size);

        for n in 0..size {
            items.push(Mutex::new(Descriptor::new(
                DescriptorID(n),
                BufferPoolID(n),
            )));
        }

        Self { items }
    }
}

#[derive(Clone, Copy)]
pub struct Descriptor {
    pub id: DescriptorID,
    pub dirty: bool,
    pub pool_id: BufferPoolID,
    pin_count: usize,
}

impl Descriptor {
    pub fn new(id: DescriptorID, pool_id: BufferPoolID) -> Self {
        Self {
            id,
            dirty: false,
            pool_id,
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
}

#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct DescriptorID(pub usize);

impl DescriptorID {
    fn value(&self) -> usize {
        self.0
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
