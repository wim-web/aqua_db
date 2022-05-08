#[derive(Copy, Clone)]
pub struct BufferPoolID(pub usize);

impl BufferPoolID {
    pub fn value(&self) -> usize {
        self.0
    }
}
