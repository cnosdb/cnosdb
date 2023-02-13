/// Wraps an iterator with `peek()` method that returns an optional reference
/// to the next element.
#[derive(Clone, Debug)]
pub struct BufferedIterator<I: Iterator> {
    iter: I,
    /// Remember a peeked value, even if it was None.
    peeked: Option<Option<I::Item>>,
}

impl<I: Iterator> BufferedIterator<I> {
    pub fn new(iter: I) -> BufferedIterator<I> {
        BufferedIterator { iter, peeked: None }
    }

    pub fn peek(&mut self) -> Option<&I::Item> {
        let iter = &mut self.iter;
        self.peeked.get_or_insert_with(|| iter.next()).as_ref()
    }

    pub fn peek_mut(&mut self) -> Option<&mut I::Item> {
        let iter = &mut self.iter;
        self.peeked.get_or_insert_with(|| iter.next()).as_mut()
    }

    pub fn next(&mut self) -> Option<&I::Item> {
        let iter = &mut self.iter;
        self.peeked.insert(iter.next()).as_ref()
    }

    pub fn next_mut(&mut self) -> Option<&mut I::Item> {
        let iter = &mut self.iter;
        self.peeked.insert(iter.next()).as_mut()
    }
}
