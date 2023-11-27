use super::BatchReader;

pub fn accept<V: BatchReaderVisitor>(
    reader: &dyn BatchReader,
    visitor: &mut V,
) -> Result<(), V::Error> {
    visitor.pre_visit(reader)?;
    for child in reader.children() {
        accept(child.as_ref(), visitor)?;
    }
    visitor.post_visit(reader)?;
    Ok(())
}

pub trait BatchReaderVisitor {
    type Error;

    fn pre_visit(&mut self, reader: &dyn BatchReader) -> Result<bool, Self::Error>;

    fn post_visit(&mut self, _reader: &dyn BatchReader) -> Result<bool, Self::Error> {
        Ok(true)
    }
}
