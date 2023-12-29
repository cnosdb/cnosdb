use std::fmt;

use super::visitor::BatchReaderVisitor;
use super::BatchReader;
use crate::reader::visitor;

pub struct DisplayableBatchReader<'a> {
    inner: &'a dyn BatchReader,
}

impl<'a> DisplayableBatchReader<'a> {
    pub fn new(inner: &'a dyn BatchReader) -> Self {
        Self { inner }
    }

    pub fn indent(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            reader: &'a dyn BatchReader,
        }
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let mut visitor = IndentVisitor { f, indent: 0 };
                visitor::accept(self.reader, &mut visitor)
            }
        }
        Wrapper { reader: self.inner }
    }
}

struct IndentVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    /// Indent size
    indent: usize,
}

impl<'a, 'b> BatchReaderVisitor for IndentVisitor<'a, 'b> {
    type Error = fmt::Error;
    fn pre_visit(&mut self, reader: &dyn BatchReader) -> Result<bool, Self::Error> {
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        reader.fmt_as(self.f)?;
        writeln!(self.f)?;
        self.indent += 1;
        Ok(true)
    }

    fn post_visit(&mut self, _reader: &dyn BatchReader) -> Result<bool, Self::Error> {
        self.indent -= 1;
        Ok(true)
    }
}
