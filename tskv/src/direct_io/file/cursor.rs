use std::{
    convert::TryInto,
    io::{Error, ErrorKind, Result, SeekFrom},
    ops::Deref,
};

use super::File;

#[derive(Clone)]
pub struct FileCursor {
    file: File,
    pos: u64,
}

impl FileCursor {
    pub fn into_file(self) -> File {
        self.file
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    pub fn set_pos(&mut self, pos: u64) {
        self.pos = pos;
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let read = self.file.read_at(self.pos, buf)?;
        self.seek(SeekFrom::Current(read.try_into().unwrap()))
            .unwrap();
        Ok(read)
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let size = self.file.write_at(self.pos, buf)?;
        self.seek(SeekFrom::Current(buf.len().try_into().unwrap()))
            .unwrap();
        Ok(size)
    }

    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.pos = match pos {
            SeekFrom::Start(pos) => Some(pos),
            SeekFrom::End(delta) => {
                if delta >= 0 {
                    self.len().checked_add(delta as u64)
                } else {
                    self.len().checked_sub(-delta as u64)
                }
            }
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.pos.checked_add(delta as u64)
                } else {
                    self.pos.checked_sub(-delta as u64)
                }
            }
        }
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "underflow or overflow during seek"))?;
        Ok(self.pos)
    }
}

impl From<File> for FileCursor {
    fn from(file: File) -> Self {
        FileCursor { file, pos: 0 }
    }
}

impl Deref for FileCursor {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

#[cfg(test)]
mod test {
    use std::io::{prelude::*, BufWriter};

    use tempfile::NamedTempFile;

    use crate::direct_io::*;

    #[test]
    fn copy() {
        let fs = FileSystem::new(Options::default().max_resident(2).max_non_resident(2));

        let file_len: usize = (fs.max_page_len() as f64 * 5.3) as usize;

        let mut src = NamedTempFile::new().unwrap();
        let dst = NamedTempFile::new().unwrap();
        {
            let mut f = BufWriter::new(&mut src);
            for i in 0..file_len {
                f.write_all(&[i as u8]).unwrap();
            }
            f.flush().unwrap();
        }

        {
            let mut src_d = fs.open(src.path()).unwrap().into_cursor();
            let mut dst_d = fs.open(dst.path()).unwrap().into_cursor();

            let buf = &mut [0];
            while src_d.read(buf).unwrap() == 1 {
                dst_d.write(buf).unwrap();
            }
        }

        {
            let mut dst_d = fs.open(dst.path()).unwrap().into_cursor();
            let buf = &mut [0];
            for i in 0..file_len {
                assert_eq!(dst_d.read(buf).unwrap(), 1);
                assert_eq!(buf[0], i as u8);
            }
            assert_eq!(dst_d.read(buf).unwrap(), 0);
        }
    }
}
