use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

use crate::build::serialize::StreamWriter;

#[derive(Clone)]
pub struct FileCursor {
    f: Arc<File>,
    offset: u64,
}

impl FileCursor {
    pub fn new(f: File) -> Self {
        Self {
            f: Arc::new(f),
            offset: 0,
        }
    }
}

impl Read for FileCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.f.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.f.read_exact_at(buf, self.offset)?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}

// impl Write for FileCursor {
//     fn write(&mut self, buf: &[u8]) -> Result<usize> {
//         let n = self.f.write_at(buf, self.offset)?;
//         self.offset += n as u64;
//         Ok(n)
//     }

//     fn write_all(&mut self, buf: &[u8]) -> Result<()> {
//         self.f.write_all_at(buf, self.offset)?;
//         self.offset += buf.len() as u64;
//         Ok(())
//     }

//     fn flush(&mut self) -> Result<()> {
//         self.f.flush()
//     }
// }

impl Seek for FileCursor {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Current(i) => self.offset = (self.offset as i64 + i) as u64,
            SeekFrom::Start(i) => self.offset = i,
            SeekFrom::End(i) => self.offset = (self.f.metadata()?.len() as i64 + i) as u64,
        };
        Ok(self.offset)
    }
}

pub trait SectionType {}
impl<T: SectionType> SectionType for Section<T> {}
impl SectionType for () {}

#[derive(Debug, Copy, Clone)]
pub struct Section<P: SectionType = ()> {
    pub offset: u64,
    pub len: u64,
    _parent_type: PhantomData<P>,
}

impl<T: SectionType> StreamWriter for Section<T> {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        w.write_u64::<LittleEndian>(self.offset)?;
        w.write_u64::<LittleEndian>(self.len)?;
        Ok(std::mem::size_of::<u64> as usize * 2)
    }
}

impl<P: SectionType> Section<P> {
    pub fn new(offset: u64, len: u64) -> Self {
        Self {
            offset,
            len,
            _parent_type: PhantomData::<P>,
        }
    }

    pub fn narrow(&self, child: Section<Self>) -> Self {
        assert!(child.offset + child.len <= self.len);
        Self::new(self.offset + child.offset, child.len)
    }
}
