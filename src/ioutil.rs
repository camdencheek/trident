use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;

use crate::build::serialize::StreamWriter;

pub trait ReadAt {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    // TODO add an optional read_exact_at
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
}

impl<F: FileExt> ReadAt for F {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.read_at(buf, offset)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.read_exact_at(buf, offset)
    }
}

pub trait Len {
    fn len(&self) -> io::Result<u64>;
}

impl Len for File {
    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|m| m.len())
    }
}

pub struct Mem(pub Vec<u8>);

impl ReadAt for Mem {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let sz = buf.len().min(self.0.len() - offset as usize);
        buf[..sz].copy_from_slice(&self.0[offset as usize..offset as usize + sz]);
        Ok(sz)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let sz = buf.len().min(self.0.len() - offset as usize);
        if sz != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "could not fill buffer",
            ));
        }
        buf[..sz].copy_from_slice(&self.0[offset as usize..offset as usize + sz]);
        Ok(())
    }
}

impl Len for Mem {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.len() as u64)
    }
}

#[derive(Clone)]
pub struct Cursor<T> {
    r: T,
    offset: u64,
}

impl<T> Cursor<T> {
    pub fn new(r: T) -> Self {
        Self { r, offset: 0 }
    }

    pub fn new_in(r: T, section: Section) -> Self {
        Self {
            r,
            offset: section.offset,
        }
    }
}

impl<T> Read for Cursor<&T>
where
    T: ReadAt,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.r.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.r.read_exact_at(buf, self.offset)?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}

impl<T: Len> Seek for Cursor<&T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Current(i) => self.offset = (self.offset as i64 + i) as u64,
            SeekFrom::Start(i) => self.offset = i,
            SeekFrom::End(i) => self.offset = (self.r.len()? as i64 + i) as u64,
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
