use anyhow::Result;
use std::io::{self, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub trait StreamWrite {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize>;

    fn to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.write_to(&mut v)
            .expect("write_to should not fail if the underlying writer does not return an error");
        v
    }
}

impl StreamWrite for u8 {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        w.write_u8(*self)?;
        Ok(1)
    }
}

impl StreamWrite for u16 {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        w.write_u16::<BigEndian>(*self)?;
        Ok(2)
    }
}

impl StreamWrite for u32 {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        w.write_u32::<BigEndian>(*self)?;
        Ok(4)
    }
}

impl StreamWrite for u64 {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        w.write_u64::<BigEndian>(*self)?;
        Ok(8)
    }
}

impl<const N: usize> StreamWrite for [u8; N] {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        w.write(self)?;
        Ok(N)
    }
}

pub trait StreamRead
where
    Self: Sized,
{
    fn read_from<R: Read>(r: &mut R) -> Result<Self>;
}

impl StreamRead for u8 {
    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        Ok(r.read_u8()?)
    }
}

impl StreamRead for u16 {
    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        Ok(r.read_u16::<BigEndian>()?)
    }
}

impl StreamRead for u32 {
    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        Ok(r.read_u32::<BigEndian>()?)
    }
}

impl StreamRead for u64 {
    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        Ok(r.read_u64::<BigEndian>()?)
    }
}

impl<const N: usize> StreamRead for [u8; N] {
    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let mut buf = [0u8; N];
        r.read(&mut buf)?;
        Ok(buf)
    }
}
