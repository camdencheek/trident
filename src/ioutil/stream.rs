use std::io::{self, Write};

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

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
