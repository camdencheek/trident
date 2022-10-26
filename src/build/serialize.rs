use std::io::{Read, Write};
use std::ops::Range;

use anyhow::Result;
use bitpacking::{BitPacker, BitPacker4x};
use integer_encoding::{VarIntReader, VarIntWriter};

pub trait StreamWriter {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize>;
}

pub struct U32Compressor<'a>(pub &'a [u32]);

impl StreamWriter for U32Compressor<'_> {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        let mut size = 0;
        let mut chunks = self.0.chunks_exact(BitPacker4x::BLOCK_LEN);

        {
            let bp = BitPacker4x::new();
            let mut buf = [0u8; 4 * BitPacker4x::BLOCK_LEN];
            for chunk in chunks.by_ref() {
                let num_bits = bp.num_bits(&chunk);
                size += w.write(&[num_bits])?;
                let n = bp.compress(&chunk, &mut buf, num_bits);
                size += w.write(&buf[..n])?;
            }
        }

        for i in chunks.remainder() {
            size += w.write_varint(*i)?;
        }

        Ok(size)
    }
}

pub struct U32DeltaCompressor<'a>(pub &'a [u32]);

impl StreamWriter for U32DeltaCompressor<'_> {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        assert!(self.0.is_sorted());
        let mut size = 0;
        let mut chunks = self.0.chunks_exact(BitPacker4x::BLOCK_LEN);
        let mut last = 0;
        {
            let bp = BitPacker4x::new();
            let mut buf = [0u8; 4 * BitPacker4x::BLOCK_LEN];
            for chunk in chunks.by_ref() {
                let num_bits = bp.num_bits_sorted(last, &chunk);
                size += w.write(&[num_bits])?;
                let n = bp.compress_sorted(last, &chunk, &mut buf, num_bits);
                size += w.write(&buf[..n])?;
                last = *chunk.last().unwrap();
            }
        }

        for i in chunks.remainder() {
            size += w.write_varint(*i - last)?;
            last = *i;
        }

        Ok(size)
    }
}

pub struct U32Decompressor<R: Read> {
    r: R,
    remaining: usize,
    chunk: [u32; BitPacker4x::BLOCK_LEN],
    chunk_range: Range<usize>,
    buf: [u8; BitPacker4x::BLOCK_LEN * 4],
}

impl<R: Read> Iterator for U32Decompressor<R> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self.chunk_range.next() {
            Some(n) => Some(self.chunk[n]),
            None => {
                self.populate_next_chunk();
                Some(self.chunk[self.chunk_range.next()?])
            }
        }
    }
}

impl<R: Read> U32Decompressor<R> {
    pub fn new(r: R, count: usize) -> Self {
        Self {
            remaining: count,
            r,
            chunk: [0u32; BitPacker4x::BLOCK_LEN],
            chunk_range: 0..0,
            buf: [0u8; BitPacker4x::BLOCK_LEN * 4],
        }
    }

    fn populate_next_chunk(&mut self) {
        if self.remaining >= BitPacker4x::BLOCK_LEN {
            let bp = BitPacker4x::new();
            let num_bits = {
                let mut buf = [0; 1];
                self.r.read_exact(&mut buf).unwrap();
                buf[0]
            };
            let num_bytes = num_bits as usize * BitPacker4x::BLOCK_LEN / 8;
            self.r.read_exact(&mut self.buf[..num_bytes]).unwrap();
            let n = bp.decompress(&self.buf[..num_bytes], &mut self.chunk, num_bits);
            self.chunk_range = 0..BitPacker4x::BLOCK_LEN;
            assert!(n == num_bytes);
            self.remaining -= BitPacker4x::BLOCK_LEN;
        } else {
            for i in 0..self.remaining {
                self.chunk[i] = self.r.read_varint().unwrap();
            }
            self.chunk_range = 0..self.remaining;
            self.remaining = 0;
        }
    }
}

pub struct U32DeltaDecompressor<R: Read> {
    r: R,
    remaining: usize,
    chunk: [u32; BitPacker4x::BLOCK_LEN],
    chunk_range: Range<usize>,
    buf: [u8; BitPacker4x::BLOCK_LEN * 4],
}

impl<R: Read> Iterator for U32DeltaDecompressor<R> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self.chunk_range.next() {
            Some(n) => Some(self.chunk[n]),
            None => {
                self.populate_next_chunk();
                Some(self.chunk[self.chunk_range.next()?])
            }
        }
    }
}

impl<R: Read> U32DeltaDecompressor<R> {
    pub fn new(r: R, count: usize) -> Self {
        Self {
            remaining: count,
            r,
            chunk: [0u32; BitPacker4x::BLOCK_LEN],
            chunk_range: 0..0,
            buf: [0u8; BitPacker4x::BLOCK_LEN * 4],
        }
    }

    fn populate_next_chunk(&mut self) {
        if self.remaining >= BitPacker4x::BLOCK_LEN {
            let bp = BitPacker4x::new();
            let num_bits = {
                let mut buf = [0; 1];
                self.r.read_exact(&mut buf).unwrap();
                assert!(buf[0] < 32);
                buf[0]
            };
            let num_bytes = num_bits as usize * BitPacker4x::BLOCK_LEN / 8;
            self.r.read_exact(&mut self.buf[..num_bytes]).unwrap();
            let n = bp.decompress_sorted(
                self.chunk[BitPacker4x::BLOCK_LEN - 1],
                &self.buf[..num_bytes],
                &mut self.chunk,
                num_bits,
            );
            self.chunk_range = 0..BitPacker4x::BLOCK_LEN;
            assert!(n == num_bytes);
            self.remaining -= BitPacker4x::BLOCK_LEN;
        } else {
            let mut last = self.chunk[BitPacker4x::BLOCK_LEN - 1];
            for i in 0..self.remaining {
                self.chunk[i] = self.r.read_varint::<u32>().unwrap() + last;
                last = self.chunk[i];
            }
            self.chunk_range = 0..self.remaining;
            self.remaining = 0;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck::quickcheck;
    use std::io::Cursor;

    quickcheck! {
        fn compress_roundtrip(input: Vec<u32>) -> bool {
            let mut buf = Vec::new();
            U32Compressor(input.as_slice()).write_to(&mut buf).unwrap();
            let decompressor = U32Decompressor::new(Cursor::new(buf), input.len());
            let output: Vec<u32> = decompressor.collect();
            input == output
        }
    }

    quickcheck! {
        fn compress_roundtrip_delta(input: Vec<u32>) -> bool {
            let mut input = input;
            input.sort();
            let mut buf = Vec::new();
            U32DeltaCompressor(input.as_slice()).write_to(&mut buf).unwrap();
            let decompressor = U32DeltaDecompressor::new(Cursor::new(buf), input.len());
            let output: Vec<u32> = decompressor.collect();
            input == output
        }
    }
}
