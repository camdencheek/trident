use anyhow::Result;
use bitpacking::{BitPacker, BitPacker1x};
use integer_encoding::VarIntWriter;
use std::io::{Cursor, Read, Write};

pub trait StreamWriter {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize>;
}

pub struct U32Compressor<'a>(pub &'a [u32]);

impl StreamWriter for U32Compressor<'_> {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        let mut size = 0;

        let mut chunks = self.0.chunks_exact(BitPacker1x::BLOCK_LEN);

        let mut last = 0;
        {
            let bp = BitPacker1x::new();
            let mut buf = [0u8; 4 * BitPacker1x::BLOCK_LEN];
            for chunk in chunks.by_ref() {
                let n = bp.compress(&chunk, &mut buf, bp.num_bits(&chunk));
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

pub struct U32DeltaCompressor<'a>(pub &'a [u32]);

impl StreamWriter for U32DeltaCompressor<'_> {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        let mut size = 0;

        let mut chunks = self.0.chunks_exact(BitPacker1x::BLOCK_LEN);

        let mut last = 0;
        {
            let bp = BitPacker1x::new();
            let mut buf = [0u8; 4 * BitPacker1x::BLOCK_LEN];
            for chunk in chunks.by_ref() {
                let n = bp.compress_sorted(0, &chunk, &mut buf, bp.num_bits_sorted(0, &chunk));
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
