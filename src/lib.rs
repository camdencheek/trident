#![feature(array_windows)]
#![feature(split_array)]

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use file_cursor::FileCursor;
use fnv::{FnvHashMap, FnvHashSet};
use serialize::{StreamWriter, U32Compressor, U32DeltaCompressor};

pub mod builder;
pub mod file_cursor;
pub mod serialize;

type Trigram = [u8; 3];

#[derive(Clone, Default)]
struct PostingHeader {
    unique_successors_len: u32,
    doc_lens_len: u32,
    successors_len: u32,
    doc_ids_len: u32,
}

impl StreamWriter for PostingHeader {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        w.write_u32::<LittleEndian>(self.unique_successors_len)?;
        w.write_u32::<LittleEndian>(self.doc_lens_len)?;
        w.write_u32::<LittleEndian>(self.successors_len)?;
        w.write_u32::<LittleEndian>(self.doc_ids_len)?;
        Ok(4 * std::mem::size_of::<u32>())
    }
}

fn extract_trigrams(padded_content: &[u8]) -> FnvHashMap<Trigram, FnvHashSet<Trigram>> {
    assert!(padded_content.iter().rev().take(3).all(|&c| c == 0xFF));

    let mut res: FnvHashMap<Trigram, FnvHashSet<Trigram>> = FnvHashMap::default();

    let mut add_trigrams = |t1: Trigram, t2: Trigram| {
        match res.get_mut(&t1) {
            Some(s) => {
                s.insert(t2);
            }
            None => {
                let mut s = FnvHashSet::default();
                s.insert(t2);
                res.insert(t1, s);
            }
        };
    };

    let trigrams = padded_content.array_windows::<3>().copied();
    let successors = trigrams.clone().skip(3);
    for (trigram, successor) in trigrams.zip(successors) {
        add_trigrams(trigram, successor);
    }

    res
}

fn trigram_as_int(t: Trigram) -> u32 {
    (t[0] as u32) << 16 + (t[1] as u32) << 8 + t[2] as u32
}
