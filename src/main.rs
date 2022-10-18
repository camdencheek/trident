#![feature(array_windows)]
#![feature(split_array)]

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use file_cursor::FileCursor;
use fnv::{FnvHashMap, FnvHashSet};
use serialize::{StreamWriter, U32Compressor, U32DeltaCompressor};
use walkdir::WalkDir;

mod file_cursor;
mod serialize;

type Trigram = [u8; 3];

fn main() -> Result<()> {
    let documents = WalkDir::new("/Users/camdencheek/Downloads/srcs/linux-master")
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file());

    let mut combined: FnvHashMap<Trigram, Vec<(u32, FnvHashSet<Trigram>)>> = FnvHashMap::default();

    let mut contents = Vec::with_capacity(2 * 1024 * 1024 + 3);
    let mut total_content_size = 0;
    for (id, entry) in documents.enumerate() {
        let mut f = File::open(entry.path())?;
        // if f.metadata()?.len() > 2 * 1024 * 1024 {
        //     println!("skipping too large file {:?}", entry.path());
        //     continue;
        // }
        contents.clear();
        contents.reserve(f.metadata()?.len() as usize + 3);
        f.read_to_end(&mut contents)?;
        if let Err(e) = std::str::from_utf8(&mut contents) {
            println!("skipping non-utf8 file {:?}: {}", entry.path(), e);
            continue;
        };
        contents.make_ascii_lowercase();
        contents.extend_from_slice(&[0xFF, 0xFF, 0xFF]);
        total_content_size += contents.len();
        for (trigram, set) in extract_trigrams(&contents) {
            match combined.get_mut(&trigram) {
                Some(v) => v.push((id as u32, set)),
                None => {
                    combined.insert(trigram, vec![(id as u32, set)]);
                }
            }
        }
    }

    let mut output_file = BufWriter::new(File::create("/tmp/output.trgm")?);

    let mut doc_ids = Vec::new();
    let mut doc_lens = Vec::new();
    let mut unique_successors: FnvHashSet<Trigram> = FnvHashSet::default();
    let mut unique_successor_ids: Vec<u32> = Vec::new();
    let mut successor_ids: Vec<u32> = Vec::new();
    let mut total_index_size = 0;
    let mut buf = contents;

    let mut offsets: Vec<(Trigram, u32)> = Vec::new();

    for (trigram, docs) in combined.iter() {
        doc_ids.clear();
        doc_lens.clear();
        unique_successors.clear();
        unique_successor_ids.clear();
        successor_ids.clear();
        buf.clear();

        for (id, successors) in docs {
            doc_ids.push(*id);
            doc_lens.push(successors.len() as u32);
            unique_successors.extend(successors);
        }

        // Convert unique successor trigrams into trigram IDs.
        unique_successor_ids.extend(unique_successors.iter().copied().map(trigram_as_int));
        unique_successor_ids.sort();

        for (_, successors) in docs {
            let last_successor_id = successor_ids.last().copied().unwrap_or(0);

            successor_ids.extend(
                successors
                    .into_iter()
                    .copied()
                    .map(trigram_as_int)
                    .map(|id| unique_successor_ids.binary_search(&id).unwrap() as u32)
                    .map(|local_id| local_id + last_successor_id),
            );
            let l = successor_ids.len();
            successor_ids[l - successors.len()..].sort()
        }

        let unique_successor_id_bytes =
            U32DeltaCompressor(&unique_successor_ids).write_to(&mut buf)?;
        let doc_len_bytes = U32Compressor(&doc_lens).write_to(&mut buf)?;
        let successor_id_bytes = U32DeltaCompressor(&successor_ids).write_to(&mut buf)?;
        let doc_bytes = U32DeltaCompressor(&doc_ids).write_to(&mut buf)?;

        let header = PostingHeader {
            unique_successors_len: unique_successor_id_bytes as u32,
            doc_lens_len: doc_len_bytes as u32,
            successors_len: successor_id_bytes as u32,
            doc_ids_len: doc_bytes as u32,
        };

        offsets.push((*trigram, output_file.seek(SeekFrom::Current(0))? as u32));
        header.write_to(&mut output_file)?;
        output_file.write_all(&buf)?;
    }

    for (trigram, offset) in &offsets {
        output_file.write_all(trigram)?;
        output_file.write_u32::<LittleEndian>(*offset)?;
    }
    output_file.write_u32::<LittleEndian>(offsets.len() as u32 * 7)?;

    let index_size = output_file.seek(SeekFrom::Current(0))?;

    println!(
        "Content size: {}, Compressed size: {}, Compression ratio: {:.3}\n",
        total_content_size,
        index_size,
        index_size as f64 / total_content_size as f64
    );

    std::process::exit(0);
}

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
