#![feature(array_windows)]
#![feature(split_array)]

use anyhow::Result;
use bitpacking::{BitPacker, BitPacker4x};
use fnv::{FnvHashMap, FnvHashSet};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufWriter, Cursor, Read, Seek, Write},
};
use varint_rs::VarintWriter;
use walkdir::WalkDir;

type Trigram = [u8; 3];

fn main() -> Result<()> {
    let documents = WalkDir::new("/Users/camdencheek/src/linux")
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file());

    let mut combined: BTreeMap<Trigram, Vec<(u32, FnvHashSet<Trigram>)>> = BTreeMap::new();

    let mut contents = Vec::new();
    let mut total_content_size = 0;
    for (id, entry) in documents.enumerate() {
        let mut f = File::open(entry.path())?;
        contents.clear();
        contents.reserve(f.metadata()?.len() as usize + 3);
        f.read_to_end(&mut contents)?;
        if let Err(e) = std::str::from_utf8(&mut contents) {
            println!("failed to read string for file {:?}: {}", entry.path(), e);
            continue;
        };
        contents.extend_from_slice(&[0xFF, 0xFF, 0xFF]);
        total_content_size += contents.len();
        for (trigram, set) in file_trigrams(&contents) {
            match combined.get_mut(&trigram) {
                Some(v) => v.push((id as u32, set)),
                None => {
                    let mut v = Vec::with_capacity(16);
                    v.push((id as u32, set));
                    combined.insert(trigram, v);
                }
            }
        }
    }

    let mut trigram_ids = FnvHashMap::default();
    for (id, trigram) in combined.keys().enumerate() {
        trigram_ids.insert(*trigram, id as u32);
    }

    let compressed_size_delta = |ints: &[u32]| -> usize {
        let mut size = 0;
        let bp = BitPacker4x::new();

        let chunks = ints.chunks_exact(BitPacker4x::BLOCK_LEN);

        let mut buffer: Cursor<Vec<u8>> =
            Cursor::new(Vec::with_capacity(4 * chunks.remainder().len()));
        let mut last = 0;
        for i in chunks.remainder() {
            buffer.write_u32_varint(*i - last).unwrap();
            last = *i;
        }
        size += buffer.seek(std::io::SeekFrom::Current(0)).unwrap() as usize;

        let mut compressed = [0u8; 4 * BitPacker4x::BLOCK_LEN];
        for chunk in chunks {
            size += bp.compress_sorted(0, &chunk, &mut compressed, bp.num_bits_sorted(0, &chunk));
        }

        size
    };

    let compressed_size = |ints: &[u32]| -> usize {
        let mut size = 0;
        let bp = BitPacker4x::new();

        let chunks = ints.chunks_exact(BitPacker4x::BLOCK_LEN);

        let mut buffer: Cursor<Vec<u8>> =
            Cursor::new(Vec::with_capacity(4 * chunks.remainder().len()));
        for i in chunks.remainder() {
            buffer.write_u32_varint(*i).unwrap();
        }
        size += buffer.seek(std::io::SeekFrom::Current(0)).unwrap() as usize;

        let mut compressed = [0u8; 4 * BitPacker4x::BLOCK_LEN];
        for chunk in chunks {
            size += bp.compress(&chunk, &mut compressed, bp.num_bits(&chunk));
        }

        size
    };

    let mut buf = BufWriter::new(std::io::stdout().lock());
    let mut doc_ids = Vec::new();
    let mut doc_lens = Vec::new();
    let mut successor_ids = Vec::new();
    let mut total_index_size = 0;
    for (trigram, docs) in combined.into_iter() {
        doc_ids.clear();
        doc_lens.clear();
        successor_ids.clear();

        for (id, successors) in docs.into_iter() {
            doc_ids.push(id);
            doc_lens.push(successors.len() as u32 - 1);

            let last_successor_id = successor_ids.last().cloned().unwrap_or(0);
            successor_ids.extend(successors.into_iter().map(|s| {
                trigram_ids
                    .get(&s)
                    .unwrap_or_else(|| panic!("unknown trigram {:?}", s))
                    .clone()
                    + last_successor_id
            }))
        }

        write!(&mut buf, "Trigram {:?}:\n", trigram)?;
        let doc_bytes = compressed_size_delta(&doc_ids);
        write!(
            &mut buf,
            "\tDoc IDs: {} ids, {} bits, {:0.3} bits/id:\n",
            doc_ids.len(),
            doc_bytes * 8,
            doc_bytes as f64 * 8.0 / doc_ids.len() as f64
        )?;
        if doc_ids.len() < 5 {
            write!(&mut buf, "\t\t{:?}\n", doc_ids)?;
        }
        let doc_len_bytes = compressed_size(&doc_lens);
        write!(
            &mut buf,
            "\tDoc Lens: {} lens, {} bits, {:0.3} bits/len:\n",
            doc_lens.len(),
            doc_len_bytes * 8,
            doc_len_bytes as f64 * 8.0 / doc_lens.len() as f64
        )?;
        let successor_id_bytes = compressed_size_delta(&successor_ids);
        write!(
            &mut buf,
            "\tSuccessor IDs: {} ids, {} bits, {:0.3} bits/id:\n",
            successor_ids.len(),
            successor_id_bytes * 8,
            successor_id_bytes as f64 * 8.0 / successor_ids.len() as f64
        )?;
        if successor_ids.len() < 5 {
            write!(&mut buf, "\t\t{:?}\n", successor_ids)?;
        }
        total_index_size += doc_bytes + doc_len_bytes + successor_id_bytes + 3 + 6;
    }

    write!(
        &mut buf,
        "Content size: {}, Compressed size: {}, Compression ratio: {:.3}\n",
        total_content_size,
        total_index_size,
        total_index_size as f64 / total_content_size as f64
    )?;

    Ok(())
}

fn file_trigrams(content: &[u8]) -> BTreeMap<Trigram, FnvHashSet<Trigram>> {
    let mut res: BTreeMap<Trigram, FnvHashSet<Trigram>> = BTreeMap::new();
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

    for hexgram in content.array_windows::<6>() {
        let (t1, t2) = hexgram.split_array_ref::<3>();
        let t2 = unsafe { &*(t2.as_ptr() as *const [u8; 3]) };
        add_trigrams(*t1, *t2);
    }

    drop(add_trigrams);
    match content {
        [.., a, b, _, _, _] => {
            // add_trigrams([*a, *b, *c], [*d, *e, 0xFF]);
            // add_trigrams([*b, *c, *d], [*e, 0xFF, 0xFF]);
            // add_trigrams([*c, *d, *e], [0xFF, 0xFF, 0xFF]);
            // drop(add_trigrams);
            res.insert([*a, *b, 0xFF], FnvHashSet::default());
            res.insert([*b, 0xFF, 0xFF], FnvHashSet::default());
            res.insert([0xFF, 0xFF, 0xFF], FnvHashSet::default());
        }
        [.., b, _, _, _] => {
            // add_trigrams([*b, *c, *d], [*e, 0xFF, 0xFF]);
            // add_trigrams([*c, *d, *e], [0xFF, 0xFF, 0xFF]);
            // drop(add_trigrams);
            res.insert([*b, 0xFF, 0xFF], FnvHashSet::default());
            res.insert([0xFF, 0xFF, 0xFF], FnvHashSet::default());
        }
        [..] => {
            // add_trigrams([*c, *d, *e], [0xFF, 0xFF, 0xFF]);
            // drop(add_trigrams);
            res.insert([0xFF, 0xFF, 0xFF], FnvHashSet::default());
        }
    }

    res
}
