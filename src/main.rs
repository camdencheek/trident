#![feature(array_windows)]
#![feature(split_array)]

use anyhow::Result;
use fnv::{FnvHashMap, FnvHashSet};
use serialize::{StreamWriter, U32Compressor, U32DeltaCompressor};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Read, Write},
};
use walkdir::WalkDir;

mod serialize;

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

    let buf = &mut BufWriter::new(std::io::stdout().lock());
    let mut doc_ids = Vec::new();
    let mut doc_lens = Vec::new();
    let mut unique_successor_ids: FnvHashSet<Trigram> = FnvHashSet::default();
    let mut unique_sorted_successor_ids: Vec<u32> = Vec::new();
    let mut successor_ids: Vec<u32> = Vec::new();
    let mut total_index_size = 0;
    for (trigram, docs) in combined.into_iter() {
        doc_ids.clear();
        doc_lens.clear();
        unique_successor_ids.clear();
        unique_sorted_successor_ids.clear();
        successor_ids.clear();

        for (id, successors) in &docs {
            doc_ids.push(*id);
            doc_lens.push(successors.len() as u32);
            unique_successor_ids.extend(successors);
        }

        // Convert unique successor trigrams into trigram IDs.
        unique_sorted_successor_ids.extend(
            unique_successor_ids
                .iter()
                .map(|t| trigram_ids.get(t).unwrap()),
        );
        unique_sorted_successor_ids.sort();

        for (_, successors) in docs {
            let last_successor_id = successor_ids.last().copied().unwrap_or(0);
            successor_ids.extend(
                successors
                    .iter()
                    .map(|s| trigram_ids.get(s).unwrap())
                    .map(|id| unique_sorted_successor_ids.binary_search(id).unwrap() as u32)
                    .map(|local_id| local_id + last_successor_id),
            );
            let l = successor_ids.len();
            successor_ids[l - successors.len()..].sort()
        }

        write!(buf, "Trigram {:?}:\n", trigram)?;
        let sink = &mut std::io::sink();
        let doc_bytes = U32DeltaCompressor(&doc_ids).write_to(sink)?;
        write!(
            buf,
            "\tDoc IDs: count={}, bytes={}\n",
            doc_ids.len(),
            doc_bytes
        )?;
        let doc_len_bytes = U32Compressor(&doc_lens).write_to(sink)?;
        write!(
            buf,
            "\tDoc lens: count={}, bytes={}\n",
            doc_lens.len(),
            doc_len_bytes
        )?;
        let unique_successor_id_bytes =
            U32DeltaCompressor(&unique_sorted_successor_ids).write_to(sink)?;
        write!(
            buf,
            "\tUnique successors: count={}, bytes={}\n",
            unique_sorted_successor_ids.len(),
            unique_successor_id_bytes
        )?;
        let successor_id_bytes = U32DeltaCompressor(&successor_ids).write_to(sink)?;
        write!(
            buf,
            "\tSuccessors: count={}, bytes={}\n",
            successor_ids.len(),
            successor_id_bytes,
        )?;
        total_index_size +=
            doc_bytes + doc_len_bytes + unique_successor_id_bytes + successor_id_bytes + 3 + 6;
    }

    write!(
        buf,
        "Content size: {}, Compressed size: {}, Compression ratio: {:.3}\n",
        total_content_size,
        total_index_size,
        total_index_size as f64 / total_content_size as f64
    )?;

    Ok(())
}

fn file_trigrams(content: &[u8]) -> FnvHashMap<Trigram, FnvHashSet<Trigram>> {
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

    for hexgram in content.array_windows::<6>() {
        let (t1, t2) = hexgram.split_array_ref::<3>();
        let t2 = unsafe { &*(t2.as_ptr() as *const [u8; 3]) };
        add_trigrams(*t1, *t2);
    }

    drop(add_trigrams);
    match content {
        [.., a, b, _, _, _] => {
            res.insert([*a, *b, 0xFF], FnvHashSet::default());
            res.insert([*b, 0xFF, 0xFF], FnvHashSet::default());
            res.insert([0xFF, 0xFF, 0xFF], FnvHashSet::default());
        }
        [.., b, _, _, _] => {
            res.insert([*b, 0xFF, 0xFF], FnvHashSet::default());
            res.insert([0xFF, 0xFF, 0xFF], FnvHashSet::default());
        }
        [..] => {
            res.insert([0xFF, 0xFF, 0xFF], FnvHashSet::default());
        }
    }

    res
}
