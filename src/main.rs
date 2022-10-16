#![feature(array_windows)]
#![feature(split_array)]

use std::fs::File;
use std::io::{BufWriter, Read, Write};

use anyhow::Result;
use fnv::{FnvHashMap, FnvHashSet};
use histogram::Histogram;
use serialize::{StreamWriter, U32Compressor, U32DeltaCompressor};
use walkdir::WalkDir;

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
        for (trigram, set) in file_trigrams(&contents) {
            match combined.get_mut(&trigram) {
                Some(v) => v.push((id as u32, set)),
                None => {
                    combined.insert(trigram, vec![(id as u32, set)]);
                }
            }
        }
    }

    let buf = &mut BufWriter::new(std::io::stdout().lock());
    let mut doc_ids = Vec::new();
    let mut doc_ids_len_h = Histogram::new();
    let mut doc_ids_bytes_h = Histogram::new();
    let mut doc_lens = Vec::new();
    let mut doc_lens_len_h = Histogram::new();
    let mut doc_lens_bytes_h = Histogram::new();
    let mut unique_successors: FnvHashSet<Trigram> = FnvHashSet::default();
    let mut unique_sorted_successor_ids: Vec<u32> = Vec::new();
    let mut unique_len_h = Histogram::new();
    let mut unique_bytes_h = Histogram::new();
    let mut successor_ids: Vec<u32> = Vec::new();
    let mut successor_len_h = Histogram::new();
    let mut successor_bytes_h = Histogram::new();
    let mut trigram_h = Histogram::new();
    let mut total_index_size = 0;

    for (trigram, docs) in combined.iter() {
        doc_ids.clear();
        doc_lens.clear();
        unique_successors.clear();
        unique_sorted_successor_ids.clear();
        successor_ids.clear();

        for (id, successors) in docs {
            doc_ids.push(*id);
            doc_lens.push(successors.len() as u32);
            unique_successors.extend(successors);
        }

        // Convert unique successor trigrams into trigram IDs.
        unique_sorted_successor_ids.extend(unique_successors.iter().copied().map(trigram_as_int));
        unique_sorted_successor_ids.sort();

        for (_, successors) in docs {
            let last_successor_id = successor_ids.last().copied().unwrap_or(0);
            successor_ids.extend(
                successors
                    .into_iter()
                    .copied()
                    .map(trigram_as_int)
                    .map(|id| unique_sorted_successor_ids.binary_search(&id).unwrap() as u32)
                    .map(|local_id| local_id + last_successor_id),
            );
            let l = successor_ids.len();
            successor_ids[l - successors.len()..].sort()
        }

        let sink = &mut std::io::sink();

        let doc_bytes = U32DeltaCompressor(&doc_ids).write_to(sink)?;
        doc_ids_len_h.increment(doc_ids.len() as u64).unwrap();
        doc_ids_bytes_h.increment(doc_bytes as u64).unwrap();

        let doc_len_bytes = U32Compressor(&doc_lens).write_to(sink)?;
        doc_lens_len_h.increment(doc_lens.len() as u64).unwrap();
        doc_lens_bytes_h.increment(doc_len_bytes as u64).unwrap();

        let unique_successor_id_bytes =
            U32DeltaCompressor(&unique_sorted_successor_ids).write_to(sink)?;
        unique_len_h
            .increment(unique_sorted_successor_ids.len() as u64)
            .unwrap();
        unique_bytes_h
            .increment(unique_successor_id_bytes as u64)
            .unwrap();

        let successor_id_bytes = U32DeltaCompressor(&successor_ids).write_to(sink)?;
        successor_len_h
            .increment(successor_ids.len() as u64)
            .unwrap();
        successor_bytes_h
            .increment(successor_id_bytes as u64)
            .unwrap();

        let trigram_size =
            doc_bytes + doc_len_bytes + unique_successor_id_bytes + successor_id_bytes;
        trigram_h.increment(trigram_size as u64).unwrap();

        total_index_size += trigram_size;
    }

    write!(
        buf,
        "Content size: {}, Compressed size: {}, Compression ratio: {:.3}\n",
        total_content_size,
        total_index_size,
        total_index_size as f64 / total_content_size as f64
    )?;

    write!(buf, "Unique trigrams: {}\n", combined.len())?;

    for (name, histogram) in &[
        ("Doc ID len:", &doc_ids_len_h),
        ("Doc ID bytes:", &doc_ids_bytes_h),
        ("Doc Lens len:", &doc_lens_len_h),
        ("Doc Lens bytes:", &doc_lens_bytes_h),
        ("Unique successors len:", &unique_len_h),
        ("Unique successors bytes:", &unique_bytes_h),
        ("Successors len:", &successor_len_h),
        ("Successors bytes", &successor_bytes_h),
        ("Trigrams sizes", &trigram_h),
    ] {
        write!(
            buf,
            "{:25}{:3} {:3} {:3} {:3} {:9} -- {}\n",
            name,
            histogram.minimum().unwrap(),
            histogram.percentile(10.).unwrap(),
            histogram.mean().unwrap(),
            histogram.percentile(90.).unwrap(),
            histogram.maximum().unwrap(),
            histogram
                .into_iter()
                .map(|b| b.value() * b.count())
                .sum::<u64>(),
        )?;
    }

    Ok(())
}

fn file_trigrams(padded_content: &[u8]) -> FnvHashMap<Trigram, FnvHashSet<Trigram>> {
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

    for hexgram in padded_content.array_windows::<6>() {
        let (t1, t2) = hexgram.split_array_ref::<3>();
        let t2 = unsafe { &*(t2.as_ptr() as *const [u8; 3]) };
        add_trigrams(*t1, *t2);
    }

    res
}

fn trigram_as_int(t: Trigram) -> u32 {
    (t[0] as u32) << 16 + (t[1] as u32) << 8 + t[2] as u32
}
