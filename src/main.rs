#![feature(array_windows)]

use hashers::fnv::FNV1aHasher32;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    hash::BuildHasherDefault,
    io::{BufReader, BufWriter, Read, Write},
};

use anyhow::Result;
use memmap2::{Advice, MmapMut};

type Trigram = [u8; 3];

fn main() -> Result<()> {
    let f = File::open("/tmp/single-file.txt")?;
    let contents = BufReader::with_capacity(1024 * 1024 * 64, f)
        .bytes()
        .chain([Ok(0xFF), Ok(0xFF)].into_iter());

    // Calculate the number of each trigram
    let mut sizes: HashMap<Trigram, u32, _> = HashMap::with_capacity_and_hasher(
        256 * 256,
        BuildHasherDefault::<FNV1aHasher32>::default(),
    );
    let mut trigram = [0u8; 3];
    for (offset, char) in contents.enumerate() {
        trigram[0] = trigram[1];
        trigram[1] = trigram[2];
        trigram[2] = char?;
        if offset < 2 {
            continue;
        }

        if offset % (1024 * 1024) == 0 {
            println!("{}", offset);
        }

        match sizes.get_mut(&trigram) {
            Some(n) => *n += 1,
            None => {
                sizes.insert(trigram, 1);
            }
        }
    }

    // Calculate the cumulative number of each trigram
    let mut offsets: BTreeMap<Trigram, u32> = BTreeMap::new();
    sizes.keys().for_each(|k| {
        offsets.insert(*k, 0);
    });
    let mut cumulative_offset = 0;
    for (trigram, offset) in offsets.iter_mut() {
        *offset = cumulative_offset;
        cumulative_offset += sizes.get(trigram).unwrap();
    }

    println!("Total size: {}", cumulative_offset);

    let mut bytes =
        MmapMut::map_anon(cumulative_offset as usize * std::mem::size_of::<u32>() + 16)?;
    bytes.advise(Advice::Random)?;
    let pointers = unsafe {
        let (_, aligned_bytes, _) = bytes.align_to_mut::<usize>();
        std::slice::from_raw_parts_mut(
            aligned_bytes.as_mut_ptr() as *mut u32,
            cumulative_offset as usize,
        )
    };

    let f = File::open("/tmp/single-file.txt")?;
    let contents = BufReader::with_capacity(1024 * 1024 * 64, f)
        .bytes()
        .chain([Ok(0xFF), Ok(0xFF)].into_iter());
    let mut last_three_trigrams = [[0u8; 3]; 3];
    for (offset, char) in contents.enumerate() {
        trigram[0] = trigram[1];
        trigram[1] = trigram[2];
        trigram[2] = char?;

        if offset < 2 {
            continue;
        }

        let trigram_offset = offset - 2;
        if trigram_offset < 3 {
            last_three_trigrams[trigram_offset] = trigram;
            continue;
        }

        if trigram_offset % (1024 * 1024) == 0 {
            println!("{}", trigram_offset);
        }

        let next_trigram_offset = *offsets
            .get(&trigram)
            .expect("all trigrams should be pre-counted");

        match offsets.get_mut(&last_three_trigrams[0]) {
            Some(n) => {
                pointers[*n as usize] = next_trigram_offset;
                *n += 1;
            }
            None => panic!("all trigrams should be pre-counted"),
        };

        last_three_trigrams[0] = last_three_trigrams[1];
        last_three_trigrams[1] = last_three_trigrams[2];
        last_three_trigrams[2] = trigram;
    }

    let handle = std::io::stdout().lock();
    let mut buf = BufWriter::new(handle);
    let mut start_offset: usize = 0;
    for (trigram, end_offset) in offsets.iter() {
        let end_offset = *end_offset as usize;
        write!(buf, "{:?}\n", trigram)?;
        for pointer in &pointers[start_offset..end_offset] {
            write!(buf, "{}\n", pointer)?;
        }
        start_offset = end_offset;
    }

    Ok(())
}
