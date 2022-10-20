use std::ops::RangeFrom;
use std::{io::Write, time::Duration};

use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    serialize::{StreamWriter, U32Compressor, U32DeltaCompressor},
    Trigram,
};

pub struct IndexBuilder {
    doc_ids: RangeFrom<u32>,
    combined: FxHashMap<Trigram, Vec<(u32, FxHashSet<Trigram>)>>,
}

impl IndexBuilder {
    pub fn new() -> Self {
        Self {
            doc_ids: 0..,
            combined: FxHashMap::default(),
        }
    }

    pub fn add_doc(&mut self, content: &[u8]) -> Result<()> {
        for (trigram, set) in Self::extract_trigrams(content) {
            match self.combined.get_mut(&trigram) {
                Some(v) => v.push((self.doc_ids.next().unwrap(), set)),
                None => {
                    self.combined
                        .insert(trigram, vec![(self.doc_ids.next().unwrap(), set)]);
                }
            }
        }

        Ok(())
    }

    fn extract_trigrams(content: &[u8]) -> FxHashMap<Trigram, FxHashSet<Trigram>> {
        let mut res: FxHashMap<Trigram, FxHashSet<Trigram>> = FxHashMap::default();

        let mut buf = [0u8; 4];
        let partial_trigrams = {
            let bytes = match content {
                [.., y, z] => {
                    buf = [*y, *z, 0xFF, 0xFF];
                    &buf[..4]
                }
                [z] => {
                    buf = [*z, 0xFF, 0xFF, 0xFF];
                    &buf[..3]
                }
                _ => &buf[..0],
            };
            bytes.array_windows::<3>().copied()
        };

        let trigrams = content.array_windows::<3>().copied();
        let successors = trigrams.clone().skip(3).chain(partial_trigrams.clone());

        for (trigram, successor) in trigrams.zip(successors) {
            match res.get_mut(&trigram) {
                Some(s) => {
                    s.insert(successor);
                }
                None => {
                    res.insert(trigram, FxHashSet::from_iter([successor].into_iter()));
                }
            };
        }

        for partial in partial_trigrams {
            if !res.contains_key(&partial) {
                res.insert(partial, FxHashSet::default());
            }
        }
        res
    }

    pub fn build<W: Write>(self, w: &mut W) -> Result<()> {
        let mut buf = Vec::new();
        let mut doc_ids = Vec::new();
        let mut doc_lens = Vec::new();
        let mut unique_successors: FxHashSet<Trigram> = FxHashSet::default();
        let mut unique_successor_ids: Vec<u32> = Vec::new();
        let mut successor_ids: Vec<u32> = Vec::new();
        let mut posting_list_ends: Vec<(Trigram, u32)> = Vec::new();

        for (trigram, docs) in self.combined.iter() {
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

            let mut l: u32 = 0;
            l += header.write_to(w)? as u32;
            w.write_all(&buf)?;
            l += buf.len() as u32;
            posting_list_ends.push((
                *trigram,
                posting_list_ends.last().map(|(_, o)| *o).unwrap_or(0) + l,
            ));
        }

        for (trigram, offset) in &posting_list_ends {
            w.write_all(trigram)?;
            w.write_u32::<LittleEndian>(*offset)?;
        }
        w.write_u32::<LittleEndian>(
            posting_list_ends.last().map(|(_, o)| *o).unwrap_or(0) as u32 * 7,
        )?;
        Ok(())
    }
}

fn trigram_as_int(t: Trigram) -> u32 {
    (t[0] as u32) << 16 + (t[1] as u32) << 8 + t[2] as u32
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
