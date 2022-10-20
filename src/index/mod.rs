use std::ops::RangeFrom;
use std::time::Instant;
use std::{io::Write, time::Duration};

use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    serialize::{StreamWriter, U32Compressor, U32DeltaCompressor},
    Trigram,
};
use crate::{DocID, TrigramID};

pub mod stats;
use stats::{IndexStats, SequenceStats, TrigramPostingStats};

use self::stats::{BuildStats, ExtractStats};

pub struct IndexBuilder {
    doc_ids: RangeFrom<DocID>,
    combined: FxHashMap<Trigram, Vec<(DocID, FxHashSet<Trigram>)>>,

    // Reusable buffers
    buf_trigram_set: FxHashSet<Trigram>,
    buf_u32: Vec<u32>,

    // Stats
    creation_time: Instant,
    extract_duration: Duration,
    num_docs: usize,
    total_doc_bytes: usize,
}

impl Default for IndexBuilder {
    fn default() -> Self {
        Self {
            doc_ids: 0..,
            combined: FxHashMap::default(),
            buf_trigram_set: FxHashSet::default(),
            buf_u32: Vec::default(),
            creation_time: Instant::now(),
            extract_duration: Duration::default(),
            total_doc_bytes: 0,
            num_docs: 0,
        }
    }
}

impl IndexBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_doc(&mut self, content: &[u8]) -> Result<()> {
        let start = Instant::now();

        for (trigram, set) in Self::extract_trigrams(content) {
            match self.combined.get_mut(&trigram) {
                Some(v) => v.push((self.doc_ids.next().unwrap(), set)),
                None => {
                    self.combined
                        .insert(trigram, vec![(self.doc_ids.next().unwrap(), set)]);
                }
            }
        }

        self.extract_duration += start.elapsed();
        self.total_doc_bytes += content.len();
        self.num_docs += 1;
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

    fn build_unique_successors<W: Write>(
        &mut self,
        w: &mut W,
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<(Vec<TrigramID>, SequenceStats)> {
        // Collect the successors for each doc into a deduplicated set of unique successors.
        // TODO perf test a btree hash set, which would allow us to skip the collect into vec and
        // sort steps below.
        self.buf_trigram_set.clear();
        self.buf_trigram_set
            .extend(docs.iter().flat_map(|(_, set)| set));

        // Collect the set into a vec of the trigrams' u32 representation and sort
        let mut unique_trigrams =
            Vec::from_iter(self.buf_trigram_set.iter().copied().map(trigram_to_id));
        unique_trigrams.sort();

        let compressed_size = U32DeltaCompressor(&unique_trigrams).write_to(w)?;

        Ok((
            unique_trigrams,
            SequenceStats {
                len: self.buf_u32.len(),
                bytes: compressed_size,
            },
        ))
    }

    fn build_run_lens<W: Write>(
        &mut self,
        w: &mut W,
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<SequenceStats> {
        // Collect the set into a vec of the trigrams' u32 representation and sort
        self.buf_u32.clear();
        self.buf_u32
            .extend(docs.iter().map(|(_, successors)| successors.len() as u32));

        let compressed_size = U32Compressor(&self.buf_u32).write_to(w)?;

        Ok(SequenceStats {
            len: self.buf_u32.len(),
            bytes: compressed_size,
        })
    }

    fn build_successors<W: Write>(
        &mut self,
        w: &mut W,
        unique_trigrams: &[TrigramID],
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<SequenceStats> {
        self.buf_u32.clear();
        for (_, successors) in docs {
            let last_successor = self.buf_u32.last().copied().unwrap_or(0);
            self.buf_u32.extend(
                successors
                    .iter()
                    .copied()
                    .map(trigram_to_id)
                    .map(|t| unique_trigrams.binary_search(&t).unwrap() as u32)
                    .map(|t| t + last_successor),
            );
            let l = self.buf_u32.len();
            self.buf_u32[l - successors.len()..].sort();
        }

        assert!(self.buf_u32.is_sorted());
        let compressed_size = U32DeltaCompressor(&self.buf_u32).write_to(w)?;

        Ok(SequenceStats {
            len: self.buf_u32.len(),
            bytes: compressed_size,
        })
    }

    fn build_unique_docs<W: Write>(
        &mut self,
        w: &mut W,
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<SequenceStats> {
        self.buf_u32.clear();
        self.buf_u32.extend(docs.iter().map(|(id, _)| id));

        assert!(self.buf_u32.is_sorted());
        let compressed_size = U32DeltaCompressor(&self.buf_u32).write_to(w)?;

        Ok(SequenceStats {
            len: self.buf_u32.len(),
            bytes: compressed_size,
        })
    }

    fn build_posting<W: Write>(
        &mut self,
        w: &mut W,
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<TrigramPostingStats> {
        let mut buf = Vec::new();

        let (unique_successors, unique_successors_stats) =
            self.build_unique_successors(&mut buf, &docs)?;
        let run_lengths_stats = self.build_run_lens(&mut buf, &docs)?;
        let successors_stats = self.build_successors(&mut buf, &unique_successors, &docs)?;
        let unique_docs_stats = self.build_unique_docs(&mut buf, &docs)?;

        let header = PostingHeader {
            unique_successors_len: unique_successors_stats.bytes.try_into()?,
            doc_lens_len: run_lengths_stats.bytes.try_into()?,
            successors_len: successors_stats.bytes.try_into()?,
            doc_ids_len: unique_docs_stats.bytes.try_into()?,
        };

        let header_bytes = header.write_to(w)?;
        w.write_all(&buf)?;

        Ok(TrigramPostingStats {
            header_bytes,
            unique_successors: unique_successors_stats,
            run_lengths: run_lengths_stats,
            successors: successors_stats,
            unique_docs: unique_docs_stats,
        })
    }

    pub fn build<W: Write>(mut self, w: &mut W) -> Result<IndexStats> {
        let extract_stats = ExtractStats {
            num_docs: self.num_docs,
            doc_bytes: self.total_doc_bytes,
            unique_trigrams: self.combined.len(),
            extract_time: self.extract_duration,
        };

        let build_start = Instant::now();
        let mut build_stats = BuildStats::default();
        let mut posting_ends: Vec<(Trigram, u64)> = Vec::new();

        for (trigram, docs) in std::mem::take(&mut self.combined).into_iter() {
            let posting_stats = self.build_posting(w, &docs)?;
            build_stats.add_posting(&posting_stats);
            posting_ends.push((trigram, posting_stats.total_bytes() as u64));
        }

        // TODO compress this into blocks, btree style
        let mut offsets_len = 0;
        for (trigram, end_offset) in posting_ends {
            offsets_len += w.write(&trigram)?;
            w.write_u64::<LittleEndian>(end_offset)?;
            offsets_len += 4;
        }

        build_stats.posting_offsets_bytes = offsets_len;
        build_stats.build_time = build_start.elapsed();

        Ok(IndexStats {
            extract: extract_stats,
            build: build_stats,
            total_time: self.creation_time.elapsed(),
        })
    }
}

fn trigram_to_id(t: Trigram) -> TrigramID {
    ((t[0] as u32) << 16) + ((t[1] as u32) << 8) + t[2] as u32
}

fn trigram_from_id(t: TrigramID) -> Trigram {
    [
        ((t & 0x00FF0000) >> 16) as u8,
        ((t & 0x0000FF00) >> 8) as u8,
        (t & 0x000000FF) as u8,
    ]
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

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck::quickcheck;

    quickcheck! {
        fn trigram_id_roundtrip(b1: u8, b2: u8, b3: u8) -> bool {
            trigram_from_id(trigram_to_id([b1, b2, b3])) == [b1, b2, b3]
        }
    }
}
