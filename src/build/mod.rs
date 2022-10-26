use std::collections::BTreeMap;
use std::ops::RangeFrom;
use std::time::Instant;
use std::{io::Write, time::Duration};

use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::index::{IndexHeader, PostingHeader};
use crate::ioutil::Section;
use crate::Trigram;
use crate::{DocID, TrigramID};

pub mod serialize;
pub mod stats;
use serialize::{StreamWriter, U32DeltaCompressor};
use stats::{IndexStats, SequenceStats, TrigramPostingStats};

use self::stats::{BuildStats, ExtractStats};

pub struct IndexBuilder {
    doc_ids: RangeFrom<DocID>,
    combined: BTreeMap<Trigram, Vec<(DocID, FxHashSet<Trigram>)>>,

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
            combined: BTreeMap::default(),
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

        let doc_id = self.doc_ids.next().unwrap();
        for (trigram, set) in Self::extract_trigrams(content) {
            match self.combined.get_mut(&trigram) {
                Some(v) => v.push((doc_id, set)),
                None => {
                    self.combined.insert(trigram, vec![(doc_id, set)]);
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
            bytes.array_windows::<3>().copied().map(Trigram)
        };

        let trigrams = content.array_windows::<3>().copied().map(Trigram);
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
            Vec::from_iter(self.buf_trigram_set.iter().copied().map(u32::from));
        unique_trigrams.sort();

        let compressed_size = U32DeltaCompressor(&unique_trigrams).write_to(w)?;

        Ok((
            unique_trigrams,
            SequenceStats {
                count: self.buf_u32.len(),
                bytes: compressed_size,
            },
        ))
    }

    // Called per unique trigram
    fn build_successors<W: Write>(
        &mut self,
        w: &mut W,
        unique_successors: &[TrigramID],
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<SequenceStats> {
        self.buf_u32.clear();
        for (local_doc_id, (_, successors)) in docs.iter().enumerate() {
            let offset = local_doc_id * unique_successors.len();
            self.buf_u32.extend(
                successors
                    .iter()
                    .copied()
                    .map(u32::from)
                    .map(|t| unique_successors.binary_search(&t).unwrap() as u32)
                    .map(|t| t + offset as u32),
            );
            let l = self.buf_u32.len();
            self.buf_u32[l - successors.len()..].sort();
        }

        let compressed_size = U32DeltaCompressor(&self.buf_u32).write_to(w)?;

        Ok(SequenceStats {
            count: self.buf_u32.len(),
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

        let compressed_size = U32DeltaCompressor(&self.buf_u32).write_to(w)?;

        Ok(SequenceStats {
            count: self.buf_u32.len(),
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
        let successors_stats = self.build_successors(&mut buf, &unique_successors, &docs)?;
        let unique_docs_stats = self.build_unique_docs(&mut buf, &docs)?;

        let header = PostingHeader {
            unique_successors_count: unique_successors_stats.count.try_into()?,
            unique_successors_bytes: unique_successors_stats.bytes.try_into()?,
            successors_count: successors_stats.count.try_into()?,
            successors_bytes: successors_stats.bytes.try_into()?,
            unique_docs_count: unique_docs_stats.count.try_into()?,
            unique_docs_bytes: unique_docs_stats.bytes.try_into()?,
        };

        let header_bytes = header.write_to(w)?;
        w.write_all(&buf)?;

        Ok(TrigramPostingStats {
            header_bytes,
            unique_successors: unique_successors_stats,
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
        let mut postings_len: u64 = 0;

        for (trigram, docs) in std::mem::take(&mut self.combined).into_iter() {
            let posting_stats = self.build_posting(w, &docs)?;
            build_stats.add_posting(&posting_stats);
            postings_len += posting_stats.total_bytes() as u64;
            posting_ends.push((trigram, postings_len));
        }

        // TODO compress this into blocks, btree style
        let mut unique_trigrams_len = 0;
        for (trigram, _) in posting_ends.iter() {
            unique_trigrams_len += w.write(&<[u8; 3]>::from(*trigram))?;
        }

        let mut offsets_len = 0;
        for (_, offset) in posting_ends.iter() {
            w.write_u64::<LittleEndian>(*offset)?;
            offsets_len += 4;
        }

        let header = IndexHeader {
            trigram_postings: Section::new(0, postings_len),
            unique_trigrams: Section::new(postings_len, unique_trigrams_len as u64),
            trigram_posting_ends: Section::new(
                postings_len + unique_trigrams_len as u64,
                offsets_len,
            ),
        };

        header.write_to(w)?;

        build_stats.posting_offsets_bytes = offsets_len as usize;
        build_stats.build_time = build_start.elapsed();

        Ok(IndexStats {
            extract: extract_stats,
            build: build_stats,
            total_time: self.creation_time.elapsed(),
        })
    }
}
