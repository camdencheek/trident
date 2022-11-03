use std::collections::BTreeMap;
use std::ops::RangeFrom;
use std::time::Instant;
use std::{io::Write, time::Duration};

use anyhow::Result;
use bitpacking::{BitPacker, BitPacker4x};
use byteorder::{LittleEndian, WriteBytesExt};
use integer_encoding::{VarIntReader, VarIntWriter};
use rocksdb::SstFileWriter;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::db::{BlobIndexKey, DBKey, PartitionKey, TrigramPostingKey};
use crate::index::{IndexHeader, PostingHeader};
use crate::ioutil::{stream::StreamWrite, Section};
use crate::Trigram;
use crate::{DocID, TrigramID};

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

    fn build_unique_successors_sst(
        &mut self,
        w: &mut SstFileWriter,
        trigram: Trigram,
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<Vec<TrigramID>> {
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

        let block_id_to_key = |block_id| {
            DBKey::Partition(
                0,
                PartitionKey::BlobIndex(BlobIndexKey::TrigramPosting(
                    trigram.into(),
                    TrigramPostingKey::SuccessorsBlock(block_id as u32),
                )),
            )
            .to_vec()
        };

        for (i, chunk) in write_compressed_u32s(&self.buf_u32).iter().enumerate() {
            w.put(block_id_to_key(i), chunk)?;
        }

        Ok(unique_trigrams)
    }

    // Called per unique trigram
    fn build_successors_sst(
        &mut self,
        w: &mut SstFileWriter,
        trigram: Trigram,
        unique_successors: &[TrigramID],
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<()> {
        self.buf_u32.clear();
        for (local_doc_id, (doc_id, successors)) in docs.iter().enumerate() {
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

        let block_id_to_key = |block_id| {
            DBKey::Partition(
                0,
                PartitionKey::BlobIndex(BlobIndexKey::TrigramPosting(
                    trigram.into(),
                    TrigramPostingKey::MatrixBlock(block_id as u32),
                )),
            )
            .to_vec()
        };

        for (i, chunk) in write_compressed_u32s(&self.buf_u32).iter().enumerate() {
            w.put(block_id_to_key(i), chunk)?;
        }

        Ok(())
    }

    fn build_unique_docs_sst(
        &mut self,
        w: &mut SstFileWriter,
        trigram: Trigram,
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<()> {
        self.buf_u32.clear();
        self.buf_u32.extend(docs.iter().map(|(id, _)| id));

        let block_id_to_key = |block_id| {
            DBKey::Partition(
                0,
                PartitionKey::BlobIndex(BlobIndexKey::TrigramPosting(
                    trigram.into(),
                    TrigramPostingKey::DocsBlock(block_id as u32),
                )),
            )
            .to_vec()
        };

        for (i, chunk) in write_compressed_u32s(&self.buf_u32).iter().enumerate() {
            w.put(block_id_to_key(i), chunk)?;
        }

        Ok(())
    }

    fn build_posting_sst<'a>(
        &mut self,
        w: &mut SstFileWriter<'a>,
        trigram: Trigram,
        docs: &[(DocID, FxHashSet<Trigram>)],
    ) -> Result<()> {
        let unique_successors = self.build_unique_successors_sst(w, trigram, &docs)?;
        self.build_successors_sst(w, trigram, &unique_successors, &docs)?;
        self.build_unique_docs_sst(w, trigram, &docs)?;

        Ok(())
    }

    pub fn build_sst<'a>(mut self, w: &mut SstFileWriter<'a>) -> Result<()> {
        for (trigram, docs) in std::mem::take(&mut self.combined).into_iter() {
            self.build_posting_sst(w, trigram, &docs)?;
        }

        Ok(())
    }
}

fn write_compressed_u32s(list: &[u32]) -> Vec<Vec<u8>> {
    assert!(list.is_sorted());
    let mut chunks = list.chunks_exact(BitPacker4x::BLOCK_LEN);
    let mut last = 0;
    let mut buf = [0u8; 4 * BitPacker4x::BLOCK_LEN];
    let mut res = Vec::new();

    for chunk in chunks.by_ref() {
        let bp = BitPacker4x::new();
        let num_bits = bp.num_bits_sorted(last, &chunk);
        let mut compressed_block =
            Vec::with_capacity(1 + num_bits as usize * BitPacker4x::BLOCK_LEN);
        compressed_block.write(&[num_bits]).unwrap();
        let n = bp.compress_sorted(last, &chunk, &mut buf, num_bits);
        compressed_block.write(&buf[..n]).unwrap();
        last = *chunk.last().unwrap();
        res.push(compressed_block)
    }

    let mut remainder_chunk = Vec::new();
    for i in chunks.remainder() {
        remainder_chunk.write_varint(*i - last).unwrap();
        last = *i;
    }
    res.push(remainder_chunk);

    res
}
