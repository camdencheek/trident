use std::{
    io::{Read, Seek},
    marker::PhantomData,
};

use crate::Trigram;

pub trait ReadSeek: Read + Seek {}

struct Index<T: Read + Seek> {
    header: IndexHeader,
    unique_trigrams: Vec<Trigram>,
    // TODO this can probably be represented more densely
    trigram_posting_ends: Vec<u64>,
    inner: T,
}

impl<T: Read + Seek> Index<T> {
    fn trigram_section(&self, t: Trigram) -> Option<Section<TrigramPostings>> {
        let trigram_idx = match self.unique_trigrams.binary_search(&t) {
            Ok(idx) => idx,
            Err(_) => return None,
        };

        // The first posting has an implicit start of zero, otherwise
        // it starts at the end of the previous posting.
        let start = match trigram_idx {
            0 => 0,
            _ => self.trigram_posting_ends[trigram_idx - 1],
        };

        let end = self.trigram_posting_ends[trigram_idx];
        Some(Section::new(start, end - start))
    }
}

struct IndexHeader {
    trigram_postings: Section<FullIndex>,
    trigram_posting_ends: Section<FullIndex>,
}

trait SectionType {}

// A section relative to the full index
struct FullIndex;
impl SectionType for FullIndex {}

// A section relative to the Trigram Postings section
struct TrigramPostings;
impl SectionType for TrigramPostings {}

struct Section<T: SectionType> {
    offset: u64,
    len: u64,
    _type: PhantomData<T>,
}

impl<T: SectionType> Section<T> {
    fn new(offset: u64, len: u64) -> Self {
        Self {
            offset,
            len,
            _type: PhantomData::<T>,
        }
    }
}
