use std::{
    io::{Read, Seek},
    marker::PhantomData,
};

use anyhow::Result;

use super::ioutil::{Section, SectionType};
use crate::Trigram;

pub trait ReadSeek: Read + Seek {}

struct Index {
    header: IndexHeader,
    unique_trigrams: Vec<Trigram>,
    // TODO this can probably be represented more densely
    trigram_posting_ends: Vec<u64>,
    source: Box<dyn ReadSeek>, // boxed to reduce generic noise
}

impl Index {
    pub fn new(source: Box<dyn ReadSeek>) -> Result<Self> {
        let header = Self::read_header(source.as_ref())?;

        todo!()
    }

    fn read_header(source: &dyn ReadSeek) -> Result<IndexHeader> {
        todo!()
    }

    fn trigram_section(&self, t: Trigram) -> Option<TrigramPostingSection> {
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
    trigram_postings: TrigramPostingsSection,
    trigram_posting_ends: TrigramPostingEndsSection,
    unique_trigrams: UniqueTrigramsSection,
}

// A section relative to the full index
struct FullIndex;
impl SectionType for FullIndex {}

type UniqueTrigramsSection = Section<FullIndex>;
type TrigramPostingEndsSection = Section<FullIndex>;
type TrigramPostingsSection = Section<FullIndex>;
type TrigramPostingSection = Section<TrigramPostingsSection>;
type UniqueSuccessorsSection = Section<TrigramPostingSection>;
type UniqueDocsSection = Section<TrigramPostingSection>;
type RunLengthsSection = Section<TrigramPostingSection>;
type SuccessorsSection = Section<TrigramPostingSection>;
