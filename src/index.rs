use std::{
    io::{Read, Seek, SeekFrom},
    marker::PhantomData,
};

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt};

use super::ioutil::{Section, SectionType};
use crate::{ioutil::IOSection, DocID, Trigram};

pub trait ReadSeek: Read + Seek {}

struct Index {
    header: IndexHeader,
    // TODO this can probably be represented more densely
    // TODO even better, this should just stay on disk. Keeping it in memory for now to compare
    // more directly with Zoekt.
    unique_trigrams: Vec<Trigram>,
    trigram_posting_ends: Vec<u64>,
    source: Box<dyn ReadSeek>, // boxed to reduce generic noise
}

impl Index {
    pub fn new(mut source: Box<dyn ReadSeek>) -> Result<Self> {
        let header = Self::read_header(source.as_mut())?;

        assert!(header.unique_trigrams.len % 3 == 0);
        let n_trigrams = header.unique_trigrams.len as usize / 3;
        let mut unique_trigrams = Vec::with_capacity(n_trigrams);
        let mut unique_trigrams_reader = IOSection::new(
            source.as_mut(),
            header.unique_trigrams.offset,
            header.unique_trigrams.offset + header.unique_trigrams.len,
        );
        for _ in 0..n_trigrams {
            let mut buf: Trigram = [0u8; 3];
            unique_trigrams_reader.read_exact(&mut buf)?;
            unique_trigrams.push(buf)
        }

        assert!(header.trigram_posting_ends.len % 3 == 0);
        assert!(header.trigram_posting_ends.len as usize / 3 == n_trigrams);
        let mut trigram_posting_ends = Vec::with_capacity(n_trigrams);
        let mut trigram_ends_reader = IOSection::new(
            source.as_mut(),
            header.trigram_posting_ends.offset,
            header.trigram_posting_ends.offset + header.trigram_posting_ends.len,
        );
        for _ in 0..n_trigrams {
            trigram_posting_ends.push(trigram_ends_reader.read_u64::<LittleEndian>()?);
        }

        Ok(Self {
            header,
            unique_trigrams,
            trigram_posting_ends,
            source,
        })
    }

    // TODO move this to an IndexHeader method
    fn read_header(r: &mut dyn ReadSeek) -> Result<IndexHeader> {
        r.seek(SeekFrom::End(-(IndexHeader::SIZE_BYTES as i64)))?;
        let trigram_postings_offset = r.read_u64::<LittleEndian>()?;
        let trigram_postings_len = r.read_u64::<LittleEndian>()?;
        let unique_trigrams_offset = r.read_u64::<LittleEndian>()?;
        let unique_trigrams_len = r.read_u64::<LittleEndian>()?;
        let trigram_posting_ends_offset = r.read_u64::<LittleEndian>()?;
        let trigram_posting_ends_len = r.read_u64::<LittleEndian>()?;
        Ok(IndexHeader {
            trigram_postings: TrigramPostingsSection::new(
                trigram_postings_offset,
                trigram_postings_len,
            ),
            unique_trigrams: UniqueTrigramsSection::new(
                unique_trigrams_offset,
                unique_trigrams_len,
            ),
            trigram_posting_ends: TrigramPostingEndsSection::new(
                trigram_posting_ends_offset,
                trigram_posting_ends_len,
            ),
        })
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

    fn search(&self) -> Box<dyn Iterator<Item = DocID>> {}
}

struct IndexHeader {
    trigram_postings: TrigramPostingsSection,
    unique_trigrams: UniqueTrigramsSection,
    trigram_posting_ends: TrigramPostingEndsSection,
}

impl IndexHeader {
    const SIZE_BYTES: usize = 48;
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
