use std::io::BufReader;
use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::ioutil::Section;
use crate::build::serialize::U32DeltaDecompressor;
use crate::ioutil::{Cursor, Len, ReadAt};
use crate::TrigramID;
use crate::{build::serialize::StreamWriter, DocID, LocalDocID, Trigram};

pub trait ReadSeek: Read + Seek {}

pub struct Index<R> {
    header: IndexHeader,
    // TODO this can probably be represented more densely
    // TODO even better, this should just stay on disk. Keeping it in memory for now to compare
    // more directly with Zoekt.
    unique_trigrams: Vec<Trigram>,
    trigram_posting_ends: Vec<u64>,
    r: R,
}

impl<R> Index<R>
where
    R: ReadAt + Len,
{
    pub fn new(r: R) -> Result<Self> {
        let header = Self::read_header(&r)?;

        assert!(header.unique_trigrams.len % 3 == 0);
        let n_trigrams = header.unique_trigrams.len as usize / 3;
        let mut unique_trigrams = Vec::with_capacity(n_trigrams);
        let mut unique_trigrams_reader = reader_at(&r, header.unique_trigrams.offset);
        for _ in 0..n_trigrams {
            let mut buf = [0u8; 3];
            unique_trigrams_reader.read_exact(&mut buf)?;
            unique_trigrams.push(Trigram(buf));
        }

        assert!(header.trigram_posting_ends.len % 4 == 0);
        assert!(header.trigram_posting_ends.len as usize / 4 == n_trigrams);
        let mut trigram_posting_ends = Vec::with_capacity(n_trigrams);
        let mut trigram_ends_reader = reader_at(&r, header.trigram_posting_ends.offset);
        for _ in 0..n_trigrams {
            trigram_posting_ends.push(trigram_ends_reader.read_u64::<LittleEndian>()?);
        }

        Ok(Self {
            header,
            unique_trigrams,
            trigram_posting_ends,
            r,
        })
    }

    fn read_header<T: ReadAt + Len>(r: &T) -> Result<IndexHeader> {
        let mut cursor = Cursor::new(r);
        cursor.seek(SeekFrom::End(-(IndexHeader::SIZE_BYTES as i64)))?;
        IndexHeader::read_from(&mut cursor)
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

    pub fn search<'a>(
        &'a self,
        trigram: &Trigram,
        successor: &Trigram,
    ) -> Box<dyn Iterator<Item = DocID> + 'a> {
        // If the trigram doesn't exist, return early with an empty iterator
        let section = match self.trigram_section(*trigram) {
            Some(s) => s,
            None => return Box::new(std::iter::empty()),
        };

        let absolute_section = self.header.trigram_postings.narrow(section);
        let mut reader = reader_at(&self.r, absolute_section.offset);
        let posting_header = PostingHeader::read_from(&mut reader).unwrap();

        let tp = self.header.trigram_postings;

        let unique_successors_section =
            tp.narrow(section.narrow(posting_header.unique_successors_section()));
        let successors_section = tp.narrow(section.narrow(posting_header.successors_section()));
        let unique_docs_section = tp.narrow(section.narrow(posting_header.unique_docs_section()));

        // TODO: clean up this garbage
        let target_successor_id = TrigramID::from(*successor);
        let unique_successors_reader = reader_at(&self.r, unique_successors_section.offset);
        let target_local_successor_id = match U32DeltaDecompressor::new(
            unique_successors_reader,
            posting_header.unique_successors_count as usize,
        )
        .enumerate()
        .find_map(|(local_id, successor_id)| {
            if successor_id == target_successor_id {
                Some(local_id)
            } else {
                None
            }
        }) {
            Some(l) => l as u32,
            None => return Box::new(std::iter::empty()),
        };

        let unique_docs_reader = reader_at(&self.r, unique_docs_section.offset);
        let unique_docs_iter = U32DeltaDecompressor::new(
            unique_docs_reader,
            posting_header.unique_docs_count as usize,
        )
        .enumerate()
        .map(|(i, d)| (i as u32, d))
        .collect::<Vec<_>>(); // TODO: get rid of this

        let successors_reader = reader_at(&self.r, successors_section.offset);
        let successors_iter =
            U32DeltaDecompressor::new(successors_reader, posting_header.successors_count as usize)
                .collect::<Vec<_>>();

        let doc_iter = successors_iter
            .into_iter()
            .map(move |i| {
                (
                    i / posting_header.unique_successors_count,
                    i % posting_header.unique_successors_count,
                )
            })
            .filter_map(move |(local_doc_id, local_successor_id)| {
                if local_successor_id == target_local_successor_id {
                    Some(local_doc_id)
                } else {
                    None
                }
            });

        Box::new(DocIDMapper::new(unique_docs_iter.into_iter(), doc_iter))
    }
}

#[derive(Debug, Clone)]
pub struct IndexHeader {
    pub trigram_postings: TrigramPostingsSection,
    pub unique_trigrams: UniqueTrigramsSection,
    pub trigram_posting_ends: TrigramPostingEndsSection,
}

impl IndexHeader {
    const SIZE_BYTES: usize = 48;

    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let header = IndexHeader {
            trigram_postings: TrigramPostingsSection::new(
                r.read_u64::<LittleEndian>()?,
                r.read_u64::<LittleEndian>()?,
            ),
            unique_trigrams: UniqueTrigramsSection::new(
                r.read_u64::<LittleEndian>()?,
                r.read_u64::<LittleEndian>()?,
            ),
            trigram_posting_ends: TrigramPostingEndsSection::new(
                r.read_u64::<LittleEndian>()?,
                r.read_u64::<LittleEndian>()?,
            ),
        };

        assert!(header.unique_trigrams.len % 3 == 0);
        assert!(header.trigram_posting_ends.len % 4 == 0);
        assert!(header.unique_trigrams.len / 3 == header.trigram_posting_ends.len / 4);
        Ok(header)
    }
}

impl StreamWriter for IndexHeader {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        let mut n = self.trigram_postings.write_to(w)?;
        n += self.unique_trigrams.write_to(w)?;
        n += self.trigram_posting_ends.write_to(w)?;
        Ok(n)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostingHeader {
    pub trigram: Trigram,
    pub unique_successors_count: u32,
    pub unique_successors_bytes: u32,
    pub successors_count: u32,
    pub successors_bytes: u32,
    pub unique_docs_count: u32,
    pub unique_docs_bytes: u32,
}

impl PostingHeader {
    const SIZE_BYTES: usize = 3 + 4 * 6;

    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let mut buf = [0u8; 3];
        r.read_exact(&mut buf[..])?;
        Ok(Self {
            trigram: Trigram(buf),
            unique_successors_count: r.read_u32::<LittleEndian>()?,
            unique_successors_bytes: r.read_u32::<LittleEndian>()?,
            successors_count: r.read_u32::<LittleEndian>()?,
            successors_bytes: r.read_u32::<LittleEndian>()?,
            unique_docs_count: r.read_u32::<LittleEndian>()?,
            unique_docs_bytes: r.read_u32::<LittleEndian>()?,
        })
    }

    // TODO make these less error prone
    fn unique_successors_section(&self) -> UniqueSuccessorsSection {
        Section::new(Self::SIZE_BYTES as u64, self.unique_successors_bytes as u64)
    }

    fn successors_section(&self) -> SuccessorsSection {
        Section::new(
            Self::SIZE_BYTES as u64 + self.unique_successors_bytes as u64,
            self.successors_bytes as u64,
        )
    }

    fn unique_docs_section(&self) -> UniqueDocsSection {
        Section::new(
            Self::SIZE_BYTES as u64
                + self.unique_successors_bytes as u64
                + self.successors_bytes as u64,
            self.unique_docs_bytes as u64,
        )
    }
}

impl StreamWriter for PostingHeader {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        w.write_all(&<[u8; 3]>::from(self.trigram))?;
        w.write_u32::<LittleEndian>(self.unique_successors_count)?;
        w.write_u32::<LittleEndian>(self.unique_successors_bytes)?;
        w.write_u32::<LittleEndian>(self.successors_count)?;
        w.write_u32::<LittleEndian>(self.successors_bytes)?;
        w.write_u32::<LittleEndian>(self.unique_docs_count)?;
        w.write_u32::<LittleEndian>(self.unique_docs_bytes)?;
        Ok(6 * std::mem::size_of::<u32>() + 3)
    }
}

// Named types for each unique type of section
type UniqueTrigramsSection = Section;
type TrigramPostingEndsSection = Section;
type TrigramPostingsSection = Section;
type TrigramPostingSection = Section<TrigramPostingsSection>;
type UniqueSuccessorsSection = Section<TrigramPostingSection>;
type UniqueDocsSection = Section<TrigramPostingSection>;
type SuccessorsSection = Section<TrigramPostingSection>;

struct DocIDMapper<DI, LDI> {
    doc_id_iterator: DI,
    local_doc_iterator: LDI,
}

impl<DI, LDI> DocIDMapper<DI, LDI>
where
    DI: Iterator<Item = (LocalDocID, DocID)>,
    LDI: Iterator<Item = LocalDocID>,
{
    pub fn new(doc_id_iterator: DI, local_doc_iterator: LDI) -> Self {
        Self {
            doc_id_iterator,
            local_doc_iterator,
        }
    }
}

impl<DI, LDI> Iterator for DocIDMapper<DI, LDI>
where
    DI: Iterator<Item = (LocalDocID, DocID)>,
    LDI: Iterator<Item = LocalDocID>,
{
    type Item = DocID;

    fn next(&mut self) -> Option<Self::Item> {
        let ldi = self.local_doc_iterator.next()?;
        while let Some((local_id, doc_id)) = self.doc_id_iterator.next() {
            // TODO we can likely make this more efficient by skipping chunks at a time
            if local_id == ldi {
                return Some(doc_id);
            }
        }
        None
    }
}

fn reader_at<R: ReadAt>(r: &R, offset: u64) -> BufReader<Cursor<&R>> {
    let cursor = Cursor::new_at(r, offset);
    BufReader::new(cursor)
}
