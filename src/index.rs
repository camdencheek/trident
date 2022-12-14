use std::cmp::Ordering;
use std::io::BufReader;
use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use itertools::Itertools;

use super::ioutil::Section;
use crate::build::serialize::U32DeltaDecompressor;
use crate::ioutil::{Cursor, Len, ReadAt};
use crate::{build::serialize::StreamWriter, DocID, LocalDocIdx, Trigram};
use crate::{LocalSuccessorIdx, TrigramID};

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
        let header = Self::read_header(&r).context("read header")?;

        assert!(header.unique_trigrams.len % 3 == 0);
        let n_trigrams = header.unique_trigrams.len as usize / 3;
        let mut unique_trigrams = Vec::with_capacity(n_trigrams);
        let mut unique_trigrams_reader = reader_in(&r, header.unique_trigrams);
        for _ in 0..n_trigrams {
            let mut buf = [0u8; 3];
            unique_trigrams_reader.read_exact(&mut buf)?;
            unique_trigrams.push(Trigram(buf));
        }

        assert!(header.trigram_posting_ends.len % 4 == 0);
        assert!(header.trigram_posting_ends.len as usize / 4 == n_trigrams);
        let mut trigram_posting_ends = Vec::with_capacity(n_trigrams);
        let mut trigram_ends_reader = reader_in(&r, header.trigram_posting_ends);
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

    // Returns the posting section for the given trigram, if it exists.
    fn trigram_section(&self, t: Trigram) -> Option<TrigramPostingSection> {
        let trigram_idx = match self.unique_trigrams.binary_search(&t) {
            Ok(idx) => idx,
            // An Err variant means the trigram doesn't exist.
            Err(_) => return None,
        };

        let start = match trigram_idx {
            0 => 0,
            _ => self.trigram_posting_ends[trigram_idx - 1],
        };

        let end = self.trigram_posting_ends[trigram_idx];
        Some(Section::new(start, end - start))
    }

    // An estimate of the relative frequency of a trigram
    fn frequency(&self, t: Trigram) -> f32 {
        self.trigram_section(t).map(|s| s.len).unwrap_or(0) as f32
            / self.header.trigram_postings.len as f32
    }

    // Returns an iterator over the candidate document IDs.
    pub fn candidates<'a>(&'a self, query: &[u8]) -> Box<dyn Iterator<Item = DocID> + 'a> {
        if query.len() < 3 {
            // For now, just return an iterator over all docs if we don't have a searchable
            // trigram. This will force all docs to be brute-force searched.
            return Box::new(0..self.header.num_docs);
        }

        let (&leading_trigram, rest) = query.split_array_ref::<3>();
        let leading_trigram = Trigram(leading_trigram);
        let trigram_section = match self.trigram_section(leading_trigram) {
            Some(s) => s,
            // If the trigram doesn't exist, return early with an empty iterator
            None => return Box::new(std::iter::empty()),
        };

        let posting_header = {
            let absolute_section = self.header.trigram_postings.narrow(trigram_section);
            let mut reader = reader_in(&self.r, absolute_section);
            PostingHeader::read_from(&mut reader).unwrap()
        };

        let searcher = PostingSearcher::new(
            self.header.trigram_postings,
            trigram_section,
            posting_header,
            &self.r,
        );
        searcher.search(rest)
    }
}

struct PostingSearcher<'a, R> {
    postings_section: TrigramPostingsSection,
    posting_section: TrigramPostingSection,
    header: PostingHeader,
    r: &'a R,
}

impl<'a, R: ReadAt + Len> PostingSearcher<'a, R> {
    pub fn new(
        postings_section: TrigramPostingsSection,
        posting_section: TrigramPostingSection,
        header: PostingHeader,
        r: &'a R,
    ) -> Self {
        Self {
            postings_section,
            posting_section,
            header,
            r,
        }
    }

    fn successors(&self) -> impl Iterator<Item = TrigramID> + 'a {
        let section = self.postings_section.narrow(
            self.posting_section
                .narrow(self.header.successors_section()),
        );

        U32DeltaDecompressor::new(
            reader_in(self.r, section),
            self.header.successors_count as usize,
        )
    }

    fn matrix(&self) -> impl Iterator<Item = (LocalDocIdx, LocalSuccessorIdx)> + 'a {
        let section = self
            .postings_section
            .narrow(self.posting_section.narrow(self.header.matrix_section()));

        let raw = U32DeltaDecompressor::new(
            reader_in(self.r, section),
            self.header.matrix_count as usize,
        );

        let columns = self.header.successors_count;
        raw.map(move |i| (i / columns, i % columns))
    }

    fn docs(&self) -> impl Iterator<Item = DocID> + 'a {
        let section = self
            .postings_section
            .narrow(self.posting_section.narrow(self.header.docs_section()));

        U32DeltaDecompressor::new(reader_in(self.r, section), self.header.docs_count as usize)
    }

    fn search(self, remainder: &[u8]) -> Box<dyn Iterator<Item = DocID> + 'a> {
        match remainder.len() {
            // In the case where we have no extra successor information, we can just return the
            // list of unique doc IDs for the posting.
            0 => Box::new(self.docs()),

            // In the case where we do not have a full successor trigram, we find the range of
            // unique successor trigrams that share a prefix, then use that to filter the
            // successors matrix.
            1..=2 => {
                let mut target_prefix = 0u32;
                for b in remainder {
                    target_prefix = target_prefix << 8;
                    target_prefix += *b as u32;
                }
                let shift = (3 - remainder.len()) * 8;

                let (mut start, mut end) = (0u32, 0u32);
                for (local_successor_idx, successor) in self.successors().enumerate() {
                    let shifted = successor >> shift;
                    match shifted.cmp(&target_prefix) {
                        Ordering::Less => {
                            start = local_successor_idx as u32 + 1;
                            end = local_successor_idx as u32 + 1;
                        }
                        Ordering::Equal => {
                            end = local_successor_idx as u32 + 1;
                        }
                        _ => {}
                    }
                }

                if start == end {
                    // The range of matching successors is empty, so return early with no matches.
                    return Box::new(std::iter::empty());
                }

                let doc_iter = self
                    .matrix()
                    .filter_map(move |(local_doc_id, local_successor_id)| {
                        if (start..end).contains(&local_successor_id) {
                            Some(local_doc_id)
                        } else {
                            None
                        }
                    })
                    .dedup();

                Box::new(DocIDMapper::new(
                    self.docs().enumerate().map(|(i, j)| (i as u32, j)),
                    doc_iter,
                ))
            }

            // In the case where we have at least a full trigram, we filter to only successor
            // trigrams that exactly match that.
            _ => {
                let target_successor_id = TrigramID::from(Trigram::try_from(remainder).unwrap());
                let first_non_none =
                    self.successors()
                        .enumerate()
                        .find_map(|(local_id, successor_id)| {
                            if successor_id == target_successor_id {
                                Some(local_id)
                            } else {
                                None
                            }
                        });

                let target_local_successor_id = match first_non_none {
                    Some(l) => l as u32,
                    None => return Box::new(std::iter::empty()),
                };

                let doc_iter =
                    self.matrix()
                        .filter_map(move |(local_doc_id, local_successor_id)| {
                            if local_successor_id == target_local_successor_id {
                                Some(local_doc_id)
                            } else {
                                None
                            }
                        });

                Box::new(DocIDMapper::new(
                    self.docs().enumerate().map(|(i, j)| (i as u32, j)),
                    doc_iter,
                ))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexHeader {
    pub num_docs: u32,
    pub trigram_postings: TrigramPostingsSection,
    pub unique_trigrams: UniqueTrigramsSection,
    pub trigram_posting_ends: TrigramPostingEndsSection,
}

impl IndexHeader {
    // TODO: calculate this from member sizes
    const SIZE_BYTES: usize = 52;

    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let header = IndexHeader {
            num_docs: r.read_u32::<LittleEndian>()?,
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
        w.write_u32::<LittleEndian>(self.num_docs)?;
        let mut n = 4;
        n += self.trigram_postings.write_to(w)?;
        n += self.unique_trigrams.write_to(w)?;
        n += self.trigram_posting_ends.write_to(w)?;
        Ok(n)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostingHeader {
    pub trigram: Trigram,
    pub successors_count: u32,
    pub successors_bytes: u32,
    pub matrix_count: u32,
    pub matrix_bytes: u32,
    pub docs_count: u32,
    pub docs_bytes: u32,
}

impl PostingHeader {
    const SIZE_BYTES: usize = 3 + 4 * 6;

    fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let mut buf = [0u8; 3];
        r.read_exact(&mut buf[..])?;
        Ok(Self {
            trigram: Trigram(buf),
            successors_count: r.read_u32::<LittleEndian>()?,
            successors_bytes: r.read_u32::<LittleEndian>()?,
            matrix_count: r.read_u32::<LittleEndian>()?,
            matrix_bytes: r.read_u32::<LittleEndian>()?,
            docs_count: r.read_u32::<LittleEndian>()?,
            docs_bytes: r.read_u32::<LittleEndian>()?,
        })
    }

    // TODO make these less error prone
    fn successors_section(&self) -> SuccessorsSection {
        Section::new(Self::SIZE_BYTES as u64, self.successors_bytes as u64)
    }

    fn matrix_section(&self) -> MatrixSection {
        Section::new(
            Self::SIZE_BYTES as u64 + self.successors_bytes as u64,
            self.matrix_bytes as u64,
        )
    }

    fn docs_section(&self) -> DocsSection {
        Section::new(
            Self::SIZE_BYTES as u64 + self.successors_bytes as u64 + self.matrix_bytes as u64,
            self.docs_bytes as u64,
        )
    }
}

impl StreamWriter for PostingHeader {
    fn write_to<W: Write>(&self, w: &mut W) -> Result<usize> {
        w.write_all(&<[u8; 3]>::from(self.trigram))?;
        w.write_u32::<LittleEndian>(self.successors_count)?;
        w.write_u32::<LittleEndian>(self.successors_bytes)?;
        w.write_u32::<LittleEndian>(self.matrix_count)?;
        w.write_u32::<LittleEndian>(self.matrix_bytes)?;
        w.write_u32::<LittleEndian>(self.docs_count)?;
        w.write_u32::<LittleEndian>(self.docs_bytes)?;
        Ok(6 * std::mem::size_of::<u32>() + 3)
    }
}

// Named types for each unique type of section
type UniqueTrigramsSection = Section;
type TrigramPostingEndsSection = Section;
type TrigramPostingsSection = Section;
type TrigramPostingSection = Section<TrigramPostingsSection>;
type SuccessorsSection = Section<TrigramPostingSection>;
type DocsSection = Section<TrigramPostingSection>;
type MatrixSection = Section<TrigramPostingSection>;

struct DocIDMapper<DI, LDI> {
    doc_id_iterator: DI,
    local_doc_iterator: LDI,
}

impl<DI, LDI> DocIDMapper<DI, LDI>
where
    DI: Iterator<Item = (LocalDocIdx, DocID)>,
    LDI: Iterator<Item = LocalDocIdx>,
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
    DI: Iterator<Item = (LocalDocIdx, DocID)>,
    LDI: Iterator<Item = LocalDocIdx>,
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

fn reader_in<R: ReadAt>(r: &R, section: Section) -> BufReader<Cursor<&R>> {
    let cursor = Cursor::new_in(r, section);
    BufReader::new(cursor)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{build::IndexBuilder, ioutil::Mem};

    #[test]
    fn test_search() {
        let mut builder = IndexBuilder::new();
        builder.add_doc(b"test string 1").unwrap();
        builder.add_doc(b"test string 2").unwrap();
        builder.add_doc(b"abracadabra").unwrap();

        let mut output = Vec::new();
        builder.build(&mut output).unwrap();

        let index = Index::new(Mem(output)).unwrap();
        let doc_ids = index.candidates(b"string").collect::<Vec<DocID>>();
        assert_eq!(&doc_ids, &[0, 1]);

        let doc_ids = index.candidates(b"strin").collect::<Vec<DocID>>();
        assert_eq!(&doc_ids, &[0, 1]);

        let doc_ids = index.candidates(b"stri").collect::<Vec<DocID>>();
        assert_eq!(&doc_ids, &[0, 1]);

        let doc_ids = index.candidates(b"str").collect::<Vec<DocID>>();
        assert_eq!(&doc_ids, &[0, 1]);

        let doc_ids = index.candidates(b"abr").collect::<Vec<DocID>>();
        assert_eq!(&doc_ids, &[2]);
    }
}
