use std::time::Duration;

// Stats collected about the indexing process
#[derive(Debug)]
pub struct IndexStats {
    pub extract: ExtractStats,

    pub build: BuildStats,

    // The total time between when the builder was created and when the build completed
    pub total_time: Duration,
}

#[derive(Debug)]
pub struct ExtractStats {
    // The number of documents indexed
    pub num_docs: usize,

    // The total size of documents indexed
    pub doc_bytes: usize,

    // The number of unique trigrams in the indexed documents
    pub unique_trigrams: usize,

    // The total time it took to extract trigrams from docs
    pub extract_time: Duration,
}

#[derive(Debug)]
pub struct BuildStats {
    // The aggregated minimums for all trigram posting stats
    pub postings_min: TrigramPostingStats,

    // The aggregated maximums for all trigram posting stats
    pub postings_max: TrigramPostingStats,

    // The aggregated sum of all trigram posting stats
    pub postings_sum: TrigramPostingStats,

    pub posting_offsets_bytes: usize,

    // The total time it took to write the index to disk
    pub build_time: Duration,
}

impl Default for BuildStats {
    fn default() -> Self {
        Self {
            postings_min: TrigramPostingStats::new_max(),
            postings_max: TrigramPostingStats::default(),
            postings_sum: TrigramPostingStats::default(),
            posting_offsets_bytes: 0,
            build_time: Duration::default(),
        }
    }
}

impl BuildStats {
    pub fn add_posting(&mut self, other: &TrigramPostingStats) {
        self.postings_min = self.postings_min.min(other);
        self.postings_max = self.postings_max.max(other);
        self.postings_sum = self.postings_sum.sum(other);
    }

    pub fn total_size_bytes(&self) -> usize {
        self.postings_sum.total_bytes() + self.posting_offsets_bytes
    }
}

// Stats for a single trigram posting list
#[derive(Default, Debug)]
pub struct TrigramPostingStats {
    pub header_bytes: usize,

    // Stats for the unique successors
    pub unique_successors: SequenceStats,

    // Stats for the equal-doc run lengths in successors
    pub run_lengths: SequenceStats,

    // Stats for the successors list
    pub successors: SequenceStats,

    // Stats for the unique doc IDs
    pub unique_docs: SequenceStats,
}

impl TrigramPostingStats {
    pub fn new_max() -> Self {
        Self {
            header_bytes: usize::MAX,
            unique_successors: SequenceStats::new_max(),
            run_lengths: SequenceStats::new_max(),
            successors: SequenceStats::new_max(),
            unique_docs: SequenceStats::new_max(),
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.header_bytes
            + self.unique_successors.bytes
            + self.run_lengths.bytes
            + self.successors.bytes
            + self.unique_docs.bytes
    }

    pub fn max(&self, other: &TrigramPostingStats) -> TrigramPostingStats {
        Self {
            header_bytes: self.header_bytes.max(other.header_bytes),
            unique_successors: self.unique_successors.max(&other.unique_successors),
            unique_docs: self.unique_docs.max(&other.unique_docs),
            run_lengths: self.run_lengths.max(&other.run_lengths),
            successors: self.successors.max(&other.successors),
        }
    }

    pub fn min(&self, other: &TrigramPostingStats) -> TrigramPostingStats {
        Self {
            header_bytes: self.header_bytes.min(other.header_bytes),
            unique_successors: self.unique_successors.min(&other.unique_successors),
            unique_docs: self.unique_docs.min(&other.unique_docs),
            run_lengths: self.run_lengths.min(&other.run_lengths),
            successors: self.successors.min(&other.successors),
        }
    }

    pub fn sum(&self, other: &TrigramPostingStats) -> TrigramPostingStats {
        Self {
            header_bytes: self.header_bytes + other.header_bytes,
            unique_successors: self.unique_successors.sum(&other.unique_successors),
            unique_docs: self.unique_docs.sum(&other.unique_docs),
            run_lengths: self.run_lengths.sum(&other.run_lengths),
            successors: self.successors.sum(&other.successors),
        }
    }
}

// Stats about the serialization of an integer sequence
#[derive(Default, Debug)]
pub struct SequenceStats {
    // The length of the sequence
    pub len: usize,

    // The size of the compressed sequence in bytes
    pub bytes: usize,
}

impl SequenceStats {
    pub fn new_max() -> Self {
        Self {
            len: usize::MAX,
            bytes: usize::MAX,
        }
    }

    pub fn max(&self, other: &SequenceStats) -> SequenceStats {
        Self {
            len: self.len.max(other.len),
            bytes: self.bytes.max(other.bytes),
        }
    }

    pub fn min(&self, other: &SequenceStats) -> SequenceStats {
        Self {
            len: self.len.min(other.len),
            bytes: self.bytes.min(other.bytes),
        }
    }

    pub fn sum(&self, other: &SequenceStats) -> SequenceStats {
        Self {
            len: self.len + other.len,
            bytes: self.bytes + other.bytes,
        }
    }
}
