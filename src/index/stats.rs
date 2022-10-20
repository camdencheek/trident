// Stats collected about the indexing process
pub struct IndexStats {
    // The number of documents indexed
    pub num_docs: usize,

    // The total size of documents indexed
    pub doc_bytes: usize,

    // The number of unique trigrams in the indexed documents
    pub unique_trigrams: usize,

    // The aggregated minimums for all trigram posting stats
    pub postings_min: TrigramPostingStats,

    // The aggregated maximums for all trigram posting stats
    pub postings_max: TrigramPostingStats,

    // The aggregated sum of all trigram posting stats
    pub postings_sum: TrigramPostingStats,

    // The total size of the index on disk
    pub total_index_size_bytes: usize,

    // The total time it took to build the index
    pub index_time: Duration,
}

impl IndexStats {
    pub fn add_posting(&mut self, posting: TrigramPostingStats) {
        self.postings_min = self.postings_min.min(&posting);
        self.postings_max = self.postings_max.max(&posting);
        self.postings_sum = self.postings_max.sum(&posting);
    }
}

// Stats for a single trigram posting list
pub struct TrigramPostingStats {
    // Stats for the unique successors
    pub unique_successors: SequenceStats,

    // Stats for the unique doc IDs
    pub unique_docs: SequenceStats,

    // Stats for the equal-doc run lengths in successors
    pub run_lengths: SequenceStats,

    // Stats for the successors list
    pub successors: SequenceStats,
}

impl TrigramPostingStats {
    pub fn max(&self, other: &TrigramPostingStats) -> TrigramPostingStats {
        Self {
            unique_successors: self.unique_successors.max(&other.unique_successors),
            unique_docs: self.unique_docs.max(&other.unique_docs),
            run_lengths: self.run_lengths.max(&other.run_lengths),
            successors: self.successors.max(&other.successors),
        }
    }

    pub fn min(&self, other: &TrigramPostingStats) -> TrigramPostingStats {
        Self {
            unique_successors: self.unique_successors.min(&other.unique_successors),
            unique_docs: self.unique_docs.min(&other.unique_docs),
            run_lengths: self.run_lengths.min(&other.run_lengths),
            successors: self.successors.min(&other.successors),
        }
    }

    pub fn sum(&self, other: &TrigramPostingStats) -> TrigramPostingStats {
        Self {
            unique_successors: self.unique_successors.sum(&other.unique_successors),
            unique_docs: self.unique_docs.sum(&other.unique_docs),
            run_lengths: self.run_lengths.sum(&other.run_lengths),
            successors: self.successors.sum(&other.successors),
        }
    }
}

// Stats about the serialization of an integer sequence
struct SequenceStats {
    // The length of the sequence
    pub len: usize,

    // The size of the compressed sequence in bytes
    pub bytes: usize,
}

impl SequenceStats {
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
