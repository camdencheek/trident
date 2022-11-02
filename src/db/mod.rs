use std::io::{self, Write};

use crate::ioutil::stream::StreamWrite;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};

// TODO should these be defined in a higher-level module?
type Trigram = [u8; 3];
type ShardID = u16;
type OID = [u8; 20];
type BlockID = u32;

enum DBKey {
    Shard(ShardID, ShardKey),
}

impl DBKey {
    fn discriminant(&self) -> u8 {
        match self {
            Self::Shard(_, _) => 0,
        }
    }
}

impl StreamWrite for DBKey {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        let mut n = self.discriminant().write_to(w)?;
        match self {
            Self::Shard(id, key) => {
                n += id.write_to(w)?;
                n += key.write_to(w)?;
            }
        }
        Ok(n)
    }
}

enum ShardKey {
    BlobIndex(BlobIndexKey),
    BlobContents(OID),
}

impl ShardKey {
    fn discriminant(&self) -> u8 {
        match self {
            Self::BlobIndex(_) => 0,
            Self::BlobContents(_) => 1,
        }
    }
}

impl StreamWrite for ShardKey {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        let mut n = self.discriminant().write_to(w)?;
        match self {
            Self::BlobIndex(key) => n += key.write_to(w)?,
            Self::BlobContents(oid) => n += oid.write_to(w)?,
        };
        Ok(n)
    }
}

enum BlobIndexKey {
    TrigramPosting(Trigram, TrigramPostingKey),
}

impl BlobIndexKey {
    fn discriminant(&self) -> u8 {
        match self {
            Self::TrigramPosting(_, _) => 0,
        }
    }
}

impl StreamWrite for BlobIndexKey {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        let mut n = self.discriminant().write_to(w)?;
        match self {
            Self::TrigramPosting(trigram, key) => {
                n += trigram.write_to(w)?;
                n += key.write_to(w)?;
            }
        };
        Ok(n)
    }
}

enum TrigramPostingKey {
    SuccessorCount,
    MatrixCount,
    DocCount,
    SuccessorsBlock(BlockID),
    MatrixBlock(BlockID),
    DocsBlock(BlockID),
}

impl TrigramPostingKey {
    fn discriminant(&self) -> u8 {
        match self {
            Self::SuccessorCount => 0,
            Self::MatrixCount => 1,
            Self::DocCount => 2,
            Self::SuccessorsBlock(_) => 3,
            Self::MatrixBlock(_) => 4,
            Self::DocsBlock(_) => 5,
        }
    }
}

impl StreamWrite for TrigramPostingKey {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<usize> {
        let mut n = self.discriminant().write_to(w)?;
        match self {
            Self::SuccessorCount | Self::MatrixCount | Self::DocCount => {}
            Self::SuccessorsBlock(b) | Self::MatrixBlock(b) | Self::DocsBlock(b) => {
                n += b.write_to(w)?
            }
        };
        Ok(n)
    }
}
