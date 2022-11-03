use anyhow::anyhow;
use std::io::{self, Read, Write};

use crate::ioutil::stream::{StreamRead, StreamWrite};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};

// TODO should these be defined in a higher-level module?
type Trigram = [u8; 3];
type ShardID = u16;
type OID = [u8; 20];
type BlockID = u32;

#[derive(PartialEq, Eq, Clone, Debug)]
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

impl StreamRead for DBKey {
    fn read_from<R: Read>(r: &mut R) -> anyhow::Result<Self> {
        match r.read_u8()? {
            0 => Ok(Self::Shard(ShardID::read_from(r)?, ShardKey::read_from(r)?)),
            _ => Err(anyhow!("bad discriminant")),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
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

impl StreamRead for ShardKey {
    fn read_from<R: Read>(r: &mut R) -> anyhow::Result<Self> {
        match r.read_u8()? {
            0 => Ok(Self::BlobIndex(BlobIndexKey::read_from(r)?)),
            1 => Ok(Self::BlobContents(OID::read_from(r)?)),
            _ => Err(anyhow!("bad discriminant")),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
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

impl StreamRead for BlobIndexKey {
    fn read_from<R: Read>(r: &mut R) -> anyhow::Result<Self> {
        match r.read_u8()? {
            0 => Ok(Self::TrigramPosting(
                Trigram::read_from(r)?,
                TrigramPostingKey::read_from(r)?,
            )),
            _ => Err(anyhow!("bad discriminant")),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
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

impl StreamRead for TrigramPostingKey {
    fn read_from<R: Read>(r: &mut R) -> anyhow::Result<Self> {
        match r.read_u8()? {
            0 => Ok(Self::SuccessorCount),
            1 => Ok(Self::MatrixCount),
            2 => Ok(Self::DocCount),
            3 => Ok(Self::SuccessorsBlock(BlockID::read_from(r)?)),
            4 => Ok(Self::MatrixBlock(BlockID::read_from(r)?)),
            5 => Ok(Self::DocsBlock(BlockID::read_from(r)?)),
            _ => Err(anyhow!("bad discriminant")),
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;
    use insta::assert_debug_snapshot;
    use quickcheck::{quickcheck, Arbitrary};

    impl Arbitrary for DBKey {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self::Shard(ShardID::arbitrary(g), ShardKey::arbitrary(g))
        }
    }

    impl Arbitrary for ShardKey {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            match u8::arbitrary(g) % 2 {
                0 => Self::BlobIndex(BlobIndexKey::arbitrary(g)),
                1 => Self::BlobContents(OID::arbitrary(g)),
                _ => unreachable!(),
            }
        }
    }

    impl Arbitrary for BlobIndexKey {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self::TrigramPosting(Trigram::arbitrary(g), TrigramPostingKey::arbitrary(g))
        }
    }

    impl Arbitrary for TrigramPostingKey {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            match u8::arbitrary(g) % 6 {
                0 => Self::SuccessorCount,
                1 => Self::MatrixCount,
                2 => Self::DocCount,
                3 => Self::SuccessorsBlock(BlockID::arbitrary(g)),
                4 => Self::MatrixBlock(BlockID::arbitrary(g)),
                5 => Self::DocsBlock(BlockID::arbitrary(g)),
                _ => unreachable!(),
            }
        }
    }

    quickcheck! {
        // Test that any DBKey can be roundtripped
        fn db_key_roundtrip(key: DBKey) -> bool {
            let v = key.to_vec();
            let mut r = Cursor::new(v);
            key == DBKey::read_from(&mut r).unwrap()
        }
    }

    // This test checks that the sort order of the set of keys does not change.
    // This is important for iterating over entries in order.
    #[test]
    fn stable_sort_order() {
        let keys = [
            DBKey::Shard(
                42,
                ShardKey::BlobIndex(BlobIndexKey::TrigramPosting(
                    *b"abc",
                    TrigramPostingKey::DocCount,
                )),
            ),
            DBKey::Shard(
                42,
                ShardKey::BlobIndex(BlobIndexKey::TrigramPosting(
                    *b"abc",
                    TrigramPostingKey::MatrixBlock(24),
                )),
            ),
            DBKey::Shard(
                42,
                ShardKey::BlobIndex(BlobIndexKey::TrigramPosting(
                    *b"abc",
                    TrigramPostingKey::SuccessorCount,
                )),
            ),
            DBKey::Shard(
                42,
                ShardKey::BlobIndex(BlobIndexKey::TrigramPosting(
                    *b"abc",
                    TrigramPostingKey::MatrixBlock(42),
                )),
            ),
            DBKey::Shard(42, ShardKey::BlobContents([0; 20])),
            DBKey::Shard(42, ShardKey::BlobContents([2; 20])),
            DBKey::Shard(
                35,
                ShardKey::BlobIndex(BlobIndexKey::TrigramPosting(
                    *b"abc",
                    TrigramPostingKey::DocCount,
                )),
            ),
        ];

        let mut serialized = keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
        serialized.sort();

        assert_debug_snapshot!(serialized)
    }
}
