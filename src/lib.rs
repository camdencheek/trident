#![feature(array_windows)]
#![feature(is_sorted)]
#![feature(split_array)]

use std::fmt;

pub mod build;
pub mod index;
pub mod ioutil;

type TrigramID = u32;
type LocalSuccessorID = u32;
type DocID = u32;
type LocalDocID = u32;

#[derive(Default, Eq, PartialOrd, Ord, PartialEq, Hash, Copy, Clone)]
pub struct Trigram([u8; 3]);

impl fmt::Debug for Trigram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("\"{}\"", unsafe {
            std::str::from_utf8_unchecked(
                &self
                    .0
                    .iter()
                    .copied()
                    .flat_map(std::ascii::escape_default)
                    .collect::<Vec<u8>>(),
            )
        }))
    }
}

impl From<TrigramID> for Trigram {
    fn from(u: u32) -> Self {
        Trigram([
            ((u & 0x00FF0000) >> 16) as u8,
            ((u & 0x0000FF00) >> 8) as u8,
            (u & 0x000000FF) as u8,
        ])
    }
}

impl From<Trigram> for TrigramID {
    fn from(t: Trigram) -> Self {
        ((t.0[0] as u32) << 16) + ((t.0[1] as u32) << 8) + t.0[2] as u32
    }
}

impl From<Trigram> for [u8; 3] {
    fn from(t: Trigram) -> Self {
        t.0
    }
}

impl From<[u8; 3]> for Trigram {
    fn from(t: [u8; 3]) -> Self {
        Trigram(t)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use quickcheck::{quickcheck, Arbitrary};

    impl Arbitrary for Trigram {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self([u8::arbitrary(g), u8::arbitrary(g), u8::arbitrary(g)])
        }
    }

    quickcheck! {
        fn trigram_id_roundtrip(t: Trigram) -> bool {
            Trigram::from(TrigramID::from(t)) == t
        }
    }

    quickcheck! {
        fn trigram_as_u32_maintains_sort_order(t1: Trigram, t2: Trigram) -> bool {
            t1.cmp(&t2) == u32::from(t1).cmp(&u32::from(t2))
        }
    }
}
