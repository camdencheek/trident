#![feature(array_windows)]
#![feature(is_sorted)]
#![feature(split_array)]

pub mod build;
pub mod index;
pub mod ioutil;

type TrigramID = u32;
type LocalSuccessorID = u32;
type DocID = u32;
type LocalDocID = u32;

#[derive(Eq, PartialOrd, Ord, PartialEq, Hash, Copy, Clone)]
pub struct Trigram([u8; 3]);

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
    use quickcheck::quickcheck;

    quickcheck! {
        fn trigram_id_roundtrip(b1: u8, b2: u8, b3: u8) -> bool {
            Trigram::from(TrigramID::from(Trigram([b1, b2, b3]))) == Trigram([b1, b2, b3])
        }
    }
}
