#![feature(array_windows)]
#![feature(is_sorted)]
#![feature(split_array)]

pub mod build;
pub mod index;
pub mod ioutil;

type Trigram = [u8; 3];
type TrigramID = u32;
type LocalTrigramID = u32;
type DocID = u32;
