#![feature(array_windows)]
#![feature(is_sorted)]
#![feature(split_array)]

pub mod file_cursor;
pub mod index;
pub mod serialize;

type Trigram = [u8; 3];
type TrigramID = u32;
type LocalTrigramID = u32;
type DocID = u32;
