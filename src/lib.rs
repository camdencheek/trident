#![feature(array_windows)]
#![feature(split_array)]

pub mod file_cursor;
pub mod index;
pub mod serialize;

type Trigram = [u8; 3];
