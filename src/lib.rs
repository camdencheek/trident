#![feature(array_windows)]
#![feature(split_array)]

pub mod builder;
pub mod file_cursor;
pub mod serialize;

type Trigram = [u8; 3];
