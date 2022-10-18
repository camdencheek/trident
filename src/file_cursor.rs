use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;

pub struct FileCursor {
    f: File,
    offset: u64,
}

impl FileCursor {
    pub fn new(f: File) -> Self {
        Self { f, offset: 0 }
    }
}

impl Read for FileCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.f.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.f.read_exact_at(buf, self.offset)?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}

impl Write for FileCursor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.f.write_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.f.write_all_at(buf, self.offset)?;
        self.offset += buf.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.f.flush()
    }
}
