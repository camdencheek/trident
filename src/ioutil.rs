use std::fs::File;
use std::io::{Read, Result, Seek, SeekFrom, Write};
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
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = self.f.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.f.read_exact_at(buf, self.offset)?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}

impl Write for FileCursor {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = self.f.write_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.f.write_all_at(buf, self.offset)?;
        self.offset += buf.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.f.flush()
    }
}

impl Seek for FileCursor {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Current(i) => self.offset = (self.offset as i64 + i) as u64,
            SeekFrom::Start(i) => self.offset = i,
            SeekFrom::End(i) => self.offset = (self.f.metadata()?.len() as i64 + i) as u64,
        };
        Ok(self.offset)
    }
}

struct IOSection<T> {
    start: u64,
    end: u64,
    rel_offset: u64,
    inner: T,
}

impl<R: Read> Read for IOSection<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Read nothing if the cursor is past end
        if self.rel_offset >= self.end {
            return Ok(0);
        }
        let sz = buf.len().max((self.end - self.rel_offset) as usize);
        let buf = &mut buf[..sz];
        let n = self.inner.read(buf)?;
        self.rel_offset += n as u64;
        Ok(n)
    }

    // TODO more efficient optional implementations for things like read_exact
}

impl<W: Write> Write for IOSection<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        // Write nothing if the cursor is past end
        if self.rel_offset >= self.end {
            return Ok(0);
        }
        let sz = buf.len().max((self.end - self.rel_offset) as usize);
        let buf = &buf[..sz];
        let n = self.inner.write(buf)?;
        self.rel_offset += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

impl<S: Seek> Seek for IOSection<S> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Current(i) => {
                let mut new = self.rel_offset as i64 + i;
                if new < 0 {
                    // TODO: we should probably throw an error here
                    new = 0;
                }
                self.rel_offset = new as u64;
            }
            SeekFrom::Start(i) => self.rel_offset = i,
            SeekFrom::End(i) => {
                let mut new = self.end as i64 + i;
                if new < 0 {
                    // TODO: we should probably throw an error here
                    new = 0;
                }
                self.rel_offset = new as u64;
            }
        };
        self.inner
            .seek(SeekFrom::Start(self.rel_offset + self.start))?;
        Ok(self.rel_offset)
    }
}
