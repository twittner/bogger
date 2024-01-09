mod block;
mod reader;
mod writer;

use std::{path::Path, io, ffi::OsStr};
use tokio::fs;

use crate::BLOCK_FILENAME_PREFIX;

pub use block::{BlockInfo, BlockNum};
pub use reader::{EntryReader, ReadError};
pub use writer::{EntryWriter, WriteError};

pub(crate) use writer::latest_block_number;

#[derive(Debug)]
pub struct Config {
    max_buffer_len: usize,
    max_block_len: u64,
    max_entry_len: u16
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_buffer_len: 8192,
            max_block_len: 1024 * 1024,
            max_entry_len: 1024
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Self::default()
    }

    pub fn with_max_buffer_len(mut self, val: usize) -> Self {
        self.max_buffer_len = val;
        self
    }

    pub fn with_max_block_len(mut self, val: u64) -> Self {
        self.max_block_len = val;
        self
    }

    pub fn with_max_entry_len(mut self, val: u16) -> Self {
        self.max_entry_len = val;
        self
    }
}

pub async fn delete_blocks<P>(dir: P, to: BlockNum) -> io::Result<()>
where
    P: AsRef<Path>
{
    let mut dir = fs::read_dir(dir.as_ref()).await?;
    while let Some(e) = dir.next_entry().await? {
        if !e.file_name().to_str().map(|n| n.starts_with(BLOCK_FILENAME_PREFIX)).unwrap_or(false) {
            continue
        }
        if !e.file_type().await?.is_file() {
            continue
        }
        let p = e.path();
        if read_block_num(&p) < to {
            fs::remove_file(&p).await?
        }
    }
    Ok(())
}

fn block_file_name(n: BlockNum) -> String {
    format!("{BLOCK_FILENAME_PREFIX}{}", n.value())
}

pub(crate) fn read_block_num(p: impl AsRef<Path>) -> BlockNum {
    let n = p.as_ref()
        .extension()
        .and_then(OsStr::to_str)
        .and_then(|e| e.parse().ok())
        .unwrap_or(0);
    BlockNum::from(n)
}
