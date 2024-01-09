use crate::{CRC32C, BLOCK_FILENAME_PREFIX};
use std::{path::{Path, PathBuf}, io};
use tokio::{io::{BufWriter, AsyncWriteExt}, fs::{File, OpenOptions, self}};
use super::{Config, block_file_name, read_block_num};
use super::block::{Block, BlockInfo, BlockNum, BlockHeader};

#[derive(Debug)]
pub struct EntryWriter {
    header: BlockHeader,
    config: Config,
    directory: PathBuf,
    current: Block<BufWriter<File>>,
    buffer: Vec<u8>
}

impl EntryWriter {
    pub async fn open<P>(dir: P, cfg: Config) -> Result<Self, WriteError>
    where
        P: AsRef<Path>
    {
        let path = dir.as_ref().to_path_buf();
        if !path.is_dir() {
            return Err(WriteError::NoDir(path))
        }
        let num = latest_block_number(&path).await?.add(1u8);
        let buf = cfg.max_buffer_len;
        let mut this = Self {
            header: BlockHeader::default(),
            config: cfg,
            current: {
                let f = append_to(buf, path.join(block_file_name(num))).await?;
                let i = BlockInfo::zero().with_number(num);
                Block::new(f).with_info(i)
            },
            directory: path,
            buffer: Vec::new()
        };
        this.write_header().await?;
        Ok(this)
    }

    pub async fn append(&mut self, entry: &[u8]) -> Result<(), WriteError> {
        if entry.len() > self.config.max_entry_len.into() {
            return Err(WriteError::EntrySize)
        }
        let crc = CRC32C.checksum(entry);
        self.buffer.clear();
        self.buffer.extend_from_slice(&(entry.len() as u16).to_be_bytes());
        self.buffer.extend_from_slice(entry);
        self.buffer.extend_from_slice(&crc.to_be_bytes());
        if self.current.info().offset() + self.buffer.len() as u64 > self.config.max_block_len {
            self.start_new_block().await?
        }
        self.current.file_mut().write_all(&self.buffer).await?;
        self.current.info_mut().add_offset(self.buffer.len() as u64);
        Ok(())
    }

    pub async fn sync(&mut self) -> Result<(), WriteError> {
        self.current.file_mut().flush().await?;
        self.current.file_mut().get_mut().sync_data().await?;
        Ok(())
    }

    async fn start_new_block(&mut self) -> Result<(), WriteError> {
        self.sync().await?;
        let n = self.current.info().number().add(1u8);
        let f = append_to(self.config.max_buffer_len, self.directory.join(block_file_name(n))).await?;
        let i = BlockInfo::zero().with_number(n);
        self.current = Block::new(f).with_info(i);
        self.write_header().await?;
        Ok(())
    }

    async fn write_header(&mut self) -> Result<(), WriteError> {
        self.current.file_mut().write_u64(self.header.to_u64()).await?;
        self.current.info_mut().add_offset(8u8);
        Ok(())
    }
}

async fn append_to(buf: usize, path: impl AsRef<Path>) -> Result<BufWriter<File>, WriteError> {
    OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(path)
        .await
        .map(|f| BufWriter::with_capacity(buf, f))
        .map_err(WriteError::Io)
}

pub(crate) async fn latest_block_number(dir: &Path) -> io::Result<BlockNum> {
    let mut latest = BlockNum::zero();
    let mut dir = fs::read_dir(dir).await?;
    while let Some(e) = dir.next_entry().await? {
        if !e.file_name().to_str().map(|n| n.starts_with(BLOCK_FILENAME_PREFIX)).unwrap_or(false) {
            continue
        }
        if !e.file_type().await?.is_file() {
            continue
        }
        let n = read_block_num(e.path());
        if latest < n {
            latest = n
        }
    }
    Ok(latest)
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("not a directory: {0:?}")]
    NoDir(PathBuf),

    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("entry too large")]
    EntrySize
}
