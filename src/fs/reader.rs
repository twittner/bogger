use std::{path::Path, io::SeekFrom};

use bytes::{BytesMut, Bytes};
use tokio::{io::{BufReader, self, AsyncReadExt, AsyncSeekExt}, fs::File};

use crate::{CRC32C, BlockInfo};
use super::{block::BlockHeader, block_file_name};

#[derive(Debug)]
pub struct EntryReader {
    inner: BufReader<File>,
    buffer: BytesMut,
    info: BlockInfo
}

impl EntryReader {
    pub async fn open<P>(dir: P, info: BlockInfo) -> Result<Self, ReadError>
    where
        P: AsRef<Path>
    {
        let mut file = {
            let path = dir.as_ref().join(block_file_name(info.number()));
            BufReader::with_capacity(32 * 1024, File::open(path).await?)
        };
        read_header(&mut file).await?;
        let info =
            if info.offset() == 0 {
                info.with_offset(8u8) // header length
            } else {
                file.seek(SeekFrom::Start(info.offset())).await?;
                info
            };
        Ok(Self {
            inner: file,
            buffer: BytesMut::new(),
            info
        })
    }

    pub fn block_info(&self) -> BlockInfo {
        self.info
    }

    pub async fn reset(&mut self, info: BlockInfo) -> Result<(), ReadError> {
        assert_eq!(info.number(), self.info.number());
        self.inner.seek(SeekFrom::Start(info.offset())).await?;
        self.info = info;
        Ok(())
    }

    pub async fn next_entry(&mut self) -> Result<Option<(Bytes, u32)>, ReadError> {
        match self.inner.read_u16().await {
            Ok(len) => {
                self.buffer.clear();
                self.buffer.resize(len as usize, 0);
                self.inner.read_exact(&mut self.buffer).await?;
                let crc = self.inner.read_u32().await?;
                self.info.add_offset(2 + len + 4);
                if crc != CRC32C.checksum(&self.buffer) {
                    return Err(ReadError::Crc)
                }
                Ok(Some((self.buffer.split().freeze(), crc)))
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }
}

async fn read_header(r: &mut BufReader<File>) -> Result<BlockHeader, ReadError> {
    let number = r.read_u64().await?;
    if let Some(h) = BlockHeader::from_u64(number) {
        if h.version() != 1 {
            return Err(ReadError::Header(Some(h.version())))
        }
        Ok(h)
    } else {
        Err(ReadError::Header(None))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("crc check failed")]
    Crc,

    #[error("header {0:?} not supported")]
    Header(Option<u8>),
}
