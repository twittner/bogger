mod fs;
mod logger;
mod forward;

pub use fs::{BlockInfo, BlockNum, EntryReader, EntryWriter, Config, ReadError, WriteError};
pub use fs::delete_blocks;
pub use logger::{Logger, LogError};
pub use forward::{Forwarder, ForwardError, Record, Handshake, HandshakeResponse, Ack};

const CRC32C: crc::Crc<u32> =
    crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

const BLOCK_FILENAME_PREFIX: &str = "block.";
