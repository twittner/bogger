use std::{path::{PathBuf, Path}, time::Duration, io, fmt, convert::Infallible, iter::repeat};

use bytes::Bytes;
use futures_util::future::{self, Either};
use minicbor::{Encode, Decode, Encoder, encode::{self, Write}, Decoder, decode};
use minicbor_io::{AsyncWriter, AsyncReader};
use tokio::{net::{TcpStream, tcp::{OwnedWriteHalf, OwnedReadHalf}}, time::sleep, fs, spawn};
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat, TokioAsyncReadCompatExt};
use tracing::{debug, error, trace, warn};

use crate::{BlockInfo, fs::{read_block_num, latest_block_number}, EntryReader, ReadError, BLOCK_FILENAME_PREFIX, delete_blocks, CRC32C, BlockNum};

type Reader = AsyncReader<Compat<OwnedReadHalf>>;
type Writer = AsyncWriter<Compat<OwnedWriteHalf>>;

#[derive(Debug)]
pub struct Forwarder {
    id: String,
    directory: PathBuf,
    address: String,
    latest: BlockNum
}

impl Forwarder {
    pub async fn new<P, S>(id: S, dir: P, address: &str) -> Result<Self, ForwardError>
    where
        P: AsRef<Path>,
        S: ToString
    {
        let path = dir.as_ref().to_path_buf();
        if !path.is_dir() {
            return Err(ForwardError::NoDir(path))
        }
        let latest = latest_block_number(&path).await?;
        Ok(Self {
            id: id.to_string(),
            directory: path,
            address: address.to_string(),
            latest
        })
    }

    pub async fn go(self) -> ! {
        loop {
            let (r, w, s) = self.connect().await;
            let forwarder = spawn(forward(self.directory.clone(), w, s));
            let receiver  = spawn(handle_acks(self.directory.clone(), r));
            match future::select(forwarder, receiver).await {
                Either::Right((Ok(Ok(())), f)) => {
                    warn!("connection to remote lost");
                    f.abort()
                }
                Either::Left((Ok(Ok(_)), _)) => {
                    unreachable!("forwarder never returns an ok value")
                }
                Either::Left((Ok(Err(err)), r)) => {
                    error!(%err, "forwarder error");
                    r.abort()
                }
                Either::Right((Ok(Err(err)), f)) => {
                    error!(%err, "receiver error");
                    f.abort()
                }
                Either::Left((Err(err), r)) => {
                    error!(%err, "receiver task error");
                    r.abort()
                }
                Either::Right((Err(err), f)) => {
                    error!(%err, "forwarder task error");
                    f.abort()
                }
            }
        }
    }

    async fn connect(&self) -> (Reader, Writer, BlockInfo) {
        let mut delays = [1, 1, 1, 1, 1, 5, 5, 5, 5, 5].into_iter().chain(repeat(10));
        loop {
            debug!(addr = %self.address, "connecting...");
            match TcpStream::connect(&self.address).await {
                Ok(s) => {
                    let addr = s.peer_addr().ok();
                    debug!(remote = ?addr, "connected");
                    let (r, w) = s.into_split();
                    let mut r = AsyncReader::new(r.compat());
                    let mut w = AsyncWriter::new(w.compat_write());
                    if let Err(err) = w.write(Handshake::new(&self.id, self.latest)).await {
                        error!(%err, remote = ?addr, "failed to send handshake");
                        continue
                    }
                    match r.read::<HandshakeResponse>().await {
                        Ok(Some(HandshakeResponse::Go { start })) => {
                            debug! {
                                remote = ?addr,
                                start  = %start,
                                "received handshake response"
                            }
                            return (r, w, start)
                        }
                        Ok(Some(HandshakeResponse::Abort { message })) => {
                            error! {
                                remote  = ?addr,
                                message = %message,
                                "server sent abort response"
                            }
                            panic!("server sent abort message")
                        }
                        Ok(None) => error! {
                            remote = ?addr, "remote closed connection after handshake"
                        },
                        Err(err) => error! {
                            %err, remote = ?addr, "failed to receive handshake response"
                        }
                    }
                }
                Err(err) => {
                    error!(%err, addr = %self.address, "failed to connect");
                    sleep(Duration::from_secs(delays.next().unwrap_or(10))).await
                }
            }
        }
    }
}

async fn handle_acks(dir: PathBuf, mut rsock: Reader) -> Result<(), ForwardError> {
    let mut prev = Ack::zero();
    while let Some(ack) = rsock.read::<Ack>().await? {
        if ack.info.number() > prev.info.number() {
            prev = ack;
            delete_blocks(&dir, ack.info.number()).await?;
        }
    }
    Ok(())
}

async fn forward(dir: PathBuf, mut wsock: Writer, start: BlockInfo) -> Result<Infallible, ForwardError> {
    let (mut info, mut size) = (start, 0);

    'main: loop {
        (info, size) = updated_block(&dir, info, size).await;
        let mut errors = 0;
        let mut reader = loop {
            match EntryReader::open(&dir, info).await {
                Ok(reader) => break reader,
                Err(err) => {
                    error!(%info, %err, "error opening block");
                    sleep(Duration::from_secs(5)).await
                }
            }
            if errors < 3 {
                errors += 1;
                sleep(Duration::from_secs(1)).await
            } else {
                error!(%info, "moving to next block");
                info.add_number(1);
                size = 0;
                continue 'main
            }
        };
        while let Some((bytes, crc)) = reader.next_entry().await? {
            let r = Record { info, item: Binary(bytes), crc };
            wsock.write(&r).await?;
            info = reader.block_info()
        }
    }
}

async fn updated_block(dir: &Path, info: BlockInfo, size: u64) -> (BlockInfo, u64) {
    async fn find_updated_block(dir: &Path, info: BlockInfo, size: u64) -> io::Result<Option<(BlockInfo, u64)>> {
        trace!(?dir, %info, "looking for block updates");
        let mut dir = fs::read_dir(dir).await?;
        let mut closest: Option<(BlockInfo, u64)> = None;
        while let Some(e) = dir.next_entry().await? {
            if !e.file_name().to_str().map(|n| n.starts_with(BLOCK_FILENAME_PREFIX)).unwrap_or(false) {
                continue
            }
            if !e.file_type().await?.is_file() {
                continue
            }
            let n = read_block_num(e.path());
            if n == info.number() {
                let s = e.metadata().await?.len();
                if s > size {
                    return Ok(Some((info, s)))
                }
            }
            if n > info.number() && closest.map(|(c, _)| n < c.number()).unwrap_or(true) {
                let s = e.metadata().await?.len();
                if s > 0 {
                    closest = Some((BlockInfo::zero().with_number(n), s))
                }
            }
        }
        Ok(closest)
    }

    loop {
        match find_updated_block(dir, info, size).await {
            Ok(Some(val)) => return val,
            Ok(None) => sleep(Duration::from_secs(1)).await,
            Err(err) => {
                error!{
                    path  = ?dir,
                    size  = %size,
                    info  = %info,
                    err   = %err,
                    "failed to find updated block"
                }
                sleep(Duration::from_secs(5)).await
            }
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct Handshake<'a> {
    #[n(0)] id: &'a str,
    #[n(1)] latest: BlockNum
}

impl<'a> Handshake<'a> {
    pub fn new(id: &'a str, latest: BlockNum) -> Self {
        Self { id, latest }
    }

    pub fn id(&self) -> &'a str {
        self.id
    }

    pub fn latest(&self) -> BlockNum {
        self.latest
    }
}

#[derive(Debug, Encode, Decode)]
pub enum HandshakeResponse<'a> {
    #[n(0)] Go {
        #[n(0)] start: BlockInfo
    },
    #[n(1)] Abort {
        #[n(0)] message: &'a str
    }
}

impl<'a> HandshakeResponse<'a> {
    pub fn go(start: BlockInfo) -> Self {
        Self::Go { start }
    }

    pub fn abort(msg: &'a str) -> Self {
        Self::Abort { message: msg }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct Record {
    #[n(0)] info: BlockInfo,
    #[n(1)] item: Binary,
    #[n(2)] crc: u32
}

impl Record {
    pub fn info(&self) -> BlockInfo {
        self.info
    }

    pub fn item(&self) -> impl AsRef<[u8]> + Clone + fmt::Debug {
        self.item.clone()
    }

    pub fn crc(&self) -> u32 {
        self.crc
    }

    pub fn is_valid(&self) -> bool {
        self.crc == CRC32C.checksum(self.item.as_ref())
    }
}

#[derive(Debug, Clone, Copy, Encode, Decode)]
pub struct Ack {
    #[n(0)] info: BlockInfo
}

impl Ack {
    pub fn new(info: BlockInfo) -> Self {
        Self { info }
    }

    pub fn zero() -> Self {
        Ack { info: BlockInfo::zero() }
    }

    pub fn info(&self) -> BlockInfo {
        self.info
    }
}

impl fmt::Display for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{info: {}}}", self.info)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    #[error("not a directory: {0:?}")]
    NoDir(PathBuf),

    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("read error: {0}")]
    Read(#[from] ReadError),

    #[error("send error: {0}")]
    Send(#[from] minicbor_io::Error)
}

#[derive(Debug, Clone)]
struct Binary(Bytes);

impl AsRef<[u8]> for Binary {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<C> Encode<C> for Binary {
    fn encode<W>(&self, e: &mut Encoder<W>, _: &mut C) -> Result<(), encode::Error<W::Error>>
    where
        W: Write
    {
        e.bytes(&self.0)?.ok()
    }
}

impl<'b, C> Decode<'b, C> for Binary {
    fn decode(d: &mut Decoder<'b>, _: &mut C) -> Result<Self, decode::Error> {
        d.bytes().map(|b| Binary(Bytes::copy_from_slice(b)))
    }
}
