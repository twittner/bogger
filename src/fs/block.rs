use std::fmt;

use minicbor::{Encode, Decode};

const HEADER_V1: u64 =
    u64::from_be_bytes([b'b', b'l', b'o', b'c', b'k', 1, 0, 0]);

#[derive(Debug, Clone, Copy)]
pub struct BlockHeader(u64);

impl Default for BlockHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockHeader {
    pub fn new() -> Self {
        Self(HEADER_V1)
    }

    pub fn from_u64(n: u64) -> Option<Self> {
        match n {
            HEADER_V1 => Some(Self(n)),
            _         => None
        }
    }

    pub fn to_u64(self) -> u64 {
        self.0
    }

    pub fn version(self) -> u8 {
        ((self.0 & 0xFF_00_00) >> 16) as u8
    }

    #[allow(unused)]
    pub fn with_version(self, v: u8) -> Self {
        Self(self.0 & 0xFF_FF_FF_FF_FF_00_FF_FF | ((v as u64) << 16))
    }
}

#[derive(Debug)]
pub struct Block<F> {
    info: BlockInfo,
    file: F
}

impl<F> Block<F> {
    pub fn new(f: F) -> Self {
        Self {
            info: BlockInfo::zero(),
            file: f
        }
    }

    pub fn info(&self) -> &BlockInfo {
        &self.info
    }

    pub fn with_info(mut self, i: BlockInfo) -> Self {
        self.info = i;
        self
    }

    pub fn info_mut(&mut self) -> &mut BlockInfo {
        &mut self.info
    }

    pub fn file_mut(&mut self) -> &mut F {
        &mut self.file
    }
}

#[derive(Debug, Clone, Copy, Encode, Decode, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockInfo {
    #[n(0)] number: BlockNum,
    #[n(1)] offset: u64
}

impl fmt::Display for BlockInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{block: {}, offset: {}}}", self.number.value(), self.offset)
    }
}

impl BlockInfo {
    pub fn zero() -> Self {
        Self {
            number: BlockNum::zero(),
            offset: 0
        }
    }

    pub fn is_zero(&self) -> bool {
        self.number.is_zero() && self.offset == 0
    }

    pub fn number(&self) -> BlockNum {
        self.number
    }

    pub fn with_number<N: Into<BlockNum>>(mut self, n: N) -> Self {
        self.number = n.into();
        self
    }

    pub fn set_number<N: Into<BlockNum>>(&mut self, n: N) {
        self.number = n.into()
    }

    pub fn add_number<N: Into<BlockNum>>(&mut self, n: N) {
        self.number = self.number.add(n.into().value())
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn with_offset<N: Into<u64>>(mut self, o: N) -> Self {
        self.offset = o.into();
        self
    }

    pub fn set_offset<N: Into<u64>>(&mut self, o: N) {
        self.offset = o.into()
    }

    pub fn add_offset<N: Into<u64>>(&mut self, o: N) {
        self.offset += o.into()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
#[cbor(transparent)]
pub struct BlockNum(#[n(0)] u64);

impl fmt::Display for BlockNum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl BlockNum {
    pub fn zero() -> Self {
        Self(0)
    }

    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }

    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn add<N: Into<u64>>(&self, n: N) -> BlockNum {
        Self(self.0 + n.into())
    }
}

impl From<u64> for BlockNum {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

#[cfg(test)]
mod tests {
    use quickcheck::quickcheck;
    use super::BlockHeader;

    quickcheck! {
        fn header_version(v: u8) -> bool {
            v == BlockHeader::new().with_version(v).version()
        }
    }
}
