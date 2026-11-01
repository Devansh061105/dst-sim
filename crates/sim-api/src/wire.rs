//! Little-endian encoding helpers for messages that cross a real network.
//!
//! In simulation messages are passed as Rust values; a real runtime needs bytes. Decoding never
//! panics: whatever arrives off a socket is untrusted, so a malformed message is just dropped.

/// Reads values off the front of a byte slice; every read returns `None` past the end.
#[derive(Debug)]
pub struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Reader(bytes)
    }

    pub fn u8(&mut self) -> Option<u8> {
        let (&b, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(b)
    }

    pub fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    pub fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    pub fn bool(&mut self) -> Option<bool> {
        match self.u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    /// A length-prefixed byte string, as written by [`put_bytes`].
    pub fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.u64()?).ok()?;
        self.take(len)
    }

    pub fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.0.len() < len {
            return None;
        }
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Some(head)
    }

    /// `Some(value)` only if nothing is left over, so trailing garbage is rejected too.
    pub fn finish<T>(self, value: T) -> Option<T> {
        self.0.is_empty().then_some(value)
    }
}

/// Appends `v`, little-endian.
pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend(v.to_le_bytes());
}

/// Appends `v`, little-endian.
pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend(v.to_le_bytes());
}

/// Appends a length-prefixed byte string (read back with [`Reader::bytes`]).
pub fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_what_was_written_and_rejects_short_or_long_input() {
        let mut out = vec![1];
        put_u32(&mut out, 7);
        put_u64(&mut out, u64::MAX);
        put_bytes(&mut out, b"abc");
        let mut r = Reader::new(&out);
        assert_eq!((r.bool(), r.u32(), r.u64()), (Some(true), Some(7), Some(u64::MAX)));
        assert_eq!(r.bytes(), Some(&b"abc"[..]));
        assert_eq!(r.finish(()), Some(()));

        assert_eq!(Reader::new(&out[..3]).u64(), None);
        let mut long = Reader::new(&out);
        long.u8();
        assert_eq!(long.finish(()), None, "trailing bytes");
        assert_eq!(Reader::new(&[2]).bool(), None);
        // A huge length prefix is refused, not allocated.
        let mut huge = Vec::new();
        put_u64(&mut huge, u64::MAX);
        assert_eq!(Reader::new(&huge).bytes(), None);
    }
}
