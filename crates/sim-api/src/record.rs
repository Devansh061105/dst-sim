//! Checksummed records for append-only files such as write-ahead logs.
//!
//! Each record is `[crc32: u32 LE][len: u32 LE][payload]`, where the checksum covers the length
//! bytes and the payload. After a crash the end of a log can hold a torn record, zeros or a
//! partly written unsynced tail; [`decode`] returns the records before the first invalid one and
//! where that valid prefix ends, so the caller can truncate the garbage away.
//!
//! The checksum covers the length too because the CRC of empty input is 0: otherwise an all-zero
//! region (common after a crash) would decode as an endless run of valid empty records.

/// The records that decoded cleanly, and the byte length of that valid prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decoded {
    pub records: Vec<Vec<u8>>,
    pub valid_len: u64,
}

const HEADER: usize = 8;

pub fn encode(payload: &[u8]) -> Vec<u8> {
    let len = u32::try_from(payload.len()).expect("record payload larger than 4 GiB");
    let len_bytes = len.to_le_bytes();
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(&checksum(&len_bytes, payload).to_le_bytes());
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(payload);
    out
}

/// Decodes records from the start of `bytes`, stopping at the first truncated or corrupt one.
pub fn decode(bytes: &[u8]) -> Decoded {
    let mut records = Vec::new();
    let mut pos = 0;
    while bytes.len() - pos >= HEADER {
        let crc = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("4 bytes"));
        let len_bytes: [u8; 4] = bytes[pos + 4..pos + HEADER].try_into().expect("4 bytes");
        let start = pos + HEADER;
        let Some(end) = start.checked_add(u32::from_le_bytes(len_bytes) as usize) else { break };
        if end > bytes.len() {
            break;
        }
        let payload = &bytes[start..end];
        if checksum(&len_bytes, payload) != crc {
            break;
        }
        records.push(payload.to_vec());
        pos = end;
    }
    Decoded { records, valid_len: pos as u64 }
}

fn checksum(len_bytes: &[u8], payload: &[u8]) -> u32 {
    !update(update(!0, len_bytes), payload)
}

/// CRC-32 (IEEE 802.3, the one used by zip, PNG and Ethernet).
pub fn crc32(bytes: &[u8]) -> u32 {
    !update(!0, bytes)
}

fn update(mut crc: u32, bytes: &[u8]) -> u32 {
    for &b in bytes {
        crc = TABLE[((crc ^ u32::from(b)) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

/// Lookup table for the reflected polynomial 0xEDB88320, built at compile time.
const TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { 0xedb8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
};

#[cfg(test)]
mod tests {
    use super::*;

    fn log(payloads: &[&[u8]]) -> Vec<u8> {
        payloads.iter().flat_map(|p| encode(p)).collect()
    }

    #[test]
    fn crc32_reference_vector() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn round_trip() {
        let bytes = log(&[b"alpha", b"", b"gamma"]);
        let decoded = decode(&bytes);
        assert_eq!(decoded.records, [b"alpha".to_vec(), vec![], b"gamma".to_vec()]);
        assert_eq!(decoded.valid_len, bytes.len() as u64);
    }

    #[test]
    fn torn_tail_at_every_byte_keeps_the_complete_prefix() {
        let first = encode(b"first");
        let bytes = log(&[b"first", b"second"]);
        for cut in 0..bytes.len() {
            let decoded = decode(&bytes[..cut]);
            let expect = if cut >= first.len() { 1 } else { 0 };
            assert_eq!(decoded.records.len(), expect, "cut at {cut}");
            assert_eq!(decoded.valid_len, if expect == 1 { first.len() as u64 } else { 0 });
        }
    }

    #[test]
    fn any_flipped_bit_is_detected() {
        let bytes = encode(b"payload");
        for bit in 0..bytes.len() * 8 {
            let mut corrupt = bytes.clone();
            corrupt[bit / 8] ^= 1 << (bit % 8);
            assert!(decode(&corrupt).records.is_empty(), "flip of bit {bit} went unnoticed");
        }
    }

    #[test]
    fn zeroed_region_is_not_a_record() {
        let mut bytes = log(&[b"kept"]);
        let kept = bytes.len() as u64;
        bytes.extend_from_slice(&[0; 64]);
        assert_eq!(decode(&bytes), Decoded { records: vec![b"kept".to_vec()], valid_len: kept });
    }
}
