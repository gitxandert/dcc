// Content hashing for data units.

use std::io::{self, Read, Seek, SeekFrom};

use sha2::{Digest, Sha256};

use crate::svs::layout::{DataUnit, DataUnitKind};

// ---------------------------------------------------------------------------
// Coarse fingerprint
// ---------------------------------------------------------------------------

/// Number of bytes sampled from each window (start, middle, end) when
/// computing a coarse fingerprint.  Small enough to remain cheap; large
/// enough to distinguish most real payloads.
pub const COARSE_SAMPLE_BYTES: usize = 64;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET: u64 = 14695981039346656037;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 1099511628211;

fn fnv1a_feed(state: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *state ^= b as u64;
        *state = state.wrapping_mul(FNV_PRIME);
    }
}

/// Compute a coarse fingerprint for `unit` using bounded payload sampling.
///
/// Reads at most `3 × COARSE_SAMPLE_BYTES` from the unit: a window at the
/// start, a window at the midpoint, and a window at the end (the latter two
/// are skipped for very small units where they would overlap with the first).
/// The sampled bytes are mixed with structural fields (kind, length,
/// compression) via FNV-1a.
///
/// This value is suitable for **candidate matching only**.  It is not a
/// proof of exact byte equality — use `hash_unit` for that.
pub fn coarse_fingerprint<R: Read + Seek>(
    reader: &mut R,
    unit: &DataUnit,
    file_len: u64,
    compression: Option<u16>,
) -> Result<u64, HashError> {
    let end = unit
        .offset
        .checked_add(unit.length)
        .ok_or(HashError::OutOfBounds { offset: unit.offset, length: unit.length })?;
    if end > file_len {
        return Err(HashError::OutOfBounds { offset: unit.offset, length: unit.length });
    }

    // Seed with structural fields so units with identical sampled bytes but
    // different kinds, sizes, or compression schemes still diverge.
    let kind_byte: u8 = match unit.kind {
        DataUnitKind::Tile => 0,
        DataUnitKind::Strip => 1,
        DataUnitKind::MetadataBlob => 2,
        DataUnitKind::AssociatedImage => 3,
    };
    let mut state = FNV_OFFSET;
    fnv1a_feed(&mut state, &[kind_byte]);
    fnv1a_feed(&mut state, &unit.length.to_le_bytes());
    if let Some(c) = compression {
        fnv1a_feed(&mut state, &c.to_le_bytes());
    }

    // Read `n` bytes at absolute file offset `pos` and mix into state.
    let mut sample_at = |pos: u64, n: usize| -> Result<(), HashError> {
        let mut buf = [0u8; COARSE_SAMPLE_BYTES];
        reader.seek(SeekFrom::Start(pos))?;
        reader.read_exact(&mut buf[..n])?;
        fnv1a_feed(&mut state, &buf[..n]);
        Ok(())
    };

    let n = (unit.length as usize).min(COARSE_SAMPLE_BYTES);

    // First window.
    sample_at(unit.offset, n)?;

    // Middle window — only when it does not overlap with the first window.
    if unit.length > (2 * COARSE_SAMPLE_BYTES) as u64 {
        let mid_off = unit.offset + unit.length / 2;
        sample_at(mid_off, COARSE_SAMPLE_BYTES)?;
    }

    // Last window — only when it does not overlap with the first window.
    if unit.length > COARSE_SAMPLE_BYTES as u64 {
        let tail_off = unit.offset + unit.length - COARSE_SAMPLE_BYTES as u64;
        sample_at(tail_off, COARSE_SAMPLE_BYTES)?;
    }

    Ok(state)
}

/// Hash errors that can arise during payload reading.
#[derive(Debug)]
pub enum HashError {
    Io(io::Error),
    /// The unit's offset + length would exceed the file bounds.
    OutOfBounds { offset: u64, length: u64 },
}

impl From<io::Error> for HashError {
    fn from(e: io::Error) -> Self {
        HashError::Io(e)
    }
}

impl std::fmt::Display for HashError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HashError::Io(e) => write!(f, "I/O error: {e}"),
            HashError::OutOfBounds { offset, length } => {
                write!(f, "unit out of bounds: offset={offset} length={length}")
            }
        }
    }
}

/// Read the payload of `unit` from `reader` and return its SHA-256 digest.
///
/// Performs a ranged read (seek + bounded read); the full file is never loaded
/// into memory.  `file_len` is used to validate that the unit's byte range
/// lies within the file before issuing any I/O.
pub fn hash_unit<R: Read + Seek>(
    reader: &mut R,
    unit: &DataUnit,
    file_len: u64,
) -> Result<[u8; 32], HashError> {
    // Guard against corrupt or synthetic units that extend past the file.
    let end = unit
        .offset
        .checked_add(unit.length)
        .ok_or(HashError::OutOfBounds {
            offset: unit.offset,
            length: unit.length,
        })?;
    if end > file_len {
        return Err(HashError::OutOfBounds {
            offset: unit.offset,
            length: unit.length,
        });
    }

    reader.seek(SeekFrom::Start(unit.offset))?;

    let mut hasher = Sha256::new();
    let mut remaining = unit.length;
    let mut buf = [0u8; 65536];

    while remaining > 0 {
        let to_read = (remaining as usize).min(buf.len());
        reader.read_exact(&mut buf[..to_read])?;
        hasher.update(&buf[..to_read]);
        remaining -= to_read as u64;
    }

    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::svs::layout::{DataUnit, DataUnitKind};
    use std::io::Cursor;

    fn make_unit(offset: u64, length: u64) -> DataUnit {
        DataUnit {
            kind: DataUnitKind::Tile,
            offset,
            length,
            ifd_index: 0,
            unit_index: 0,
            strong_hash: None,
        }
    }

    #[test]
    fn hash_is_stable() {
        let data = b"hello, world";
        let mut cursor = Cursor::new(data);
        let unit = make_unit(0, data.len() as u64);

        let h1 = hash_unit(&mut cursor, &unit, data.len() as u64).unwrap();
        let h2 = hash_unit(&mut cursor, &unit, data.len() as u64).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_payloads_produce_different_hashes() {
        let data = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut cursor = Cursor::new(data);
        let unit_a = make_unit(0, 4);
        let unit_b = make_unit(4, 4); // "aaaa" same bytes, same hash
        let unit_c = make_unit(0, 8); // longer range

        let ha = hash_unit(&mut cursor, &unit_a, data.len() as u64).unwrap();
        let hb = hash_unit(&mut cursor, &unit_b, data.len() as u64).unwrap();
        // same bytes → same hash
        assert_eq!(ha, hb);

        let hc = hash_unit(&mut cursor, &unit_c, data.len() as u64).unwrap();
        // different length → different hash
        assert_ne!(ha, hc);
    }

    #[test]
    fn out_of_bounds_is_rejected() {
        let data = b"short";
        let mut cursor = Cursor::new(data);
        // unit extends past the 5-byte file
        let unit = make_unit(3, 10);
        let result = hash_unit(&mut cursor, &unit, data.len() as u64);
        assert!(matches!(result, Err(HashError::OutOfBounds { .. })));
    }

    #[test]
    fn subrange_hash_matches_known_sha256() {
        // SHA-256("world") = 486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7
        let data = b"hello world";
        let mut cursor = Cursor::new(data);
        let unit = make_unit(6, 5); // "world"
        let hash = hash_unit(&mut cursor, &unit, data.len() as u64).unwrap();
        let expected = hex_to_bytes("486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7");
        assert_eq!(&hash, expected.as_slice());
    }

    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    // ── coarse_fingerprint ────────────────────────────────────────────────

    #[test]
    fn coarse_fp_is_stable() {
        let data: Vec<u8> = (0u8..=255).cycle().take(512).collect();
        let unit = make_unit(0, data.len() as u64);
        let file_len = data.len() as u64;

        let fp1 = coarse_fingerprint(&mut Cursor::new(&data), &unit, file_len, None).unwrap();
        let fp2 = coarse_fingerprint(&mut Cursor::new(&data), &unit, file_len, None).unwrap();
        assert_eq!(fp1, fp2, "same input must produce the same coarse fingerprint");
    }

    #[test]
    fn coarse_fp_differs_for_distinct_payloads() {
        let data_a: Vec<u8> = vec![0xAA; 256];
        let data_b: Vec<u8> = vec![0xBB; 256];
        let unit = make_unit(0, 256);
        let file_len = 256;

        let fp_a = coarse_fingerprint(&mut Cursor::new(&data_a), &unit, file_len, None).unwrap();
        let fp_b = coarse_fingerprint(&mut Cursor::new(&data_b), &unit, file_len, None).unwrap();
        assert_ne!(fp_a, fp_b);
    }

    #[test]
    fn coarse_fp_differs_for_different_lengths() {
        // Same bytes at offset 0, but different lengths → different fingerprints
        // because length is mixed into the structural seed.
        let data: Vec<u8> = vec![0xCC; 256];
        let unit_short = make_unit(0, 64);
        let unit_long = make_unit(0, 256);
        let file_len = 256;

        let fp_short =
            coarse_fingerprint(&mut Cursor::new(&data), &unit_short, file_len, None).unwrap();
        let fp_long =
            coarse_fingerprint(&mut Cursor::new(&data), &unit_long, file_len, None).unwrap();
        assert_ne!(fp_short, fp_long);
    }

    #[test]
    fn coarse_fp_differs_for_different_compression() {
        let data: Vec<u8> = vec![0xDD; 128];
        let unit = make_unit(0, 128);
        let file_len = 128;

        let fp_none =
            coarse_fingerprint(&mut Cursor::new(&data), &unit, file_len, None).unwrap();
        let fp_jpeg =
            coarse_fingerprint(&mut Cursor::new(&data), &unit, file_len, Some(7)).unwrap();
        assert_ne!(fp_none, fp_jpeg);
    }

    #[test]
    fn coarse_fp_out_of_bounds_is_rejected() {
        let data = b"short";
        let unit = make_unit(3, 10);
        let result = coarse_fingerprint(&mut Cursor::new(data), &unit, data.len() as u64, None);
        assert!(matches!(result, Err(HashError::OutOfBounds { .. })));
    }

    #[test]
    fn coarse_fp_small_unit_uses_only_first_window() {
        // Unit smaller than COARSE_SAMPLE_BYTES — only the first window fires.
        // Verify it does not panic and returns a consistent value.
        let data: Vec<u8> = vec![0xEE; 16];
        let unit = make_unit(0, 16);
        let file_len = 16;

        let fp1 = coarse_fingerprint(&mut Cursor::new(&data), &unit, file_len, None).unwrap();
        let fp2 = coarse_fingerprint(&mut Cursor::new(&data), &unit, file_len, None).unwrap();
        assert_eq!(fp1, fp2);
    }
}
