// Content hashing for data units.

use std::io::{self, Read, Seek, SeekFrom};

use sha2::{Digest, Sha256};

use crate::svs::layout::DataUnit;

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
}
