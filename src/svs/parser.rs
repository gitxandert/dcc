// TIFF/SVS header and IFD parsing.

use std::io::{self, Read, Seek, SeekFrom};

use crate::svs::tiff::{
    ByteOrder, FieldType, RawBigIfdEntry, RawIfdEntry, TiffHeader, BIGTIFF_MAGIC, BYTE_ORDER_BE,
    BYTE_ORDER_LE, TIFF_MAGIC,
};

// ---------------------------------------------------------------------------
// Byte-order helpers
// ---------------------------------------------------------------------------

pub(crate) fn u16_from(bytes: [u8; 2], order: ByteOrder) -> u16 {
    match order {
        ByteOrder::LittleEndian => u16::from_le_bytes(bytes),
        ByteOrder::BigEndian => u16::from_be_bytes(bytes),
    }
}

pub(crate) fn u32_from(bytes: [u8; 4], order: ByteOrder) -> u32 {
    match order {
        ByteOrder::LittleEndian => u32::from_le_bytes(bytes),
        ByteOrder::BigEndian => u32::from_be_bytes(bytes),
    }
}

pub(crate) fn u64_from(bytes: [u8; 8], order: ByteOrder) -> u64 {
    match order {
        ByteOrder::LittleEndian => u64::from_le_bytes(bytes),
        ByteOrder::BigEndian => u64::from_be_bytes(bytes),
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during TIFF parsing.
#[derive(Debug)]
pub enum ParseError {
    Io(io::Error),
    /// The first two bytes do not match either byte-order mark.
    UnknownByteOrder(u16),
    /// The magic number is not 42 (standard TIFF).
    UnsupportedMagic(u16),
    /// The IFD chain exceeds the guard limit — likely a corrupt or circular file.
    TooManyIfds(usize),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Io(e) => write!(f, "I/O error: {e}"),
            ParseError::UnknownByteOrder(raw) => {
                write!(f, "unrecognised byte-order mark: 0x{raw:04X}")
            }
            ParseError::UnsupportedMagic(m) => {
                if *m == BIGTIFF_MAGIC {
                    write!(f, "BigTIFF (magic=43) is not yet supported")
                } else {
                    write!(f, "unsupported TIFF magic: {m} (expected 42)")
                }
            }
            ParseError::TooManyIfds(n) => {
                write!(f, "IFD chain exceeds limit ({n}); file may be corrupt or circular")
            }
        }
    }
}

impl std::error::Error for ParseError {}

impl From<io::Error> for ParseError {
    fn from(e: io::Error) -> Self {
        ParseError::Io(e)
    }
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

/// Parse the 8-byte standard TIFF header from the current position of `r`.
///
/// On success the reader is positioned just after the header (offset 8).
/// Returns `ParseError::UnsupportedMagic(43)` for BigTIFF rather than
/// silently misinterpreting it.
pub fn parse_header<R: Read + Seek>(r: &mut R) -> Result<TiffHeader, ParseError> {
    r.seek(SeekFrom::Start(0))?;

    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;

    // Determine byte order from first two bytes.
    let bom = u16::from_le_bytes([buf[0], buf[1]]);
    let byte_order = match bom {
        BYTE_ORDER_LE => ByteOrder::LittleEndian,
        BYTE_ORDER_BE => ByteOrder::BigEndian,
        _ => return Err(ParseError::UnknownByteOrder(bom)),
    };

    let magic = u16_from([buf[2], buf[3]], byte_order);
    if magic != TIFF_MAGIC {
        return Err(ParseError::UnsupportedMagic(magic));
    }

    let first_ifd_offset = u32_from([buf[4], buf[5], buf[6], buf[7]], byte_order);

    Ok(TiffHeader { byte_order, magic, first_ifd_offset })
}

// ---------------------------------------------------------------------------
// IFD traversal
// ---------------------------------------------------------------------------

/// Guard against corrupt or circular IFD chains.
const MAX_IFDS: usize = 1024;

/// Parse one IFD at `offset`.
///
/// Returns `(entries, next_ifd_offset)`.  A `next_ifd_offset` of 0 means this
/// is the last IFD in the chain.
pub fn parse_ifd<R: Read + Seek>(
    r: &mut R,
    offset: u32,
    byte_order: ByteOrder,
) -> Result<(Vec<RawIfdEntry>, u32), ParseError> {
    r.seek(SeekFrom::Start(offset as u64))?;

    let mut buf2 = [0u8; 2];
    r.read_exact(&mut buf2)?;
    let entry_count = u16_from(buf2, byte_order) as usize;

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let mut buf = [0u8; 12];
        r.read_exact(&mut buf)?;

        let tag = u16_from([buf[0], buf[1]], byte_order);
        let type_raw = u16_from([buf[2], buf[3]], byte_order);
        let count = u32_from([buf[4], buf[5], buf[6], buf[7]], byte_order);
        let value_bytes = [buf[8], buf[9], buf[10], buf[11]];

        entries.push(RawIfdEntry {
            tag,
            field_type: FieldType::from_u16(type_raw),
            count,
            value_bytes,
        });
    }

    let mut buf4 = [0u8; 4];
    r.read_exact(&mut buf4)?;
    let next_ifd_offset = u32_from(buf4, byte_order);

    Ok((entries, next_ifd_offset))
}

/// Traverse the full IFD chain starting at `first_offset`.
///
/// Returns one `Vec<RawIfdEntry>` per IFD in chain order.
pub fn parse_ifd_chain<R: Read + Seek>(
    r: &mut R,
    first_offset: u32,
    byte_order: ByteOrder,
) -> Result<Vec<Vec<RawIfdEntry>>, ParseError> {
    let mut result = Vec::new();
    let mut current_offset = first_offset;

    while current_offset != 0 {
        if result.len() >= MAX_IFDS {
            return Err(ParseError::TooManyIfds(result.len()));
        }
        let (entries, next_offset) = parse_ifd(r, current_offset, byte_order)?;
        result.push(entries);
        current_offset = next_offset;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// BigTIFF header, IFD, and value resolution
// ---------------------------------------------------------------------------

/// Parse the 16-byte BigTIFF header from the start of `r`.
///
/// BigTIFF header layout:
///   [0..2]   byte-order mark  (0x4949 or 0x4D4D)
///   [2..4]   magic            (43)
///   [4..6]   bytesize of offsets (always 8)
///   [6..8]   reserved         (always 0)
///   [8..16]  offset of first IFD (u64)
fn parse_bigtiff_header<R: Read + Seek>(
    r: &mut R,
) -> Result<(ByteOrder, u64), ParseError> {
    r.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; 16];
    r.read_exact(&mut buf)?;

    let bom = u16::from_le_bytes([buf[0], buf[1]]);
    let byte_order = match bom {
        BYTE_ORDER_LE => ByteOrder::LittleEndian,
        BYTE_ORDER_BE => ByteOrder::BigEndian,
        _ => return Err(ParseError::UnknownByteOrder(bom)),
    };

    let first_ifd_offset = u64_from(
        [buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]],
        byte_order,
    );

    Ok((byte_order, first_ifd_offset))
}

/// Parse one BigTIFF IFD at `offset`.
///
/// BigTIFF IFD layout:
///   8 bytes  entry count (u64)
///   n × 20 bytes  entries: tag(u16) + type(u16) + count(u64) + value(8 bytes)
///   8 bytes  next IFD offset (u64)
///
/// Returns `(entries, next_ifd_offset)`.  A `next_ifd_offset` of 0 means
/// this is the last IFD in the chain.
fn parse_bigtiff_ifd<R: Read + Seek>(
    r: &mut R,
    offset: u64,
    byte_order: ByteOrder,
) -> Result<(Vec<RawBigIfdEntry>, u64), ParseError> {
    r.seek(SeekFrom::Start(offset))?;

    let mut buf8 = [0u8; 8];
    r.read_exact(&mut buf8)?;
    let entry_count = u64_from(buf8, byte_order) as usize;

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let mut buf = [0u8; 20];
        r.read_exact(&mut buf)?;

        let tag = u16_from([buf[0], buf[1]], byte_order);
        let type_raw = u16_from([buf[2], buf[3]], byte_order);
        let count = u64_from(
            [buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11]],
            byte_order,
        );
        let value_bytes = [
            buf[12], buf[13], buf[14], buf[15],
            buf[16], buf[17], buf[18], buf[19],
        ];

        entries.push(RawBigIfdEntry {
            tag,
            field_type: FieldType::from_u16(type_raw),
            count,
            value_bytes,
        });
    }

    r.read_exact(&mut buf8)?;
    let next_ifd_offset = u64_from(buf8, byte_order);

    Ok((entries, next_ifd_offset))
}

/// Traverse the full BigTIFF IFD chain starting at `first_offset`.
fn parse_bigtiff_ifd_chain<R: Read + Seek>(
    r: &mut R,
    first_offset: u64,
    byte_order: ByteOrder,
) -> Result<Vec<Vec<RawBigIfdEntry>>, ParseError> {
    let mut result = Vec::new();
    let mut current_offset = first_offset;

    while current_offset != 0 {
        if result.len() >= MAX_IFDS {
            return Err(ParseError::TooManyIfds(result.len()));
        }
        let (entries, next_offset) = parse_bigtiff_ifd(r, current_offset, byte_order)?;
        result.push(entries);
        current_offset = next_offset;
    }

    Ok(result)
}

/// Resolve a BigTIFF IFD entry's values as a `Vec<u64>`.
///
/// Handles all numeric types; 8-byte inline values are read directly from
/// `value_bytes`; out-of-line values are fetched from the file.
fn resolve_u64_values<R: Read + Seek>(
    r: &mut R,
    entry: &RawBigIfdEntry,
    byte_order: ByteOrder,
) -> Result<Vec<u64>, ParseError> {
    let count = entry.count as usize;

    if entry.is_inline() {
        let mut result = Vec::with_capacity(count);
        match entry.field_type {
            FieldType::Byte | FieldType::SByte | FieldType::Undefined => {
                for i in 0..count {
                    result.push(entry.value_bytes[i] as u64);
                }
            }
            FieldType::Short | FieldType::SShort => {
                for i in 0..count {
                    let bytes = [entry.value_bytes[i * 2], entry.value_bytes[i * 2 + 1]];
                    result.push(u16_from(bytes, byte_order) as u64);
                }
            }
            FieldType::Long | FieldType::SLong | FieldType::Float => {
                for i in 0..count {
                    let bytes = [
                        entry.value_bytes[i * 4],
                        entry.value_bytes[i * 4 + 1],
                        entry.value_bytes[i * 4 + 2],
                        entry.value_bytes[i * 4 + 3],
                    ];
                    result.push(u32_from(bytes, byte_order) as u64);
                }
            }
            _ => {
                // Long8, SLong8, Ifd8, or any 8-byte value — one inline value.
                result.push(entry.value_as_offset(byte_order));
            }
        }
        Ok(result)
    } else {
        let offset = entry.value_as_offset(byte_order);
        r.seek(SeekFrom::Start(offset))?;

        let mut result = Vec::with_capacity(count);
        match entry.field_type {
            FieldType::Byte | FieldType::SByte | FieldType::Undefined => {
                let mut buf = vec![0u8; count];
                r.read_exact(&mut buf)?;
                result.extend(buf.iter().map(|&b| b as u64));
            }
            FieldType::Short | FieldType::SShort => {
                let mut buf = vec![0u8; count * 2];
                r.read_exact(&mut buf)?;
                result.extend(
                    buf.chunks_exact(2)
                        .map(|c| u16_from([c[0], c[1]], byte_order) as u64),
                );
            }
            FieldType::Long | FieldType::SLong | FieldType::Float => {
                let mut buf = vec![0u8; count * 4];
                r.read_exact(&mut buf)?;
                result.extend(
                    buf.chunks_exact(4)
                        .map(|c| u32_from([c[0], c[1], c[2], c[3]], byte_order) as u64),
                );
            }
            _ => {
                // Long8, SLong8, Ifd8, Double, or unknown — treat as 8-byte.
                let mut buf = vec![0u8; count * 8];
                r.read_exact(&mut buf)?;
                result.extend(
                    buf.chunks_exact(8)
                        .map(|c| u64_from([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]], byte_order)),
                );
            }
        }
        Ok(result)
    }
}

/// Convert BigTIFF raw IFD entries into an [`Ifd`] struct.
fn bigtiff_ifd_entries_to_layout<R: Read + Seek>(
    r: &mut R,
    ifd_index: usize,
    entries: &[RawBigIfdEntry],
    byte_order: ByteOrder,
) -> Result<crate::svs::layout::Ifd, ParseError> {
    use crate::svs::layout::{DataUnit, DataUnitKind};
    use crate::svs::tiff::tag;

    let mut width: u32 = 0;
    let mut height: u32 = 0;
    let mut tile_width: Option<u32> = None;
    let mut tile_height: Option<u32> = None;
    let mut tile_offsets: Vec<u64> = Vec::new();
    let mut tile_byte_counts: Vec<u64> = Vec::new();

    for entry in entries {
        match entry.tag {
            tag::IMAGE_WIDTH => {
                if let Some(&w) = resolve_u64_values(r, entry, byte_order)?.first() {
                    width = w as u32;
                }
            }
            tag::IMAGE_LENGTH => {
                if let Some(&h) = resolve_u64_values(r, entry, byte_order)?.first() {
                    height = h as u32;
                }
            }
            tag::TILE_WIDTH => {
                if let Some(&tw) = resolve_u64_values(r, entry, byte_order)?.first() {
                    tile_width = Some(tw as u32);
                }
            }
            tag::TILE_LENGTH => {
                if let Some(&th) = resolve_u64_values(r, entry, byte_order)?.first() {
                    tile_height = Some(th as u32);
                }
            }
            tag::TILE_OFFSETS => {
                tile_offsets = resolve_u64_values(r, entry, byte_order)?;
            }
            tag::TILE_BYTE_COUNTS => {
                tile_byte_counts = resolve_u64_values(r, entry, byte_order)?;
            }
            _ => {}
        }
    }

    let data_units = tile_offsets
        .iter()
        .zip(tile_byte_counts.iter())
        .enumerate()
        .map(|(unit_index, (&offset, &length))| DataUnit {
            kind: DataUnitKind::Tile,
            offset,
            length,
            ifd_index,
            unit_index,
        })
        .collect();

    Ok(crate::svs::layout::Ifd {
        index: ifd_index,
        width,
        height,
        tile_width,
        tile_height,
        compression: None,
        description: None,
        data_units,
    })
}

// ---------------------------------------------------------------------------
// Value resolution
// ---------------------------------------------------------------------------

/// Resolve an IFD entry's values as a `Vec<u32>`.
///
/// Handles `BYTE`, `SHORT`, and `LONG` types (and their signed variants).
/// For inline values the `value_bytes` field is re-interpreted in `byte_order`;
/// for out-of-line values the function seeks to the stored offset and reads
/// `entry.count` elements from the file.
pub fn resolve_u32_values<R: Read + Seek>(
    r: &mut R,
    entry: &RawIfdEntry,
    byte_order: ByteOrder,
) -> Result<Vec<u32>, ParseError> {
    let count = entry.count as usize;

    if entry.is_inline() {
        let mut result = Vec::with_capacity(count);
        match entry.field_type {
            FieldType::Byte | FieldType::SByte | FieldType::Undefined => {
                for i in 0..count {
                    result.push(entry.value_bytes[i] as u32);
                }
            }
            FieldType::Short | FieldType::SShort => {
                for i in 0..count {
                    let bytes = [entry.value_bytes[i * 2], entry.value_bytes[i * 2 + 1]];
                    result.push(u16_from(bytes, byte_order) as u32);
                }
            }
            _ => {
                // LONG count=1 inline — or any other type that fits in 4 bytes.
                result.push(entry.value_as_offset(byte_order));
            }
        }
        Ok(result)
    } else {
        let offset = entry.value_as_offset(byte_order);
        r.seek(SeekFrom::Start(offset as u64))?;

        let mut result = Vec::with_capacity(count);
        match entry.field_type {
            FieldType::Byte | FieldType::SByte | FieldType::Undefined => {
                let mut buf = vec![0u8; count];
                r.read_exact(&mut buf)?;
                result.extend(buf.iter().map(|&b| b as u32));
            }
            FieldType::Short | FieldType::SShort => {
                let mut buf = vec![0u8; count * 2];
                r.read_exact(&mut buf)?;
                result.extend(
                    buf.chunks_exact(2)
                        .map(|c| u16_from([c[0], c[1]], byte_order) as u32),
                );
            }
            _ => {
                // LONG, SLONG, FLOAT, and unknowns — read as 4-byte values.
                let mut buf = vec![0u8; count * 4];
                r.read_exact(&mut buf)?;
                result.extend(
                    buf.chunks_exact(4)
                        .map(|c| u32_from([c[0], c[1], c[2], c[3]], byte_order)),
                );
            }
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Layout construction
// ---------------------------------------------------------------------------

/// Convert raw IFD entries into an [`Ifd`] struct, reading out-of-line values
/// from `r` as needed.
///
/// Only the Phase 1 tags are extracted; other tags are silently skipped.
fn ifd_entries_to_layout<R: Read + Seek>(
    r: &mut R,
    ifd_index: usize,
    entries: &[RawIfdEntry],
    byte_order: ByteOrder,
) -> Result<crate::svs::layout::Ifd, ParseError> {
    use crate::svs::layout::{DataUnit, DataUnitKind};
    use crate::svs::tiff::tag;

    let mut width: u32 = 0;
    let mut height: u32 = 0;
    let mut tile_width: Option<u32> = None;
    let mut tile_height: Option<u32> = None;
    let mut tile_offsets: Vec<u32> = Vec::new();
    let mut tile_byte_counts: Vec<u32> = Vec::new();

    for entry in entries {
        match entry.tag {
            tag::IMAGE_WIDTH => {
                let v = resolve_u32_values(r, entry, byte_order)?;
                if let Some(&w) = v.first() {
                    width = w;
                }
            }
            tag::IMAGE_LENGTH => {
                let v = resolve_u32_values(r, entry, byte_order)?;
                if let Some(&h) = v.first() {
                    height = h;
                }
            }
            tag::TILE_WIDTH => {
                let v = resolve_u32_values(r, entry, byte_order)?;
                if let Some(&tw) = v.first() {
                    tile_width = Some(tw);
                }
            }
            tag::TILE_LENGTH => {
                let v = resolve_u32_values(r, entry, byte_order)?;
                if let Some(&th) = v.first() {
                    tile_height = Some(th);
                }
            }
            tag::TILE_OFFSETS => {
                tile_offsets = resolve_u32_values(r, entry, byte_order)?;
            }
            tag::TILE_BYTE_COUNTS => {
                tile_byte_counts = resolve_u32_values(r, entry, byte_order)?;
            }
            _ => {} // unknown / unneeded tags are skipped
        }
    }

    let data_units = tile_offsets
        .iter()
        .zip(tile_byte_counts.iter())
        .enumerate()
        .map(|(unit_index, (&offset, &length))| DataUnit {
            kind: DataUnitKind::Tile,
            offset: offset as u64,
            length: length as u64,
            ifd_index,
            unit_index,
        })
        .collect();

    Ok(crate::svs::layout::Ifd {
        index: ifd_index,
        width,
        height,
        tile_width,
        tile_height,
        compression: None,
        description: None,
        data_units,
    })
}

/// Parse a complete TIFF/SVS file (Classic or BigTIFF) into an [`SvsFile`].
///
/// Detects Classic TIFF (magic 42) and BigTIFF (magic 43) automatically.
/// `file_len` is the total byte length of the file on disk; the caller is
/// responsible for passing this correctly (typically from `fs::metadata`).
pub fn parse_svs_file<R: Read + Seek>(
    r: &mut R,
    path: std::path::PathBuf,
    file_len: u64,
) -> Result<crate::svs::layout::SvsFile, ParseError> {
    // Peek at the first 4 bytes to determine byte order and magic number.
    r.seek(SeekFrom::Start(0))?;
    let mut peek = [0u8; 4];
    r.read_exact(&mut peek)?;
    let bom = u16::from_le_bytes([peek[0], peek[1]]);
    let byte_order = match bom {
        BYTE_ORDER_LE => ByteOrder::LittleEndian,
        BYTE_ORDER_BE => ByteOrder::BigEndian,
        _ => return Err(ParseError::UnknownByteOrder(bom)),
    };
    let magic = u16_from([peek[2], peek[3]], byte_order);

    if magic == BIGTIFF_MAGIC {
        let (byte_order, first_ifd_offset) = parse_bigtiff_header(r)?;
        let raw_ifds = parse_bigtiff_ifd_chain(r, first_ifd_offset, byte_order)?;
        let mut ifds = Vec::with_capacity(raw_ifds.len());
        for (ifd_index, entries) in raw_ifds.into_iter().enumerate() {
            ifds.push(bigtiff_ifd_entries_to_layout(r, ifd_index, &entries, byte_order)?);
        }
        Ok(crate::svs::layout::SvsFile { path, ifds, raw_len: file_len })
    } else {
        let header = parse_header(r)?;
        let byte_order = header.byte_order;
        let raw_ifds = parse_ifd_chain(r, header.first_ifd_offset, byte_order)?;
        let mut ifds = Vec::with_capacity(raw_ifds.len());
        for (ifd_index, entries) in raw_ifds.into_iter().enumerate() {
            ifds.push(ifd_entries_to_layout(r, ifd_index, &entries, byte_order)?);
        }
        Ok(crate::svs::layout::SvsFile { path, ifds, raw_len: file_len })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // -----------------------------------------------------------------------
    // Header tests (unchanged from before)
    // -----------------------------------------------------------------------

    fn le_header(first_ifd_offset: u32) -> Vec<u8> {
        let mut v = vec![0x49, 0x49, 42, 0]; // "II" + magic LE
        v.extend_from_slice(&first_ifd_offset.to_le_bytes());
        v
    }

    fn be_header(first_ifd_offset: u32) -> Vec<u8> {
        let mut v = vec![0x4D, 0x4D, 0, 42]; // "MM" + magic BE
        v.extend_from_slice(&first_ifd_offset.to_be_bytes());
        v
    }

    #[test]
    fn parse_little_endian_header() {
        let data = le_header(8);
        let hdr = parse_header(&mut Cursor::new(data)).unwrap();
        assert_eq!(hdr.byte_order, ByteOrder::LittleEndian);
        assert_eq!(hdr.magic, 42);
        assert_eq!(hdr.first_ifd_offset, 8);
    }

    #[test]
    fn parse_big_endian_header() {
        let data = be_header(0x0000_0100);
        let hdr = parse_header(&mut Cursor::new(data)).unwrap();
        assert_eq!(hdr.byte_order, ByteOrder::BigEndian);
        assert_eq!(hdr.magic, 42);
        assert_eq!(hdr.first_ifd_offset, 0x100);
    }

    #[test]
    fn rejects_unknown_byte_order() {
        let data = vec![0xAB, 0xCD, 42, 0, 8, 0, 0, 0];
        let err = parse_header(&mut Cursor::new(data)).unwrap_err();
        assert!(matches!(err, ParseError::UnknownByteOrder(0xCDAB)));
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut data = le_header(8);
        data[2] = 99;
        data[3] = 0;
        let err = parse_header(&mut Cursor::new(data)).unwrap_err();
        assert!(matches!(err, ParseError::UnsupportedMagic(99)));
    }

    #[test]
    fn rejects_bigtiff() {
        let mut data = le_header(8);
        data[2] = 43;
        data[3] = 0;
        let err = parse_header(&mut Cursor::new(data)).unwrap_err();
        assert!(matches!(err, ParseError::UnsupportedMagic(43)));
    }

    #[test]
    fn parse_header_positions_reader_at_offset_8() {
        let mut data = le_header(8);
        data.extend_from_slice(&[0xDE, 0xAD]);
        let mut cur = Cursor::new(data);
        parse_header(&mut cur).unwrap();
        assert_eq!(cur.position(), 8);
    }

    // -----------------------------------------------------------------------
    // IFD helpers
    // -----------------------------------------------------------------------

    /// Build a minimal LE TIFF with one IFD containing the given entries.
    ///
    /// All values must fit inline (≤ 4 bytes).  `next_ifd_offset` is appended
    /// after the entries; pass 0 for a single-IFD file.
    fn build_le_ifd(
        entries: &[(u16, u16, u32, [u8; 4])], // (tag, type, count, value_bytes)
        next_ifd_offset: u32,
    ) -> Vec<u8> {
        // IFD starts at byte 8 (right after the header).
        let mut data = le_header(8);

        // Entry count.
        data.extend_from_slice(&(entries.len() as u16).to_le_bytes());

        for &(tag, typ, count, value_bytes) in entries {
            data.extend_from_slice(&tag.to_le_bytes());
            data.extend_from_slice(&typ.to_le_bytes());
            data.extend_from_slice(&count.to_le_bytes());
            data.extend_from_slice(&value_bytes);
        }

        data.extend_from_slice(&next_ifd_offset.to_le_bytes());
        data
    }

    // -----------------------------------------------------------------------
    // parse_ifd tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_ifd_reads_entry_count_and_tags() {
        // Three SHORT entries (inline): ImageWidth=100, ImageLength=50, TileWidth=256.
        // SHORT type = 3; count = 1; value bytes = [value_lo, value_hi, 0, 0] in LE.
        let entries = [
            (256u16, 3u16, 1u32, [100u8, 0, 0, 0]), // ImageWidth = 100
            (257u16, 3u16, 1u32, [50u8, 0, 0, 0]),  // ImageLength = 50
            (322u16, 3u16, 1u32, [0u8, 1, 0, 0]),   // TileWidth = 256
        ];
        let data = build_le_ifd(&entries, 0);
        let mut cur = Cursor::new(data);

        let (raw_entries, next) = parse_ifd(&mut cur, 8, ByteOrder::LittleEndian).unwrap();

        assert_eq!(raw_entries.len(), 3);
        assert_eq!(raw_entries[0].tag, 256);
        assert_eq!(raw_entries[1].tag, 257);
        assert_eq!(raw_entries[2].tag, 322);
        assert_eq!(next, 0);
    }

    #[test]
    fn parse_ifd_returns_next_ifd_offset() {
        let entries = [(256u16, 3u16, 1u32, [10u8, 0, 0, 0])];
        // next_ifd_offset = 99
        let data = build_le_ifd(&entries, 99);
        let mut cur = Cursor::new(data);

        let (_, next) = parse_ifd(&mut cur, 8, ByteOrder::LittleEndian).unwrap();
        assert_eq!(next, 99);
    }

    // -----------------------------------------------------------------------
    // parse_ifd_chain tests
    // -----------------------------------------------------------------------

    #[test]
    fn chain_single_ifd() {
        let entries = [(256u16, 3u16, 1u32, [4u8, 0, 0, 0])];
        let data = build_le_ifd(&entries, 0);
        let mut cur = Cursor::new(data);

        let chain = parse_ifd_chain(&mut cur, 8, ByteOrder::LittleEndian).unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].len(), 1);
    }

    #[test]
    fn chain_two_ifds() {
        // Build a file with two IFDs in sequence.
        //
        // IFD 0 at offset 8:   1 entry (12 bytes) + next_offset (4 bytes)
        //   = 2 + 12 + 4 = 18 bytes → IFD 1 starts at 8 + 18 = 26
        // IFD 1 at offset 26:  1 entry + next_offset=0
        let ifd1_offset: u32 = 8 + 2 + 12 + 4; // = 26

        let entry_a = (256u16, 3u16, 1u32, [10u8, 0, 0, 0]);
        let entry_b = (257u16, 3u16, 1u32, [20u8, 0, 0, 0]);

        // Build both IFDs manually.
        let mut data = le_header(8);

        // IFD 0: 1 entry, next = ifd1_offset
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&entry_a.0.to_le_bytes());
        data.extend_from_slice(&entry_a.1.to_le_bytes());
        data.extend_from_slice(&entry_a.2.to_le_bytes());
        data.extend_from_slice(&entry_a.3);
        data.extend_from_slice(&ifd1_offset.to_le_bytes());

        // IFD 1: 1 entry, next = 0
        data.extend_from_slice(&1u16.to_le_bytes());
        data.extend_from_slice(&entry_b.0.to_le_bytes());
        data.extend_from_slice(&entry_b.1.to_le_bytes());
        data.extend_from_slice(&entry_b.2.to_le_bytes());
        data.extend_from_slice(&entry_b.3);
        data.extend_from_slice(&0u32.to_le_bytes());

        let mut cur = Cursor::new(data);
        let chain = parse_ifd_chain(&mut cur, 8, ByteOrder::LittleEndian).unwrap();

        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0][0].tag, 256);
        assert_eq!(chain[1][0].tag, 257);
    }

    #[test]
    fn chain_empty_when_first_offset_is_zero() {
        let data = le_header(8); // header only, first_ifd_offset ignored
        let mut cur = Cursor::new(data);
        let chain = parse_ifd_chain(&mut cur, 0, ByteOrder::LittleEndian).unwrap();
        assert!(chain.is_empty());
    }

    // -----------------------------------------------------------------------
    // resolve_u32_values tests
    // -----------------------------------------------------------------------

    fn make_entry(
        tag: u16,
        field_type: FieldType,
        count: u32,
        value_bytes: [u8; 4],
    ) -> RawIfdEntry {
        RawIfdEntry { tag, field_type, count, value_bytes }
    }

    #[test]
    fn resolve_short_inline_le() {
        // SHORT count=1, value=256 = [0x00, 0x01, 0x00, 0x00] in LE
        let entry = make_entry(256, FieldType::Short, 1, [0x00, 0x01, 0x00, 0x00]);
        let vals = resolve_u32_values(&mut Cursor::new(vec![]), &entry, ByteOrder::LittleEndian)
            .unwrap();
        assert_eq!(vals, vec![256]);
    }

    #[test]
    fn resolve_two_shorts_inline_le() {
        // SHORT count=2, values=[10, 20] = [0x0A, 0x00, 0x14, 0x00] in LE
        let entry = make_entry(0, FieldType::Short, 2, [0x0A, 0x00, 0x14, 0x00]);
        let vals = resolve_u32_values(&mut Cursor::new(vec![]), &entry, ByteOrder::LittleEndian)
            .unwrap();
        assert_eq!(vals, vec![10, 20]);
    }

    #[test]
    fn resolve_long_inline_le() {
        // LONG count=1, value=0x1234_5678 in LE
        let entry =
            make_entry(0, FieldType::Long, 1, [0x78, 0x56, 0x34, 0x12]);
        let vals = resolve_u32_values(&mut Cursor::new(vec![]), &entry, ByteOrder::LittleEndian)
            .unwrap();
        assert_eq!(vals, vec![0x1234_5678]);
    }

    #[test]
    fn resolve_longs_out_of_line_le() {
        // LONG count=3, stored at offset 4 in a small buffer.
        // value_bytes encodes the offset (4) as LE u32.
        let mut buf = vec![0u8; 4]; // padding before offset 4
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.extend_from_slice(&20u32.to_le_bytes());
        buf.extend_from_slice(&30u32.to_le_bytes());

        let entry = make_entry(0, FieldType::Long, 3, [4, 0, 0, 0]); // offset = 4
        let vals =
            resolve_u32_values(&mut Cursor::new(buf), &entry, ByteOrder::LittleEndian).unwrap();
        assert_eq!(vals, vec![10, 20, 30]);
    }

    #[test]
    fn resolve_short_inline_be() {
        // SHORT count=1, value=256 = [0x01, 0x00, 0x00, 0x00] in BE
        let entry = make_entry(256, FieldType::Short, 1, [0x01, 0x00, 0x00, 0x00]);
        let vals = resolve_u32_values(&mut Cursor::new(vec![]), &entry, ByteOrder::BigEndian)
            .unwrap();
        assert_eq!(vals, vec![256]);
    }

    // -----------------------------------------------------------------------
    // parse_svs_file tests
    // -----------------------------------------------------------------------

    /// Build a LE TIFF with one tiled IFD.
    ///
    /// The tile payload array is stored out-of-line (2 tiles × 4 bytes = 8 bytes,
    /// which exceeds the 4-byte inline limit).
    fn build_tiled_tiff() -> Vec<u8> {
        // Layout:
        //   [0..8)    header          (8 bytes)
        //   [8..76)   IFD             (2 + 6×12 + 4 = 78 bytes) → ends at 86
        //   [86..94)  TileOffsets[]   (2 × u32 = 8 bytes)
        //   [94..102) TileByteCounts[]
        //   [102..118) tile data      (2 × 8 bytes)
        //
        // Recalculate: IFD at 8, entry_count u16 + 6 entries × 12 + next_offset u32
        //   = 2 + 72 + 4 = 78 bytes → IFD ends at 8 + 78 = 86
        let tile_offsets_offset: u32 = 86;
        let tile_counts_offset: u32 = 94;
        let tile0_offset: u32 = 102;
        let tile1_offset: u32 = 110;

        let entries: &[(u16, u16, u32, [u8; 4])] = &[
            // ImageWidth = 256 (SHORT inline)
            (256, 3, 1, [0x00, 0x01, 0x00, 0x00]),
            // ImageLength = 256 (SHORT inline)
            (257, 3, 1, [0x00, 0x01, 0x00, 0x00]),
            // TileWidth = 128 (SHORT inline)
            (322, 3, 1, [0x80, 0x00, 0x00, 0x00]),
            // TileLength = 128 (SHORT inline)
            (323, 3, 1, [0x80, 0x00, 0x00, 0x00]),
            // TileOffsets: 2 LONGs out-of-line
            (324, 4, 2, tile_offsets_offset.to_le_bytes()),
            // TileByteCounts: 2 LONGs out-of-line
            (325, 4, 2, tile_counts_offset.to_le_bytes()),
        ];

        let mut data = build_le_ifd(entries, 0);

        // Append TileOffsets array.
        data.extend_from_slice(&tile0_offset.to_le_bytes());
        data.extend_from_slice(&tile1_offset.to_le_bytes());

        // Append TileByteCounts array.
        data.extend_from_slice(&8u32.to_le_bytes());
        data.extend_from_slice(&8u32.to_le_bytes());

        // Append tile payloads (dummy bytes).
        data.extend_from_slice(&[0xAA; 8]);
        data.extend_from_slice(&[0xBB; 8]);

        data
    }

    #[test]
    fn parse_svs_file_tiled() {
        use crate::svs::layout::DataUnitKind;
        use std::path::PathBuf;

        let data = build_tiled_tiff();
        let file_len = data.len() as u64;
        let mut cur = Cursor::new(data);

        let svs =
            parse_svs_file(&mut cur, PathBuf::from("test.svs"), file_len).unwrap();

        assert_eq!(svs.ifds.len(), 1);
        let ifd = &svs.ifds[0];
        assert_eq!(ifd.width, 256);
        assert_eq!(ifd.height, 256);
        assert_eq!(ifd.tile_width, Some(128));
        assert_eq!(ifd.tile_height, Some(128));
        assert_eq!(ifd.data_units.len(), 2);

        assert_eq!(ifd.data_units[0].kind, DataUnitKind::Tile);
        assert_eq!(ifd.data_units[0].offset, 102);
        assert_eq!(ifd.data_units[0].length, 8);

        assert_eq!(ifd.data_units[1].kind, DataUnitKind::Tile);
        assert_eq!(ifd.data_units[1].offset, 110);
        assert_eq!(ifd.data_units[1].length, 8);
    }

    // -----------------------------------------------------------------------
    // BigTIFF tests
    // -----------------------------------------------------------------------

    /// Build a minimal LE BigTIFF header (16 bytes) pointing to `first_ifd_offset`.
    fn bigtiff_le_header(first_ifd_offset: u64) -> Vec<u8> {
        let mut v = vec![
            0x49, 0x49, // "II" - little endian
            43, 0,      // magic = 43
            8, 0,       // bytesize of offsets = 8
            0, 0,       // reserved
        ];
        v.extend_from_slice(&first_ifd_offset.to_le_bytes());
        v
    }

    #[test]
    fn parse_bigtiff_header_le() {
        let data = bigtiff_le_header(0x0000_0000_0000_0010);
        let (order, offset) = parse_bigtiff_header(&mut Cursor::new(data)).unwrap();
        assert_eq!(order, ByteOrder::LittleEndian);
        assert_eq!(offset, 0x10);
    }

    /// Build a BigTIFF file with a single tiled IFD.
    ///
    /// Layout:
    ///   [0..16)    BigTIFF header
    ///   [16..16+8+n*20+8)  IFD
    ///   [...]      TileOffsets[], TileByteCounts[], tile payloads
    fn build_bigtiff_tiled() -> Vec<u8> {
        // IFD starts at byte 16 (right after the 16-byte header).
        // IFD: 8-byte entry_count + 6 entries × 20 bytes + 8-byte next_offset
        //    = 8 + 120 + 8 = 136 bytes → IFD ends at 16 + 136 = 152
        let ifd_offset: u64 = 16;
        let tile_offsets_arr: u64 = 152;
        let tile_counts_arr: u64 = 152 + 16; // 2 × u64 = 16 bytes
        let tile0_offset: u64 = tile_counts_arr + 16;
        let tile1_offset: u64 = tile0_offset + 8;

        let mut data = bigtiff_le_header(ifd_offset);

        // Entry count (u64 = 6).
        data.extend_from_slice(&6u64.to_le_bytes());

        // Helper: emit one 20-byte BigTIFF IFD entry.
        let mut entry = |tag: u16, typ: u16, count: u64, value: u64| {
            let mut e = Vec::with_capacity(20);
            e.extend_from_slice(&tag.to_le_bytes());
            e.extend_from_slice(&typ.to_le_bytes());
            e.extend_from_slice(&count.to_le_bytes());
            e.extend_from_slice(&value.to_le_bytes());
            e
        };

        // ImageWidth = 512 (SHORT inline, type=3)
        data.extend_from_slice(&entry(256, 3, 1, 512));
        // ImageLength = 512
        data.extend_from_slice(&entry(257, 3, 1, 512));
        // TileWidth = 256
        data.extend_from_slice(&entry(322, 3, 1, 256));
        // TileLength = 256
        data.extend_from_slice(&entry(323, 3, 1, 256));
        // TileOffsets: 2 LONG8 values out-of-line (type=16)
        data.extend_from_slice(&entry(324, 16, 2, tile_offsets_arr));
        // TileByteCounts: 2 LONG8 values out-of-line (type=16)
        data.extend_from_slice(&entry(325, 16, 2, tile_counts_arr));

        // next_ifd_offset = 0
        data.extend_from_slice(&0u64.to_le_bytes());

        // TileOffsets array.
        data.extend_from_slice(&tile0_offset.to_le_bytes());
        data.extend_from_slice(&tile1_offset.to_le_bytes());

        // TileByteCounts array.
        data.extend_from_slice(&8u64.to_le_bytes());
        data.extend_from_slice(&8u64.to_le_bytes());

        // Tile payloads.
        data.extend_from_slice(&[0xAA; 8]);
        data.extend_from_slice(&[0xBB; 8]);

        data
    }

    #[test]
    fn parse_svs_file_bigtiff_tiled() {
        use crate::svs::layout::DataUnitKind;
        use std::path::PathBuf;

        let data = build_bigtiff_tiled();
        let file_len = data.len() as u64;
        let mut cur = Cursor::new(data);

        let svs = parse_svs_file(&mut cur, PathBuf::from("test.svs"), file_len).unwrap();

        assert_eq!(svs.ifds.len(), 1);
        let ifd = &svs.ifds[0];
        assert_eq!(ifd.width, 512);
        assert_eq!(ifd.height, 512);
        assert_eq!(ifd.tile_width, Some(256));
        assert_eq!(ifd.tile_height, Some(256));
        assert_eq!(ifd.data_units.len(), 2);
        assert_eq!(ifd.data_units[0].kind, DataUnitKind::Tile);
        assert_eq!(ifd.data_units[0].length, 8);
        assert_eq!(ifd.data_units[1].kind, DataUnitKind::Tile);
        assert_eq!(ifd.data_units[1].length, 8);
    }

    #[test]
    fn parse_svs_file_no_tiles_produces_empty_data_units() {
        // IFD with only ImageWidth and ImageLength — no tile tags.
        let entries = [
            (256u16, 3u16, 1u32, [64u8, 0, 0, 0]), // width=64
            (257u16, 3u16, 1u32, [32u8, 0, 0, 0]), // height=32
        ];
        let data = build_le_ifd(&entries, 0);
        let file_len = data.len() as u64;
        let mut cur = Cursor::new(data);

        let svs = parse_svs_file(
            &mut cur,
            std::path::PathBuf::from("bare.svs"),
            file_len,
        )
        .unwrap();

        let ifd = &svs.ifds[0];
        assert_eq!(ifd.width, 64);
        assert_eq!(ifd.height, 32);
        assert!(ifd.tile_width.is_none());
        assert!(ifd.data_units.is_empty());
    }
}
