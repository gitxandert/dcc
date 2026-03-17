// Extract raw ImageDescription bytes from each IFD of a Classic TIFF file.
//
// The bytes are returned as raw (including the TIFF null terminator) so they
// can be compared byte-for-byte across files and embedded directly into
// archetype shared blocks.
//
// BigTIFF files are not yet supported and will return
// `ArchError::UnsupportedFormat`.

use std::io::{Read, Seek, SeekFrom};

use crate::svs::parser::{
    parse_bigtiff_header, parse_bigtiff_ifd_chain, parse_header, parse_ifd_chain, u16_from,
    ParseError,
};
use crate::svs::tiff::{tag, BIGTIFF_MAGIC, BYTE_ORDER_BE, BYTE_ORDER_LE};

use super::ArchError;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Raw description bytes extracted from one IFD position in a file.
#[derive(Debug, Clone)]
pub struct IfdDescBytes {
    /// Zero-based IFD chain index.
    pub ifd_index: usize,
    /// Raw bytes of the `ImageDescription` tag value, including the TIFF null
    /// terminator.  `None` if this IFD has no `ImageDescription` tag.
    pub bytes: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Extract the raw `ImageDescription` bytes from every IFD in a TIFF file
/// accessible via `r`.
///
/// Supports both Classic TIFF (magic 42, 32-bit offsets) and BigTIFF (magic
/// 43, 64-bit offsets).  Returns one [`IfdDescBytes`] per IFD in chain order.
pub fn extract_descriptions<R: Read + Seek>(r: &mut R) -> Result<Vec<IfdDescBytes>, ArchError> {
    // Peek at the first 4 bytes to detect byte order and magic, mirroring
    // the approach used by `parse_svs_file`.
    r.seek(SeekFrom::Start(0))?;
    let mut peek = [0u8; 4];
    r.read_exact(&mut peek)?;

    let bom = u16::from_le_bytes([peek[0], peek[1]]);
    let byte_order = match bom {
        BYTE_ORDER_LE => crate::svs::tiff::ByteOrder::LittleEndian,
        BYTE_ORDER_BE => crate::svs::tiff::ByteOrder::BigEndian,
        _ => return Err(ArchError::Parse(ParseError::UnknownByteOrder(bom))),
    };
    let magic = u16_from([peek[2], peek[3]], byte_order);

    if magic == BIGTIFF_MAGIC {
        extract_descriptions_bigtiff(r)
    } else {
        extract_descriptions_classic(r)
    }
}

/// Classic TIFF (magic 42) path.
fn extract_descriptions_classic<R: Read + Seek>(
    r: &mut R,
) -> Result<Vec<IfdDescBytes>, ArchError> {
    let header = parse_header(r)?;
    let byte_order = header.byte_order;
    let raw_ifds = parse_ifd_chain(r, header.first_ifd_offset, byte_order)?;

    let mut results = Vec::with_capacity(raw_ifds.len());
    for (ifd_index, entries) in raw_ifds.iter().enumerate() {
        let mut desc_bytes: Option<Vec<u8>> = None;
        for entry in entries {
            if entry.tag == tag::IMAGE_DESCRIPTION {
                let count = entry.count as usize;
                let bytes = if entry.is_inline() {
                    entry.value_bytes[..count.min(4)].to_vec()
                } else {
                    let offset = entry.value_as_offset(byte_order);
                    r.seek(SeekFrom::Start(offset as u64))?;
                    let mut buf = vec![0u8; count];
                    r.read_exact(&mut buf)?;
                    buf
                };
                desc_bytes = Some(bytes);
                break;
            }
        }
        results.push(IfdDescBytes { ifd_index, bytes: desc_bytes });
    }
    Ok(results)
}

/// BigTIFF (magic 43) path.
fn extract_descriptions_bigtiff<R: Read + Seek>(
    r: &mut R,
) -> Result<Vec<IfdDescBytes>, ArchError> {
    let (byte_order, first_ifd_offset) = parse_bigtiff_header(r)?;
    let raw_ifds = parse_bigtiff_ifd_chain(r, first_ifd_offset, byte_order)?;

    let mut results = Vec::with_capacity(raw_ifds.len());
    for (ifd_index, entries) in raw_ifds.iter().enumerate() {
        let mut desc_bytes: Option<Vec<u8>> = None;
        for entry in entries {
            if entry.tag == tag::IMAGE_DESCRIPTION {
                let count = entry.count as usize;
                let bytes = if entry.is_inline() {
                    // BigTIFF inline threshold is 8 bytes.
                    entry.value_bytes[..count.min(8)].to_vec()
                } else {
                    let offset = entry.value_as_offset(byte_order);
                    r.seek(SeekFrom::Start(offset))?;
                    let mut buf = vec![0u8; count];
                    r.read_exact(&mut buf)?;
                    buf
                };
                desc_bytes = Some(bytes);
                break;
            }
        }
        results.push(IfdDescBytes { ifd_index, bytes: desc_bytes });
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // -----------------------------------------------------------------------
    // Minimal TIFF fixture builder
    // -----------------------------------------------------------------------

    /// Build a Classic TIFF LE byte buffer with N IFDs.
    ///
    /// Each element of `descs` is either `Some("description text")` or `None`
    /// (no ImageDescription tag for that IFD).  Values are always stored
    /// out-of-line to exercise the seek path.
    fn build_tiff(descs: &[Option<&str>]) -> Vec<u8> {
        let n = descs.len();

        // Compute IFD byte offsets.
        //   IFD with description:    2 (count) + 1*12 (entry) + 4 (next) = 18 bytes
        //   IFD without description: 2 (count) + 0*12 (entry) + 4 (next) =  6 bytes
        let mut ifd_offsets: Vec<u32> = Vec::with_capacity(n);
        let mut pos: u32 = 8; // Header occupies bytes 0-7.
        for desc in descs {
            ifd_offsets.push(pos);
            pos += if desc.is_some() { 18 } else { 6 };
        }

        // Descriptions follow all IFDs.
        let desc_base = pos;
        let mut desc_offsets: Vec<Option<u32>> = Vec::with_capacity(n);
        let mut dpos = desc_base;
        for desc in descs {
            if let Some(s) = desc {
                desc_offsets.push(Some(dpos));
                dpos += s.len() as u32 + 1; // +1 for null terminator
            } else {
                desc_offsets.push(None);
            }
        }

        let mut data: Vec<u8> = Vec::new();

        // Header.
        data.extend_from_slice(&[0x49, 0x49]); // "II" = little-endian
        data.extend_from_slice(&42u16.to_le_bytes()); // TIFF magic
        data.extend_from_slice(&8u32.to_le_bytes()); // first IFD at offset 8

        // IFDs.
        for (i, desc) in descs.iter().enumerate() {
            let next = if i + 1 < n { ifd_offsets[i + 1] } else { 0u32 };
            if let (Some(s), Some(off)) = (desc, desc_offsets[i]) {
                let count = s.len() as u32 + 1;
                data.extend_from_slice(&1u16.to_le_bytes()); // 1 entry
                data.extend_from_slice(&270u16.to_le_bytes()); // tag: ImageDescription
                data.extend_from_slice(&2u16.to_le_bytes()); // type: ASCII
                data.extend_from_slice(&count.to_le_bytes()); // count (including \0)
                data.extend_from_slice(&off.to_le_bytes()); // value offset
            } else {
                data.extend_from_slice(&0u16.to_le_bytes()); // 0 entries
            }
            data.extend_from_slice(&next.to_le_bytes()); // next IFD offset
        }

        // Description data.
        for desc in descs {
            if let Some(s) = desc {
                data.extend_from_slice(s.as_bytes());
                data.push(0); // null terminator
            }
        }

        data
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn single_ifd_with_description() {
        let data = build_tiff(&[Some("Aperio Image Library v11.2.1")]);
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        assert_eq!(result.len(), 1);
        let bytes = result[0].bytes.as_ref().expect("should have description");
        // Null-terminated; raw bytes include the trailing \0.
        assert_eq!(&bytes[..bytes.len() - 1], b"Aperio Image Library v11.2.1");
        assert_eq!(*bytes.last().unwrap(), 0u8);
    }

    #[test]
    fn single_ifd_no_description() {
        let data = build_tiff(&[None]);
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].bytes.is_none());
    }

    #[test]
    fn two_ifds_both_with_descriptions() {
        let data = build_tiff(&[Some("desc_zero"), Some("desc_one")]);
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].ifd_index, 0);
        assert_eq!(result[1].ifd_index, 1);
        let b0 = result[0].bytes.as_ref().unwrap();
        assert_eq!(&b0[..9], b"desc_zero");
        let b1 = result[1].bytes.as_ref().unwrap();
        assert_eq!(&b1[..8], b"desc_one");
    }

    #[test]
    fn two_ifds_second_has_no_description() {
        let data = build_tiff(&[Some("first desc"), None]);
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result[0].bytes.is_some());
        assert!(result[1].bytes.is_none());
    }

    #[test]
    fn empty_tiff_returns_empty_vec() {
        // A well-formed TIFF with no IFDs (first_ifd_offset = 0).
        let mut data = vec![0x49u8, 0x49, 42, 0]; // II + magic
        data.extend_from_slice(&0u32.to_le_bytes()); // first_ifd_offset = 0
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // BigTIFF fixture builder
    // -----------------------------------------------------------------------

    /// Build a LE BigTIFF byte buffer with N IFDs.
    ///
    /// BigTIFF header: 16 bytes (II + magic 43 + offset-size 8 + reserved 0 +
    /// first_ifd_offset u64).
    /// Each IFD entry is 20 bytes: tag(u16) + type(u16) + count(u64) +
    /// value/offset(u64).
    fn build_bigtiff(descs: &[Option<&str>]) -> Vec<u8> {
        let n = descs.len();

        // IFD sizes:
        //   with description:    8 (count) + 1*20 (entry) + 8 (next) = 36 bytes
        //   without description: 8 (count) + 0*20 (entry) + 8 (next) = 16 bytes
        let mut ifd_offsets: Vec<u64> = Vec::with_capacity(n);
        let mut pos: u64 = 16; // Header occupies bytes 0-15.
        for desc in descs {
            ifd_offsets.push(pos);
            pos += if desc.is_some() { 36 } else { 16 };
        }

        // Descriptions follow all IFDs.
        let desc_base = pos;
        let mut desc_offsets: Vec<Option<u64>> = Vec::with_capacity(n);
        let mut dpos = desc_base;
        for desc in descs {
            if let Some(s) = desc {
                desc_offsets.push(Some(dpos));
                dpos += s.len() as u64 + 1;
            } else {
                desc_offsets.push(None);
            }
        }

        let mut data: Vec<u8> = Vec::new();

        // BigTIFF header (16 bytes).
        data.extend_from_slice(&[0x49, 0x49]); // "II"
        data.extend_from_slice(&43u16.to_le_bytes()); // magic
        data.extend_from_slice(&8u16.to_le_bytes()); // offset size = 8
        data.extend_from_slice(&0u16.to_le_bytes()); // reserved
        data.extend_from_slice(&16u64.to_le_bytes()); // first IFD at offset 16

        // IFDs.
        for (i, desc) in descs.iter().enumerate() {
            let next = if i + 1 < n { ifd_offsets[i + 1] } else { 0u64 };
            if let (Some(s), Some(off)) = (desc, desc_offsets[i]) {
                let count = s.len() as u64 + 1;
                data.extend_from_slice(&1u64.to_le_bytes()); // 1 entry
                data.extend_from_slice(&270u16.to_le_bytes()); // ImageDescription
                data.extend_from_slice(&2u16.to_le_bytes()); // ASCII
                data.extend_from_slice(&count.to_le_bytes()); // count
                data.extend_from_slice(&off.to_le_bytes()); // value offset
            } else {
                data.extend_from_slice(&0u64.to_le_bytes()); // 0 entries
            }
            data.extend_from_slice(&next.to_le_bytes()); // next IFD
        }

        // Description data.
        for desc in descs {
            if let Some(s) = desc {
                data.extend_from_slice(s.as_bytes());
                data.push(0);
            }
        }

        data
    }

    // -----------------------------------------------------------------------
    // BigTIFF tests
    // -----------------------------------------------------------------------

    #[test]
    fn bigtiff_single_ifd_with_description() {
        let data = build_bigtiff(&[Some("Aperio Image Library v11.2.1")]);
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        assert_eq!(result.len(), 1);
        let bytes = result[0].bytes.as_ref().expect("should have description");
        assert_eq!(&bytes[..bytes.len() - 1], b"Aperio Image Library v11.2.1");
        assert_eq!(*bytes.last().unwrap(), 0u8);
    }

    #[test]
    fn bigtiff_two_ifds_both_with_descriptions() {
        let data = build_bigtiff(&[Some("desc_zero"), Some("desc_one")]);
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(&result[0].bytes.as_ref().unwrap()[..9], b"desc_zero");
        assert_eq!(&result[1].bytes.as_ref().unwrap()[..8], b"desc_one");
    }

    #[test]
    fn bigtiff_ifd_with_no_description() {
        let data = build_bigtiff(&[None]);
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].bytes.is_none());
    }

    #[test]
    fn ifd_indices_are_sequential() {
        let data = build_tiff(&[Some("a"), Some("b"), Some("c")]);
        let mut cur = Cursor::new(data);
        let result = extract_descriptions(&mut cur).unwrap();
        for (i, r) in result.iter().enumerate() {
            assert_eq!(r.ifd_index, i);
        }
    }
}
