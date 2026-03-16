// Per-file unit manifests: parse an SVS file and record structural unit data.
//
// Manifests are built from metadata and unit boundaries only.  No full payload
// hashing occurs here.  A bounded coarse fingerprint is computed for each unit
// to support candidate matching (Step 4).  Strong hashing (SHA-256) is deferred
// to the exact-confirmation pass (Step 5).

use std::io::{self, Read, Seek};
use std::path::{Path, PathBuf};

use crate::fingerprint::hash::{coarse_fingerprint, HashError};
use crate::svs::layout::{AssociatedImageKind, DataUnitKind};
use crate::svs::parser::{parse_svs_file, ParseError};

/// A single data unit entry within a manifest.
#[derive(Debug, Clone)]
pub struct UnitRecord {
    pub ifd_index: usize,
    pub unit_index: usize,
    pub kind: DataUnitKind,
    pub offset: u64,
    pub length: u64,
    /// TIFF compression code for the IFD this unit belongs to.
    pub compression: Option<u16>,
    /// Human-readable role label, set for associated images (label, macro, thumbnail).
    pub role: Option<String>,
    /// Bounded coarse fingerprint for candidate matching.  Populated during
    /// manifest construction; `None` only if fingerprinting was skipped.
    pub coarse_fp: Option<u64>,
    /// Full SHA-256 of the payload.  `None` until the exact-confirmation pass
    /// explicitly populates it.
    pub strong_hash: Option<[u8; 32]>,
}

/// All data units extracted from a single SVS file.
#[derive(Debug)]
pub struct UnitManifest {
    /// Path to the source file.
    pub path: PathBuf,
    /// Caller-assigned file identifier (e.g. index within a directory scan).
    pub file_id: usize,
    pub units: Vec<UnitRecord>,
}

/// Errors that can occur while building a manifest.
#[derive(Debug)]
pub enum ManifestError {
    Io(io::Error),
    Parse(ParseError),
    /// I/O error or out-of-bounds condition during fingerprinting.
    Fingerprint(HashError),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestError::Io(e) => write!(f, "I/O error: {e}"),
            ManifestError::Parse(e) => write!(f, "parse error: {e}"),
            ManifestError::Fingerprint(e) => write!(f, "fingerprint error: {e}"),
        }
    }
}

impl From<io::Error> for ManifestError {
    fn from(e: io::Error) -> Self {
        ManifestError::Io(e)
    }
}

impl From<ParseError> for ManifestError {
    fn from(e: ParseError) -> Self {
        ManifestError::Parse(e)
    }
}

impl From<HashError> for ManifestError {
    fn from(e: HashError) -> Self {
        ManifestError::Fingerprint(e)
    }
}

/// Build a `UnitManifest` from any seekable reader.
///
/// Parses TIFF/SVS structure and records one `UnitRecord` per data unit.
/// A bounded coarse fingerprint is computed for each unit; no full-payload
/// SHA-256 is performed.
///
/// `file_len` must equal the total byte length of the file (used for bounds
/// checks during coarse fingerprinting).  Useful for tests that supply a
/// `Cursor`.
pub fn build_manifest_from_reader<R: Read + Seek>(
    reader: &mut R,
    path: PathBuf,
    file_id: usize,
    file_len: u64,
) -> Result<UnitManifest, ManifestError> {
    build_manifest_impl(reader, path, file_id, file_len, |_, _| {})
}

/// Same as [`build_manifest_from_reader`] but calls `on_unit(units_done,
/// total_units)` after each unit is processed.
///
/// `total_units` is the count of all data units across all IFDs in the file,
/// computed once after parsing.  `units_done` counts from 1 up to
/// `total_units`.  The callback is not invoked if the file contains no data
/// units.
pub fn build_manifest_from_reader_cb<R, F>(
    reader: &mut R,
    path: PathBuf,
    file_id: usize,
    file_len: u64,
    on_unit: F,
) -> Result<UnitManifest, ManifestError>
where
    R: Read + Seek,
    F: FnMut(usize, usize),
{
    build_manifest_impl(reader, path, file_id, file_len, on_unit)
}

/// Build a `UnitManifest` by opening the file at `path` from disk.
pub fn build_manifest(path: &Path, file_id: usize) -> Result<UnitManifest, ManifestError> {
    let mut f = std::fs::File::open(path)?;
    let file_len = f.metadata()?.len();
    build_manifest_from_reader(&mut f, path.to_owned(), file_id, file_len)
}

fn build_manifest_impl<R, F>(
    reader: &mut R,
    path: PathBuf,
    file_id: usize,
    file_len: u64,
    mut on_unit: F,
) -> Result<UnitManifest, ManifestError>
where
    R: Read + Seek,
    F: FnMut(usize, usize),
{
    let svs = parse_svs_file(reader, path.clone(), file_len)?;

    let total_units: usize = svs.ifds.iter().map(|ifd| ifd.data_units.len()).sum();
    let mut done: usize = 0;

    let mut units = Vec::new();
    for ifd in &svs.ifds {
        let role: Option<String> = ifd.associated_image.as_ref().map(|k| match k {
            AssociatedImageKind::Label => "label",
            AssociatedImageKind::Macro => "macro",
            AssociatedImageKind::Thumbnail => "thumbnail",
        }.to_string());

        for unit in &ifd.data_units {
            let coarse_fp =
                Some(coarse_fingerprint(reader, unit, file_len, ifd.compression)?);

            units.push(UnitRecord {
                ifd_index: unit.ifd_index,
                unit_index: unit.unit_index,
                kind: unit.kind,
                offset: unit.offset,
                length: unit.length,
                compression: ifd.compression,
                role: role.clone(),
                coarse_fp,
                strong_hash: None,
            });

            done += 1;
            on_unit(done, total_units);
        }
    }

    Ok(UnitManifest { path, file_id, units })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal little-endian TIFF with one tiled IFD.
    ///
    /// Layout (offsets are absolute from start of file):
    ///   [0..8)      header
    ///   [8..86)     IFD  (2 + 6×12 + 4 = 78 bytes)
    ///   [86..94)    TileOffsets[]   (2 × u32)
    ///   [94..102)   TileByteCounts[] (2 × u32)
    ///   [102..106)  tile 0 payload  (4 bytes, all 0xAA)
    ///   [106..110)  tile 1 payload  (4 bytes, all 0xBB)
    fn build_tiled_tiff() -> Vec<u8> {
        let tile_offsets_offset: u32 = 86;
        let tile_counts_offset: u32 = 94;
        let tile0_offset: u32 = 102;
        let tile1_offset: u32 = 106;
        let tile_len: u32 = 4;

        // Header: LE byte-order, TIFF magic=42, IFD at offset 8.
        let mut data: Vec<u8> = vec![
            0x49, 0x49, // "II" little-endian
            42, 0,      // TIFF magic
            8, 0, 0, 0, // first IFD offset
        ];

        // IFD: entry count (u16) + entries (6 × 12 bytes) + next IFD offset (u32=0)
        data.extend_from_slice(&6u16.to_le_bytes()); // entry count

        // Helper: write one IFD entry (tag u16, type u16, count u32, value 4 bytes)
        let mut write_entry = |tag: u16, typ: u16, count: u32, value: [u8; 4]| {
            data.extend_from_slice(&tag.to_le_bytes());
            data.extend_from_slice(&typ.to_le_bytes());
            data.extend_from_slice(&count.to_le_bytes());
            data.extend_from_slice(&value);
        };

        // ImageWidth  = 256 (SHORT)
        write_entry(256, 3, 1, [0x00, 0x01, 0x00, 0x00]);
        // ImageLength = 256 (SHORT)
        write_entry(257, 3, 1, [0x00, 0x01, 0x00, 0x00]);
        // TileWidth   = 128 (SHORT)
        write_entry(322, 3, 1, [0x80, 0x00, 0x00, 0x00]);
        // TileLength  = 128 (SHORT)
        write_entry(323, 3, 1, [0x80, 0x00, 0x00, 0x00]);
        // TileOffsets: 2 LONGs, out-of-line
        write_entry(324, 4, 2, tile_offsets_offset.to_le_bytes());
        // TileByteCounts: 2 LONGs, out-of-line
        write_entry(325, 4, 2, tile_counts_offset.to_le_bytes());

        data.extend_from_slice(&0u32.to_le_bytes()); // next IFD offset = 0 (end)

        // TileOffsets array
        data.extend_from_slice(&tile0_offset.to_le_bytes());
        data.extend_from_slice(&tile1_offset.to_le_bytes());

        // TileByteCounts array
        data.extend_from_slice(&tile_len.to_le_bytes());
        data.extend_from_slice(&tile_len.to_le_bytes());

        // Tile payloads
        data.extend_from_slice(&[0xAA; 4]); // tile 0
        data.extend_from_slice(&[0xBB; 4]); // tile 1

        data
    }

    #[test]
    fn manifest_contains_all_units() {
        let tiff = build_tiled_tiff();
        let file_len = tiff.len() as u64;
        let mut cursor = Cursor::new(tiff);
        let manifest = build_manifest_from_reader(
            &mut cursor,
            PathBuf::from("test.svs"),
            0,
            file_len,
        )
        .unwrap();

        assert_eq!(manifest.file_id, 0);
        assert_eq!(manifest.path, PathBuf::from("test.svs"));
        assert_eq!(manifest.units.len(), 2, "should have one record per tile");

        for record in &manifest.units {
            assert_eq!(record.kind, DataUnitKind::Tile);
            assert_eq!(record.length, 4);
        }
    }

    #[test]
    fn unit_records_carry_correct_metadata() {
        let tiff = build_tiled_tiff();
        let file_len = tiff.len() as u64;
        let mut cursor = Cursor::new(tiff);
        let manifest = build_manifest_from_reader(
            &mut cursor,
            PathBuf::from("test.svs"),
            7,
            file_len,
        )
        .unwrap();

        let r0 = &manifest.units[0];
        assert_eq!(r0.ifd_index, 0);
        assert_eq!(r0.unit_index, 0);
        assert_eq!(r0.offset, 102);
        assert_eq!(r0.length, 4);
        // Tiled TIFF with no Compression tag → None.
        assert_eq!(r0.compression, None);
        // Primary pyramid IFD → no role.
        assert_eq!(r0.role, None);

        let r1 = &manifest.units[1];
        assert_eq!(r1.ifd_index, 0);
        assert_eq!(r1.unit_index, 1);
        assert_eq!(r1.offset, 106);
        assert_eq!(r1.length, 4);
    }

    #[test]
    fn strong_hash_is_not_populated_by_manifest_builder() {
        let tiff = build_tiled_tiff();
        let file_len = tiff.len() as u64;
        let mut cursor = Cursor::new(tiff);
        let manifest = build_manifest_from_reader(
            &mut cursor,
            PathBuf::from("test.svs"),
            0,
            file_len,
        )
        .unwrap();

        for record in &manifest.units {
            assert!(
                record.strong_hash.is_none(),
                "strong_hash must remain None until the confirmation pass"
            );
        }
    }

    #[test]
    fn coarse_fp_is_populated() {
        let tiff = build_tiled_tiff();
        let file_len = tiff.len() as u64;
        let mut cursor = Cursor::new(tiff);
        let manifest = build_manifest_from_reader(
            &mut cursor,
            PathBuf::from("test.svs"),
            0,
            file_len,
        )
        .unwrap();

        for record in &manifest.units {
            assert!(record.coarse_fp.is_some(), "coarse_fp must be populated");
        }
    }

    #[test]
    fn coarse_fp_is_stable_across_builds() {
        let tiff = build_tiled_tiff();
        let file_len = tiff.len() as u64;

        let m1 = build_manifest_from_reader(
            &mut Cursor::new(tiff.clone()),
            PathBuf::from("a.svs"),
            0,
            file_len,
        )
        .unwrap();
        let m2 = build_manifest_from_reader(
            &mut Cursor::new(tiff),
            PathBuf::from("b.svs"),
            1,
            file_len,
        )
        .unwrap();

        // Same file content → same coarse fingerprints, regardless of path or file_id.
        assert_eq!(m1.units[0].coarse_fp, m2.units[0].coarse_fp);
        assert_eq!(m1.units[1].coarse_fp, m2.units[1].coarse_fp);
    }

    #[test]
    fn distinct_payloads_produce_distinct_coarse_fps() {
        let tiff = build_tiled_tiff();
        let file_len = tiff.len() as u64;
        let mut cursor = Cursor::new(tiff);
        let manifest = build_manifest_from_reader(
            &mut cursor,
            PathBuf::from("test.svs"),
            0,
            file_len,
        )
        .unwrap();

        // tile 0 is all 0xAA, tile 1 is all 0xBB — must fingerprint differently.
        assert_ne!(
            manifest.units[0].coarse_fp,
            manifest.units[1].coarse_fp,
            "different payloads must produce different coarse fingerprints"
        );
    }
}
