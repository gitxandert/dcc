// SVS/TIFF image layout: IFDs, tiles, strips.

use std::path::PathBuf;

/// A parsed SVS (TIFF-based) file.
///
/// Only the header and IFD metadata are stored here; pixel data is accessed
/// by offset from the original file on disk.
#[derive(Debug)]
pub struct SvsFile {
    pub path: PathBuf,
    pub ifds: Vec<Ifd>,
    /// Total byte length of the file on disk.
    pub raw_len: u64,
}

/// One Image File Directory from the TIFF chain.
#[derive(Debug)]
pub struct Ifd {
    /// Zero-based index in the IFD chain.
    pub index: usize,
    pub width: u32,
    pub height: u32,
    /// Tile width in pixels, if the image is tiled.
    pub tile_width: Option<u32>,
    /// Tile height in pixels, if the image is tiled.
    pub tile_height: Option<u32>,
    /// TIFF compression tag value (e.g. 1 = none, 7 = JPEG).
    pub compression: Option<u16>,
    /// Contents of the ImageDescription tag, if present.
    pub description: Option<String>,
    /// Ordered list of data units (tiles or strips) in this IFD.
    pub data_units: Vec<DataUnit>,
}

/// A single addressable data payload within an SVS file.
#[derive(Debug)]
pub struct DataUnit {
    pub kind: DataUnitKind,
    /// Byte offset of the payload from the start of the file.
    pub offset: u64,
    /// Byte length of the payload.
    pub length: u64,
    /// IFD index this unit belongs to.
    pub ifd_index: usize,
    /// Zero-based index of this unit within its IFD.
    pub unit_index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataUnitKind {
    Tile,
    Strip,
    MetadataBlob,
    AssociatedImage,
}
