// TIFF data types and constants.

/// Magic bytes indicating little-endian byte order ("II").
pub const BYTE_ORDER_LE: u16 = 0x4949;

/// Magic bytes indicating big-endian byte order ("MM").
pub const BYTE_ORDER_BE: u16 = 0x4D4D;

/// Standard TIFF magic number (42).
pub const TIFF_MAGIC: u16 = 42;

/// BigTIFF magic number (43).
pub const BIGTIFF_MAGIC: u16 = 43;

/// TIFF byte order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteOrder {
    LittleEndian,
    BigEndian,
}

/// Parsed TIFF header (first 8 bytes of a standard TIFF file).
///
/// Layout:
///   [0..2]  byte-order mark  (0x4949 or 0x4D4D)
///   [2..4]  magic            (42)
///   [4..8]  offset of first IFD
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TiffHeader {
    pub byte_order: ByteOrder,
    /// Always 42 for standard TIFF.
    pub magic: u16,
    /// Byte offset of the first IFD from the start of the file.
    pub first_ifd_offset: u32,
}

/// TIFF field type identifier (the 2-byte type code inside each IFD entry).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Byte,
    Ascii,
    Short,
    Long,
    Rational,
    SByte,
    Undefined,
    SShort,
    SLong,
    SRational,
    Float,
    Double,
    Unknown(u16),
}

impl FieldType {
    pub fn from_u16(v: u16) -> Self {
        match v {
            1 => FieldType::Byte,
            2 => FieldType::Ascii,
            3 => FieldType::Short,
            4 => FieldType::Long,
            5 => FieldType::Rational,
            6 => FieldType::SByte,
            7 => FieldType::Undefined,
            8 => FieldType::SShort,
            9 => FieldType::SLong,
            10 => FieldType::SRational,
            11 => FieldType::Float,
            12 => FieldType::Double,
            other => FieldType::Unknown(other),
        }
    }

    /// Byte size of one element of this type, or `None` for unknown types.
    pub fn element_size(self) -> Option<u64> {
        match self {
            FieldType::Byte
            | FieldType::Ascii
            | FieldType::SByte
            | FieldType::Undefined => Some(1),
            FieldType::Short | FieldType::SShort => Some(2),
            FieldType::Long | FieldType::SLong | FieldType::Float => Some(4),
            FieldType::Rational | FieldType::SRational | FieldType::Double => Some(8),
            FieldType::Unknown(_) => None,
        }
    }
}

/// One raw 12-byte Classic TIFF IFD entry, before value resolution.
#[derive(Debug, Clone)]
pub struct RawIfdEntry {
    pub tag: u16,
    pub field_type: FieldType,
    pub count: u32,
    /// The raw 4 bytes of the value/offset field, in file byte order.
    pub value_bytes: [u8; 4],
}

impl RawIfdEntry {
    /// Interpret `value_bytes` as a file offset (u32 in `byte_order`).
    pub fn value_as_offset(&self, byte_order: ByteOrder) -> u32 {
        match byte_order {
            ByteOrder::LittleEndian => u32::from_le_bytes(self.value_bytes),
            ByteOrder::BigEndian => u32::from_be_bytes(self.value_bytes),
        }
    }

    /// Total byte size of the value payload, or `None` for unknown field types.
    pub fn payload_size(&self) -> Option<u64> {
        self.field_type.element_size().map(|s| s * self.count as u64)
    }

    /// True if the value fits inline in the 4-byte value field.
    pub fn is_inline(&self) -> bool {
        self.payload_size().map(|s| s <= 4).unwrap_or(false)
    }
}

/// Well-known TIFF tag identifiers referenced during parsing.
///
/// Only tags required for Phase 1 inspection are listed here.
pub mod tag {
    pub const IMAGE_WIDTH: u16 = 256;
    pub const IMAGE_LENGTH: u16 = 257;
    pub const TILE_WIDTH: u16 = 322;
    pub const TILE_LENGTH: u16 = 323;
    pub const TILE_OFFSETS: u16 = 324;
    pub const TILE_BYTE_COUNTS: u16 = 325;
}
