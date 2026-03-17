// Archetype byte encoding for SVS corpus compression.
//
// An archetype file (.arch) encodes the byte sequences that are shared across
// all members of an archetype group, as identified by the similarity pipeline.
//
// Format:
//   The file is a flat sequence of regions.  Each region is one of:
//     - Shared block: [u32 LE: length][bytes…]
//     - Gap marker:   [0xFF, 0xFF, 0xFF, 0xFF]
//
// Regions are emitted in structural order: IFD 0 description, IFD 1
// description, …  Within each description, consecutive matching runs of
// ≥ MIN_SHARED_BYTES are encoded as shared blocks; differing spans become
// gap markers.
//
// The gap magic value (0xFFFFFFFF) is assumed never to appear as a legitimate
// four-byte content length.

pub mod extract;
pub mod compare;
pub mod encode;
pub mod build;

pub use build::build_archetype_bytes;
pub use encode::{ArchSegment, GAP_MAGIC};

use std::io;

// ---------------------------------------------------------------------------
// Tuning
// ---------------------------------------------------------------------------

/// Minimum byte run length for a shared sequence to be encoded as a block.
/// Runs shorter than this are treated as gaps.
pub const MIN_SHARED_BYTES: usize = 4;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ArchError {
    Io(io::Error),
    Parse(crate::svs::parser::ParseError),
    NoMembers,
    UnsupportedFormat(String),
}

impl std::fmt::Display for ArchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchError::Io(e) => write!(f, "I/O error: {e}"),
            ArchError::Parse(e) => write!(f, "parse error: {e}"),
            ArchError::NoMembers => write!(f, "archetype has no member files"),
            ArchError::UnsupportedFormat(s) => write!(f, "unsupported format: {s}"),
        }
    }
}

impl std::error::Error for ArchError {}

impl From<io::Error> for ArchError {
    fn from(e: io::Error) -> Self {
        ArchError::Io(e)
    }
}

impl From<crate::svs::parser::ParseError> for ArchError {
    fn from(e: crate::svs::parser::ParseError) -> Self {
        ArchError::Parse(e)
    }
}
