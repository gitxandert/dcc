// Encode an ordered sequence of archetype segments into the .arch binary
// format.
//
// Format per segment:
//   Shared block:  [u32 LE: byte_count][byte_count bytes of shared data]
//   Gap marker:    [0xFF, 0xFF, 0xFF, 0xFF]
//
// The gap magic (0xFFFF_FFFF as u32) is reserved; shared blocks are assumed
// to never reach that length.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The four-byte gap marker written to an `.arch` file when shared content is
/// interrupted by file-specific bytes.
pub const GAP_MAGIC: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// One region of the archetype byte stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchSegment {
    /// A run of bytes present (identically) in every member of the archetype
    /// group.  Taken verbatim from the representative file.
    Shared(Vec<u8>),
    /// A span of file-specific content with no shared bytes, or a structural
    /// position where the representative file has no description.
    Gap,
}

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

/// Encode an ordered slice of segments into the `.arch` binary representation.
///
/// Segments are written sequentially with no padding or alignment.
pub fn encode_segments(segments: &[ArchSegment]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();
    for seg in segments {
        match seg {
            ArchSegment::Shared(bytes) => {
                let len = bytes.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(bytes);
            }
            ArchSegment::Gap => {
                out.extend_from_slice(&GAP_MAGIC);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_empty_segment_list() {
        assert_eq!(encode_segments(&[]), Vec::<u8>::new());
    }

    #[test]
    fn encode_single_shared_block() {
        let segs = [ArchSegment::Shared(vec![1, 2, 3, 4])];
        let out = encode_segments(&segs);
        // u32 LE length (4) followed by the bytes.
        assert_eq!(out, [4, 0, 0, 0, 1, 2, 3, 4]);
    }

    #[test]
    fn encode_single_gap() {
        let segs = [ArchSegment::Gap];
        let out = encode_segments(&segs);
        assert_eq!(out, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn encode_gap_then_shared_then_gap() {
        let segs = [
            ArchSegment::Gap,
            ArchSegment::Shared(vec![1, 2, 3, 4, 5]),
            ArchSegment::Gap,
        ];
        let out = encode_segments(&segs);
        #[rustfmt::skip]
        let expected: &[u8] = &[
            0xFF, 0xFF, 0xFF, 0xFF,   // gap
            5, 0, 0, 0,               // len = 5
            1, 2, 3, 4, 5,            // payload
            0xFF, 0xFF, 0xFF, 0xFF,   // gap
        ];
        assert_eq!(out, expected);
    }

    #[test]
    fn encode_two_consecutive_shared_blocks() {
        let segs = [
            ArchSegment::Shared(vec![0xAA, 0xBB]),
            ArchSegment::Shared(vec![0xCC]),
        ];
        let out = encode_segments(&segs);
        #[rustfmt::skip]
        let expected: &[u8] = &[
            2, 0, 0, 0, 0xAA, 0xBB,  // first block
            1, 0, 0, 0, 0xCC,         // second block
        ];
        assert_eq!(out, expected);
    }

    #[test]
    fn shared_block_length_is_little_endian() {
        // Length 256 (0x00000100) should appear as [0x00, 0x01, 0x00, 0x00] in LE.
        let payload = vec![0xAAu8; 256];
        let segs = [ArchSegment::Shared(payload)];
        let out = encode_segments(&segs);
        assert_eq!(&out[..4], &[0x00, 0x01, 0x00, 0x00]);
    }
}
