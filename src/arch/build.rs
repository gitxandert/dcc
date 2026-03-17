// Orchestrate archetype byte encoding for one archetype group.
//
// Pipeline:
//   1. Collect member file paths from the archetype's `member_file_ids`,
//      sorted alphabetically.  The first (alphabetically) is the
//      representative file.
//   2. Extract raw ImageDescription bytes from every member using
//      `extract_descriptions`.
//   3. For each IFD position, find shared byte runs across all members via
//      `find_shared_runs`.
//   4. Encode the resulting segments with `encode_segments`.
//
// The segment-building step (`build_segments_from_descriptions`) is exposed
// separately so it can be unit-tested without file I/O.

use std::fs::File;
use std::path::PathBuf;

use crate::similarity::archetype::Archetype;
use crate::similarity::profile::FileProfile;

use super::{ArchError, MIN_SHARED_BYTES};
use super::compare::find_shared_runs;
use super::encode::{encode_segments, ArchSegment};
use super::extract::{extract_descriptions, IfdDescBytes};

// ---------------------------------------------------------------------------
// Segment builder (pure — no file I/O, testable in isolation)
// ---------------------------------------------------------------------------

/// Build the ordered list of [`ArchSegment`]s for one archetype group.
///
/// `ifd_count` is the number of IFD positions the archetype covers.
/// `representative` holds the description bytes extracted from the
/// representative (first alphabetically) member file.
/// `others` holds the corresponding data for every other member.
///
/// For each IFD position:
/// - If the representative has no description → `Gap`.
/// - Otherwise compare the representative bytes against all others and emit
///   `Shared(…)` blocks for runs ≥ `MIN_SHARED_BYTES`, with `Gap` markers
///   before, between, and after differing spans.
pub(crate) fn build_segments_from_descriptions(
    ifd_count: usize,
    representative: &[IfdDescBytes],
    others: &[Vec<IfdDescBytes>],
) -> Vec<ArchSegment> {
    let mut segments: Vec<ArchSegment> = Vec::new();

    for ifd_i in 0..ifd_count {
        let ref_bytes = representative.get(ifd_i).and_then(|d| d.bytes.as_deref());

        match ref_bytes {
            None => {
                // Representative has no description at this IFD → gap.
                segments.push(ArchSegment::Gap);
            }
            Some(ref_b) => {
                // Gather the corresponding bytes from every other member at
                // this IFD position.  Members that lack an entry at position
                // `ifd_i` are silently skipped (they cannot contribute to a
                // shared run anyway).
                let other_slices: Vec<&[u8]> = others
                    .iter()
                    .filter_map(|descs| descs.get(ifd_i).and_then(|d| d.bytes.as_deref()))
                    .collect();

                let runs = find_shared_runs(ref_b, &other_slices, MIN_SHARED_BYTES);

                if runs.is_empty() {
                    segments.push(ArchSegment::Gap);
                } else {
                    let mut pos: usize = 0;
                    for run in runs {
                        if run.start > pos {
                            // Non-shared bytes before this run.
                            segments.push(ArchSegment::Gap);
                        }
                        segments.push(ArchSegment::Shared(ref_b[run.start..run.end].to_vec()));
                        pos = run.end;
                    }
                    if pos < ref_b.len() {
                        // Trailing non-shared bytes.
                        segments.push(ArchSegment::Gap);
                    }
                }
            }
        }
    }

    segments
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Build the binary `.arch` encoding for one archetype group.
///
/// Opens each member file, extracts `ImageDescription` bytes, finds shared
/// runs, and encodes the result.
///
/// Returns [`ArchError::NoMembers`] if `archetype.member_file_ids` is empty.
pub fn build_archetype_bytes(
    archetype: &Archetype,
    profiles: &[FileProfile],
) -> Result<Vec<u8>, ArchError> {
    if archetype.member_file_ids.is_empty() {
        return Err(ArchError::NoMembers);
    }

    // Resolve paths and sort alphabetically; first is the representative.
    let mut member_paths: Vec<PathBuf> = archetype
        .member_file_ids
        .iter()
        .filter_map(|&id| profiles.iter().find(|p| p.file_id == id))
        .map(|p| p.path.clone())
        .collect();
    member_paths.sort();

    // Extract description bytes from every member.
    let all_descs: Vec<Vec<IfdDescBytes>> = member_paths
        .iter()
        .map(|path| {
            let mut f = File::open(path)?;
            extract_descriptions(&mut f)
        })
        .collect::<Result<_, ArchError>>()?;

    let representative = &all_descs[0];
    let others = &all_descs[1..];

    let ifd_count = archetype.skeleton.ifd_count;
    let segments = build_segments_from_descriptions(ifd_count, representative, others);

    Ok(encode_segments(&segments))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use crate::similarity::archetype::{Archetype, IfdSkeleton, StructuralSkeleton};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_ifd_desc(ifd_index: usize, desc: Option<&str>) -> IfdDescBytes {
        IfdDescBytes {
            ifd_index,
            bytes: desc.map(|s| {
                let mut b = s.as_bytes().to_vec();
                b.push(0); // null terminator
                b
            }),
        }
    }

    fn gap_count(segs: &[ArchSegment]) -> usize {
        segs.iter().filter(|s| **s == ArchSegment::Gap).count()
    }

    fn shared_count(segs: &[ArchSegment]) -> usize {
        segs.iter()
            .filter(|s| matches!(s, ArchSegment::Shared(_)))
            .count()
    }

    // -----------------------------------------------------------------------
    // build_segments_from_descriptions tests
    // -----------------------------------------------------------------------

    #[test]
    fn single_member_all_bytes_become_one_shared_block() {
        let rep = vec![make_ifd_desc(0, Some("Aperio Image Library v11.2.1"))];
        let segs = build_segments_from_descriptions(1, &rep, &[]);
        assert_eq!(shared_count(&segs), 1);
        assert_eq!(gap_count(&segs), 0);
    }

    #[test]
    fn two_members_identical_description_is_one_shared_block() {
        let rep = vec![make_ifd_desc(0, Some("Aperio Image Library v11.2.1"))];
        let other = vec![vec![make_ifd_desc(0, Some("Aperio Image Library v11.2.1"))]];
        let segs = build_segments_from_descriptions(1, &rep, &other);
        assert_eq!(shared_count(&segs), 1);
        assert_eq!(gap_count(&segs), 0);
    }

    #[test]
    fn shared_prefix_then_diverge_yields_shared_and_gap() {
        // "Aperio|MPP = 0." is shared; the decimal digits differ.
        let rep = vec![make_ifd_desc(0, Some("Aperio|MPP = 0.4952"))];
        let other = vec![vec![make_ifd_desc(0, Some("Aperio|MPP = 0.5000"))]];
        let segs = build_segments_from_descriptions(1, &rep, &other);
        assert_eq!(shared_count(&segs), 1, "one shared prefix run expected");
        assert_eq!(gap_count(&segs), 1, "one trailing gap expected");
    }

    #[test]
    fn no_description_in_representative_yields_gap() {
        let rep = vec![make_ifd_desc(0, None)];
        let segs = build_segments_from_descriptions(1, &rep, &[]);
        assert_eq!(segs, [ArchSegment::Gap]);
    }

    #[test]
    fn completely_different_descriptions_yield_one_gap() {
        let rep = vec![make_ifd_desc(0, Some("AAAAAAAAAAAAAAAA"))];
        let other = vec![vec![make_ifd_desc(0, Some("BBBBBBBBBBBBBBBB"))]];
        let segs = build_segments_from_descriptions(1, &rep, &other);
        assert_eq!(segs, [ArchSegment::Gap]);
    }

    #[test]
    fn multiple_ifd_positions_processed_in_order() {
        let rep = vec![
            make_ifd_desc(0, Some("Aperio Image Library")),
            make_ifd_desc(1, None),
        ];
        let other = vec![vec![
            make_ifd_desc(0, Some("Aperio Image Library")),
            make_ifd_desc(1, None),
        ]];
        let segs = build_segments_from_descriptions(2, &rep, &other);
        assert_eq!(segs.len(), 2);
        assert!(matches!(&segs[0], ArchSegment::Shared(_)));
        assert_eq!(segs[1], ArchSegment::Gap);
    }

    #[test]
    fn shared_content_bytes_match_representative() {
        let desc = "Aperio Image Library v11.2.1";
        let rep = vec![make_ifd_desc(0, Some(desc))];
        let segs = build_segments_from_descriptions(1, &rep, &[]);
        if let ArchSegment::Shared(bytes) = &segs[0] {
            // The shared bytes include the null terminator added by the helper.
            let without_null = &bytes[..bytes.len() - 1];
            assert_eq!(without_null, desc.as_bytes());
        } else {
            panic!("expected Shared segment");
        }
    }

    // -----------------------------------------------------------------------
    // build_archetype_bytes: error path tests (no file I/O required)
    // -----------------------------------------------------------------------

    fn make_archetype(member_file_ids: Vec<usize>) -> Archetype {
        Archetype {
            id: 0,
            member_file_ids,
            skeleton: StructuralSkeleton {
                ifd_count: 1,
                per_ifd: vec![IfdSkeleton {
                    compression: None,
                    is_tiled: false,
                    tile_width: None,
                    tile_height: None,
                    role: None,
                }],
            },
            common_tokens: BTreeSet::new(),
            variable_tokens: BTreeSet::new(),
        }
    }

    #[test]
    fn no_members_returns_error() {
        let arch = make_archetype(vec![]);
        let result = build_archetype_bytes(&arch, &[]);
        assert!(matches!(result, Err(ArchError::NoMembers)));
    }
}
