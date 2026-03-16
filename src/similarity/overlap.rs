// File-level overlap accounting — Phase 3, Step 1.
//
// Translates Phase 2 confirmed unit matches into per-file totals and
// file-pair overlap counts that later stages use for similarity scoring.
//
// Two key questions this module answers:
//
//   1. How many confirmed bytes does file A share with file B?
//   2. How many confirmed groups contain units from both A and B?
//
// Counting rule: presence-based, not multiplicity-weighted.
//
//   For each confirmed group, the set of DISTINCT files is extracted first.
//   The group then contributes `payload_len` bytes to each unordered file
//   pair exactly once, regardless of how many units from each file are in
//   the group.
//
//   Rationale: if file A contains the same payload three times and file B
//   contains it once, the shared value is one payload-length, not three.
//   Counting multiplicities would overstate the similarity and is harder
//   to reason about.  This conservative rule can be revisited once the
//   prototype is working.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::fingerprint::manifest::UnitManifest;
use crate::fingerprint::similarity::ConfirmedGroup;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Per-file descriptor used throughout the similarity pipeline.
///
/// `eligible_bytes` and `eligible_units` count all data-unit payloads in the
/// manifest — tiles, strips, metadata blobs, and associated images.
/// Zero-length units are included (they contribute 0 to `eligible_bytes`).
/// Definitions are kept consistent across runs and explicit in code.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub file_id: usize,
    pub path: PathBuf,
    /// Sum of all data-unit payload lengths (all unit kinds, all IFDs).
    pub eligible_bytes: u64,
    /// Total number of data units in the manifest.
    pub eligible_units: u64,
}

/// Canonical unordered pair of distinct file IDs.
///
/// Always stored with `a <= b` so that `FilePair::new(x, y)` and
/// `FilePair::new(y, x)` produce the same key and map lookups are stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FilePair {
    pub a: usize,
    pub b: usize,
}

impl FilePair {
    /// Construct a canonical pair, ensuring `a <= b`.
    pub fn new(x: usize, y: usize) -> Self {
        if x <= y {
            Self { a: x, b: y }
        } else {
            Self { a: y, b: x }
        }
    }
}

/// Accumulated overlap between one file pair.
#[derive(Debug, Default, Clone)]
pub struct PairOverlap {
    /// Total confirmed payload bytes shared by this file pair.
    ///
    /// Each confirmed group contributes `payload_len` at most once per
    /// distinct file pair (presence-based; see module docstring).
    pub shared_bytes: u64,
    /// Number of confirmed groups in which both files appear.
    pub shared_unit_pairs: u64,
}

// ---------------------------------------------------------------------------
// Build file entries
// ---------------------------------------------------------------------------

/// Derive a `FileEntry` for each manifest, preserving the manifest's
/// `file_id` and computing eligible-byte totals.
///
/// Manifests should be provided in a deterministic order (e.g. sorted by
/// path) before calling this function so that `FileEntry` ordering is stable
/// across runs.
///
/// All unit kinds (Tile, Strip, MetadataBlob, AssociatedImage) contribute to
/// `eligible_bytes`.  Zero-length units are counted in `eligible_units` but
/// add 0 to `eligible_bytes`.
pub fn build_file_entries(manifests: &[UnitManifest]) -> Vec<FileEntry> {
    manifests
        .iter()
        .map(|m| {
            // Sum all payload lengths regardless of unit kind.
            let eligible_bytes: u64 = m.units.iter().map(|u| u.length).sum();
            let eligible_units: u64 = m.units.len() as u64;
            FileEntry {
                file_id: m.file_id,
                path: m.path.clone(),
                eligible_bytes,
                eligible_units,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Accumulate pair overlaps
// ---------------------------------------------------------------------------

/// Extract the set of distinct file IDs represented in a confirmed group.
///
/// Returns a sorted `Vec` of unique `file_id` values.
fn distinct_files_in_group(group: &ConfirmedGroup) -> Vec<usize> {
    let mut ids: BTreeSet<usize> = BTreeSet::new();
    for u in &group.units {
        ids.insert(u.file_id);
    }
    ids.into_iter().collect()
}

/// Convert confirmed match groups into pairwise file-overlap accounting.
///
/// For each confirmed group, the distinct files present in that group are
/// enumerated, and every unordered pair of those files receives:
///
/// - `shared_bytes  += group.key.length`   (the payload length of the group)
/// - `shared_unit_pairs += 1`
///
/// Groups whose units all belong to a single file (within-file duplicates)
/// generate no file pairs and are silently skipped.
///
/// The returned `BTreeMap` uses `FilePair` as the key, so iteration order
/// over pairs is deterministic.
pub fn accumulate_pair_overlaps(
    confirmed_groups: &[ConfirmedGroup],
) -> BTreeMap<FilePair, PairOverlap> {
    let mut overlaps: BTreeMap<FilePair, PairOverlap> = BTreeMap::new();

    for group in confirmed_groups {
        let payload_len = group.key.length;
        let files = distinct_files_in_group(group);

        // Enumerate all unordered pairs of distinct files.
        for i in 0..files.len() {
            for j in (i + 1)..files.len() {
                let pair = FilePair::new(files[i], files[j]);
                let entry = overlaps.entry(pair).or_default();
                entry.shared_bytes += payload_len;
                entry.shared_unit_pairs += 1;
            }
        }
    }

    overlaps
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fingerprint::similarity::{CandidateKey, CandidateUnit, ConfirmedGroup};
    use crate::fingerprint::manifest::{UnitManifest, UnitRecord};
    use crate::svs::layout::DataUnitKind;
    use std::path::PathBuf;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_manifest(file_id: usize, unit_lengths: &[u64]) -> UnitManifest {
        let units = unit_lengths
            .iter()
            .enumerate()
            .map(|(i, &len)| UnitRecord {
                ifd_index: 0,
                unit_index: i,
                kind: DataUnitKind::Tile,
                offset: i as u64 * 4096,
                length: len,
                compression: Some(7),
                role: None,
                coarse_fp: Some(0xDEAD),
                strong_hash: None,
            })
            .collect();
        UnitManifest {
            path: PathBuf::from(format!("file{file_id}.svs")),
            file_id,
            units,
        }
    }

    fn make_group(payload_len: u64, file_ids: &[usize]) -> ConfirmedGroup {
        let units = file_ids
            .iter()
            .enumerate()
            .map(|(i, &fid)| CandidateUnit {
                file_id: fid,
                ifd_index: 0,
                unit_index: i,
                offset: 0,
            })
            .collect();
        ConfirmedGroup {
            key: CandidateKey {
                kind: DataUnitKind::Tile,
                compression: Some(7),
                length: payload_len,
                coarse_fp: 0xCAFE,
            },
            strong_hash: [0u8; 32],
            units,
        }
    }

    // ── build_file_entries ───────────────────────────────────────────────────

    #[test]
    fn file_entries_empty_input() {
        let entries = build_file_entries(&[]);
        assert!(entries.is_empty());
    }

    #[test]
    fn file_entries_preserves_file_id_and_path() {
        let m = make_manifest(3, &[1024, 2048]);
        let entries = build_file_entries(&[m]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_id, 3);
        assert_eq!(entries[0].path, PathBuf::from("file3.svs"));
    }

    #[test]
    fn file_entries_eligible_bytes_sums_all_unit_lengths() {
        let m = make_manifest(0, &[1024, 2048, 512]);
        let entries = build_file_entries(&[m]);
        assert_eq!(entries[0].eligible_bytes, 1024 + 2048 + 512);
        assert_eq!(entries[0].eligible_units, 3);
    }

    #[test]
    fn file_entries_zero_length_units_counted_in_units_not_bytes() {
        let m = make_manifest(0, &[0, 512, 0]);
        let entries = build_file_entries(&[m]);
        assert_eq!(entries[0].eligible_bytes, 512);
        assert_eq!(entries[0].eligible_units, 3);
    }

    #[test]
    fn file_entries_empty_manifest_yields_zero_totals() {
        let m = make_manifest(0, &[]);
        let entries = build_file_entries(&[m]);
        assert_eq!(entries[0].eligible_bytes, 0);
        assert_eq!(entries[0].eligible_units, 0);
    }

    // ── accumulate_pair_overlaps ─────────────────────────────────────────────

    #[test]
    fn pair_overlaps_empty_groups_yields_empty_map() {
        let overlaps = accumulate_pair_overlaps(&[]);
        assert!(overlaps.is_empty());
    }

    #[test]
    fn pair_overlaps_single_file_group_yields_no_pairs() {
        // Group with units from only one file — no cross-file pair.
        let group = make_group(4096, &[0, 0]);
        let overlaps = accumulate_pair_overlaps(&[group]);
        assert!(overlaps.is_empty(), "within-file duplicates should not produce a file pair");
    }

    #[test]
    fn pair_overlaps_two_file_group_produces_one_pair() {
        let group = make_group(4096, &[0, 1]);
        let overlaps = accumulate_pair_overlaps(&[group]);
        assert_eq!(overlaps.len(), 1);
        let pair = FilePair::new(0, 1);
        let ov = &overlaps[&pair];
        assert_eq!(ov.shared_bytes, 4096);
        assert_eq!(ov.shared_unit_pairs, 1);
    }

    #[test]
    fn pair_overlaps_three_file_group_produces_three_pairs() {
        // Group with files A=0, B=1, C=2 → pairs (0,1), (0,2), (1,2).
        let group = make_group(1000, &[0, 1, 2]);
        let overlaps = accumulate_pair_overlaps(&[group]);
        assert_eq!(overlaps.len(), 3);

        for &(a, b) in &[(0, 1), (0, 2), (1, 2)] {
            let pair = FilePair::new(a, b);
            assert_eq!(
                overlaps[&pair].shared_bytes, 1000,
                "pair ({a},{b}) should have 1000 shared bytes"
            );
            assert_eq!(overlaps[&pair].shared_unit_pairs, 1);
        }
    }

    #[test]
    fn pair_overlaps_multiplicity_within_file_does_not_inflate_bytes() {
        // File 0 has three units, file 1 has one — all in the same group.
        // Presence-based rule: contribute payload_len once per distinct pair.
        let group = make_group(512, &[0, 0, 0, 1]);
        let overlaps = accumulate_pair_overlaps(&[group]);
        assert_eq!(overlaps.len(), 1);
        let pair = FilePair::new(0, 1);
        // Should be 512 × 1 (one distinct pair), not 512 × 3.
        assert_eq!(overlaps[&pair].shared_bytes, 512);
        assert_eq!(overlaps[&pair].shared_unit_pairs, 1);
    }

    #[test]
    fn pair_overlaps_multiple_groups_accumulate_correctly() {
        // Two groups both involving files 0 and 1.
        let g1 = make_group(1024, &[0, 1]);
        let g2 = make_group(2048, &[0, 1]);
        let overlaps = accumulate_pair_overlaps(&[g1, g2]);
        assert_eq!(overlaps.len(), 1);
        let pair = FilePair::new(0, 1);
        assert_eq!(overlaps[&pair].shared_bytes, 1024 + 2048);
        assert_eq!(overlaps[&pair].shared_unit_pairs, 2);
    }

    #[test]
    fn pair_overlaps_independent_pairs_do_not_interfere() {
        // Groups for (0,1) and (2,3) are separate — should produce two entries.
        let g1 = make_group(100, &[0, 1]);
        let g2 = make_group(200, &[2, 3]);
        let overlaps = accumulate_pair_overlaps(&[g1, g2]);
        assert_eq!(overlaps.len(), 2);
        assert_eq!(overlaps[&FilePair::new(0, 1)].shared_bytes, 100);
        assert_eq!(overlaps[&FilePair::new(2, 3)].shared_bytes, 200);
    }

    // ── FilePair canonicalization ────────────────────────────────────────────

    #[test]
    fn file_pair_canonical_ordering() {
        let p1 = FilePair::new(3, 1);
        let p2 = FilePair::new(1, 3);
        assert_eq!(p1, p2);
        assert_eq!(p1.a, 1);
        assert_eq!(p1.b, 3);
    }

    #[test]
    fn file_pair_equal_ids_yields_same_pair() {
        // Degenerate case: same file id on both sides.  Not used for overlap
        // (distinct_files_in_group prevents it), but the type should not panic.
        let p = FilePair::new(5, 5);
        assert_eq!(p.a, 5);
        assert_eq!(p.b, 5);
    }
}
