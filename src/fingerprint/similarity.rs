// Cross-file candidate matching.
//
// Groups data units from multiple manifests by conservative structural keys
// (kind, compression, length, coarse_fp).  Units that share all four fields
// are *candidate* duplicates — plausible matches that have not yet been
// confirmed by full-payload comparison.
//
// This is Step 4 of Phase 2.  Exact confirmation (Step 5) runs only on the
// candidate groups produced here.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use rayon::prelude::*;

use crate::fingerprint::hash::{hash_unit, HashError};
use crate::fingerprint::manifest::UnitManifest;
use crate::svs::layout::{DataUnit, DataUnitKind};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The four fields that must agree for two units to be considered candidates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CandidateKey {
    pub kind: DataUnitKind,
    pub compression: Option<u16>,
    pub length: u64,
    pub coarse_fp: u64,
}

/// A single unit that belongs to a candidate group.
#[derive(Debug, Clone)]
pub struct CandidateUnit {
    pub file_id: usize,
    pub ifd_index: usize,
    pub unit_index: usize,
    pub offset: u64,
}

/// A set of units — potentially spanning multiple files — that share the same
/// `CandidateKey`.
#[derive(Debug)]
pub struct CandidateGroup {
    pub key: CandidateKey,
    /// All units with this key, from any file.
    pub units: Vec<CandidateUnit>,
}

impl CandidateGroup {
    /// True when units in this group come from at least two distinct files.
    pub fn is_cross_file(&self) -> bool {
        let first = self.units[0].file_id;
        self.units.iter().any(|u| u.file_id != first)
    }
}

/// Candidate overlap statistics for one file.
#[derive(Debug)]
pub struct PerFileOverlap {
    pub file_id: usize,
    pub path: PathBuf,
    pub total_units: usize,
    pub total_bytes: u64,
    /// Units that appear in a cross-file candidate group (could be stored as
    /// references to another file's unit).
    pub candidate_units: usize,
    pub candidate_bytes: u64,
}

/// Top-level result of the candidate-matching pass.
#[derive(Debug)]
pub struct CandidateReport {
    /// All groups with two or more units (same-file or cross-file).
    pub groups: Vec<CandidateGroup>,
    /// Number of groups where units come from at least two files.
    pub cross_file_groups: usize,
    /// Total units across all groups (sum of group sizes).
    pub total_candidate_units: usize,
    /// Upper-bound savings estimate: for each group, `length × (count − 1)`.
    /// Represents bytes that could be replaced by references if all candidates
    /// are confirmed exact matches.
    pub candidate_reusable_bytes: u64,
    pub per_file: Vec<PerFileOverlap>,
}

// ---------------------------------------------------------------------------
// Core function
// ---------------------------------------------------------------------------

/// Group units from `manifests` into candidate sets using conservative keys.
///
/// Units without a `coarse_fp` are skipped — they cannot be matched.
/// A group must contain at least two units to appear in the report; singletons
/// are discarded.
pub fn find_candidates(manifests: &[UnitManifest]) -> CandidateReport {
    // ── group by key ────────────────────────────────────────────────────────
    let mut map: HashMap<CandidateKey, Vec<CandidateUnit>> = HashMap::new();

    for manifest in manifests {
        for unit in &manifest.units {
            let coarse_fp = match unit.coarse_fp {
                Some(fp) => fp,
                None => continue,
            };
            let key = CandidateKey {
                kind: unit.kind,
                compression: unit.compression,
                length: unit.length,
                coarse_fp,
            };
            map.entry(key).or_default().push(CandidateUnit {
                file_id: manifest.file_id,
                ifd_index: unit.ifd_index,
                unit_index: unit.unit_index,
                offset: unit.offset,
            });
        }
    }

    // ── discard singletons ──────────────────────────────────────────────────
    let mut groups: Vec<CandidateGroup> = map
        .into_iter()
        .filter(|(_, units)| units.len() >= 2)
        .map(|(key, units)| CandidateGroup { key, units })
        .collect();

    // Deterministic ordering: sort by (kind discriminant, length desc, coarse_fp).
    groups.sort_by_key(|g| {
        let kd: u8 = match g.key.kind {
            DataUnitKind::Tile => 0,
            DataUnitKind::Strip => 1,
            DataUnitKind::MetadataBlob => 2,
            DataUnitKind::AssociatedImage => 3,
        };
        (kd, std::cmp::Reverse(g.key.length), g.key.coarse_fp)
    });

    // ── identify cross-file candidates ──────────────────────────────────────
    // Build a set of (file_id, ifd_index, unit_index) for units that are in a
    // cross-file group.  These are the units whose bytes could be served by a
    // reference to another file.
    let mut cross_set: HashSet<(usize, usize, usize)> = HashSet::new();
    let mut cross_file_groups: usize = 0;

    for group in &groups {
        if group.is_cross_file() {
            cross_file_groups += 1;
            for u in &group.units {
                cross_set.insert((u.file_id, u.ifd_index, u.unit_index));
            }
        }
    }

    // ── aggregate totals ────────────────────────────────────────────────────
    let total_candidate_units: usize = groups.iter().map(|g| g.units.len()).sum();

    // Reusable bytes = bytes that could be replaced by references.
    // For a group of N identical units with payload length L:
    //   1 copy is kept as the base; N-1 could become references.
    let candidate_reusable_bytes: u64 = groups
        .iter()
        .map(|g| g.key.length * (g.units.len() as u64 - 1))
        .sum();

    // ── per-file overlap ────────────────────────────────────────────────────
    let per_file: Vec<PerFileOverlap> = manifests
        .iter()
        .map(|m| {
            let total_bytes: u64 = m.units.iter().map(|u| u.length).sum();
            let mut candidate_units = 0usize;
            let mut candidate_bytes = 0u64;

            for unit in &m.units {
                if cross_set.contains(&(m.file_id, unit.ifd_index, unit.unit_index)) {
                    candidate_units += 1;
                    candidate_bytes += unit.length;
                }
            }

            PerFileOverlap {
                file_id: m.file_id,
                path: m.path.clone(),
                total_units: m.units.len(),
                total_bytes,
                candidate_units,
                candidate_bytes,
            }
        })
        .collect();

    CandidateReport {
        cross_file_groups,
        total_candidate_units,
        candidate_reusable_bytes,
        groups,
        per_file,
    }
}

// ============================================================================
// Step 5: Exact confirmation
// ============================================================================

/// Error from the exact confirmation pass.
#[derive(Debug)]
pub enum ConfirmError {
    Io(std::io::Error),
    /// A `file_id` referenced by the candidate report has no matching entry in
    /// the supplied manifests.
    UnknownFile(usize),
    Hash(HashError),
}

impl From<std::io::Error> for ConfirmError {
    fn from(e: std::io::Error) -> Self {
        ConfirmError::Io(e)
    }
}

impl From<HashError> for ConfirmError {
    fn from(e: HashError) -> Self {
        ConfirmError::Hash(e)
    }
}

impl std::fmt::Display for ConfirmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfirmError::Io(e) => write!(f, "I/O error: {e}"),
            ConfirmError::UnknownFile(id) => write!(f, "unknown file_id: {id}"),
            ConfirmError::Hash(e) => write!(f, "hash error: {e}"),
        }
    }
}

/// A set of units confirmed to be byte-identical via SHA-256.
#[derive(Debug)]
pub struct ConfirmedGroup {
    pub key: CandidateKey,
    pub strong_hash: [u8; 32],
    pub units: Vec<CandidateUnit>,
}

impl ConfirmedGroup {
    pub fn is_cross_file(&self) -> bool {
        let first = self.units[0].file_id;
        self.units.iter().any(|u| u.file_id != first)
    }
}

/// Confirmed overlap statistics for one file.
#[derive(Debug)]
pub struct PerFileConfirmed {
    pub file_id: usize,
    pub path: PathBuf,
    /// Units that appear in a confirmed cross-file group.
    pub confirmed_units: usize,
    pub confirmed_bytes: u64,
}

/// Result of the exact confirmation pass.
#[derive(Debug)]
pub struct ConfirmedReport {
    pub groups: Vec<ConfirmedGroup>,
    /// Groups whose units come from at least two distinct files.
    pub cross_file_groups: usize,
    /// Sum of all confirmed group sizes.
    pub total_confirmed_units: usize,
    /// Upper-bound savings: `length × (count − 1)` per group.
    pub confirmed_reusable_bytes: u64,
    /// Candidate groups that yielded no confirmed sub-group — every unit hashed
    /// to a unique value (coarse-fingerprint collision).
    pub false_positive_groups: usize,
    pub per_file: Vec<PerFileConfirmed>,
}

/// Confirm exact byte identity for every candidate group in `report`.
///
/// Each source file is opened at most once.  For each candidate group, units
/// are hashed with SHA-256 and then sub-grouped by digest.  Sub-groups with
/// fewer than two units are discarded.  The returned `ConfirmedReport`
/// distinguishes confirmed exact matches from coarse-fingerprint false
/// positives.
pub fn confirm_candidates(
    report: &CandidateReport,
    manifests: &[UnitManifest],
) -> Result<ConfirmedReport, ConfirmError> {
    // ── build file_id → path map ────────────────────────────────────────────
    let path_map: HashMap<usize, &std::path::Path> = manifests
        .iter()
        .map(|m| (m.file_id, m.path.as_path()))
        .collect();

    // ── collect hash requests, batched by file ──────────────────────────────
    // For each unit in a candidate group we record enough to reconstruct a
    // DataUnit (needed by hash_unit) and to map the result back to its group.
    struct HashReq {
        group_idx: usize,
        unit_pos: usize,
        offset: u64,
        length: u64,
        kind: DataUnitKind,
        ifd_index: usize,
        unit_index: usize,
    }

    let mut by_file: HashMap<usize, Vec<HashReq>> = HashMap::new();
    for (gi, group) in report.groups.iter().enumerate() {
        for (ui, unit) in group.units.iter().enumerate() {
            by_file.entry(unit.file_id).or_default().push(HashReq {
                group_idx: gi,
                unit_pos: ui,
                offset: unit.offset,
                length: group.key.length,
                kind: group.key.kind,
                ifd_index: unit.ifd_index,
                unit_index: unit.unit_index,
            });
        }
    }

    // ── hash every requested unit (parallel per file) ───────────────────────
    // Each file is opened and hashed independently, so we process files in
    // parallel with rayon.  Results are flattened into a single lookup map.
    let hashes: HashMap<(usize, usize), [u8; 32]> = by_file
        .par_iter()
        .map(|(file_id, reqs)| -> Result<Vec<_>, ConfirmError> {
            let path = path_map
                .get(file_id)
                .ok_or(ConfirmError::UnknownFile(*file_id))?;

            let mut reader = std::fs::File::open(path)?;
            let file_len = reader.metadata()?.len();

            let mut entries = Vec::with_capacity(reqs.len());
            for req in reqs {
                let unit = DataUnit {
                    kind: req.kind,
                    offset: req.offset,
                    length: req.length,
                    ifd_index: req.ifd_index,
                    unit_index: req.unit_index,
                    strong_hash: None,
                };
                let digest = hash_unit(&mut reader, &unit, file_len)?;
                entries.push(((req.group_idx, req.unit_pos), digest));
            }
            Ok(entries)
        })
        .collect::<Result<Vec<_>, ConfirmError>>()?
        .into_iter()
        .flatten()
        .collect();

    // ── sub-group each candidate group by digest ────────────────────────────
    let mut confirmed: Vec<ConfirmedGroup> = Vec::new();
    let mut false_positive_groups: usize = 0;

    for (gi, group) in report.groups.iter().enumerate() {
        let mut by_hash: HashMap<[u8; 32], Vec<CandidateUnit>> = HashMap::new();
        for (ui, unit) in group.units.iter().enumerate() {
            if let Some(&digest) = hashes.get(&(gi, ui)) {
                by_hash.entry(digest).or_default().push(unit.clone());
            }
        }

        let before = confirmed.len();
        for (strong_hash, units) in by_hash {
            if units.len() >= 2 {
                confirmed.push(ConfirmedGroup {
                    key: group.key.clone(),
                    strong_hash,
                    units,
                });
            }
        }
        if confirmed.len() == before {
            false_positive_groups += 1;
        }
    }

    // Deterministic order.
    confirmed.sort_by_key(|g| {
        let kd: u8 = match g.key.kind {
            DataUnitKind::Tile => 0,
            DataUnitKind::Strip => 1,
            DataUnitKind::MetadataBlob => 2,
            DataUnitKind::AssociatedImage => 3,
        };
        (kd, std::cmp::Reverse(g.key.length), g.strong_hash)
    });

    // ── aggregate stats ─────────────────────────────────────────────────────
    let mut cross_file_set: HashSet<(usize, usize, usize)> = HashSet::new();
    let mut cross_file_groups = 0usize;
    for g in &confirmed {
        if g.is_cross_file() {
            cross_file_groups += 1;
            for u in &g.units {
                cross_file_set.insert((u.file_id, u.ifd_index, u.unit_index));
            }
        }
    }

    let total_confirmed_units: usize = confirmed.iter().map(|g| g.units.len()).sum();
    let confirmed_reusable_bytes: u64 = confirmed
        .iter()
        .map(|g| g.key.length * (g.units.len() as u64 - 1))
        .sum();

    let per_file: Vec<PerFileConfirmed> = manifests
        .iter()
        .map(|m| {
            let mut confirmed_units = 0usize;
            let mut confirmed_bytes = 0u64;
            for unit in &m.units {
                if cross_file_set.contains(&(m.file_id, unit.ifd_index, unit.unit_index)) {
                    confirmed_units += 1;
                    confirmed_bytes += unit.length;
                }
            }
            PerFileConfirmed {
                file_id: m.file_id,
                path: m.path.clone(),
                confirmed_units,
                confirmed_bytes,
            }
        })
        .collect();

    Ok(ConfirmedReport {
        groups: confirmed,
        cross_file_groups,
        total_confirmed_units,
        confirmed_reusable_bytes,
        false_positive_groups,
        per_file,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fingerprint::manifest::{UnitManifest, UnitRecord};
    use crate::svs::layout::DataUnitKind;
    use std::path::PathBuf;

    fn make_record(
        ifd_index: usize,
        unit_index: usize,
        kind: DataUnitKind,
        length: u64,
        compression: Option<u16>,
        coarse_fp: Option<u64>,
    ) -> UnitRecord {
        UnitRecord {
            ifd_index,
            unit_index,
            kind,
            offset: 0,
            length,
            compression,
            role: None,
            coarse_fp,
            strong_hash: None,
        }
    }

    fn make_manifest(file_id: usize, units: Vec<UnitRecord>) -> UnitManifest {
        UnitManifest {
            path: PathBuf::from(format!("file{file_id}.svs")),
            file_id,
            units,
        }
    }

    #[test]
    fn empty_input_produces_empty_report() {
        let report = find_candidates(&[]);
        assert_eq!(report.groups.len(), 0);
        assert_eq!(report.cross_file_groups, 0);
        assert_eq!(report.total_candidate_units, 0);
        assert_eq!(report.candidate_reusable_bytes, 0);
        assert_eq!(report.per_file.len(), 0);
    }

    #[test]
    fn single_file_no_within_file_duplicates_produces_no_groups() {
        let m = make_manifest(0, vec![
            make_record(0, 0, DataUnitKind::Tile, 1024, Some(7), Some(0xABCD)),
            make_record(0, 1, DataUnitKind::Tile, 1024, Some(7), Some(0x1234)),
        ]);
        let report = find_candidates(&[m]);
        assert_eq!(report.groups.len(), 0);
    }

    #[test]
    fn single_file_within_file_duplicates_form_a_group() {
        // Two units in the same file with identical keys.
        let m = make_manifest(0, vec![
            make_record(0, 0, DataUnitKind::Tile, 512, Some(7), Some(0xBEEF)),
            make_record(0, 1, DataUnitKind::Tile, 512, Some(7), Some(0xBEEF)),
        ]);
        let report = find_candidates(&[m]);
        assert_eq!(report.groups.len(), 1);
        assert_eq!(report.groups[0].units.len(), 2);
        // Same-file group is not cross-file.
        assert_eq!(report.cross_file_groups, 0);
        // Per-file candidate_units should be 0 (not cross-file).
        assert_eq!(report.per_file[0].candidate_units, 0);
    }

    #[test]
    fn two_files_with_matching_unit_form_cross_file_group() {
        let unit = make_record(0, 0, DataUnitKind::Tile, 2048, Some(7), Some(0xCAFE));
        let m0 = make_manifest(0, vec![unit.clone()]);
        let m1 = make_manifest(1, vec![unit]);
        let report = find_candidates(&[m0, m1]);

        assert_eq!(report.groups.len(), 1);
        assert_eq!(report.cross_file_groups, 1);
        assert_eq!(report.groups[0].units.len(), 2);
        assert!(report.groups[0].is_cross_file());

        // Reusable bytes = length × (count - 1) = 2048 × 1 = 2048.
        assert_eq!(report.candidate_reusable_bytes, 2048);

        // Both files should report candidate overlap.
        for pf in &report.per_file {
            assert_eq!(pf.candidate_units, 1);
            assert_eq!(pf.candidate_bytes, 2048);
        }
    }

    #[test]
    fn key_difference_in_compression_prevents_grouping() {
        let m0 = make_manifest(0, vec![
            make_record(0, 0, DataUnitKind::Tile, 512, Some(7), Some(0xDEAD)),
        ]);
        let m1 = make_manifest(1, vec![
            make_record(0, 0, DataUnitKind::Tile, 512, Some(1), Some(0xDEAD)),
        ]);
        let report = find_candidates(&[m0, m1]);
        assert_eq!(report.groups.len(), 0);
    }

    #[test]
    fn key_difference_in_length_prevents_grouping() {
        let m0 = make_manifest(0, vec![
            make_record(0, 0, DataUnitKind::Tile, 512, Some(7), Some(0x1111)),
        ]);
        let m1 = make_manifest(1, vec![
            make_record(0, 0, DataUnitKind::Tile, 1024, Some(7), Some(0x1111)),
        ]);
        let report = find_candidates(&[m0, m1]);
        assert_eq!(report.groups.len(), 0);
    }

    #[test]
    fn units_without_coarse_fp_are_excluded() {
        let m0 = make_manifest(0, vec![
            make_record(0, 0, DataUnitKind::Tile, 256, None, None),
        ]);
        let m1 = make_manifest(1, vec![
            make_record(0, 0, DataUnitKind::Tile, 256, None, None),
        ]);
        let report = find_candidates(&[m0, m1]);
        assert_eq!(report.groups.len(), 0);
    }

    #[test]
    fn reusable_bytes_formula_is_count_minus_one_times_length() {
        // 3 identical units across 3 files.
        let unit = make_record(0, 0, DataUnitKind::Tile, 4096, Some(7), Some(0xF00D));
        let manifests: Vec<UnitManifest> = (0..3)
            .map(|i| make_manifest(i, vec![unit.clone()]))
            .collect();
        let report = find_candidates(&manifests);

        assert_eq!(report.groups.len(), 1);
        // 4096 × (3 - 1) = 8192
        assert_eq!(report.candidate_reusable_bytes, 8192);
    }

    #[test]
    fn per_file_totals_are_computed_correctly() {
        let shared = make_record(0, 0, DataUnitKind::Tile, 1024, Some(7), Some(0xAAAA));
        let unique0 = make_record(0, 1, DataUnitKind::Tile, 512, Some(7), Some(0x0001));
        let unique1 = make_record(0, 1, DataUnitKind::Tile, 512, Some(7), Some(0x0002));

        let m0 = make_manifest(0, vec![shared.clone(), unique0]);
        let m1 = make_manifest(1, vec![shared, unique1]);
        let report = find_candidates(&[m0, m1]);

        for pf in &report.per_file {
            assert_eq!(pf.total_units, 2);
            assert_eq!(pf.total_bytes, 1536);   // 1024 + 512
            assert_eq!(pf.candidate_units, 1);  // only the shared unit
            assert_eq!(pf.candidate_bytes, 1024);
        }
    }

    // ── Step 5: confirm_candidates ───────────────────────────────────────────

    /// Write `data` to a uniquely-named temp file and return its path.
    /// Each call uses the thread id + a per-call counter to avoid collisions
    /// when tests run in parallel.
    fn write_temp(tag: &str, data: &[u8]) -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .join(format!("dcc_sim_test_{tag}_{n}.bin"));
        std::fs::write(&path, data).unwrap();
        path
    }

    /// Build a manifest by writing `data` to a temp file, then reading it back.
    /// Only used for confirmation tests that need real file paths.
    fn manifest_from_bytes(file_id: usize, data: &[u8], tag: &str) -> (UnitManifest, PathBuf) {
        // We need a real TIFF to get units out of the parser.  Reuse the same
        // synthetic tiled TIFF from the manifest tests, but splice `data` in as
        // the tile payload so we can control what gets hashed.
        //
        // For confirmation tests we bypass the TIFF parser entirely: we
        // construct the manifest directly, pointing at known offsets in a raw
        // binary blob.  The confirmation pass only cares about the path and the
        // (offset, length) pairs stored in the manifest.

        let path = write_temp(tag, data);

        // Construct a single-unit manifest pointing at the entire blob.
        let record = UnitRecord {
            ifd_index: 0,
            unit_index: 0,
            kind: DataUnitKind::Tile,
            offset: 0,
            length: data.len() as u64,
            compression: Some(7),
            role: None,
            coarse_fp: Some(0xDEAD),   // fixed — we control candidate grouping manually
            strong_hash: None,
        };
        let manifest = UnitManifest { path: path.clone(), file_id, units: vec![record] };
        (manifest, path)
    }

    fn make_candidate_report_for(manifests: &[UnitManifest]) -> CandidateReport {
        find_candidates(manifests)
    }

    #[test]
    fn confirmed_empty_when_no_candidates() {
        let report = CandidateReport {
            groups: vec![],
            cross_file_groups: 0,
            total_candidate_units: 0,
            candidate_reusable_bytes: 0,
            per_file: vec![],
        };
        let confirmed = confirm_candidates(&report, &[]).unwrap();
        assert_eq!(confirmed.groups.len(), 0);
        assert_eq!(confirmed.false_positive_groups, 0);
        assert_eq!(confirmed.total_confirmed_units, 0);
        assert_eq!(confirmed.confirmed_reusable_bytes, 0);
    }

    #[test]
    fn identical_payloads_produce_confirmed_group() {
        let data = vec![0xAB; 128];
        let (m0, _p0) = manifest_from_bytes(0, &data, "identical_a");
        let (m1, _p1) = manifest_from_bytes(1, &data, "identical_b");

        let candidates = make_candidate_report_for(&[m0.clone(), m1.clone()]);
        assert_eq!(candidates.groups.len(), 1, "prerequisite: one candidate group");

        let confirmed = confirm_candidates(&candidates, &[m0, m1]).unwrap();
        assert_eq!(confirmed.groups.len(), 1);
        assert_eq!(confirmed.cross_file_groups, 1);
        assert_eq!(confirmed.false_positive_groups, 0);
        assert_eq!(confirmed.groups[0].units.len(), 2);
        assert_eq!(confirmed.confirmed_reusable_bytes, 128); // 128 × (2-1)
    }

    #[test]
    fn different_payloads_with_same_coarse_key_become_false_positive() {
        // Manually construct two manifests whose records share kind/compression/
        // length/coarse_fp but whose file bytes differ — simulating a coarse
        // fingerprint collision.
        let data_a = vec![0xAA; 64];
        let data_b = vec![0xBB; 64];

        let path_a = write_temp("fp_collision_a", &data_a);
        let path_b = write_temp("fp_collision_b", &data_b);

        let make = |file_id: usize, path: PathBuf| UnitManifest {
            path,
            file_id,
            units: vec![UnitRecord {
                ifd_index: 0,
                unit_index: 0,
                kind: DataUnitKind::Tile,
                offset: 0,
                length: 64,
                compression: Some(7),
                role: None,
                coarse_fp: Some(0xCAFE),  // same coarse_fp → candidate group forms
                strong_hash: None,
            }],
        };

        let m0 = make(0, path_a);
        let m1 = make(1, path_b);

        let candidates = find_candidates(&[m0.clone(), m1.clone()]);
        assert_eq!(candidates.groups.len(), 1, "prerequisite: one candidate group");

        let confirmed = confirm_candidates(&candidates, &[m0, m1]).unwrap();
        assert_eq!(confirmed.groups.len(), 0,
            "different bytes must not produce a confirmed group");
        assert_eq!(confirmed.false_positive_groups, 1);
        assert_eq!(confirmed.confirmed_reusable_bytes, 0);
    }

    #[test]
    fn candidate_group_splits_into_two_confirmed_subgroups() {
        // Three units: two share bytes_a, one has bytes_b.
        // All have the same coarse key so they form one candidate group,
        // which then splits into one confirmed group of 2 + one false positive.
        let bytes_a = vec![0x11u8; 32];
        let bytes_b = vec![0x22u8; 32];

        let path_a0 = write_temp("split_a0", &bytes_a);
        let path_a1 = write_temp("split_a1", &bytes_a);
        let path_b = write_temp("split_b", &bytes_b);

        let make = |file_id: usize, path: PathBuf| UnitManifest {
            path,
            file_id,
            units: vec![UnitRecord {
                ifd_index: 0,
                unit_index: 0,
                kind: DataUnitKind::Tile,
                offset: 0,
                length: 32,
                compression: None,
                role: None,
                coarse_fp: Some(0xF00D),
                strong_hash: None,
            }],
        };

        let m0 = make(0, path_a0);
        let m1 = make(1, path_a1);
        let m2 = make(2, path_b);

        let candidates = find_candidates(&[m0.clone(), m1.clone(), m2.clone()]);
        assert_eq!(candidates.groups.len(), 1, "prerequisite: one candidate group of 3");

        let confirmed = confirm_candidates(&candidates, &[m0, m1, m2]).unwrap();

        // One confirmed group (the two 0x11 units) and the 0x22 unit is a
        // false positive — but since the group as a whole produced one
        // confirmed sub-group, false_positive_groups is 0.
        assert_eq!(confirmed.groups.len(), 1);
        assert_eq!(confirmed.groups[0].units.len(), 2);
        assert_eq!(confirmed.false_positive_groups, 0);
        assert_eq!(confirmed.confirmed_reusable_bytes, 32);
    }

    #[test]
    fn per_file_confirmed_stats_are_correct() {
        let data = vec![0x55u8; 256];
        let (m0, _) = manifest_from_bytes(0, &data, "pf_conf_a");
        let (m1, _) = manifest_from_bytes(1, &data, "pf_conf_b");

        let candidates = find_candidates(&[m0.clone(), m1.clone()]);
        let confirmed = confirm_candidates(&candidates, &[m0, m1]).unwrap();

        for pf in &confirmed.per_file {
            assert_eq!(pf.confirmed_units, 1);
            assert_eq!(pf.confirmed_bytes, 256);
        }
    }
}
