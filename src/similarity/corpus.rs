// Corpus-level structural statistics across a set of FileProfiles.
//
// Answers questions like:
//   - What is the most common IFD count?
//   - What widths/heights appear at each IFD position?
//   - Which description tokens appear in most files?
//   - What is the spread of file sizes?
//
// All statistics are derived purely from FileProfile values — no additional
// I/O is required.

use std::collections::BTreeMap;

use crate::similarity::profile::FileProfile;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Frequency distribution of observed values for one field.
///
/// `BTreeMap` gives deterministic iteration order (ascending key) which is
/// desirable for consistent report output.
pub type FreqMap<K> = BTreeMap<K, usize>;

/// Per-IFD-position statistics, aggregated across all files that have an IFD
/// at that position.
#[derive(Debug)]
pub struct IfdPositionStats {
    /// Zero-based IFD position in the chain.
    pub position: usize,
    /// Number of files that have an IFD at this position.
    pub file_count: usize,
    /// Image width → count.
    pub width_dist: FreqMap<u32>,
    /// Image height → count.
    pub height_dist: FreqMap<u32>,
    /// Compression tag value (or `None` if absent) → count.
    pub compression_dist: FreqMap<Option<u16>>,
    /// `(tile_width, tile_height)` pair → count.  `(None, None)` means
    /// strip-organised.
    pub tile_size_dist: FreqMap<(Option<u32>, Option<u32>)>,
}

impl IfdPositionStats {
    fn new(position: usize) -> Self {
        Self {
            position,
            file_count: 0,
            width_dist: BTreeMap::new(),
            height_dist: BTreeMap::new(),
            compression_dist: BTreeMap::new(),
            tile_size_dist: BTreeMap::new(),
        }
    }

    /// The most common width value and its count.  `None` if no files have
    /// this IFD position.
    pub fn mode_width(&self) -> Option<(u32, usize)> {
        mode_of(&self.width_dist)
    }

    /// The most common height value and its count.
    pub fn mode_height(&self) -> Option<(u32, usize)> {
        mode_of(&self.height_dist)
    }

    /// The most common compression tag and its count.
    pub fn mode_compression(&self) -> Option<(Option<u16>, usize)> {
        mode_of(&self.compression_dist)
    }

    /// The most common tile-size pair and its count.
    pub fn mode_tile_size(&self) -> Option<((Option<u32>, Option<u32>), usize)> {
        mode_of(&self.tile_size_dist)
    }
}

/// Corpus-level summary statistics derived from a slice of `FileProfile`s.
#[derive(Debug)]
pub struct CorpusStats {
    pub file_count: usize,
    pub file_size_min: u64,
    pub file_size_max: u64,
    /// IFD count → number of files with that IFD count.
    pub ifd_count_dist: FreqMap<usize>,
    /// Per IFD position (index == position).  Length equals the maximum IFD
    /// count observed across all files.
    pub ifd_positions: Vec<IfdPositionStats>,
    /// Description token → number of distinct files in which it appears.
    pub token_freq: FreqMap<String>,
    /// Description preamble (text before first `|`) → number of files.
    pub preamble_freq: FreqMap<String>,
}

impl CorpusStats {
    /// The most common IFD count and how many files have it.
    pub fn mode_ifd_count(&self) -> Option<(usize, usize)> {
        mode_of(&self.ifd_count_dist)
    }

    /// Tokens that appear in at least `min_files` distinct files, sorted by
    /// descending frequency then ascending token text.
    pub fn common_tokens(&self, min_files: usize) -> Vec<(&str, usize)> {
        let mut v: Vec<(&str, usize)> = self
            .token_freq
            .iter()
            .filter(|&(_, &count)| count >= min_files)
            .map(|(tok, &count)| (tok.as_str(), count))
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        v
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Compute corpus statistics from a slice of profiles.
///
/// Profiles may be in any order; results are deterministic due to `BTreeMap`.
pub fn compute_corpus_stats(profiles: &[FileProfile]) -> CorpusStats {
    let file_count = profiles.len();
    let file_size_min = profiles.iter().map(|p| p.file_size).min().unwrap_or(0);
    let file_size_max = profiles.iter().map(|p| p.file_size).max().unwrap_or(0);

    // IFD count distribution.
    let mut ifd_count_dist: FreqMap<usize> = BTreeMap::new();
    for p in profiles {
        *ifd_count_dist.entry(p.ifd_count).or_insert(0) += 1;
    }

    // Initialise per-position stats for every position up to the maximum.
    let max_ifds = profiles.iter().map(|p| p.ifd_count).max().unwrap_or(0);
    let mut ifd_positions: Vec<IfdPositionStats> =
        (0..max_ifds).map(IfdPositionStats::new).collect();

    for p in profiles {
        for ifd in &p.ifds {
            let pos = &mut ifd_positions[ifd.index];
            pos.file_count += 1;
            *pos.width_dist.entry(ifd.width).or_insert(0) += 1;
            *pos.height_dist.entry(ifd.height).or_insert(0) += 1;
            *pos.compression_dist.entry(ifd.compression).or_insert(0) += 1;
            *pos.tile_size_dist
                .entry((ifd.tile_width, ifd.tile_height))
                .or_insert(0) += 1;
        }
    }

    // Description token frequency (per-file, not per-occurrence).
    let mut token_freq: FreqMap<String> = BTreeMap::new();
    for p in profiles {
        for token in &p.description_tokens {
            *token_freq.entry(token.clone()).or_insert(0) += 1;
        }
    }

    // Preamble frequency.
    let mut preamble_freq: FreqMap<String> = BTreeMap::new();
    for p in profiles {
        if let Some(pre) = &p.description_preamble {
            *preamble_freq.entry(pre.clone()).or_insert(0) += 1;
        }
    }

    CorpusStats {
        file_count,
        file_size_min,
        file_size_max,
        ifd_count_dist,
        ifd_positions,
        token_freq,
        preamble_freq,
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Return the key with the highest count in a `FreqMap`, breaking ties by
/// the smallest key.  Returns `None` if the map is empty.
fn mode_of<K: Clone + Ord>(map: &FreqMap<K>) -> Option<(K, usize)> {
    map.iter()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0).reverse()))
        .map(|(k, &v)| (k.clone(), v))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::similarity::profile::{FileProfile, IfdProfile};
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    fn make_profile(file_id: usize, ifd_count: usize, file_size: u64) -> FileProfile {
        let ifds = (0..ifd_count)
            .map(|i| IfdProfile {
                index: i,
                width: 1000 * (i as u32 + 1),
                height: 800 * (i as u32 + 1),
                compression: Some(7),
                is_tiled: true,
                tile_width: Some(256),
                tile_height: Some(256),
                unit_count: 10,
                role: None,
                description: None,
            })
            .collect();
        FileProfile {
            file_id,
            path: PathBuf::from(format!("file{file_id}.svs")),
            file_size,
            ifd_count,
            ifds,
            description_tokens: BTreeSet::new(),
            description_preamble: None,
        }
    }

    #[test]
    fn empty_corpus_yields_zero_stats() {
        let stats = compute_corpus_stats(&[]);
        assert_eq!(stats.file_count, 0);
        assert_eq!(stats.file_size_min, 0);
        assert_eq!(stats.file_size_max, 0);
        assert!(stats.ifd_count_dist.is_empty());
        assert!(stats.ifd_positions.is_empty());
    }

    #[test]
    fn ifd_count_distribution_is_correct() {
        let profiles = vec![
            make_profile(0, 6, 1000),
            make_profile(1, 6, 2000),
            make_profile(2, 7, 3000),
        ];
        let stats = compute_corpus_stats(&profiles);
        assert_eq!(stats.ifd_count_dist[&6], 2);
        assert_eq!(stats.ifd_count_dist[&7], 1);
    }

    #[test]
    fn mode_ifd_count_returns_most_common() {
        let profiles = vec![
            make_profile(0, 6, 1000),
            make_profile(1, 6, 2000),
            make_profile(2, 7, 3000),
        ];
        let stats = compute_corpus_stats(&profiles);
        assert_eq!(stats.mode_ifd_count(), Some((6, 2)));
    }

    #[test]
    fn file_size_range_is_correct() {
        let profiles = vec![
            make_profile(0, 3, 500),
            make_profile(1, 3, 1500),
            make_profile(2, 3, 1000),
        ];
        let stats = compute_corpus_stats(&profiles);
        assert_eq!(stats.file_size_min, 500);
        assert_eq!(stats.file_size_max, 1500);
    }

    #[test]
    fn ifd_position_stats_file_count() {
        // Two files: one with 3 IFDs, one with 2.  Position 2 should count only 1 file.
        let profiles = vec![make_profile(0, 3, 1000), make_profile(1, 2, 1000)];
        let stats = compute_corpus_stats(&profiles);
        assert_eq!(stats.ifd_positions.len(), 3);
        assert_eq!(stats.ifd_positions[0].file_count, 2);
        assert_eq!(stats.ifd_positions[1].file_count, 2);
        assert_eq!(stats.ifd_positions[2].file_count, 1);
    }

    #[test]
    fn token_freq_counts_per_file() {
        let mut p0 = make_profile(0, 1, 100);
        let mut p1 = make_profile(1, 1, 100);
        p0.description_tokens.insert("aperio".to_string());
        p0.description_tokens.insert("unique".to_string());
        p1.description_tokens.insert("aperio".to_string());

        let stats = compute_corpus_stats(&[p0, p1]);
        assert_eq!(stats.token_freq["aperio"], 2);
        assert_eq!(stats.token_freq["unique"], 1);
    }
}
