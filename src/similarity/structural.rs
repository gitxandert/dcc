// Structural pairwise similarity between FileProfiles.
//
// Computes a similarity score in [0, 1] from three components:
//
//   score = W_IFD × ifd_count_score
//         + W_STRUCT × structure_score
//         + W_DESC × description_score
//
//   W_IFD    = 0.30
//   W_STRUCT = 0.50
//   W_DESC   = 0.20
//
// ifd_count_score
//   1.0  → same count
//   0.5  → differ by 1
//   0.0  → differ by 2+
//
// structure_score
//   Average over shared IFD positions of a per-position agreement value.
//   Per position: 0.4 × compression_match + 0.3 × width_match + 0.3 × height_match
//   Dimension match: 1.0 when ratio ≤ 1.25, else 0.0 (±25% tolerance).
//   "Shared" positions: min(ifd_count_a, ifd_count_b).
//
// description_score
//   Jaccard similarity of the two description token sets.
//   Both-empty case: 1.0 (vacuously equal — no description is a similarity signal).
//
// All weights and formulas are named constants, explicit in code and in
// report output.

use std::collections::BTreeSet;

use crate::similarity::profile::FileProfile;

// ---------------------------------------------------------------------------
// Weight constants — kept as named values so they are visible in reports.
// ---------------------------------------------------------------------------

pub const WEIGHT_IFD_COUNT: f64 = 0.30;
pub const WEIGHT_STRUCTURE: f64 = 0.50;
pub const WEIGHT_DESCRIPTION: f64 = 0.20;

// Dimension match tolerance: two dimension values agree when max/min ≤ this.
const DIM_TOLERANCE: f64 = 1.25;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Structural similarity score for one unordered file pair.
///
/// The component scores are kept alongside the aggregate so callers can
/// inspect what drove the result.
#[derive(Debug, Clone)]
pub struct StructuralScore {
    pub file_id_a: usize,
    pub file_id_b: usize,
    /// IFD-count agreement: 1.0 / 0.5 / 0.0.
    pub ifd_count_score: f64,
    /// Per-position structure agreement, averaged over shared positions.
    pub structure_score: f64,
    /// Description token Jaccard similarity.
    pub description_score: f64,
    /// Combined weighted score in [0, 1].
    pub score: f64,
}

// ---------------------------------------------------------------------------
// Scoring primitives
// ---------------------------------------------------------------------------

/// Per-position structural agreement between two IFDs.
///
/// Returns a value in [0, 1]:
///   0.4 for matching compression
///   0.3 for matching width (within tolerance)
///   0.3 for matching height (within tolerance)
fn ifd_position_agreement(
    a_w: u32, a_h: u32, a_comp: Option<u16>,
    b_w: u32, b_h: u32, b_comp: Option<u16>,
) -> f64 {
    let comp = if a_comp == b_comp { 1.0 } else { 0.0 };
    let w = dim_match(a_w, b_w);
    let h = dim_match(a_h, b_h);
    0.4 * comp + 0.3 * w + 0.3 * h
}

/// 1.0 if two dimension values agree within `DIM_TOLERANCE`, else 0.0.
/// Both-zero is treated as a match.
fn dim_match(a: u32, b: u32) -> f64 {
    if a == 0 && b == 0 {
        return 1.0;
    }
    if a == 0 || b == 0 {
        return 0.0;
    }
    let (lo, hi) = if a <= b { (a as f64, b as f64) } else { (b as f64, a as f64) };
    if hi / lo <= DIM_TOLERANCE { 1.0 } else { 0.0 }
}

/// Jaccard similarity between two token sets.
///
/// Both-empty → 1.0 (absence of descriptions is a similarity signal: neither
/// file has meaningful metadata to distinguish it).
fn jaccard(a: &BTreeSet<String>, b: &BTreeSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    let intersection = a.intersection(b).count();
    let union = a.len() + b.len() - intersection;
    if union == 0 { 1.0 } else { intersection as f64 / union as f64 }
}

// ---------------------------------------------------------------------------
// Public functions
// ---------------------------------------------------------------------------

/// Compute the structural similarity score for one file pair.
pub fn score_pair(a: &FileProfile, b: &FileProfile) -> StructuralScore {
    let ifd_count_score = {
        let diff = (a.ifd_count as i64 - b.ifd_count as i64).unsigned_abs() as usize;
        match diff {
            0 => 1.0,
            1 => 0.5,
            _ => 0.0,
        }
    };

    let structure_score = {
        let shared = a.ifd_count.min(b.ifd_count);
        if shared == 0 {
            0.0
        } else {
            let sum: f64 = (0..shared)
                .map(|i| {
                    let ai = &a.ifds[i];
                    let bi = &b.ifds[i];
                    ifd_position_agreement(
                        ai.width, ai.height, ai.compression,
                        bi.width, bi.height, bi.compression,
                    )
                })
                .sum();
            sum / shared as f64
        }
    };

    let description_score = jaccard(&a.description_tokens, &b.description_tokens);

    let score = WEIGHT_IFD_COUNT * ifd_count_score
              + WEIGHT_STRUCTURE  * structure_score
              + WEIGHT_DESCRIPTION * description_score;

    StructuralScore {
        file_id_a: a.file_id,
        file_id_b: b.file_id,
        ifd_count_score,
        structure_score,
        description_score,
        score,
    }
}

/// Score every unordered pair in `profiles` and return results sorted by
/// descending score, then ascending `(file_id_a, file_id_b)`.
pub fn score_all_pairs(profiles: &[FileProfile]) -> Vec<StructuralScore> {
    let mut scores = Vec::new();
    for i in 0..profiles.len() {
        for j in (i + 1)..profiles.len() {
            scores.push(score_pair(&profiles[i], &profiles[j]));
        }
    }
    scores.sort_by(|x, y| {
        y.score
            .partial_cmp(&x.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.file_id_a.cmp(&y.file_id_a))
            .then_with(|| x.file_id_b.cmp(&y.file_id_b))
    });
    scores
}

/// Human-readable description of the scoring formula, for report headers.
pub fn formula_description() -> &'static str {
    "0.30×ifd_count_agreement + 0.50×avg_position_structure + 0.20×description_jaccard"
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::similarity::profile::{FileProfile, IfdProfile};
    use std::path::PathBuf;

    fn make_profile(
        file_id: usize,
        ifd_count: usize,
        width: u32,
        compression: Option<u16>,
        tokens: &[&str],
    ) -> FileProfile {
        let ifds = (0..ifd_count)
            .map(|i| IfdProfile {
                index: i,
                width,
                height: width / 2,
                compression,
                is_tiled: true,
                tile_width: Some(256),
                tile_height: Some(256),
                unit_count: 4,
                role: None,
                description: None,
            })
            .collect();
        let mut description_tokens = BTreeSet::new();
        for t in tokens {
            description_tokens.insert(t.to_string());
        }
        FileProfile {
            file_id,
            path: PathBuf::from(format!("file{file_id}.svs")),
            file_size: 1_000_000,
            ifd_count,
            ifds,
            description_tokens,
            description_preamble: None,
        }
    }

    #[test]
    fn identical_profiles_score_one() {
        let a = make_profile(0, 6, 40000, Some(7), &["aperio", "imagescope"]);
        let b = make_profile(1, 6, 40000, Some(7), &["aperio", "imagescope"]);
        let s = score_pair(&a, &b);
        assert!((s.score - 1.0).abs() < 1e-9, "identical profiles → score 1.0, got {}", s.score);
    }

    #[test]
    fn different_ifd_counts_penalised() {
        let a = make_profile(0, 6, 40000, Some(7), &[]);
        let b = make_profile(1, 4, 40000, Some(7), &[]);
        let s = score_pair(&a, &b);
        // ifd_count_score = 0.0 → WEIGHT_IFD_COUNT penalty
        assert!(s.ifd_count_score == 0.0);
        assert!(s.score < 0.80, "score should be reduced, got {}", s.score);
    }

    #[test]
    fn ifd_count_off_by_one_is_partial() {
        let a = make_profile(0, 6, 40000, Some(7), &[]);
        let b = make_profile(1, 7, 40000, Some(7), &[]);
        let s = score_pair(&a, &b);
        assert!((s.ifd_count_score - 0.5).abs() < 1e-9);
    }

    #[test]
    fn different_compression_penalised() {
        let a = make_profile(0, 1, 40000, Some(7), &[]);
        let b = make_profile(1, 1, 40000, Some(1), &[]);
        let s = score_pair(&a, &b);
        // structure_score loses 0.4 per position → 0.6 for the single position
        assert!((s.structure_score - 0.6).abs() < 1e-9, "got {}", s.structure_score);
    }

    #[test]
    fn jaccard_both_empty_is_one() {
        let a: BTreeSet<String> = BTreeSet::new();
        let b: BTreeSet<String> = BTreeSet::new();
        assert!((jaccard(&a, &b) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn jaccard_disjoint_is_zero() {
        let a: BTreeSet<String> = ["foo".to_string()].into();
        let b: BTreeSet<String> = ["bar".to_string()].into();
        assert!((jaccard(&a, &b) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn jaccard_half_overlap() {
        let a: BTreeSet<String> = ["foo".to_string(), "bar".to_string()].into();
        let b: BTreeSet<String> = ["foo".to_string(), "baz".to_string()].into();
        // intersection=1, union=3 → 1/3
        assert!((jaccard(&a, &b) - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn score_all_pairs_sorted_descending() {
        let a = make_profile(0, 6, 40000, Some(7), &["aperio"]);
        let b = make_profile(1, 6, 40000, Some(7), &["aperio"]);
        let c = make_profile(2, 3, 10000, Some(1), &[]);
        let scores = score_all_pairs(&[a, b, c]);
        assert_eq!(scores.len(), 3);
        assert!(scores[0].score >= scores[1].score);
        assert!(scores[1].score >= scores[2].score);
    }

    #[test]
    fn dim_match_within_tolerance() {
        assert!((dim_match(1000, 1200) - 1.0).abs() < 1e-9); // 1.2 ≤ 1.25
        assert!((dim_match(1000, 1300) - 0.0).abs() < 1e-9); // 1.3 > 1.25
    }
}
