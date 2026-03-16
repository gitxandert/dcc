// Pairwise similarity scoring — Phase 3, Step 2.
//
// Converts the file-pair overlap map from `overlap.rs` into a sorted list of
// `SimilarityScore` values, one per unordered file pair.
//
// Metric used:
//
//   score(a, b) = shared_confirmed_bytes / max(eligible_bytes(a), eligible_bytes(b))
//
// This is the `SharedOverMaxBytes` metric.  It is bounded in [0.0, 1.0],
// conservative (the larger file sets the denominator), and easy to explain.
// The formula is kept explicit in code and in report output.
//
// All file pairs are scored, not only those with non-zero overlap.  Zero-
// overlap pairs score 0.0 and still appear in the output so that:
//
//   - the total pair count is explicit
//   - threshold reasoning is straightforward
//   - JSON output is complete
//
// Sorting: descending by score, then ascending by (file_id_a, file_id_b) for
// stable output on equal scores.

use std::collections::BTreeMap;

use crate::similarity::overlap::{FileEntry, FilePair, PairOverlap};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The similarity formula applied to a file pair.
///
/// Represented as an enum so it is visible in reports and can be extended
/// later without changing call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimilarityMetric {
    /// shared_confirmed_bytes / max(eligible_bytes_a, eligible_bytes_b)
    SharedOverMaxBytes,
}

impl SimilarityMetric {
    /// Human-readable formula string, suitable for report headers.
    pub fn formula(&self) -> &'static str {
        match self {
            SimilarityMetric::SharedOverMaxBytes => {
                "shared_confirmed_bytes / max(eligible_bytes_a, eligible_bytes_b)"
            }
        }
    }
}

/// Similarity score for one file pair, including the raw inputs so the score
/// is always inspectable.
#[derive(Debug, Clone)]
pub struct SimilarityScore {
    pub pair: FilePair,
    pub shared_bytes: u64,
    pub eligible_bytes_a: u64,
    pub eligible_bytes_b: u64,
    /// Score in [0.0, 1.0].  0.0 when either denominator is 0.
    pub score: f64,
}

// ---------------------------------------------------------------------------
// Core functions
// ---------------------------------------------------------------------------

/// Compute the similarity score for a single pair given the raw overlap inputs.
pub fn compute_similarity(
    shared_bytes: u64,
    eligible_a: u64,
    eligible_b: u64,
    metric: SimilarityMetric,
) -> f64 {
    match metric {
        SimilarityMetric::SharedOverMaxBytes => {
            let denom = eligible_a.max(eligible_b);
            if denom == 0 {
                0.0
            } else {
                shared_bytes as f64 / denom as f64
            }
        }
    }
}

/// Score every unordered file pair and return a sorted list.
///
/// Every pair of distinct files is included, whether or not they share any
/// confirmed bytes.  Pairs with no overlap entry in `overlaps` receive
/// `shared_bytes = 0` and `score = 0.0`.
///
/// Result is sorted: descending by score, then ascending by
/// `(pair.a, pair.b)` for deterministic ordering on equal scores.
pub fn score_file_pairs(
    files: &[FileEntry],
    overlaps: &BTreeMap<FilePair, PairOverlap>,
    metric: SimilarityMetric,
) -> Vec<SimilarityScore> {
    let mut scores: Vec<SimilarityScore> = Vec::new();

    for i in 0..files.len() {
        for j in (i + 1)..files.len() {
            let fa = &files[i];
            let fb = &files[j];
            let pair = FilePair::new(fa.file_id, fb.file_id);

            let shared_bytes = overlaps
                .get(&pair)
                .map(|ov| ov.shared_bytes)
                .unwrap_or(0);

            let score = compute_similarity(
                shared_bytes,
                fa.eligible_bytes,
                fb.eligible_bytes,
                metric,
            );

            scores.push(SimilarityScore {
                pair,
                shared_bytes,
                eligible_bytes_a: fa.eligible_bytes,
                eligible_bytes_b: fb.eligible_bytes,
                score,
            });
        }
    }

    // Descending score, then ascending (pair.a, pair.b) for stability.
    scores.sort_by(|x, y| {
        y.score
            .partial_cmp(&x.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.pair.cmp(&y.pair))
    });

    scores
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::similarity::overlap::PairOverlap;
    use std::path::PathBuf;

    fn make_entry(file_id: usize, eligible_bytes: u64) -> FileEntry {
        FileEntry {
            file_id,
            path: PathBuf::from(format!("file{file_id}.svs")),
            eligible_bytes,
            eligible_units: 1,
        }
    }

    fn make_overlap(shared_bytes: u64) -> PairOverlap {
        PairOverlap { shared_bytes, shared_unit_pairs: 1 }
    }

    // ── compute_similarity ───────────────────────────────────────────────────

    #[test]
    fn zero_denominator_returns_zero() {
        assert_eq!(
            compute_similarity(0, 0, 0, SimilarityMetric::SharedOverMaxBytes),
            0.0
        );
    }

    #[test]
    fn zero_shared_bytes_returns_zero() {
        assert_eq!(
            compute_similarity(0, 1000, 2000, SimilarityMetric::SharedOverMaxBytes),
            0.0
        );
    }

    #[test]
    fn full_overlap_returns_one() {
        let score =
            compute_similarity(500, 500, 500, SimilarityMetric::SharedOverMaxBytes);
        assert!((score - 1.0).abs() < 1e-12, "expected 1.0, got {score}");
    }

    #[test]
    fn partial_overlap_uses_max_denominator() {
        // shared=100, eligible_a=200, eligible_b=400 → 100/400 = 0.25
        let score =
            compute_similarity(100, 200, 400, SimilarityMetric::SharedOverMaxBytes);
        assert!((score - 0.25).abs() < 1e-12, "expected 0.25, got {score}");
    }

    #[test]
    fn score_is_symmetric_under_argument_swap() {
        let s1 = compute_similarity(300, 600, 900, SimilarityMetric::SharedOverMaxBytes);
        let s2 = compute_similarity(300, 900, 600, SimilarityMetric::SharedOverMaxBytes);
        assert!((s1 - s2).abs() < 1e-12, "score must be symmetric");
    }

    // ── score_file_pairs ─────────────────────────────────────────────────────

    #[test]
    fn no_files_yields_empty_scores() {
        let scores = score_file_pairs(&[], &BTreeMap::new(), SimilarityMetric::SharedOverMaxBytes);
        assert!(scores.is_empty());
    }

    #[test]
    fn single_file_yields_no_pairs() {
        let files = vec![make_entry(0, 1024)];
        let scores =
            score_file_pairs(&files, &BTreeMap::new(), SimilarityMetric::SharedOverMaxBytes);
        assert!(scores.is_empty());
    }

    #[test]
    fn two_files_no_overlap_yields_zero_score() {
        let files = vec![make_entry(0, 1024), make_entry(1, 2048)];
        let scores =
            score_file_pairs(&files, &BTreeMap::new(), SimilarityMetric::SharedOverMaxBytes);
        assert_eq!(scores.len(), 1);
        assert_eq!(scores[0].shared_bytes, 0);
        assert_eq!(scores[0].score, 0.0);
    }

    #[test]
    fn two_files_with_overlap_yields_correct_score() {
        let files = vec![make_entry(0, 1000), make_entry(1, 2000)];
        let mut overlaps = BTreeMap::new();
        overlaps.insert(FilePair::new(0, 1), make_overlap(500));

        let scores =
            score_file_pairs(&files, &overlaps, SimilarityMetric::SharedOverMaxBytes);
        assert_eq!(scores.len(), 1);
        let s = &scores[0];
        assert_eq!(s.shared_bytes, 500);
        assert_eq!(s.eligible_bytes_a, 1000);
        assert_eq!(s.eligible_bytes_b, 2000);
        // 500 / max(1000, 2000) = 500 / 2000 = 0.25
        assert!((s.score - 0.25).abs() < 1e-12);
    }

    #[test]
    fn three_files_yields_three_pairs() {
        let files = vec![
            make_entry(0, 1000),
            make_entry(1, 1000),
            make_entry(2, 1000),
        ];
        let scores =
            score_file_pairs(&files, &BTreeMap::new(), SimilarityMetric::SharedOverMaxBytes);
        assert_eq!(scores.len(), 3);
    }

    #[test]
    fn scores_sorted_descending() {
        let files = vec![
            make_entry(0, 1000),
            make_entry(1, 1000),
            make_entry(2, 1000),
        ];
        let mut overlaps = BTreeMap::new();
        // (0,1) → 0.8, (0,2) → 0.3, (1,2) → no overlap → 0.0
        overlaps.insert(FilePair::new(0, 1), make_overlap(800));
        overlaps.insert(FilePair::new(0, 2), make_overlap(300));

        let scores =
            score_file_pairs(&files, &overlaps, SimilarityMetric::SharedOverMaxBytes);
        assert_eq!(scores.len(), 3);
        assert!(scores[0].score >= scores[1].score);
        assert!(scores[1].score >= scores[2].score);
        // Top pair should be (0,1) with score 0.8.
        assert_eq!(scores[0].pair, FilePair::new(0, 1));
        assert!((scores[0].score - 0.8).abs() < 1e-12);
    }

    #[test]
    fn equal_scores_sorted_by_pair_id_ascending() {
        // Three files, pairs (0,1) and (0,2) share 500 bytes from 1000 → both 0.5.
        let files = vec![
            make_entry(0, 1000),
            make_entry(1, 1000),
            make_entry(2, 1000),
        ];
        let mut overlaps = BTreeMap::new();
        overlaps.insert(FilePair::new(0, 1), make_overlap(500));
        overlaps.insert(FilePair::new(0, 2), make_overlap(500));

        let scores =
            score_file_pairs(&files, &overlaps, SimilarityMetric::SharedOverMaxBytes);
        // Tied scores: (0,1) should come before (0,2) by ascending pair id.
        assert_eq!(scores[0].pair, FilePair::new(0, 1));
        assert_eq!(scores[1].pair, FilePair::new(0, 2));
    }

    #[test]
    fn score_pairs_preserves_eligible_bytes_in_output() {
        let files = vec![make_entry(0, 3000), make_entry(1, 7000)];
        let mut overlaps = BTreeMap::new();
        overlaps.insert(FilePair::new(0, 1), make_overlap(1000));

        let scores =
            score_file_pairs(&files, &overlaps, SimilarityMetric::SharedOverMaxBytes);
        let s = &scores[0];
        // eligible_bytes_a/b should match the FileEntry values.
        assert!(
            (s.eligible_bytes_a == 3000 && s.eligible_bytes_b == 7000)
            || (s.eligible_bytes_a == 7000 && s.eligible_bytes_b == 3000)
        );
    }

    #[test]
    fn metric_formula_string_is_stable() {
        assert_eq!(
            SimilarityMetric::SharedOverMaxBytes.formula(),
            "shared_confirmed_bytes / max(eligible_bytes_a, eligible_bytes_b)"
        );
    }
}
