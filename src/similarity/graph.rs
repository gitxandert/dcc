// Thresholded similarity graph — Phase 3, Step 4.
//
// Builds an undirected graph over files where an edge between A and B means
// their similarity score is at or above the chosen threshold.
//
// Design choices:
//
//   - Every file is always a node, even if it has no edges.  This ensures
//     that singleton files become one-member clusters in step 5 rather than
//     being silently dropped.
//
//   - Adjacency is stored in a `BTreeMap<usize, BTreeSet<usize>>` so that
//     iteration order over nodes and neighbours is deterministic.  This makes
//     connected-component output stable across runs without extra sorting.
//
//   - Threshold comparison is `score >= threshold` (inclusive).  The exact
//     boundary is documented here so it is easy to change if needed.
//
//   - The threshold is always supplied by the caller and is never inferred
//     automatically.  A sensible default (DEFAULT_THRESHOLD) is provided for
//     use by the CLI when the user does not specify one.

use std::collections::{BTreeMap, BTreeSet};

use crate::similarity::metric::SimilarityScore;
use crate::similarity::overlap::FileEntry;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Conservative default clustering threshold.
///
/// At 0.35, two files must share at least 35 % of the larger file's eligible
/// bytes as confirmed-exact duplicates before they are placed in the same
/// cluster.  This is intentionally strict to avoid over-grouping.
pub const DEFAULT_THRESHOLD: f64 = 0.35;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Undirected similarity graph over file IDs.
///
/// Every file appears as a node (entry in `adjacency`) even if it has no
/// neighbours.  Edges are symmetric: if `adjacency[a]` contains `b` then
/// `adjacency[b]` also contains `a`.
#[derive(Debug, Default, Clone)]
pub struct SimilarityGraph {
    /// node → set of neighbours at or above the clustering threshold.
    pub adjacency: BTreeMap<usize, BTreeSet<usize>>,
}

impl SimilarityGraph {
    /// Returns the set of file IDs that are nodes in this graph.
    pub fn nodes(&self) -> impl Iterator<Item = usize> + '_ {
        self.adjacency.keys().copied()
    }

    /// Returns the neighbours of `node`, or an empty slice if `node` is
    /// isolated (or not present).
    pub fn neighbours(&self, node: usize) -> impl Iterator<Item = usize> + '_ {
        self.adjacency
            .get(&node)
            .into_iter()
            .flat_map(|s| s.iter().copied())
    }

    /// Total number of undirected edges in the graph.
    pub fn edge_count(&self) -> usize {
        // Each undirected edge is stored twice (once per endpoint).
        self.adjacency.values().map(|s| s.len()).sum::<usize>() / 2
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

/// Build a thresholded similarity graph from pre-computed pair scores.
///
/// Every file in `files` becomes a node.  An undirected edge is added between
/// files A and B when `score >= threshold` (inclusive).
///
/// `scores` need not be exhaustive — missing pairs are treated as having a
/// score of 0.0 (no edge).  `files` drives the node set; `scores` drives the
/// edge set.
pub fn build_similarity_graph(
    files: &[FileEntry],
    scores: &[SimilarityScore],
    threshold: f64,
) -> SimilarityGraph {
    let mut adjacency: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();

    // Insert every file as a node, even if it ends up with no edges.
    for f in files {
        adjacency.entry(f.file_id).or_default();
    }

    // Add undirected edges for pairs that meet the threshold.
    for s in scores {
        if s.score >= threshold {
            adjacency.entry(s.pair.a).or_default().insert(s.pair.b);
            adjacency.entry(s.pair.b).or_default().insert(s.pair.a);
        }
    }

    SimilarityGraph { adjacency }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::similarity::metric::SimilarityScore;
    use crate::similarity::overlap::FilePair;
    use std::path::PathBuf;

    fn make_entry(file_id: usize) -> FileEntry {
        FileEntry {
            file_id,
            path: PathBuf::from(format!("file{file_id}.svs")),
            eligible_bytes: 1000,
            eligible_units: 1,
        }
    }

    fn make_score(a: usize, b: usize, score: f64) -> SimilarityScore {
        SimilarityScore {
            pair: FilePair::new(a, b),
            shared_bytes: (score * 1000.0) as u64,
            eligible_bytes_a: 1000,
            eligible_bytes_b: 1000,
            score,
        }
    }

    // ── node presence ────────────────────────────────────────────────────────

    #[test]
    fn empty_files_yields_empty_graph() {
        let g = build_similarity_graph(&[], &[], DEFAULT_THRESHOLD);
        assert_eq!(g.adjacency.len(), 0);
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn all_files_appear_as_nodes_even_with_no_edges() {
        let files = vec![make_entry(0), make_entry(1), make_entry(2)];
        let g = build_similarity_graph(&files, &[], DEFAULT_THRESHOLD);
        assert_eq!(g.adjacency.len(), 3);
        for id in 0..3 {
            assert!(g.adjacency.contains_key(&id), "node {id} missing");
            assert_eq!(g.neighbours(id).count(), 0, "node {id} should be isolated");
        }
        assert_eq!(g.edge_count(), 0);
    }

    // ── threshold inclusion ──────────────────────────────────────────────────

    #[test]
    fn score_exactly_at_threshold_creates_edge() {
        let files = vec![make_entry(0), make_entry(1)];
        let scores = vec![make_score(0, 1, DEFAULT_THRESHOLD)];
        let g = build_similarity_graph(&files, &scores, DEFAULT_THRESHOLD);
        assert_eq!(g.edge_count(), 1);
        assert!(g.neighbours(0).any(|n| n == 1));
    }

    #[test]
    fn score_just_below_threshold_does_not_create_edge() {
        let files = vec![make_entry(0), make_entry(1)];
        let scores = vec![make_score(0, 1, DEFAULT_THRESHOLD - 1e-10)];
        let g = build_similarity_graph(&files, &scores, DEFAULT_THRESHOLD);
        assert_eq!(g.edge_count(), 0);
        assert_eq!(g.neighbours(0).count(), 0);
    }

    #[test]
    fn score_above_threshold_creates_edge() {
        let files = vec![make_entry(0), make_entry(1)];
        let g = build_similarity_graph(&files, &[make_score(0, 1, 0.9)], 0.5);
        assert_eq!(g.edge_count(), 1);
    }

    // ── symmetry ─────────────────────────────────────────────────────────────

    #[test]
    fn edges_are_undirected_symmetric() {
        let files = vec![make_entry(0), make_entry(1)];
        let scores = vec![make_score(0, 1, 0.8)];
        let g = build_similarity_graph(&files, &scores, 0.5);
        // Both directions must be present.
        assert!(g.neighbours(0).any(|n| n == 1), "0 should see 1");
        assert!(g.neighbours(1).any(|n| n == 0), "1 should see 0");
    }

    // ── multiple edges ────────────────────────────────────────────────────────

    #[test]
    fn multiple_edges_across_three_files() {
        let files = vec![make_entry(0), make_entry(1), make_entry(2)];
        // (0,1) above, (0,2) above, (1,2) below.
        let scores = vec![
            make_score(0, 1, 0.8),
            make_score(0, 2, 0.6),
            make_score(1, 2, 0.1),
        ];
        let g = build_similarity_graph(&files, &scores, 0.5);
        assert_eq!(g.edge_count(), 2);
        assert!(g.neighbours(0).any(|n| n == 1));
        assert!(g.neighbours(0).any(|n| n == 2));
        assert_eq!(g.neighbours(1).filter(|&n| n == 2).count(), 0);
    }

    // ── determinism ──────────────────────────────────────────────────────────

    #[test]
    fn node_iteration_order_is_deterministic() {
        let files = vec![make_entry(3), make_entry(1), make_entry(2)];
        let g = build_similarity_graph(&files, &[], DEFAULT_THRESHOLD);
        let ids: Vec<usize> = g.nodes().collect();
        // BTreeMap guarantees ascending order.
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn neighbour_iteration_order_is_deterministic() {
        let files = vec![make_entry(0), make_entry(1), make_entry(2)];
        let scores = vec![make_score(0, 2, 0.9), make_score(0, 1, 0.9)];
        let g = build_similarity_graph(&files, &scores, 0.5);
        let neighbours: Vec<usize> = g.neighbours(0).collect();
        // BTreeSet guarantees ascending order.
        assert_eq!(neighbours, vec![1, 2]);
    }

    // ── edge_count ────────────────────────────────────────────────────────────

    #[test]
    fn edge_count_counts_undirected_edges() {
        let files = vec![make_entry(0), make_entry(1), make_entry(2)];
        let scores = vec![
            make_score(0, 1, 0.9),
            make_score(0, 2, 0.9),
            make_score(1, 2, 0.9),
        ];
        let g = build_similarity_graph(&files, &scores, 0.5);
        // Three distinct pairs → three undirected edges.
        assert_eq!(g.edge_count(), 3);
    }
}
