// Connected-components clustering and diagnostics — Phase 3, Step 5.
//
// Converts a `SimilarityGraph` into a `Vec<Cluster>` by finding connected
// components.  Every node becomes a member of exactly one cluster; isolated
// nodes become singleton clusters.
//
// Determinism guarantees:
//
//   - Nodes are visited in ascending `file_id` order (BTreeMap iteration).
//   - Members within each cluster are sorted ascending before storage.
//   - Clusters are sorted by their smallest member id before `cluster_id`s
//     are assigned, so ids are stable across runs given the same input.
//
// `ClusterStats` provides min/max/avg internal pairwise similarity for each
// cluster, which helps identify bridge clusters (A~B~C but A≁C) without
// needing a more sophisticated algorithm.

use std::collections::{BTreeSet, VecDeque};

use crate::similarity::graph::SimilarityGraph;
use crate::similarity::metric::SimilarityScore;
use crate::similarity::overlap::{FileEntry, FilePair};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// One cluster produced by the clustering pass.
#[derive(Debug, Clone)]
pub struct Cluster {
    pub cluster_id: usize,
    /// File IDs of members, sorted ascending.
    pub members: Vec<usize>,
}

impl Cluster {
    /// True when the cluster contains exactly one file.
    pub fn is_singleton(&self) -> bool {
        self.members.len() == 1
    }
}

/// Diagnostic statistics for one cluster.
#[derive(Debug, Clone)]
pub struct ClusterStats {
    pub cluster_id: usize,
    pub member_count: usize,
    /// Sum of `eligible_bytes` for all member files.
    pub total_eligible_bytes: u64,
    /// Minimum internal pairwise similarity.  `None` for singleton clusters.
    pub min_internal_score: Option<f64>,
    /// Maximum internal pairwise similarity.  `None` for singleton clusters.
    pub max_internal_score: Option<f64>,
    /// Mean internal pairwise similarity.  `None` for singleton clusters.
    pub avg_internal_score: Option<f64>,
}

/// Clustering algorithm to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMode {
    /// Connected components over the thresholded similarity graph.
    Components,
    /// Greedy seed-based clustering (step 7 — not yet implemented).
    Greedy,
}

impl ClusterMode {
    pub fn label(&self) -> &'static str {
        match self {
            ClusterMode::Components => "components",
            ClusterMode::Greedy => "greedy",
        }
    }
}

// ---------------------------------------------------------------------------
// Connected-components clustering
// ---------------------------------------------------------------------------

/// Cluster `graph` into connected components.
///
/// Every node in the graph appears in exactly one returned cluster.  Isolated
/// nodes become one-member clusters.  The output is sorted and labelled
/// deterministically (see module docstring).
pub fn connected_components(graph: &SimilarityGraph) -> Vec<Cluster> {
    let mut visited: BTreeSet<usize> = BTreeSet::new();
    // Accumulate components before assigning ids (we sort them first).
    let mut components: Vec<Vec<usize>> = Vec::new();

    // Iterate nodes in ascending order — guaranteed by BTreeMap.
    for start in graph.nodes() {
        if visited.contains(&start) {
            continue;
        }

        // BFS from `start`.
        let mut component: Vec<usize> = Vec::new();
        let mut queue: VecDeque<usize> = VecDeque::new();
        queue.push_back(start);
        visited.insert(start);

        while let Some(node) = queue.pop_front() {
            component.push(node);
            for neighbour in graph.neighbours(node) {
                if !visited.contains(&neighbour) {
                    visited.insert(neighbour);
                    queue.push_back(neighbour);
                }
            }
        }

        // Members are collected in BFS order, but we sort for stability.
        component.sort_unstable();
        components.push(component);
    }

    // Sort components by their smallest member id for deterministic cluster_id
    // assignment across runs.
    components.sort_by_key(|c| c[0]);

    components
        .into_iter()
        .enumerate()
        .map(|(id, members)| Cluster { cluster_id: id, members })
        .collect()
}

// ---------------------------------------------------------------------------
// Cluster diagnostics
// ---------------------------------------------------------------------------

/// Compute diagnostic statistics for each cluster.
///
/// For each cluster the function gathers all internal pairwise similarity
/// scores (pairs where both files are cluster members) and computes min,
/// max, and average.  Singleton clusters return `None` for all three values
/// since there are no internal pairs.
pub fn compute_cluster_stats(
    clusters: &[Cluster],
    files: &[FileEntry],
    scores: &[SimilarityScore],
) -> Vec<ClusterStats> {
    // Build a lookup from file_id → eligible_bytes.
    let eligible: std::collections::BTreeMap<usize, u64> = files
        .iter()
        .map(|f| (f.file_id, f.eligible_bytes))
        .collect();

    // Build a lookup from FilePair → score for O(log n) internal-pair queries.
    let score_map: std::collections::BTreeMap<FilePair, f64> = scores
        .iter()
        .map(|s| (s.pair, s.score))
        .collect();

    clusters
        .iter()
        .map(|c| {
            let total_eligible_bytes: u64 = c
                .members
                .iter()
                .map(|id| eligible.get(id).copied().unwrap_or(0))
                .sum();

            // Collect internal pairwise scores.
            let mut internal: Vec<f64> = Vec::new();
            for i in 0..c.members.len() {
                for j in (i + 1)..c.members.len() {
                    let pair = FilePair::new(c.members[i], c.members[j]);
                    // Use 0.0 for pairs absent from the score map (shouldn't
                    // happen in practice, but be defensive).
                    let s = score_map.get(&pair).copied().unwrap_or(0.0);
                    internal.push(s);
                }
            }

            let (min_s, max_s, avg_s) = if internal.is_empty() {
                (None, None, None)
            } else {
                let min = internal.iter().cloned().fold(f64::INFINITY, f64::min);
                let max = internal.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                let avg = internal.iter().sum::<f64>() / internal.len() as f64;
                (Some(min), Some(max), Some(avg))
            };

            ClusterStats {
                cluster_id: c.cluster_id,
                member_count: c.members.len(),
                total_eligible_bytes,
                min_internal_score: min_s,
                max_internal_score: max_s,
                avg_internal_score: avg_s,
            }
        })
        .collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::similarity::graph::{build_similarity_graph, DEFAULT_THRESHOLD};
    use crate::similarity::metric::SimilarityScore;
    use crate::similarity::overlap::{FileEntry, FilePair};
    use std::path::PathBuf;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_entry(file_id: usize, eligible_bytes: u64) -> FileEntry {
        FileEntry {
            file_id,
            path: PathBuf::from(format!("file{file_id}.svs")),
            eligible_bytes,
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

    /// Build a graph and cluster it in one step.
    fn cluster_from_scores(
        file_ids: &[usize],
        scored_pairs: &[(usize, usize, f64)],
        threshold: f64,
    ) -> Vec<Cluster> {
        let files: Vec<FileEntry> = file_ids.iter().map(|&id| make_entry(id, 1000)).collect();
        let scores: Vec<SimilarityScore> = scored_pairs
            .iter()
            .map(|&(a, b, s)| make_score(a, b, s))
            .collect();
        let graph = build_similarity_graph(&files, &scores, threshold);
        connected_components(&graph)
    }

    // ── connected_components ─────────────────────────────────────────────────

    #[test]
    fn empty_graph_yields_no_clusters() {
        let files: Vec<FileEntry> = vec![];
        let graph = build_similarity_graph(&files, &[], DEFAULT_THRESHOLD);
        let clusters = connected_components(&graph);
        assert!(clusters.is_empty());
    }

    #[test]
    fn all_isolated_nodes_become_singletons() {
        let clusters = cluster_from_scores(&[0, 1, 2], &[], DEFAULT_THRESHOLD);
        assert_eq!(clusters.len(), 3);
        for c in &clusters {
            assert!(c.is_singleton());
        }
    }

    #[test]
    fn two_connected_files_form_one_cluster() {
        let clusters =
            cluster_from_scores(&[0, 1], &[(0, 1, 0.9)], DEFAULT_THRESHOLD);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members, vec![0, 1]);
    }

    #[test]
    fn two_separate_components() {
        // (0,1) connected, (2,3) connected, no edge between the groups.
        let clusters = cluster_from_scores(
            &[0, 1, 2, 3],
            &[(0, 1, 0.9), (2, 3, 0.9)],
            DEFAULT_THRESHOLD,
        );
        assert_eq!(clusters.len(), 2);
        assert_eq!(clusters[0].members, vec![0, 1]);
        assert_eq!(clusters[1].members, vec![2, 3]);
    }

    #[test]
    fn bridge_chain_abc_forms_one_component() {
        // A~B, B~C but A≁C — connected components still groups all three.
        let clusters = cluster_from_scores(
            &[0, 1, 2],
            &[(0, 1, 0.9), (1, 2, 0.9)],
            DEFAULT_THRESHOLD,
        );
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members, vec![0, 1, 2]);
    }

    #[test]
    fn members_sorted_ascending_within_cluster() {
        // Supply files in reverse order to ensure sorting is applied.
        let clusters = cluster_from_scores(&[2, 0, 1], &[(0, 2, 0.9)], DEFAULT_THRESHOLD);
        // (0,2) connected; 1 isolated.
        let connected = clusters.iter().find(|c| c.members.len() == 2).unwrap();
        assert_eq!(connected.members, vec![0, 2]);
    }

    #[test]
    fn clusters_sorted_by_smallest_member_id() {
        // Two components: {3,4} and {0,1}.  Cluster 0 should be {0,1}.
        let clusters = cluster_from_scores(
            &[0, 1, 3, 4],
            &[(0, 1, 0.9), (3, 4, 0.9)],
            DEFAULT_THRESHOLD,
        );
        assert_eq!(clusters.len(), 2);
        assert_eq!(clusters[0].cluster_id, 0);
        assert_eq!(clusters[0].members, vec![0, 1]);
        assert_eq!(clusters[1].cluster_id, 1);
        assert_eq!(clusters[1].members, vec![3, 4]);
    }

    #[test]
    fn score_below_threshold_keeps_files_separate() {
        let clusters = cluster_from_scores(
            &[0, 1],
            &[(0, 1, DEFAULT_THRESHOLD - 1e-10)],
            DEFAULT_THRESHOLD,
        );
        assert_eq!(clusters.len(), 2);
        assert!(clusters.iter().all(|c| c.is_singleton()));
    }

    #[test]
    fn score_at_threshold_connects_files() {
        let clusters = cluster_from_scores(
            &[0, 1],
            &[(0, 1, DEFAULT_THRESHOLD)],
            DEFAULT_THRESHOLD,
        );
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].members.len(), 2);
    }

    // ── compute_cluster_stats ────────────────────────────────────────────────

    #[test]
    fn singleton_stats_have_no_internal_scores() {
        let files = vec![make_entry(0, 5000)];
        let graph = build_similarity_graph(&files, &[], DEFAULT_THRESHOLD);
        let clusters = connected_components(&graph);
        let stats = compute_cluster_stats(&clusters, &files, &[]);
        assert_eq!(stats.len(), 1);
        let s = &stats[0];
        assert_eq!(s.member_count, 1);
        assert_eq!(s.total_eligible_bytes, 5000);
        assert!(s.min_internal_score.is_none());
        assert!(s.max_internal_score.is_none());
        assert!(s.avg_internal_score.is_none());
    }

    #[test]
    fn two_member_cluster_stats_use_single_pair_score() {
        let files = vec![make_entry(0, 1000), make_entry(1, 2000)];
        let scores = vec![make_score(0, 1, 0.8)];
        let graph = build_similarity_graph(&files, &scores, 0.5);
        let clusters = connected_components(&graph);
        let stats = compute_cluster_stats(&clusters, &files, &scores);
        assert_eq!(stats.len(), 1);
        let s = &stats[0];
        assert_eq!(s.member_count, 2);
        assert_eq!(s.total_eligible_bytes, 3000);
        assert!((s.min_internal_score.unwrap() - 0.8).abs() < 1e-12);
        assert!((s.max_internal_score.unwrap() - 0.8).abs() < 1e-12);
        assert!((s.avg_internal_score.unwrap() - 0.8).abs() < 1e-12);
    }

    #[test]
    fn three_member_cluster_stats_compute_correctly() {
        // Triangle: (0,1)=0.9, (0,2)=0.6, (1,2)=0.7 → min=0.6, max=0.9, avg=11/15≈0.7333
        let files = vec![make_entry(0, 1000), make_entry(1, 1000), make_entry(2, 1000)];
        let scores = vec![
            make_score(0, 1, 0.9),
            make_score(0, 2, 0.6),
            make_score(1, 2, 0.7),
        ];
        let graph = build_similarity_graph(&files, &scores, 0.5);
        let clusters = connected_components(&graph);
        assert_eq!(clusters.len(), 1);
        let stats = compute_cluster_stats(&clusters, &files, &scores);
        let s = &stats[0];
        assert!((s.min_internal_score.unwrap() - 0.6).abs() < 1e-12);
        assert!((s.max_internal_score.unwrap() - 0.9).abs() < 1e-12);
        let expected_avg = (0.9 + 0.6 + 0.7) / 3.0;
        assert!((s.avg_internal_score.unwrap() - expected_avg).abs() < 1e-12);
    }

    #[test]
    fn total_eligible_bytes_sums_all_members() {
        let files = vec![make_entry(0, 100), make_entry(1, 200), make_entry(2, 300)];
        let scores = vec![make_score(0, 1, 0.9), make_score(1, 2, 0.9)];
        let graph = build_similarity_graph(&files, &scores, 0.5);
        let clusters = connected_components(&graph);
        let stats = compute_cluster_stats(&clusters, &files, &scores);
        assert_eq!(stats[0].total_eligible_bytes, 600);
    }
}
