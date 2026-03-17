// Metadata archetypes for a corpus of SVS files.
//
// An archetype summarises the structural pattern shared by a group of files.
// Two files belong to the same archetype when they agree on every *uniform*
// structural field:
//
//   - IFD count
//   - Per-IFD compression code
//   - Per-IFD tiling (tiled vs strip, and tile dimensions when tiled)
//   - Per-IFD role (label / macro / thumbnail / none)
//
// Exact image dimensions are *not* part of the grouping key because they vary
// per slide and carry no archetype-level meaning.
//
// Within a structural group, files are further split by description-token
// similarity (Jaccard >= TOKEN_THRESHOLD).  Two files whose scanner metadata
// fields diverge beyond this threshold represent genuinely different patterns
// and belong to separate archetypes.
//
// The resulting archetypes are peers — there is no mandatory primary/variant
// tree.  Where archetypes share a token set or one token set is a subset of
// another, that relationship is recorded as an annotation for display.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::similarity::profile::FileProfile;

// ---------------------------------------------------------------------------
// Tuning
// ---------------------------------------------------------------------------

/// Minimum Jaccard similarity for two files to belong to the same token
/// sub-cluster within a structural group.
pub const TOKEN_THRESHOLD: f64 = 0.5;

// ---------------------------------------------------------------------------
// Structural skeleton (coarse grouping key)
// ---------------------------------------------------------------------------

/// The uniform structural fields used as the coarse grouping key.
/// Image dimensions are excluded — they vary per slide.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StructuralSkeleton {
    pub ifd_count: usize,
    pub per_ifd: Vec<IfdSkeleton>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IfdSkeleton {
    pub compression: Option<u16>,
    pub is_tiled: bool,
    pub tile_width: Option<u32>,
    pub tile_height: Option<u32>,
    /// "label", "macro", "thumbnail", or `None` for pyramid levels.
    pub role: Option<String>,
}

pub fn skeleton_of(p: &FileProfile) -> StructuralSkeleton {
    StructuralSkeleton {
        ifd_count: p.ifd_count,
        per_ifd: p.ifds.iter().map(|ifd| IfdSkeleton {
            compression: ifd.compression,
            is_tiled: ifd.is_tiled,
            tile_width: ifd.tile_width,
            tile_height: ifd.tile_height,
            role: ifd.role.clone(),
        }).collect(),
    }
}

// ---------------------------------------------------------------------------
// Archetype
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Archetype {
    /// Zero-based index assigned after sorting by descending member count.
    pub id: usize,
    pub member_file_ids: Vec<usize>,
    pub skeleton: StructuralSkeleton,
    /// Tokens present in *every* member file.
    pub common_tokens: BTreeSet<String>,
    /// Tokens present in some but not all member files (variable within group).
    pub variable_tokens: BTreeSet<String>,
}

impl Archetype {
    pub fn member_count(&self) -> usize {
        self.member_file_ids.len()
    }
}

// ---------------------------------------------------------------------------
// Inter-archetype relations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ArchetypeRelation {
    /// Same token intersection as another archetype, different skeleton.
    SameTokens {
        other_id: usize,
        /// Short human-readable description of what structurally differs.
        structural_note: String,
    },
    /// This archetype's common tokens are a proper subset of the other's.
    TokenSubsetOf { other_id: usize },
    /// This archetype's common tokens are a proper superset of the other's.
    TokenSupersetOf { other_id: usize },
}

#[derive(Debug)]
pub struct ArchetypeNode {
    pub archetype: Archetype,
    pub relations: Vec<ArchetypeRelation>,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn derive_archetypes(profiles: &[FileProfile]) -> Vec<ArchetypeNode> {
    if profiles.is_empty() {
        return Vec::new();
    }

    // Step 1: group by structural skeleton.
    let mut skeleton_groups: BTreeMap<StructuralSkeleton, Vec<usize>> = BTreeMap::new();
    for p in profiles {
        skeleton_groups.entry(skeleton_of(p)).or_default().push(p.file_id);
    }

    // Step 2: within each skeleton group, sub-cluster by token Jaccard.
    let mut archetypes: Vec<Archetype> = Vec::new();
    for (skel, file_ids) in &skeleton_groups {
        let group: Vec<&FileProfile> = file_ids
            .iter()
            .filter_map(|&id| profiles.iter().find(|p| p.file_id == id))
            .collect();

        for sub_ids in token_clusters(&group, TOKEN_THRESHOLD) {
            let members: Vec<&FileProfile> = sub_ids
                .iter()
                .filter_map(|&id| profiles.iter().find(|p| p.file_id == id))
                .collect();
            archetypes.push(Archetype {
                id: 0,
                member_file_ids: sub_ids,
                skeleton: skel.clone(),
                common_tokens: token_intersection(&members),
                variable_tokens: token_variable(&members),
            });
        }
    }

    // Step 3: sort by descending member count, then IFD count ascending.
    archetypes.sort_by(|a, b| {
        b.member_count()
            .cmp(&a.member_count())
            .then_with(|| a.skeleton.ifd_count.cmp(&b.skeleton.ifd_count))
            .then_with(|| a.common_tokens.len().cmp(&b.common_tokens.len()))
    });
    for (i, a) in archetypes.iter_mut().enumerate() {
        a.id = i;
    }

    // Step 4: pairwise relation annotations.
    let n = archetypes.len();
    let mut rels: Vec<Vec<ArchetypeRelation>> = vec![Vec::new(); n];
    for i in 0..n {
        for j in (i + 1)..n {
            let ta = &archetypes[i].common_tokens;
            let tb = &archetypes[j].common_tokens;

            if ta == tb {
                let note = skeleton_diff_note(&archetypes[i].skeleton, &archetypes[j].skeleton);
                rels[i].push(ArchetypeRelation::SameTokens { other_id: j, structural_note: note.clone() });
                rels[j].push(ArchetypeRelation::SameTokens { other_id: i, structural_note: note });
            } else if !ta.is_empty() && ta.is_subset(tb) {
                rels[i].push(ArchetypeRelation::TokenSubsetOf { other_id: j });
                rels[j].push(ArchetypeRelation::TokenSupersetOf { other_id: i });
            } else if !tb.is_empty() && tb.is_subset(ta) {
                rels[j].push(ArchetypeRelation::TokenSubsetOf { other_id: i });
                rels[i].push(ArchetypeRelation::TokenSupersetOf { other_id: j });
            }
        }
    }

    archetypes
        .into_iter()
        .zip(rels)
        .map(|(archetype, relations)| ArchetypeNode { archetype, relations })
        .collect()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn token_clusters(profiles: &[&FileProfile], threshold: f64) -> Vec<Vec<usize>> {
    let n = profiles.len();
    if n == 0 { return Vec::new(); }
    if n == 1 { return vec![vec![profiles[0].file_id]]; }

    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for i in 0..n {
        for j in (i + 1)..n {
            if jaccard(&profiles[i].description_tokens, &profiles[j].description_tokens) >= threshold {
                adj[i].push(j);
                adj[j].push(i);
            }
        }
    }

    let mut visited = vec![false; n];
    let mut components: Vec<Vec<usize>> = Vec::new();
    for start in 0..n {
        if visited[start] { continue; }
        let mut comp = Vec::new();
        let mut q = VecDeque::new();
        q.push_back(start);
        visited[start] = true;
        while let Some(node) = q.pop_front() {
            comp.push(profiles[node].file_id);
            for &nb in &adj[node] {
                if !visited[nb] { visited[nb] = true; q.push_back(nb); }
            }
        }
        comp.sort();
        components.push(comp);
    }
    components
}

/// Jaccard similarity of two token sets.  Both-empty → 1.0.
pub fn jaccard(a: &BTreeSet<String>, b: &BTreeSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() { return 1.0; }
    let inter = a.intersection(b).count();
    let union = a.len() + b.len() - inter;
    if union == 0 { 1.0 } else { inter as f64 / union as f64 }
}

fn token_intersection(members: &[&FileProfile]) -> BTreeSet<String> {
    if members.is_empty() { return BTreeSet::new(); }
    let mut r = members[0].description_tokens.clone();
    for m in &members[1..] {
        r = r.intersection(&m.description_tokens).cloned().collect();
    }
    r
}

fn token_variable(members: &[&FileProfile]) -> BTreeSet<String> {
    let mut union: BTreeSet<String> = BTreeSet::new();
    for m in members { union.extend(m.description_tokens.iter().cloned()); }
    let inter = token_intersection(members);
    union.difference(&inter).cloned().collect()
}

fn skeleton_diff_note(a: &StructuralSkeleton, b: &StructuralSkeleton) -> String {
    let mut parts: Vec<String> = Vec::new();
    let delta = b.ifd_count as i32 - a.ifd_count as i32;
    if delta != 0 {
        let sign = if delta > 0 { "+" } else { "" };
        parts.push(format!("IFD count {sign}{delta} ({} vs {})", b.ifd_count, a.ifd_count));
    }
    let shared = a.per_ifd.len().min(b.per_ifd.len());
    for pos in 0..shared {
        let ai = &a.per_ifd[pos];
        let bi = &b.per_ifd[pos];
        if ai.compression != bi.compression {
            parts.push(format!("IFD {pos} compression {:?}\u{2192}{:?}", ai.compression, bi.compression));
        }
        if ai.is_tiled != bi.is_tiled {
            let (f, t) = if ai.is_tiled { ("tiled", "strip") } else { ("strip", "tiled") };
            parts.push(format!("IFD {pos} layout {f}\u{2192}{t}"));
        }
    }
    if parts.is_empty() { "structural difference".to_string() } else { parts.join("; ") }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::similarity::profile::IfdProfile;
    use std::path::PathBuf;

    fn make_profile(file_id: usize, ifd_count: usize, tokens: &[&str]) -> FileProfile {
        FileProfile {
            file_id,
            path: PathBuf::from(format!("file{file_id}.svs")),
            file_size: 1024,
            ifd_count,
            ifds: (0..ifd_count).map(|i| IfdProfile {
                index: i,
                width: 1000 * (i as u32 + 1),
                height: 800 * (i as u32 + 1),
                compression: Some(7),
                is_tiled: true,
                tile_width: Some(256),
                tile_height: Some(256),
                unit_count: 4,
                role: None,
                description: None,
            }).collect(),
            description_tokens: tokens.iter().map(|t| t.to_string()).collect(),
            description_preamble: None,
        }
    }

    #[test]
    fn same_structure_and_tokens_yields_one_archetype() {
        let profiles = vec![
            make_profile(0, 7, &["aperio", "mpp", "appmag"]),
            make_profile(1, 7, &["aperio", "mpp", "appmag"]),
            make_profile(2, 7, &["aperio", "mpp", "appmag"]),
        ];
        let nodes = derive_archetypes(&profiles);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].archetype.member_count(), 3);
        assert_eq!(nodes[0].archetype.common_tokens.len(), 3);
    }

    #[test]
    fn different_ifd_count_same_tokens_are_peers_with_relation() {
        let profiles = vec![
            make_profile(0, 7, &["aperio", "mpp"]),
            make_profile(1, 7, &["aperio", "mpp"]),
            make_profile(2, 7, &["aperio", "mpp"]),
            make_profile(3, 6, &["aperio", "mpp"]),
            make_profile(4, 6, &["aperio", "mpp"]),
        ];
        let nodes = derive_archetypes(&profiles);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].archetype.skeleton.ifd_count, 7);
        assert_eq!(nodes[1].archetype.skeleton.ifd_count, 6);
        let rel0 = nodes[0].relations.iter().any(|r| matches!(r, ArchetypeRelation::SameTokens { other_id: 1, .. }));
        let rel1 = nodes[1].relations.iter().any(|r| matches!(r, ArchetypeRelation::SameTokens { other_id: 0, .. }));
        assert!(rel0);
        assert!(rel1);
    }

    #[test]
    fn divergent_tokens_split_same_skeleton_into_two_archetypes() {
        let profiles = vec![
            make_profile(0, 7, &["aperio", "appmag", "mpp", "scanscope", "filename"]),
            make_profile(1, 7, &["aperio", "appmag", "mpp", "scanscope", "filename"]),
            make_profile(2, 7, &["hamamatsu", "objectivemag", "pixelsize", "ndpis", "sourcefile"]),
            make_profile(3, 7, &["hamamatsu", "objectivemag", "pixelsize", "ndpis", "sourcefile"]),
        ];
        let nodes = derive_archetypes(&profiles);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].archetype.member_count(), 2);
        assert_eq!(nodes[1].archetype.member_count(), 2);
    }

    #[test]
    fn token_subset_relation_recorded() {
        let profiles = vec![
            make_profile(0, 7, &["aperio", "mpp", "appmag", "extra"]),
            make_profile(1, 7, &["aperio", "mpp", "appmag", "extra"]),
            make_profile(2, 6, &["aperio", "mpp", "appmag"]),
        ];
        let nodes = derive_archetypes(&profiles);
        assert_eq!(nodes.len(), 2);
        let sub = nodes[1].relations.iter().any(|r| matches!(r, ArchetypeRelation::TokenSubsetOf { other_id: 0 }));
        assert!(sub, "smaller-token archetype should be subset of larger");
    }

    #[test]
    fn variable_tokens_excluded_from_common() {
        let profiles = vec![
            make_profile(0, 3, &["aperio", "mpp", "unique_a"]),
            make_profile(1, 3, &["aperio", "mpp", "unique_b"]),
        ];
        let nodes = derive_archetypes(&profiles);
        assert_eq!(nodes.len(), 1);
        let a = &nodes[0].archetype;
        assert!(a.common_tokens.contains("aperio") && a.common_tokens.contains("mpp"));
        assert!(!a.common_tokens.contains("unique_a") && !a.common_tokens.contains("unique_b"));
        assert!(a.variable_tokens.contains("unique_a") && a.variable_tokens.contains("unique_b"));
    }
}
