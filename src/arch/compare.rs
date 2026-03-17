// Byte-level comparison across multiple files at the same structural position.
//
// `find_shared_runs` identifies contiguous spans within a reference byte slice
// where all other slices agree, byte-for-byte, at the same positions.  Spans
// shorter than `min_shared` are discarded so that trivially short matches do
// not inflate the archetype.

use std::ops::Range;

// ---------------------------------------------------------------------------
// Core function
// ---------------------------------------------------------------------------

/// Find runs of bytes that are identical in `reference` and every slice in
/// `others` at the same positions.
///
/// Comparison is limited to the shortest prefix that is present in all
/// inputs (i.e. `min(reference.len(), others[i].len())` for all `i`).
/// Positions beyond the shortest sequence are not considered.
///
/// Returns ranges within `reference` where the run is at least `min_shared`
/// bytes long, in ascending order.
///
/// # Single-member archetype
///
/// When `others` is empty there is no other file to disagree with, so every
/// byte of `reference` is treated as shared and a single range covering the
/// entire slice is returned (provided `reference.len() >= min_shared`).
pub fn find_shared_runs(
    reference: &[u8],
    others: &[&[u8]],
    min_shared: usize,
) -> Vec<Range<usize>> {
    if reference.is_empty() {
        return Vec::new();
    }

    if others.is_empty() {
        // Single-member archetype: all bytes are shared by definition.
        if reference.len() >= min_shared {
            return vec![0..reference.len()];
        } else {
            return Vec::new();
        }
    }

    // Only compare positions present in every sequence.
    let comparable_len = others.iter().fold(reference.len(), |acc, o| acc.min(o.len()));

    // Determine which positions agree across all sequences.
    let mut is_match = vec![true; comparable_len];
    for other in others {
        for (i, m) in is_match.iter_mut().enumerate() {
            if reference[i] != other[i] {
                *m = false;
            }
        }
    }

    // Collect runs of consecutive matching positions that meet the minimum
    // length requirement.
    let mut runs: Vec<Range<usize>> = Vec::new();
    let mut run_start: Option<usize> = None;

    for (i, &matched) in is_match.iter().enumerate() {
        match (matched, run_start) {
            (true, None) => run_start = Some(i),
            (false, Some(s)) => {
                if i - s >= min_shared {
                    runs.push(s..i);
                }
                run_start = None;
            }
            _ => {}
        }
    }

    // Flush any open run at the end of the comparable region.
    if let Some(s) = run_start {
        if comparable_len - s >= min_shared {
            runs.push(s..comparable_len);
        }
    }

    runs
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_sequences_yield_one_full_run() {
        let a = b"Aperio Image Library v11.2.1";
        let runs = find_shared_runs(a, &[a], 4);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0], 0..a.len());
    }

    #[test]
    fn shared_prefix_diverges_at_known_position() {
        // First 27 bytes are identical; byte 27 differs.
        let a = b"Aperio|AppMag = 20|MPP = 0.4952";
        let b = b"Aperio|AppMag = 20|MPP = 0.5000";
        let shared_len = a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count();
        let runs = find_shared_runs(a, &[b], 4);
        assert!(!runs.is_empty());
        assert_eq!(runs[0].start, 0);
        assert_eq!(runs[0].end, shared_len);
    }

    #[test]
    fn no_matching_bytes_returns_empty() {
        let a = b"AAAA";
        let b = b"BBBB";
        let runs = find_shared_runs(a, &[b], 1);
        assert!(runs.is_empty());
    }

    #[test]
    fn run_below_min_shared_is_excluded() {
        // 3 matching bytes — excluded when min_shared = 4.
        let a = b"ABC_XXXXXXXX";
        let b = b"ABC!YYYYYYYY";
        let runs = find_shared_runs(a, &[b], 4);
        assert!(runs.is_empty(), "3-byte run should be excluded with min=4");
    }

    #[test]
    fn run_exactly_at_min_shared_is_included() {
        let a = b"ABCD_XXXXXXXX";
        let b = b"ABCD!YYYYYYYY";
        let runs = find_shared_runs(a, &[b], 4);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0], 0..4);
    }

    #[test]
    fn single_member_returns_entire_slice() {
        let a = b"anything at all";
        let runs = find_shared_runs(a, &[], 4);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0], 0..a.len());
    }

    #[test]
    fn single_member_too_short_returns_empty() {
        let a = b"abc";
        let runs = find_shared_runs(a, &[], 4);
        assert!(runs.is_empty());
    }

    #[test]
    fn empty_reference_returns_empty() {
        let runs = find_shared_runs(b"", &[b"ABCD"], 4);
        assert!(runs.is_empty());
    }

    #[test]
    fn multiple_separate_runs() {
        // Bytes 0..5 match, 5..8 differ, 8..13 match.
        let a = b"AAAAA___BBBBB";
        let b = b"AAAAA!!!BBBBB";
        let runs = find_shared_runs(a, &[b], 4);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0], 0..5);
        assert_eq!(runs[1], 8..13);
    }

    #[test]
    fn three_members_must_all_agree() {
        // Position 4 differs in the third member only.
        let a = b"AAAAXbbbbb";
        let b = b"AAAAXbbbbb";
        let c = b"AAAAYbbbbb"; // differs at index 4
        let runs = find_shared_runs(a, &[b, c], 4);
        // Bytes 0..4 are shared; byte 4 is not; bytes 5..10 are shared.
        assert!(runs.iter().any(|r| *r == (0..4)));
        assert!(runs.iter().any(|r| *r == (5..10)));
    }

    #[test]
    fn comparison_limited_to_shortest_other() {
        // `other` is shorter than `reference`; positions beyond `other.len()`
        // should not be included in any run.
        let a = b"ABCDEFGH";
        let b = b"ABCD"; // length 4
        let runs = find_shared_runs(a, &[b], 4);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0], 0..4); // capped at b.len()
    }
}
