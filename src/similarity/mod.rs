// Similarity analysis for SVS corpora.
//
// Structural analysis (default):
//
//   profile    — FileProfile: structural metadata from parsed SvsFile, no I/O
//   corpus     — corpus-level frequency statistics across profiles
//   structural — pairwise structural similarity scoring
//
// Payload-based analysis (Phase 2/3, used by packing pipeline):
//
//   overlap    — FileEntry accounting and confirmed-group overlap accumulation
//   metric     — pairwise similarity scoring from confirmed byte matches
//   graph      — thresholded similarity graph
//   cluster    — connected-components clustering

pub mod profile;
pub mod corpus;
pub mod structural;

pub mod overlap;
pub mod metric;
pub mod graph;
pub mod cluster;
