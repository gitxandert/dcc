// Structural profile derived from SVS metadata — no payload I/O required.
//
// A `FileProfile` captures everything that describes the shape of an SVS file:
// IFD count, per-IFD dimensions, compression, layout, and description tokens.
// It is built directly from a `SvsFile` (which only reads TIFF headers and IFD
// tags) so the entire corpus can be profiled with minimal I/O.

use std::collections::BTreeSet;
use std::path::PathBuf;

use crate::svs::layout::{AssociatedImageKind, SvsFile};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Structural profile of one IFD within a file.
#[derive(Debug, Clone)]
pub struct IfdProfile {
    /// Zero-based index in the IFD chain.
    pub index: usize,
    pub width: u32,
    pub height: u32,
    pub compression: Option<u16>,
    /// `true` when the image is tile-organised, `false` for strip-organised.
    pub is_tiled: bool,
    pub tile_width: Option<u32>,
    pub tile_height: Option<u32>,
    /// Number of data units (tiles or strips) recorded in this IFD.
    pub unit_count: usize,
    /// Human-readable role string for associated images: "label", "macro",
    /// "thumbnail".  `None` for pyramid levels.
    pub role: Option<String>,
    /// Raw ImageDescription string, if present.
    pub description: Option<String>,
}

/// Structural profile of one SVS file.
///
/// Built entirely from parsed TIFF/IFD metadata — no tile or strip bytes are
/// read.  This is the primary input to corpus statistics and structural
/// pairwise similarity.
#[derive(Debug, Clone)]
pub struct FileProfile {
    pub file_id: usize,
    pub path: PathBuf,
    /// Total byte length of the file on disk.
    pub file_size: u64,
    pub ifd_count: usize,
    pub ifds: Vec<IfdProfile>,
    /// Union of description tokens extracted from all IFDs in this file.
    pub description_tokens: BTreeSet<String>,
    /// Preamble (text before the first `|`) from the first IFD that has a
    /// description, if any.  This is the vendor/scanner identifier line in
    /// most SVS files.
    pub description_preamble: Option<String>,
}

/// A compact string that uniquely identifies the structural shape of a file.
///
/// Two files have the same signature if and only if they have identical:
///   - IFD count
///   - Per-IFD: width, height, compression, layout (tiled/strip), tile size
///
/// Used for grouping files by exact structural equivalence.
pub type StructuralSignature = String;

// ---------------------------------------------------------------------------
// Description helpers
// ---------------------------------------------------------------------------

/// Tokenise an SVS `ImageDescription` value into lowercase words.
///
/// SVS descriptions are typically pipe-delimited key-value strings:
///
///   `Aperio Image Library v11.2.1|AppMag = 20|MPP = 0.4952|...`
///
/// Strategy:
///   1. Split on `|`, `;`, `\n`, `\r`.
///   2. From each segment, take the key side of `key = value` pairs.
///   3. Split remaining text on whitespace and punctuation.
///   4. Lower-case, drop tokens < 2 chars or that are purely numeric.
pub fn tokenise_description(desc: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    for segment in desc.split(['|', ';', '\n', '\r']) {
        // For "Key = Value" pairs, keep only the key.
        let key_part = segment.split('=').next().unwrap_or(segment);
        for word in key_part.split_whitespace() {
            let t: String = word
                .chars()
                .filter(|c| c.is_alphanumeric())
                .collect::<String>()
                .to_lowercase();
            if t.len() >= 2 && !t.chars().all(|c| c.is_ascii_digit()) {
                tokens.insert(t);
            }
        }
    }
    tokens
}

fn role_label(kind: &AssociatedImageKind) -> &'static str {
    match kind {
        AssociatedImageKind::Label => "label",
        AssociatedImageKind::Macro => "macro",
        AssociatedImageKind::Thumbnail => "thumbnail",
    }
}

// ---------------------------------------------------------------------------
// Profile builder
// ---------------------------------------------------------------------------

/// Build a `FileProfile` from an already-parsed `SvsFile`.
///
/// No additional I/O is performed — all information comes from the in-memory
/// `SvsFile` produced by `parse_svs_file`.
pub fn build_profile(file_id: usize, svs: &SvsFile) -> FileProfile {
    let mut all_tokens: BTreeSet<String> = BTreeSet::new();
    let mut preamble: Option<String> = None;
    let mut ifds: Vec<IfdProfile> = Vec::new();

    for ifd in &svs.ifds {
        let desc = ifd.description.clone();

        if let Some(ref d) = desc {
            // Tokens from every IFD.
            all_tokens.extend(tokenise_description(d));
            // Preamble from the first IFD that has a description.
            if preamble.is_none() {
                let pre = d.split('|').next().unwrap_or(d.as_str()).trim().to_string();
                if !pre.is_empty() {
                    preamble = Some(pre);
                }
            }
        }

        ifds.push(IfdProfile {
            index: ifd.index,
            width: ifd.width,
            height: ifd.height,
            compression: ifd.compression,
            is_tiled: ifd.tile_width.is_some(),
            tile_width: ifd.tile_width,
            tile_height: ifd.tile_height,
            unit_count: ifd.data_units.len(),
            role: ifd.associated_image.as_ref().map(|r| role_label(r).to_string()),
            description: desc,
        });
    }

    FileProfile {
        file_id,
        path: svs.path.clone(),
        file_size: svs.raw_len,
        ifd_count: svs.ifds.len(),
        ifds,
        description_tokens: all_tokens,
        description_preamble: preamble,
    }
}

// ---------------------------------------------------------------------------
// Structural signature
// ---------------------------------------------------------------------------

/// Produce the structural signature for a file profile.
///
/// Format (example, 3-IFD file):
///   `n=3 [0:40000x30000/c=7/tiled=256x256] [1:20000x15000/c=7/tiled=256x256] [2:strip]`
pub fn structural_signature(p: &FileProfile) -> StructuralSignature {
    let mut s = format!("n={}", p.ifd_count);
    for ifd in &p.ifds {
        let layout = match (ifd.tile_width, ifd.tile_height) {
            (Some(tw), Some(th)) => format!("tiled={}x{}", tw, th),
            _ => "strip".to_string(),
        };
        let comp = ifd.compression.map(|c| c.to_string()).unwrap_or_else(|| "?".to_string());
        s.push_str(&format!(
            " [{}:{}x{}/c={}/{}]",
            ifd.index, ifd.width, ifd.height, comp, layout
        ));
    }
    s
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenise_drops_short_and_numeric() {
        let tokens = tokenise_description("Aperio|AppMag = 20|MPP = 0.4952");
        assert!(tokens.contains("aperio"));
        assert!(tokens.contains("appmag"));
        assert!(tokens.contains("mpp"));
        // "20" is purely numeric → dropped.
        assert!(!tokens.contains("20"));
        // Single-char tokens dropped.
        assert!(!tokens.contains("v"));
    }

    #[test]
    fn tokenise_pipe_delimited() {
        let tokens = tokenise_description("Scanner|Label|Macro");
        assert!(tokens.contains("scanner"));
        assert!(tokens.contains("label"));
        assert!(tokens.contains("macro"));
    }

    #[test]
    fn tokenise_empty_string() {
        let tokens = tokenise_description("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenise_key_value_keeps_key() {
        let tokens = tokenise_description("AppMag = 20");
        assert!(tokens.contains("appmag"), "key should be kept");
    }
}
