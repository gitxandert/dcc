// CLI argument parsing and dispatch.

use std::fs::File;
use std::io::Write;

use crate::svs::parser::parse_svs_file;

// ---------------------------------------------------------------------------
// Progress bar
// ---------------------------------------------------------------------------

/// A simple in-place progress bar written with raw ANSI escape sequences.
///
/// Layout (redrawn in place on every update):
///
///   [=============>          ] 5/12  filename.svs
///   ! warning or error line 1
///   ! warning or error line 2
///   ! warning or error line 3
///
/// Lines are reserved on `init()` and overwritten on each `update()`.
/// On non-TTY stdout the struct is a no-op.
struct ProgressBar {
    total: usize,
    /// Rolling window of recent warning/error messages.
    messages: Vec<String>,
    is_tty: bool,
}

impl ProgressBar {
    const BAR_WIDTH: usize = 38;
    const MAX_MSGS: usize = 3;
    /// Total lines owned by the widget (1 bar + messages).
    const LINES: usize = 1 + Self::MAX_MSGS;

    fn new(total: usize) -> Self {
        // Detect TTY using the POSIX isatty(1) call via a libc-free approach:
        // try to get the terminal size with ioctl TIOCGWINSZ on fd 1.
        // If it fails the fd is not a terminal.
        let is_tty = libc_isatty(1);
        Self { total, messages: Vec::new(), is_tty }
    }

    /// Reserve vertical space and hide the cursor.
    fn init(&self) {
        if !self.is_tty { return; }
        let mut out = std::io::stdout();
        // Hide cursor.
        write!(out, "\x1b[?25l").unwrap();
        // Print the reserved blank lines so we have room to move back into.
        for _ in 0..Self::LINES {
            writeln!(out).unwrap();
        }
        out.flush().unwrap();
    }

    /// Redraw the widget in place.
    fn render(&self, current: usize, label: &str) {
        if !self.is_tty { return; }
        let mut out = std::io::stdout();

        // Move cursor up to the first line of our reserved area.
        write!(out, "\x1b[{}A", Self::LINES).unwrap();

        // Progress bar line.
        let bar = Self::format_bar(current, self.total);
        // Truncate label so the line stays tidy.
        let label = truncate(label, 30);
        write!(out, "\r\x1b[2K{bar}  {label}\n").unwrap();

        // Message lines.
        for i in 0..Self::MAX_MSGS {
            let msg = self.messages.get(i).map(String::as_str).unwrap_or("");
            if msg.is_empty() {
                write!(out, "\r\x1b[2K\n").unwrap();
            } else {
                write!(out, "\r\x1b[2K! {msg}\n").unwrap();
            }
        }

        out.flush().unwrap();
    }

    /// Advance the counter, optionally record a message, and redraw.
    fn update(&mut self, current: usize, label: &str, message: Option<&str>) {
        if let Some(msg) = message {
            if self.messages.len() >= Self::MAX_MSGS {
                self.messages.remove(0);
            }
            self.messages.push(msg.to_string());
        }
        self.render(current, label);
    }

    /// Draw the completed state and restore the cursor.
    fn finish(&self) {
        if !self.is_tty { return; }
        self.render(self.total, "done");
        let mut out = std::io::stdout();
        // Restore cursor.
        write!(out, "\x1b[?25h").unwrap();
        // Blank separator before the stats output.
        writeln!(out).unwrap();
        out.flush().unwrap();
    }

    fn format_bar(current: usize, total: usize) -> String {
        let filled = if total == 0 {
            Self::BAR_WIDTH
        } else {
            (current * Self::BAR_WIDTH / total).min(Self::BAR_WIDTH)
        };
        let done = current >= total;

        let mut bar = String::from("[\x1b[7m");
        for i in 0..Self::BAR_WIDTH {
            if i < filled {
                bar.push(' ');
            } else if i == filled && !done {
                bar.push_str("\x1b[0m");
            } else {
                bar.push(' ');
            }
        }
        bar.push_str("\x1b[0m]");
        format!("{bar} {current}/{total}")
    }
}

/// Returns true if file descriptor `fd` refers to a terminal.
///
/// Uses the raw `isatty` syscall so we have no dependency on libc.
fn libc_isatty(fd: i32) -> bool {
    // SAFETY: isatty is always safe to call with any fd value.
    unsafe extern "C" {
        fn isatty(fd: i32) -> i32;
    }
    unsafe { isatty(fd) != 0 }
}

fn truncate(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

pub fn run(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    // Skip the binary name.
    args.next();

    match args.next().as_deref() {
        Some("inspect") => {
            let path = args
                .next()
                .ok_or_else(|| "usage: dcc inspect <file>".to_string())?;
            cmd_inspect(&path)
        }
        Some("stats") => {
            let dir = args
                .next()
                .ok_or_else(|| "usage: dcc stats <dir>".to_string())?;
            cmd_stats(&dir)
        }
        Some(cmd) => Err(format!(
            "unknown command: {cmd}\nusage: dcc inspect <file> | dcc stats <dir>"
        )),
        None => Err("usage: dcc inspect <file> | dcc stats <dir>".to_string()),
    }
}

fn cmd_inspect(path: &str) -> Result<(), String> {
    use crate::svs::tiff::{BIGTIFF_MAGIC, BYTE_ORDER_BE, BYTE_ORDER_LE, TIFF_MAGIC};
    use std::io::Read;

    let file_len = std::fs::metadata(path)
        .map_err(|e| format!("{path}: {e}"))?
        .len();

    let mut f = File::open(path).map_err(|e| format!("{path}: {e}"))?;

    // Read the first 4 bytes to extract byte-order mark and TIFF magic.
    // parse_svs_file will re-seek to 0 internally.
    let mut hdr_buf = [0u8; 4];
    f.read_exact(&mut hdr_buf).map_err(|e| format!("{path}: {e}"))?;

    let bom = u16::from_le_bytes([hdr_buf[0], hdr_buf[1]]);
    let bo_label = match bom {
        BYTE_ORDER_LE => "little-endian",
        BYTE_ORDER_BE => "big-endian",
        _ => return Err(format!("{path}: unrecognised byte-order mark: 0x{bom:04X}")),
    };

    let magic = match bom {
        BYTE_ORDER_LE => u16::from_le_bytes([hdr_buf[2], hdr_buf[3]]),
        _ => u16::from_be_bytes([hdr_buf[2], hdr_buf[3]]),
    };
    let tiff_label = match magic {
        TIFF_MAGIC => "classic (42)",
        BIGTIFF_MAGIC => "BigTIFF (43)",
        other => return Err(format!("{path}: unsupported TIFF magic: {other}")),
    };

    let svs =
        parse_svs_file(&mut f, path.into(), file_len).map_err(|e| format!("{path}: {e}"))?;

    println!("file:       {path}");
    println!("byte order: {bo_label}");
    println!("tiff:       {tiff_label}");
    println!("IFD count:  {}", svs.ifds.len());

    for ifd in &svs.ifds {
        let org = match (ifd.tile_width, ifd.tile_height) {
            (Some(tw), Some(th)) => {
                format!("tile {}×{}  {} tiles", tw, th, ifd.data_units.len())
            }
            _ => {
                let rps = ifd
                    .rows_per_strip
                    .map(|r| format!("  rows/strip: {r}"))
                    .unwrap_or_default();
                format!("strip{}  {} strips", rps, ifd.data_units.len())
            }
        };
        let comp = ifd
            .compression
            .map(|c| format!("  {}", compression_label(c)))
            .unwrap_or_default();
        let assoc = ifd
            .associated_image
            .as_ref()
            .map(|k| format!("  [associated: {}]", associated_image_label(k)))
            .unwrap_or_default();
        println!("  IFD[{}]  {}×{}  {}{}{}", ifd.index, ifd.width, ifd.height, org, comp, assoc);

        if let Some(desc) = &ifd.description {
            if !desc.is_empty() {
                let display = if desc.len() > 80 {
                    format!("{}…", &desc[..80])
                } else {
                    desc.clone()
                };
                println!("    description: {display}");
            }
        }

        // Print tile/strip offset+length for each data unit (capped to avoid
        // flooding output on large files).
        const UNIT_CAP: usize = 8;
        let units = &ifd.data_units;
        let shown = units.len().min(UNIT_CAP);
        for u in &units[..shown] {
            println!(
                "    unit[{:>4}]  offset={:<12}  length={}",
                u.unit_index, u.offset, u.length
            );
        }
        if units.len() > UNIT_CAP {
            println!("    … {} more units", units.len() - UNIT_CAP);
        }
    }

    Ok(())
}

fn associated_image_label(kind: &crate::svs::layout::AssociatedImageKind) -> &'static str {
    use crate::svs::layout::AssociatedImageKind;
    match kind {
        AssociatedImageKind::Label => "label",
        AssociatedImageKind::Macro => "macro",
        AssociatedImageKind::Thumbnail => "thumbnail",
    }
}

fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn cmd_stats(dir: &str) -> Result<(), String> {
    use crate::svs::layout::{AssociatedImageKind, DataUnitKind};
    use std::collections::BTreeMap;

    // Collect .svs paths.
    let read_dir =
        std::fs::read_dir(dir).map_err(|e| format!("{dir}: {e}"))?;

    let mut paths: Vec<std::path::PathBuf> = read_dir
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("svs") {
                Some(p)
            } else {
                None
            }
        })
        .collect();

    if paths.is_empty() {
        return Err(format!("{dir}: no .svs files found"));
    }

    paths.sort();

    // Per-file parse results plus any warnings.
    struct FileSummary {
        name: String,
        file_size: u64,
        ifd_count: usize,
        pyramid_levels: usize,
        tile_count: u64,
        strip_count: u64,
        assoc_kinds: Vec<AssociatedImageKind>,
        payload_bytes: u64,
        tile_geoms: Vec<(u32, u32)>,
        compressions: Vec<u16>,
        warnings: Vec<String>,
    }

    let mut summaries: Vec<FileSummary> = Vec::new();
    let mut global_warnings: Vec<String> = Vec::new();

    let mut pb = ProgressBar::new(paths.len());
    pb.init();

    for (file_idx, path) in paths.iter().enumerate() {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_string();

        pb.update(file_idx, &name, None);

        let file_size = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(e) => {
                let msg = format!("{name}: stat failed: {e}");
                pb.update(file_idx, &name, Some(&msg));
                global_warnings.push(msg);
                continue;
            }
        };

        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                let msg = format!("{name}: open failed: {e}");
                pb.update(file_idx, &name, Some(&msg));
                global_warnings.push(msg);
                continue;
            }
        };

        let svs = match crate::svs::parser::parse_svs_file(&mut f, path.clone(), file_size) {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("{name}: parse failed: {e}");
                pb.update(file_idx, &name, Some(&msg));
                global_warnings.push(msg);
                continue;
            }
        };

        let mut tile_count: u64 = 0;
        let mut strip_count: u64 = 0;
        let mut payload_bytes: u64 = 0;
        let mut assoc_kinds: Vec<AssociatedImageKind> = Vec::new();
        let mut tile_geoms: Vec<(u32, u32)> = Vec::new();
        let mut compressions: Vec<u16> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();
        let mut pyramid_levels: usize = 0;

        for ifd in &svs.ifds {
            for unit in &ifd.data_units {
                payload_bytes += unit.length;
                match unit.kind {
                    DataUnitKind::Tile => tile_count += 1,
                    DataUnitKind::Strip => strip_count += 1,
                    _ => {}
                }
            }

            match &ifd.associated_image {
                Some(kind) => assoc_kinds.push(kind.clone()),
                None => pyramid_levels += 1,
            }

            if let (Some(tw), Some(th)) = (ifd.tile_width, ifd.tile_height) {
                let geom = (tw, th);
                if !tile_geoms.contains(&geom) {
                    tile_geoms.push(geom);
                }
            }

            if let Some(c) = ifd.compression {
                if !compressions.contains(&c) {
                    compressions.push(c);
                }
            }

            // Warn on unresolved associated image kind (strip IFD with no
            // description clue).  The parser assigns a kind anyway via the
            // area-based fallback, so this only fires if something is truly
            // ambiguous.
            if ifd.associated_image.is_none()
                && ifd.tile_width.is_none()
                && ifd.description.as_deref().map(|d| {
                    !d.to_lowercase().contains("label")
                        && !d.to_lowercase().contains("macro")
                })
                .unwrap_or(true)
                && !ifd.data_units.is_empty()
            {
                // Only warn if this looks like an associated image candidate
                // (strip-organised, description absent or unrecognised).
                // The heuristic: if there are 6+ IFDs, a strip IFD without a
                // description clue is suspicious.
                if svs.ifds.len() >= 4 && ifd.index > 0 {
                    warnings.push(format!(
                        "IFD[{}] strip-organised, associated image kind may be ambiguous",
                        ifd.index
                    ));
                }
            }
        }

        // Surface per-file warnings in the progress bar.
        let first_warning = warnings.first().map(String::as_str);
        pb.update(file_idx + 1, &name, first_warning);

        summaries.push(FileSummary {
            name,
            file_size,
            ifd_count: svs.ifds.len(),
            pyramid_levels,
            tile_count,
            strip_count,
            assoc_kinds,
            payload_bytes,
            tile_geoms,
            compressions,
            warnings,
        });
    }

    pb.finish();

    // ── aggregate ───────────────────────────────────────────────────────────
    let total_size: u64 = summaries.iter().map(|s| s.file_size).sum();
    let total_ifds: usize = summaries.iter().map(|s| s.ifd_count).sum();
    let total_tiles: u64 = summaries.iter().map(|s| s.tile_count).sum();
    let total_strips: u64 = summaries.iter().map(|s| s.strip_count).sum();
    let total_payload: u64 = summaries.iter().map(|s| s.payload_bytes).sum();
    let total_units: u64 = total_tiles + total_strips;

    // Compression code → file count.
    let mut comp_files: BTreeMap<u16, usize> = BTreeMap::new();
    for s in &summaries {
        for &c in &s.compressions {
            *comp_files.entry(c).or_insert(0) += 1;
        }
    }

    // Tile geometry → file count.
    let mut geom_files: BTreeMap<(u32, u32), usize> = BTreeMap::new();
    for s in &summaries {
        for &g in &s.tile_geoms {
            *geom_files.entry(g).or_insert(0) += 1;
        }
    }

    // ── output ──────────────────────────────────────────────────────────────
    println!("directory:    {dir}");
    println!("files:        {}", summaries.len());
    println!("total size:   {}", format_size(total_size));

    // Per-file table.
    let sep = "─".repeat(80);
    println!("\n{sep}");
    println!(
        "{:<28} {:>9}  {:>5}  {:>6}  {:>8}  {:>7}  {}",
        "file", "size", "IFDs", "levels", "tiles", "strips", "assoc"
    );
    println!("{sep}");

    for s in &summaries {
        let assoc_str = s
            .assoc_kinds
            .iter()
            .map(|k| match k {
                AssociatedImageKind::Label => "label",
                AssociatedImageKind::Macro => "macro",
                AssociatedImageKind::Thumbnail => "thumbnail",
            })
            .collect::<Vec<_>>()
            .join(" ");

        println!(
            "{:<28.28} {:>9}  {:>5}  {:>6}  {:>8}  {:>7}  {}",
            s.name,
            format_size(s.file_size),
            s.ifd_count,
            s.pyramid_levels,
            s.tile_count,
            s.strip_count,
            assoc_str,
        );
    }
    println!("{sep}");

    // Compression types.
    println!("\n─── compression types seen {}", "─".repeat(53));
    if comp_files.is_empty() {
        println!("  (none detected)");
    } else {
        for (&code, &count) in &comp_files {
            println!("  {:30} {:>2} file{}", compression_label(code), count, if count == 1 { "" } else { "s" });
        }
    }

    // Tile geometry.
    println!("\n─── tile geometry {}", "─".repeat(62));
    if geom_files.is_empty() {
        println!("  (no tiled IFDs)");
    } else {
        for (&(tw, th), &count) in &geom_files {
            println!("  {:>4}×{:<4}  {:>2} file{}", tw, th, count, if count == 1 { "" } else { "s" });
        }
    }

    // Corpus totals.
    let metadata_overhead = if total_size >= total_payload {
        total_size - total_payload
    } else {
        0
    };
    println!("\n─── corpus totals {}", "─".repeat(62));
    println!("  total IFDs:           {total_ifds}");
    println!("  total data units:     {total_units}");
    println!("    tiles:              {total_tiles}");
    println!("    strips:             {total_strips}");
    println!("  total payload bytes:  {}", format_size(total_payload));
    println!("  total file size:      {}", format_size(total_size));
    println!("  metadata overhead:    {}", format_size(metadata_overhead));

    // Warnings.
    let all_warnings: Vec<String> = summaries
        .iter()
        .flat_map(|s| s.warnings.iter().map(|w| format!("  {}: {}", s.name, w)))
        .chain(global_warnings.iter().map(|w| format!("  {w}")))
        .collect();

    if !all_warnings.is_empty() {
        println!("\n─── warnings {}", "─".repeat(67));
        for w in &all_warnings {
            println!("{w}");
        }
    }

    Ok(())
}

fn compression_label(code: u16) -> String {
    let name = match code {
        1 => "uncompressed",
        5 => "LZW",
        6 => "JPEG (old-style)",
        7 => "JPEG",
        8 => "deflate",
        32773 => "PackBits",
        33003 | 33005 => "JPEG 2000",
        _ => "unknown",
    };
    format!("{name} ({code})")
}
