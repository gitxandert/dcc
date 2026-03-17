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

        let mut bar = String::from("|\x1b[7m");
        for i in 0..Self::BAR_WIDTH {
            if i < filled {
                bar.push(' ');
            } else if i == filled && !done {
                bar.push_str("\x1b[0m");
            } else {
                bar.push(' ');
            }
        }
        bar.push_str("\x1b[0m|");
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
        Some("fingerprint") => {
            let mut json = false;
            let mut file: Option<String> = None;
            for arg in args {
                match arg.as_str() {
                    "--json" => json = true,
                    _ if file.is_none() => file = Some(arg),
                    _ => return Err("usage: dcc fingerprint [--json] <file>".to_string()),
                }
            }
            let path = file
                .ok_or_else(|| "usage: dcc fingerprint [--json] <file>".to_string())?;
            cmd_fingerprint(&path, json)
        }
        Some("similarity") => {
            let mut min_score: Option<f64> = None;
            let mut top_pairs: Option<usize> = None;
            let mut json = false;
            let mut dir: Option<String> = None;
            let mut pending: Option<&str> = None;
            let usage = "usage: dcc similarity [--min-score F] [--top N] [--json] <dir>";
            for arg in args {
                if let Some(flag) = pending.take() {
                    match flag {
                        "--min-score" => {
                            min_score = Some(arg.parse::<f64>().map_err(|_| {
                                "--min-score requires a float".to_string()
                            })?);
                        }
                        "--top" => {
                            let n = arg.parse::<usize>().map_err(|_| {
                                "--top requires a positive integer".to_string()
                            })?;
                            if n == 0 {
                                return Err("--top must be at least 1".to_string());
                            }
                            top_pairs = Some(n);
                        }
                        _ => unreachable!(),
                    }
                } else {
                    match arg.as_str() {
                        "--json" => json = true,
                        "--min-score" | "--top" => {
                            pending = Some(match arg.as_str() {
                                "--min-score" => "--min-score",
                                "--top" => "--top",
                                _ => unreachable!(),
                            })
                        }
                        _ if dir.is_none() => dir = Some(arg),
                        _ => return Err(usage.to_string()),
                    }
                }
            }
            if pending.is_some() {
                return Err(format!("{}: flag requires a value", pending.unwrap()));
            }
            let dir = dir.ok_or_else(|| usage.to_string())?;
            cmd_similarity(&dir, min_score, top_pairs, json)
        }
        Some("arch") => {
            let dir = args
                .next()
                .ok_or_else(|| "usage: dcc arch <dir>".to_string())?;
            cmd_arch(&dir)
        }
        Some(cmd) => Err(format!(
            "unknown command: {cmd}\nusage: dcc inspect <file> | dcc stats <dir> | dcc fingerprint [--json] <file> | dcc similarity [--min-score F] [--top N] [--json] <dir> | dcc arch <dir>"
        )),
        None => Err("usage: dcc inspect <file> | dcc stats <dir> | dcc fingerprint [--json] <file> | dcc similarity [--min-score F] [--top N] [--json] <dir> | dcc arch <dir>".to_string()),
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

fn cmd_fingerprint(path: &str, json: bool) -> Result<(), String> {
    use crate::fingerprint::manifest::build_manifest_from_reader_cb;

    let file_len = std::fs::metadata(path)
        .map_err(|e| format!("{path}: {e}"))?
        .len();

    let name = std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(path)
        .to_string();

    let mut f = File::open(path).map_err(|e| format!("{path}: {e}"))?;

    // Progress bar is initialised on the first callback invocation, once the
    // total unit count is known.
    let mut pb: Option<ProgressBar> = None;

    let manifest = build_manifest_from_reader_cb(
        &mut f,
        path.into(),
        0,
        file_len,
        |done, total| {
            let bar = pb.get_or_insert_with(|| {
                let b = ProgressBar::new(total);
                b.init();
                b
            });
            bar.update(done, &name, None);
        },
    )
    .map_err(|e| format!("{path}: {e}"))?;

    if let Some(ref bar) = pb {
        bar.finish();
    }

    if json {
        let out_name = format!("fingerprint_{name}.json");
        let mut out_file = File::create(&out_name)
            .map_err(|e| format!("{out_name}: {e}"))?;
        write_fingerprint_json(&mut out_file, path, &manifest.units)
            .map_err(|e| format!("{out_name}: {e}"))?;
        println!("wrote {out_name}");
    } else {
        print_fingerprint_text(path, &manifest.units);
    }

    Ok(())
}

fn kind_label(kind: crate::svs::layout::DataUnitKind) -> &'static str {
    use crate::svs::layout::DataUnitKind;
    match kind {
        DataUnitKind::Tile => "tile",
        DataUnitKind::Strip => "strip",
        DataUnitKind::MetadataBlob => "metadata",
        DataUnitKind::AssociatedImage => "associated",
    }
}

fn print_fingerprint_text(path: &str, units: &[crate::fingerprint::manifest::UnitRecord]) {
    println!("file:   {path}");
    println!("units:  {}", units.len());

    if units.is_empty() {
        return;
    }

    let sep = "─".repeat(106);
    println!("\n{sep}");
    println!(
        "{:>6}  {:>4}  {:>5}  {:<10}  {:>14}  {:>12}  {:>16}  {}",
        "idx", "ifd", "unit", "kind", "offset", "length", "coarse_fp", "compression"
    );
    println!("{sep}");

    for (i, u) in units.iter().enumerate() {
        let coarse = u
            .coarse_fp
            .map(|v| format!("{v:016x}"))
            .unwrap_or_else(|| "-".to_string());
        let comp = u
            .compression
            .map(|c| compression_label(c))
            .unwrap_or_else(|| "none".to_string());
        println!(
            "{:>6}  {:>4}  {:>5}  {:<10}  {:>14}  {:>12}  {:>16}  {}",
            i,
            u.ifd_index,
            u.unit_index,
            kind_label(u.kind),
            u.offset,
            u.length,
            coarse,
            comp,
        );
    }
    println!("{sep}");
}

fn write_fingerprint_json(
    w: &mut dyn std::io::Write,
    path: &str,
    units: &[crate::fingerprint::manifest::UnitRecord],
) -> std::io::Result<()> {
    // Hand-rolled JSON — avoids a serde dependency.
    writeln!(w, "{{")?;
    writeln!(w, "  \"path\": \"{}\",", path.replace('\\', "\\\\").replace('"', "\\\""))?;
    writeln!(w, "  \"unit_count\": {},", units.len())?;
    writeln!(w, "  \"units\": [")?;

    for (i, u) in units.iter().enumerate() {
        let comma = if i + 1 < units.len() { "," } else { "" };
        let coarse = u
            .coarse_fp
            .map(|v| format!("{v:016x}"))
            .unwrap_or_default();
        let comp = u
            .compression
            .map(|c| c.to_string())
            .unwrap_or_else(|| "null".to_string());
        let role = u
            .role
            .as_deref()
            .map(|r| format!("\"{}\"", r))
            .unwrap_or_else(|| "null".to_string());
        writeln!(
            w,
            "    {{\"idx\": {}, \"ifd_index\": {}, \"unit_index\": {}, \"kind\": \"{}\", \"offset\": {}, \"length\": {}, \"compression\": {}, \"role\": {}, \"coarse_fp\": \"{}\"}}{}",
            i,
            u.ifd_index,
            u.unit_index,
            kind_label(u.kind),
            u.offset,
            u.length,
            comp,
            role,
            coarse,
            comma,
        )?;
    }

    writeln!(w, "  ]")?;
    writeln!(w, "}}")?;
    Ok(())
}

fn write_structural_json(
    w: &mut dyn std::io::Write,
    profiles: &[crate::similarity::profile::FileProfile],
    scores: &[crate::similarity::structural::StructuralScore],
) -> std::io::Result<()> {
    use crate::similarity::structural::formula_description;

    let name_of = |file_id: usize| -> &str {
        profiles
            .iter()
            .find(|p| p.file_id == file_id)
            .and_then(|p| p.path.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or("?")
    };

    writeln!(w, "{{")?;
    writeln!(w, "  \"formula\": \"{}\",", formula_description())?;
    writeln!(w, "  \"pair_count\": {},", scores.len())?;
    writeln!(w, "  \"pairs\": [")?;
    for (i, s) in scores.iter().enumerate() {
        let comma = if i + 1 < scores.len() { "," } else { "" };
        let name_a = name_of(s.file_id_a);
        let name_b = name_of(s.file_id_b);
        writeln!(
            w,
            "    {{\"file_a\": \"{name_a}\", \"file_b\": \"{name_b}\", \
             \"score\": {:.6}, \"ifd_count\": {:.4}, \"structure\": {:.4}, \"description\": {:.4}}}{}",
            s.score, s.ifd_count_score, s.structure_score, s.description_score, comma,
        )?;
    }
    writeln!(w, "  ]")?;
    writeln!(w, "}}")?;
    Ok(())
}

fn cmd_similarity(
    dir: &str,
    min_score: Option<f64>,
    top_pairs: Option<usize>,
    json: bool,
) -> Result<(), String> {
    use crate::similarity::corpus::compute_corpus_stats;
    use crate::similarity::profile::build_profile;
    use crate::similarity::structural::{formula_description, score_all_pairs};

    // ── collect .svs paths ───────────────────────────────────────────────────
    let read_dir = std::fs::read_dir(dir).map_err(|e| format!("{dir}: {e}"))?;
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

    // ── parse all SVS files (metadata only) ─────────────────────────────────
    let total = paths.len();
    let mut bar = ProgressBar::new(total);
    bar.init();

    let mut profiles = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    for (i, path) in paths.iter().enumerate() {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?").to_string();
        let label = format!("{}/{}: {name}", i + 1, total);
        bar.update(i, &label, None);

        let result = std::fs::metadata(path)
            .map_err(|e| format!("{name}: {e}"))
            .and_then(|meta| {
                File::open(path)
                    .map_err(|e| format!("{name}: {e}"))
                    .and_then(|mut f| {
                        parse_svs_file(&mut f, path.clone(), meta.len())
                            .map_err(|e| format!("{name}: {e}"))
                    })
            });

        match result {
            Ok(svs) => profiles.push(build_profile(i, &svs)),
            Err(e) => warnings.push(e),
        }
    }

    bar.update(total, "done", None);
    bar.finish();

    if profiles.is_empty() {
        return Err(format!("{dir}: no files could be parsed"));
    }

    // ── compute statistics ───────────────────────────────────────────────────
    let stats = compute_corpus_stats(&profiles);
    let mut scores = score_all_pairs(&profiles);

    let sep = "─".repeat(79);

    // =========================================================================
    // Section 1: Corpus overview
    // =========================================================================
    println!("directory:  {dir}");
    println!("files:      {}", profiles.len());
    println!("file sizes: {}  –  {}", format_size(stats.file_size_min), format_size(stats.file_size_max));

    println!("\n{sep}");
    println!("IFD count distribution");
    println!("{sep}");
    for (count, freq) in &stats.ifd_count_dist {
        let bar_len = (freq * 20 / profiles.len()).max(if *freq > 0 { 1 } else { 0 });
        let bar = "#".repeat(bar_len);
        let pct = 100.0 * (*freq as f64) / (profiles.len() as f64);
        println!("  {count:>3} IFDs: {bar:<20}  {freq}/{} files  ({pct:.0}%)", profiles.len());
    }

    // =========================================================================
    // Section 2: IFD position analysis
    // =========================================================================
    println!("\n{sep}");
    println!("IFD position analysis");
    println!("{sep}");
    for pos in &stats.ifd_positions {
        println!(
            "  IFD {}  ({}/{} files)",
            pos.position, pos.file_count, profiles.len()
        );

        // Width.
        print!("    width:        ");
        print_dist_compact(&pos.width_dist, pos.file_count, |v| format!("{v}px"));
        println!();

        // Height.
        print!("    height:       ");
        print_dist_compact(&pos.height_dist, pos.file_count, |v| format!("{v}px"));
        println!();

        // Compression.
        print!("    compression:  ");
        print_dist_compact(&pos.compression_dist, pos.file_count, |v| {
            match v {
                Some(code) => compression_label(*code),
                None => "absent".to_string(),
            }
        });
        println!();

        // Layout / tile size.
        print!("    layout:       ");
        print_dist_compact(&pos.tile_size_dist, pos.file_count, |v| match v {
            (Some(tw), Some(th)) => format!("tiled {tw}×{th}"),
            _ => "strip-organised".to_string(),
        });
        println!();
    }

    // =========================================================================
    // Section 3: Description patterns
    // =========================================================================
    println!("\n{sep}");
    println!("description patterns");
    println!("{sep}");

    // Token frequency: tokens shared by >=2 files.
    let common = stats.common_tokens(2);
    if !common.is_empty() {
        println!();
        println!("  most common description tokens (≥2 files, sorted by frequency):");
        // Show top 20 to keep output readable.
        for (token, count) in common.iter().take(20) {
            println!("    {:>2}/{} files  {}", count, profiles.len(), token);
        }
        if common.len() > 20 {
            println!("    ... ({} more tokens omitted)", common.len() - 20);
        }
    }

    // Tokens unique to a single file (anomalies / per-slide metadata).
    let unique_tokens: Vec<&str> = stats.token_freq
        .iter()
        .filter(|&(_, &v)| v == 1)
        .map(|(k, _)| k.as_str())
        .collect();
    if !unique_tokens.is_empty() {
        let shown: Vec<&str> = unique_tokens.iter().copied().take(10).collect();
        let rest = unique_tokens.len().saturating_sub(10);
        let sample = shown.join(", ");
        if rest > 0 {
            println!();
            println!("  tokens unique to one file ({} total): {sample}, +{rest} more", unique_tokens.len());
        } else {
            println!();
            println!("  tokens unique to one file ({}): {sample}", unique_tokens.len());
        }
    }
    
    // =========================================================================
    // Section 4: Metadata archetypes
    // =========================================================================
    {
        use crate::similarity::archetype::derive_archetypes;
        let nodes = derive_archetypes(&profiles);
        print_archetypes(&nodes, profiles.len());
    }

    // =========================================================================
    // Section 7: Pairwise structural similarity
    // =========================================================================
    // Apply filters before deciding output path.
    if let Some(thresh) = min_score {
        scores.retain(|s| s.score >= thresh);
    }
    if let Some(n) = top_pairs {
        scores.truncate(n);
    }

    if json {
        let dir_name = std::path::Path::new(dir)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("corpus");
        let out_name = format!("similarity_{dir_name}.json");
        let mut out_file = File::create(&out_name)
            .map_err(|e| format!("{out_name}: {e}"))?;
        write_structural_json(&mut out_file, &profiles, &scores)
            .map_err(|e| format!("{out_name}: {e}"))?;
        println!("\nwrote {out_name}");
    } else {
        let filter_note = min_score
            .map(|t| format!("  (min-score ≥ {t:.3})"))
            .unwrap_or_default();
        let top_note = top_pairs
            .map(|n| format!("  (top {n})"))
            .unwrap_or_default();

        println!("\n{sep}");
        println!("pairwise structural similarity{filter_note}{top_note}");
        println!("formula: {}", formula_description());
        println!("{sep}");
        println!(
            "  {:<6}  {:<5}  {:<6}  {:<5}  {:<28}  {}",
            "score", "ifd", "struct", "desc", "file_a", "file_b"
        );
        println!("  {}", "─".repeat(73));

        if scores.is_empty() {
            println!("  (no pairs match the current filters)");
        } else {
            for s in &scores {
                let name_a = profiles
                    .iter()
                    .find(|p| p.file_id == s.file_id_a)
                    .and_then(|p| p.path.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("?");
                let name_b = profiles
                    .iter()
                    .find(|p| p.file_id == s.file_id_b)
                    .and_then(|p| p.path.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("?");
                println!(
                    "  {:.4}  {:.3}  {:.4}  {:.3}  {:<28.28}  {}",
                    s.score, s.ifd_count_score, s.structure_score, s.description_score,
                    name_a, name_b,
                );
            }
        }
    }

    // ── warnings ─────────────────────────────────────────────────────────────
    if !warnings.is_empty() {
        println!("\n─── warnings {}", "─".repeat(66));
        for w in &warnings {
            println!("  {w}");
        }
    }

    Ok(())
}

fn cmd_arch(dir: &str) -> Result<(), String> {
    use crate::arch::build_archetype_bytes;
    use crate::similarity::archetype::derive_archetypes;
    use crate::similarity::profile::build_profile;

    // ── collect .svs paths ───────────────────────────────────────────────────
    let read_dir = std::fs::read_dir(dir).map_err(|e| format!("{dir}: {e}"))?;
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

    // ── parse all SVS files (metadata only) ─────────────────────────────────
    let total = paths.len();
    let mut bar = ProgressBar::new(total);
    bar.init();

    let mut profiles = Vec::new();
    let mut parse_warnings: Vec<String> = Vec::new();

    for (i, path) in paths.iter().enumerate() {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?").to_string();
        bar.update(i, &format!("{}/{}: {name}", i + 1, total), None);

        let result = std::fs::metadata(path)
            .map_err(|e| format!("{name}: {e}"))
            .and_then(|meta| {
                File::open(path)
                    .map_err(|e| format!("{name}: {e}"))
                    .and_then(|mut f| {
                        parse_svs_file(&mut f, path.clone(), meta.len())
                            .map_err(|e| format!("{name}: {e}"))
                    })
            });

        match result {
            Ok(svs) => profiles.push(build_profile(i, &svs)),
            Err(e) => parse_warnings.push(e),
        }
    }

    bar.update(total, "done", None);
    bar.finish();

    if profiles.is_empty() {
        return Err(format!("{dir}: no files could be parsed"));
    }

    // ── derive archetypes and display summary ────────────────────────────────
    let nodes = derive_archetypes(&profiles);

    print_archetypes(&nodes, profiles.len());

    if nodes.is_empty() {
        return Ok(());
    }

    let dir_name = std::path::Path::new(dir)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("corpus");

    // ── encode and write each archetype ─────────────────────────────────────
    let mut arch_warnings: Vec<String> = Vec::new();
    let mut written = 0usize;

    for node in &nodes {
        let arch = &node.archetype;
        let out_name = format!("{dir_name}_archetype_{}.arch", arch.id);

        match build_archetype_bytes(arch, &profiles) {
            Ok(bytes) => {
                std::fs::write(&out_name, &bytes)
                    .map_err(|e| format!("{out_name}: {e}"))?;
                println!(
                    "wrote {}  ({} members, {} bytes)",
                    out_name,
                    arch.member_count(),
                    bytes.len(),
                );
                written += 1;
            }
            Err(e) => {
                arch_warnings.push(format!("archetype {}: {e}", arch.id));
            }
        }
    }

    println!("\n{written}/{} archetypes written", nodes.len());

    // ── warnings ─────────────────────────────────────────────────────────────
    let all_warnings: Vec<&String> = parse_warnings.iter().chain(arch_warnings.iter()).collect();
    if !all_warnings.is_empty() {
        println!("\n─── warnings {}", "─".repeat(66));
        for w in all_warnings {
            println!("  {w}");
        }
    }

    Ok(())
}

/// Print the metadata archetype summary section to stdout.
///
/// Shared between `cmd_similarity` and `cmd_arch`.
fn print_archetypes(
    nodes: &[crate::similarity::archetype::ArchetypeNode],
    file_count: usize,
) {
    let sep = "─".repeat(79);

    let archetype_word = if nodes.len() == 1 { "archetype" } else { "archetypes" };
    println!("\n{sep}");
    println!("metadata archetypes  ({} {archetype_word}, {file_count} files)", nodes.len());
    println!("{sep}");

    if nodes.is_empty() {
        println!("  (none)");
        return;
    }

    let label_of = |id: usize| -> String {
        let ch = (b'A' + (id % 26) as u8) as char;
        if id < 26 { ch.to_string() } else { format!("{ch}{}", id / 26) }
    };

    let first_with_tokens =
        |tokens: &std::collections::BTreeSet<String>, exclude_id: usize| -> Option<usize> {
            nodes
                .iter()
                .find(|n| n.archetype.id != exclude_id && &n.archetype.common_tokens == tokens)
                .map(|n| n.archetype.id)
        };

    for node in nodes {
        let a = &node.archetype;
        let lbl = label_of(a.id);
        let nf = a.member_count();
        let files_word = if nf == 1 { "file" } else { "files" };

        println!("  archetype {}  ({nf} {files_word}, {} IFDs)", lbl, a.skeleton.ifd_count);

        // IFD structure — collapse consecutive identical slots into ranges.
        let skel = &a.skeleton;
        if !skel.per_ifd.is_empty() {
            struct Run {
                start: usize,
                end: usize,
                comp: String,
                layout: String,
                role: Option<String>,
            }
            let mut runs: Vec<Run> = Vec::new();
            for (pos, ifd) in skel.per_ifd.iter().enumerate() {
                let comp = match ifd.compression {
                    Some(c) => compression_label(c),
                    None => "untagged".to_string(),
                };
                let layout = match (ifd.tile_width, ifd.tile_height) {
                    (Some(tw), Some(th)) => format!("tiled {tw}\u{d7}{th}"),
                    _ => "strip".to_string(),
                };
                let role = ifd.role.clone();
                if let Some(last) = runs.last_mut() {
                    if last.comp == comp && last.layout == layout && last.role == role {
                        last.end = pos;
                        continue;
                    }
                }
                runs.push(Run { start: pos, end: pos, comp, layout, role });
            }
            for r in &runs {
                let pos_label = if r.start == r.end {
                    format!("IFD {}", r.start)
                } else {
                    format!("IFD {}\u{2013}{}", r.start, r.end)
                };
                let role_note = r.role.as_deref().map(|s| format!("  [{s}]")).unwrap_or_default();
                println!("    {pos_label}:  {}  {}{role_note}", r.comp, r.layout);
            }
        }

        // Common tokens — full list, or "same as X" back-reference.
        let n_tok = a.common_tokens.len();
        let n_var = a.variable_tokens.len();
        if n_tok == 0 && n_var == 0 {
            println!("    tokens: (none)");
        } else {
            let same_as = first_with_tokens(&a.common_tokens, a.id);
            if let Some(other_id) = same_as {
                println!("    tokens ({n_tok} common): [same as {}]", label_of(other_id));
            } else {
                let list: Vec<&str> = a.common_tokens.iter().map(String::as_str).collect();
                let shown = list.iter().take(12).cloned().collect::<Vec<_>>().join(", ");
                let rest = n_tok.saturating_sub(12);
                if rest > 0 {
                    println!("    tokens ({n_tok} common): {shown}, +{rest} more");
                } else {
                    println!("    tokens ({n_tok} common): {shown}");
                }
            }
            if n_var > 0 {
                println!("    tokens ({n_var} variable, not shown)");
            }
        }
    }
}

/// Print a compact distribution summary to stdout (no newline appended).
///
/// Shows the top values sorted by descending count.  If there is only one
/// distinct value and it covers all entries, prints "value (all N)".
/// Otherwise lists top entries as "v1 (k1), v2 (k2), ..." with a "+N more"
/// tail when there are many.
fn print_dist_compact<K, F>(dist: &std::collections::BTreeMap<K, usize>, total: usize, label: F)
where
    K: Ord,
    F: Fn(&K) -> String,
{
    if dist.is_empty() {
        print!("(none)");
        return;
    }
    // Sort by descending count.
    let mut entries: Vec<(&K, usize)> = dist.iter().map(|(k, &v)| (k, v)).collect();
    entries.sort_by(|a, b| b.1.cmp(&a.1));

    if entries.len() == 1 {
        let (k, count) = entries[0];
        if count == total {
            print!("{} (all {})", label(k), total);
        } else {
            print!("{} ({}/{})", label(k), count, total);
        }
        return;
    }

    // Show top 3, then "+N more".
    const SHOW: usize = 3;
    let shown = entries.len().min(SHOW);
    let parts: Vec<String> = entries[..shown]
        .iter()
        .map(|(k, count)| format!("{} ({}/{})", label(k), count, total))
        .collect();
    print!("{}", parts.join("  |  "));
    if entries.len() > SHOW {
        print!("  +{} more", entries.len() - SHOW);
    }
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
