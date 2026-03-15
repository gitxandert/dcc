// CLI argument parsing and dispatch.

use std::fs::File;

use crate::svs::parser::parse_svs_file;

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
        Some(cmd) => Err(format!("unknown command: {cmd}\nusage: dcc inspect <file>")),
        None => Err("usage: dcc inspect <file>".to_string()),
    }
}

fn cmd_inspect(path: &str) -> Result<(), String> {
    use crate::svs::tiff::{BYTE_ORDER_BE, BYTE_ORDER_LE};
    use std::io::Read;

    let file_len = std::fs::metadata(path)
        .map_err(|e| format!("{path}: {e}"))?
        .len();

    let mut f = File::open(path).map_err(|e| format!("{path}: {e}"))?;

    // Determine byte order from the BOM (first 2 bytes).  Works for both
    // Classic TIFF (magic 42) and BigTIFF (magic 43); parse_svs_file handles
    // the rest and will re-seek to 0 internally.
    let mut bom_buf = [0u8; 2];
    f.read_exact(&mut bom_buf).map_err(|e| format!("{path}: {e}"))?;
    let bom = u16::from_le_bytes(bom_buf);
    let bo_label = match bom {
        BYTE_ORDER_LE => "little-endian",
        BYTE_ORDER_BE => "big-endian",
        _ => return Err(format!("{path}: unrecognised byte-order mark: 0x{bom:04X}")),
    };

    let svs =
        parse_svs_file(&mut f, path.into(), file_len).map_err(|e| format!("{path}: {e}"))?;

    println!("file:       {path}");
    println!("byte order: {bo_label}");
    println!("IFD count:  {}", svs.ifds.len());

    for ifd in &svs.ifds {
        let tile_info = match (ifd.tile_width, ifd.tile_height) {
            (Some(tw), Some(th)) => {
                format!("  tile {}×{}  tiles: {}", tw, th, ifd.data_units.len())
            }
            _ => "  strip  tiles: 0".to_string(),
        };
        println!("  IFD[{}]  {}×{}{}", ifd.index, ifd.width, ifd.height, tile_info);
    }

    Ok(())
}
