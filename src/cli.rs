// CLI argument parsing and dispatch.

use std::fs::File;

use crate::svs::parser::parse_svs_file;
use crate::svs::tiff::ByteOrder;

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
    use crate::svs::parser::parse_header;

    let file_len = std::fs::metadata(path)
        .map_err(|e| format!("{path}: {e}"))?
        .len();

    let mut f = File::open(path).map_err(|e| format!("{path}: {e}"))?;

    // Read byte order from header first; parse_svs_file will re-seek to 0.
    let hdr = parse_header(&mut f).map_err(|e| format!("{path}: {e}"))?;
    let bo_label = match hdr.byte_order {
        ByteOrder::LittleEndian => "little-endian",
        ByteOrder::BigEndian => "big-endian",
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
