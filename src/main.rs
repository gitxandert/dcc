fn main() {
    if let Err(e) = dcc::cli::run(std::env::args()) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
