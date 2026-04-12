mod engine;

use clap::{Parser, Subcommand};
use std::time::Instant;
use std::fs;
use std::io::{Read, Write};
use flate2::write::GzEncoder;
use flate2::Compression;

#[derive(Parser)]
#[command(name = "lumpi", version = "2.0.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Pack { 
        #[arg(short, long)] input: String, 
        #[arg(short, long)] output: String 
    },
    Unpack { 
        #[arg(short, long)] input: String, 
        #[arg(short, long)] output: String 
    },
}

fn run_zstd_benchmark(content: &[u8], level: i32) -> (f64, f64) {
    let start = Instant::now();
    let compressed = zstd::encode_all(content, level).unwrap();
    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
    (compressed.len() as f64, duration_ms)
}

fn calculate_weissman_score(r_lumpi: f64, r_std: f64, t_lumpi_ms: f64, t_std_ms: f64) -> f64 {
    if r_std == 0.0 || t_lumpi_ms == 0.0 || t_std_ms == 0.0 { return 0.0; }
    (r_lumpi / r_std) * ((t_std_ms + 1.0).log10() / (t_lumpi_ms + 1.0).log10())
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Pack { input, output } => {
            println!("[Lumpi Core v2.0] Initializing pipeline...");
            
            let input_metadata = fs::metadata(input).expect("File not found");
            let original_size = input_metadata.len() as f64;
            let mut file_content = Vec::new();
            fs::File::open(input).unwrap().read_to_end(&mut file_content).unwrap();

            let start_lumpi = Instant::now();
            let mut lumpi = engine::LumpiEngine::new();
            let hash = lumpi.compress(input, output).expect("Compression error");
            let t_lumpi = start_lumpi.elapsed().as_secs_f64() * 1000.0;
            let lumpi_size = fs::metadata(output).unwrap().len() as f64;
            let lumpi_ratio = original_size / lumpi_size;

            println!("Lumpi completed. SHA-256: {}", hash);

            let start_gz = Instant::now();
            let mut gz_enc = GzEncoder::new(Vec::new(), Compression::best());
            gz_enc.write_all(&file_content).unwrap();
            let gz_out = gz_enc.finish().unwrap();
            let t_gz = start_gz.elapsed().as_secs_f64() * 1000.0;
            let gz_size = gz_out.len() as f64;
            let gz_ratio = original_size / gz_size;

            let (z1_size, z1_time) = run_zstd_benchmark(&file_content, 1);
            let (z3_size, z3_time) = run_zstd_benchmark(&file_content, 3);
            let (z19_size, z19_time) = run_zstd_benchmark(&file_content, 19);

            let w_score = calculate_weissman_score(lumpi_ratio, gz_ratio, t_lumpi, t_gz);

            println!("\n==================================================================");
            println!("COMPARATIVE ANALYSIS (Input: {:.2} MB)", original_size / 1048576.0);
            println!("==================================================================");
            println!("{:<18} | {:<12} | {:<9} | {}", "Algorithm", "Size (KB)", "Ratio", "Time (ms)");
            println!("------------------------------------------------------------------");
            println!("{:<18} | {:>10.2} | {:>7.2}x | {:.2}", "GZIP (v9)", gz_size / 1024.0, gz_ratio, t_gz);
            println!("{:<18} | {:>10.2} | {:>7.2}x | {:.2}", "Zstd (Fast - L1)", z1_size / 1024.0, original_size / z1_size, z1_time);
            println!("{:<18} | {:>10.2} | {:>7.2}x | {:.2}", "Zstd (Bal. - L3)", z3_size / 1024.0, original_size / z3_size, z3_time);
            println!("{:<18} | {:>10.2} | {:>7.2}x | {:.2}", "Zstd (Max - L19)", z19_size / 1024.0, original_size / z19_size, z19_time);
            println!("------------------------------------------------------------------");
            println!("{:<18} | {:>10.2} | {:>7.2}x | {:.2}", "LUMPI CORE 2.0", lumpi_size / 1024.0, lumpi_ratio, t_lumpi);
            println!("==================================================================");
            println!("WEISSMAN SCORE (vs GZIP): {:.3}", w_score);
            
            let lumpi_vs_zstd = lumpi_ratio / (original_size / z3_size);
            println!("Density Efficiency vs Zstd L3: {:.2}x", lumpi_vs_zstd);
            println!("==================================================================\n");
        }
        Commands::Unpack { input, output } => {
            println!("[Lumpi Core] Decompressing and verifying integrity...");
            let start = Instant::now();
            match engine::LumpiEngine::decompress(input, output) {
                Ok(true) => {
                    let t = start.elapsed().as_secs_f64() * 1000.0;
                    println!("SUCCESS: File restored in {:.2}ms. SHA-256 matches.", t);
                },
                Ok(false) => println!("CRITICAL ERROR: Hash mismatch! Data is corrupted."),
                Err(e) => eprintln!("I/O Error: {}", e),
            }
        }
    }
}