use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod generation;
mod narinfo;
mod nix;
mod prune;
mod push;
mod s3;
mod s3_keys;

#[derive(Parser)]
#[command(name = "nix-s3-generations")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Push Nix closures to S3 with generation roots for later GC")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Push Nix closures to S3
    Push(push::PushArgs),
    /// Prune dead NARs from S3 bucket
    Prune(prune::PruneArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Push(args) => push::run(args).await,
        Commands::Prune(args) => prune::run(args).await,
    }
}
