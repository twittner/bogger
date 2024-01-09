use clap::Parser;
use bogger::Forwarder;
use std::{error::Error, path::PathBuf};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory path containing blocks.
    #[arg(short, long)]
    directory: PathBuf,

    /// Network address of the destination.
    #[arg(short, long)]
    address: String
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "bogger=debug".into()))
        .with(fmt::layer())
        .init();

    Forwarder::new("test", &args.directory, &args.address).await?.go().await
}
