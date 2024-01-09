use clap::Parser;
use bogger::{BlockInfo, EntryReader};
use std::{error::Error, path::PathBuf};

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory path containing blocks.
    #[arg(short, long)]
    directory: PathBuf,

    /// Block to read.
    #[arg(short, long)]
    block_num: u64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    let mut reader = {
        let b = BlockInfo::zero().with_number(args.block_num);
        EntryReader::open(&args.directory, b).await?
    };

    loop {
        match reader.next_entry().await {
            Ok(Some((b, _crc))) => println!("{}", minicbor::display(&b)),
            Ok(None)   => break,
            Err(error) => eprintln!("{error}")
        }
    }

    Ok(())
}
