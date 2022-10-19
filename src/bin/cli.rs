use std::io::{BufWriter, Read};
use std::{fs::File, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};

use trident::builder::IndexBuilder;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
pub struct Cli {
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Index(IndexArgs),
    Search(SearchArgs),
}

#[derive(Parser, Debug)]
pub struct IndexArgs {
    #[clap(short = 'o')]
    pub output_file: Option<PathBuf>,
    pub dir: PathBuf,
}

#[derive(Parser, Debug)]
pub struct SearchArgs {
    pub shard: PathBuf,
    pub queries: Vec<String>,
    #[clap(long = "skip-index")]
    pub skip_index: bool,
    #[clap(long = "count-only")]
    pub count_only: bool,
    #[clap(long = "repeat", short = 'r', default_value = "1")]
    pub repeat: usize,
    #[clap(long = "limit", short = 'l')]
    pub limit: Option<usize>,
    #[clap(long = "cache-size")]
    pub cache_size: Option<String>,
    #[clap(long = "no-cache")]
    pub no_cache: bool,
}

fn main() -> Result<()> {
    let args = Cli::try_parse()?;
    match args.cmd {
        Command::Index(a) => index(a),
        Command::Search(a) => search(a),
    }
}

fn index(args: IndexArgs) -> Result<()> {
    let docs = WalkDir::new(args.dir)
        .into_iter()
        .filter_map(|d| d.ok())
        .filter(|d| d.file_type().is_file());

    let mut builder = IndexBuilder::new();
    let mut buf = String::new();
    for doc in docs {
        buf.clear();
        let mut f = File::open(doc.path())?;
        if let Err(e) = f.read_to_string(&mut buf) {
            println!("skipping {:?}: {}", doc.path(), e);
        };
        builder.add_doc(buf.as_bytes())?;
    }

    match args.output_file {
        Some(path) => {
            let mut f = BufWriter::new(File::create(path)?);
            builder.build(&mut f)?;
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            builder.build(&mut stdout)?;
        }
    }

    Ok(())
}

fn search(args: SearchArgs) -> Result<()> {
    unimplemented!()
}
