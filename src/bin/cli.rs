use std::io::Read;
use std::time::Instant;
use std::{fs::File, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};

use rocksdb::{Options, SstFileWriter, DB};
use trident::build::IndexBuilder;
use trident::index::Index;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
pub struct Cli {
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    // Index a new directory
    Index(IndexArgs),
    Import(ImportArgs),
    Search(SearchArgs),
}

#[derive(Parser, Debug)]
pub struct IndexArgs {
    #[clap(short = 'o')]
    pub output_file: PathBuf,
    pub dir: PathBuf,
}

#[derive(Parser, Debug)]
pub struct ImportArgs {
    pub import_path: PathBuf,
    pub index_path: PathBuf,
}

#[derive(Parser, Debug)]
pub struct SearchArgs {
    pub index_path: PathBuf,
    pub query: String,
}

fn main() -> Result<()> {
    let args = Cli::try_parse()?;
    match args.cmd {
        Command::Index(a) => index(a),
        Command::Import(a) => import(a),
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
        buf.make_ascii_lowercase();
        builder.add_doc(buf.as_bytes())?;
    }

    // TODO does this SST file self-index? Or does indexing need to happen on import?
    let opts = Options::default();
    let mut sst_writer = SstFileWriter::create(&opts);
    sst_writer.open(args.output_file)?;
    builder.build_sst(&mut sst_writer)?;
    sst_writer.finish()?;
    Ok(())
}

fn import(args: ImportArgs) -> Result<()> {
    println!("opening");
    let db = DB::open_default(args.index_path)?;
    println!("importing");
    db.ingest_external_file(vec![args.import_path])?;
    Ok(())
}

fn search(args: SearchArgs) -> Result<()> {
    let index_file = File::open(args.index_path)?;
    let index = Index::new(index_file)?;
    let opened = Instant::now();
    let found = index.candidates(args.query.as_bytes()).count();
    println!("{} results in {:0.2?}\n", found, opened.elapsed());

    Ok(())
}
