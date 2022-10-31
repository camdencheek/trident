use std::io::{BufWriter, Read};
use std::{fs::File, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};

use trident::build::stats::IndexStats;
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
    pub index_path: PathBuf,
    pub query: String,
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

    let stats = match args.output_file {
        Some(path) => {
            let mut f = BufWriter::new(File::create(path)?);
            builder.build(&mut f)?
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            builder.build(&mut stdout)?
        }
    };
    summarize_stats(stats);
    Ok(())
}

fn summarize_stats(stats: IndexStats) {
    let index_size = stats.build.total_size_bytes();
    let content_size = stats.extract.doc_bytes;
    let mbps = stats.extract.doc_bytes as f64 / 1024. / 1024. / stats.total_time.as_secs_f64();
    println!(
        "\nIndexed {} in {:.1}s at {:.2} MB/s",
        bytefmt::format(content_size as u64),
        stats.total_time.as_secs_f64(),
        mbps
    );

    let ratio = index_size as f64 / content_size as f64;
    println!(
        "Index Size: {}, Compression ratio: {:.3}",
        bytefmt::format(index_size as u64),
        ratio
    );
    println!("Index Size Breakdown:");

    let header_ratio = stats.build.postings_sum.header_bytes as f64 / index_size as f64;
    println!("\tHeaders: {:.3}", header_ratio);

    let unique_successors_ratio =
        stats.build.postings_sum.unique_successors.bytes as f64 / index_size as f64;
    println!("\tUnique successors: {:.3}", unique_successors_ratio);

    let successors_ratio = stats.build.postings_sum.successors.bytes as f64 / index_size as f64;
    println!("\tSuccessors: {:.3}", successors_ratio);

    let unique_docs_ratio = stats.build.postings_sum.unique_docs.bytes as f64 / index_size as f64;
    println!("\tUnique Docs: {:.3}", unique_docs_ratio);

    let posting_offsets_ratio = stats.build.posting_offsets_bytes as f64 / index_size as f64;
    println!("\tPosting Offsets: {:.3}", posting_offsets_ratio);

    println!("Doc count: {}", stats.extract.num_docs);
    println!("Unique trigram count: {}", stats.extract.unique_trigrams);
}

fn search(args: SearchArgs) -> Result<()> {
    let index_file = File::open(args.index_path)?;
    let index = Index::new(index_file)?;
    assert!(args.query.len() == 6);
    let mut trigram = [0u8; 3];
    trigram.copy_from_slice(args.query[0..3].as_bytes());
    let mut successor = [0u8; 3];
    successor.copy_from_slice(args.query[3..].as_bytes());
    for doc in index.search(&trigram.into(), &successor.into()) {
        println!("{}", doc);
    }
    Ok(())
}
