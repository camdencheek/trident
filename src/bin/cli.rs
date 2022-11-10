use std::io::Read;
use std::sync::Arc;
use std::time::Instant;
use std::{fs::File, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};

use parquet::format::TimeUnit;
use parquet::schema::printer::{print_parquet_metadata, print_schema};
use parquet::schema::types::SchemaDescriptor;
use rocksdb::{Options, SstFileWriter, DB};
use trident::build::IndexBuilder;
use trident::index::Index;
use walkdir::WalkDir;

use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    schema::{parser, printer, types::Type},
};

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
    let s = schema()?;
    print_schema(&mut std::io::stdout(), s.root_schema());
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

fn schema() -> Result<SchemaDescriptor> {
    let oid = new_oid_schema("oid")?;
    let path = new_string_schema("path")?;
    let content = Type::primitive_type_builder("content", PhysicalType::BYTE_ARRAY).build()?;
    let commits_added = new_bytes_schema("commits_added")?;
    let commits_removed = new_bytes_schema("commits_removed")?;
    let head_reachable = Type::primitive_type_builder("head_reachable", PhysicalType::BOOLEAN)
        .with_repetition(Repetition::REQUIRED)
        .build()?;

    let blobs = new_list("blobs", |name| {
        Ok(Type::group_type_builder(name)
            .with_repetition(Repetition::REQUIRED)
            .with_fields(&mut vec![
                Arc::new(oid.clone()),
                Arc::new(path.clone()),
                Arc::new(content.clone()),
                Arc::new(commits_added.clone()),
                Arc::new(commits_removed.clone()),
                Arc::new(head_reachable.clone()),
            ])
            .build()?)
    })?;

    let message = new_bytes_schema("message")?;

    let parents = Type::primitive_type_builder("parents", PhysicalType::FIXED_LEN_BYTE_ARRAY)
        .with_length(20)
        .with_repetition(Repetition::REPEATED)
        .build()?;

    let author_name = new_string_schema("author_name")?;
    let author_email = new_string_schema("author_email")?;
    let author_date = Type::primitive_type_builder("author_date", PhysicalType::INT64)
        .with_logical_type(Some(LogicalType::Timestamp {
            unit: TimeUnit::MILLIS(Default::default()),
            is_adjusted_to_u_t_c: true,
        }))
        .build()?;

    let committer_name = new_string_schema("committer_name")?;
    let committer_email = new_string_schema("committer_email")?;

    let committer_date = Type::primitive_type_builder("committer_date", PhysicalType::INT64)
        .with_logical_type(Some(LogicalType::Timestamp {
            unit: TimeUnit::MILLIS(Default::default()),
            is_adjusted_to_u_t_c: true,
        }))
        .build()?;

    let first_parent_reachability =
        Type::primitive_type_builder("first_parent_reachability", PhysicalType::BYTE_ARRAY)
            .build()?;

    let commits = Type::group_type_builder("commits")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut vec![
            Arc::new(oid),
            Arc::new(message),
            Arc::new(parents),
            Arc::new(author_name),
            Arc::new(author_email),
            Arc::new(author_date),
            Arc::new(committer_name),
            Arc::new(committer_email),
            Arc::new(committer_date),
            Arc::new(first_parent_reachability),
        ])
        .build()?;

    let schema = Type::group_type_builder("schema")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![
            Arc::new(blobs),
            Arc::new(new_regex_index_schema("blob_path_index")?),
            Arc::new(new_regex_index_schema("blob_content_index")?),
            Arc::new(commits),
            Arc::new(new_regex_index_schema("commit_message_index")?),
            Arc::new(new_regex_index_schema("commit_author_name_index")?),
            Arc::new(new_regex_index_schema("commit_author_email_index")?),
            Arc::new(new_regex_index_schema("commit_committer_name_index")?),
            Arc::new(new_regex_index_schema("commit_committer_email_index")?),
        ])
        .build()?;

    Ok(SchemaDescriptor::new(Arc::new(schema)))
}

fn new_list<F: Fn(&str) -> Result<Type>>(name: &str, element_gen: F) -> Result<Type> {
    let element = element_gen("element")?;
    let list = Type::group_type_builder("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut vec![Arc::new(element)])
        .build()?;

    Ok(Type::group_type_builder(name)
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![Arc::new(list)])
        .build()?)
}

fn new_oid_schema(name: &str) -> Result<Type> {
    Ok(
        Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_length(20)
            .build()?,
    )
}

fn new_string_schema(name: &str) -> Result<Type> {
    Ok(Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
        .with_logical_type(Some(LogicalType::String))
        .build()?)
}

fn new_bytes_schema(name: &str) -> Result<Type> {
    Ok(Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()?)
}

fn new_regex_index_schema(name: &str) -> Result<Type> {
    let key = Type::primitive_type_builder("key", PhysicalType::FIXED_LEN_BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .with_length(3)
        .build()?;

    let successors = Type::primitive_type_builder("successors", PhysicalType::INT32)
        .with_repetition(Repetition::REPEATED)
        .build()?;

    let matrix_row = Type::primitive_type_builder("row", PhysicalType::INT32)
        .with_repetition(Repetition::REQUIRED)
        .build()?;

    let matrix_col = Type::primitive_type_builder("column", PhysicalType::INT32)
        .with_repetition(Repetition::REQUIRED)
        .build()?;

    let matrix = Type::group_type_builder("matrix")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![Arc::new(matrix_row), Arc::new(matrix_col)])
        .build()?;

    let docs = Type::primitive_type_builder("docs", PhysicalType::INT32)
        .with_repetition(Repetition::REPEATED)
        .build()?;

    let value = Type::group_type_builder("value")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(&mut vec![
            Arc::new(successors),
            Arc::new(matrix),
            Arc::new(docs),
        ])
        .build()?;

    let key_value = Type::group_type_builder("key_value")
        .with_repetition(Repetition::REPEATED)
        .with_fields(&mut vec![Arc::new(key), Arc::new(value)])
        .build()?;

    Ok(Type::group_type_builder(name)
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(Some(LogicalType::Map))
        .with_fields(&mut vec![Arc::new(key_value)])
        .build()?)
}
