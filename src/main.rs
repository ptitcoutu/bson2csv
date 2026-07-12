use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result};
use bson::{Bson, Document};
use clap::Parser;
use crossbeam_channel::bounded;
use rayon::prelude::*;

/// Convert a BSON dump (e.g. from mongodump) to CSV.
///
/// The tool streams the input file document by document and processes
/// documents in parallel chunks (rayon) while a dedicated reader thread
/// keeps the disk busy. This scales well on multi-core machines and on
/// input files of arbitrary size (hundreds of GB).
///
/// Nested fields are addressable with dotted notation:
/// `user.address.city` for sub-documents, `tags.0` for array elements.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the input BSON file (mongodump format: concatenated documents).
    #[arg(short, long)]
    input: PathBuf,

    /// Path to the output CSV file. If omitted, writes to stdout.
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Fields to extract, in dotted notation. Repeat the flag or use commas.
    /// Example: -f _id -f user.name -f tags.0
    #[arg(short = 'f', long = "field", value_delimiter = ',', required = true)]
    fields: Vec<String>,

    /// Optional filter of the form `field=value`. Only documents whose
    /// (flattened) `field` equals `value` (string comparison) are exported.
    /// Example: --filter status=active
    #[arg(long)]
    filter: Option<String>,

    /// Optional set-membership filter of the form `field=path/to/file`.
    /// The file is loaded in memory as a hash set (one value per line,
    /// blank lines and surrounding whitespace are ignored). A document is
    /// kept iff its (flattened) `field` value is contained in the set.
    /// Can be repeated to filter on multiple fields (AND semantics).
    /// Example: --filter-in user_id=./ids.txt
    #[arg(long = "filter-in", value_name = "KEY=PATH")]
    filter_in: Vec<String>,

    /// CSV delimiter (default: ',').
    #[arg(long, default_value = ",")]
    delimiter: char,

    /// Do not write the header row.
    #[arg(long)]
    no_header: bool,

    /// Number of documents processed per chunk.
    /// Larger values improve throughput at the cost of RAM.
    #[arg(long, default_value_t = 10_000)]
    chunk_size: usize,

    /// Number of worker threads (defaults to the number of CPU cores).
    #[arg(long)]
    threads: Option<usize>,

    /// Print progress to stderr every N documents (0 disables).
    #[arg(long, default_value_t = 1_000_000)]
    progress_every: u64,
}

/// A processed chunk ready to be written to CSV.
struct ProcessedChunk {
    /// Records to write (already filtered, in original order).
    records: Vec<Vec<String>>,
    /// Total documents read in the chunk (for stats).
    total: u64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(n) = cli.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .context("Failed to configure rayon thread pool")?;
    }

    let filter = match &cli.filter {
        Some(s) => Some(parse_filter(s)?),
        None => None,
    };

    // Load set-membership filters. Each file is read once, sequentially,
    // and stored in an `Arc<HashSet<String>>` so worker threads share it
    // cheaply without copying.
    let set_filters: Vec<(String, Arc<HashSet<String>>)> = cli
        .filter_in
        .iter()
        .map(|s| parse_filter_in(s))
        .collect::<Result<Vec<_>>>()?;

    // --- Output writer -------------------------------------------------------
    let writer: Box<dyn Write> = match &cli.output {
        Some(path) => {
            let f = File::create(path)
                .with_context(|| format!("Failed to create output file: {}", path.display()))?;
            Box::new(BufWriter::with_capacity(8 * 1024 * 1024, f))
        }
        None => Box::new(BufWriter::with_capacity(1024 * 1024, io::stdout().lock())),
    };
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(cli.delimiter as u8)
        .from_writer(writer);
    if !cli.no_header {
        csv_writer.write_record(&cli.fields)?;
    }

    // --- Pipeline: reader thread -> processed chunks channel ----------------
    // Bounded channels give backpressure: at most 2 raw chunks in flight and
    // 2 processed chunks queued, so memory stays under (chunk_size * 4) docs.
    let (raw_tx, raw_rx) = bounded::<Vec<Document>>(2);
    let (out_tx, out_rx) = bounded::<ProcessedChunk>(2);

    // Reader thread: sequentially decodes BSON and pushes chunks.
    let input_path = cli.input.clone();
    let chunk_size = cli.chunk_size.max(1);
    let reader_handle = thread::spawn(move || -> Result<()> {
        let file = File::open(&input_path)
            .with_context(|| format!("Failed to open input file: {}", input_path.display()))?;
        let mut reader = BufReader::with_capacity(16 * 1024 * 1024, file);
        let mut buf: Vec<Document> = Vec::with_capacity(chunk_size);
        loop {
            match read_next_document(&mut reader)? {
                Some(doc) => {
                    buf.push(doc);
                    if buf.len() >= chunk_size {
                        let full = std::mem::replace(&mut buf, Vec::with_capacity(chunk_size));
                        // If the receiver is gone, stop cleanly.
                        if raw_tx.send(full).is_err() {
                            return Ok(());
                        }
                    }
                }
                None => break,
            }
        }
        if !buf.is_empty() {
            let _ = raw_tx.send(buf);
        }
        Ok(())
    });

    // Worker thread: takes raw chunks, processes them in parallel with rayon,
    // then forwards ordered records to the writer channel.
    let fields = cli.fields.clone();
    let filter_clone = filter.clone();
    let set_filters_clone = set_filters.clone();
    let worker_handle = thread::spawn(move || -> Result<()> {
        while let Ok(chunk) = raw_rx.recv() {
            let total = chunk.len() as u64;
            // par_iter preserves order when collected into a Vec.
            let records: Vec<Vec<String>> = chunk
                .par_iter()
                .filter_map(|doc| {
                    if let Some((ref key, ref expected)) = filter_clone {
                        match get_flattened(doc, key) {
                            Some(v) if &v == expected => {}
                            _ => return None,
                        }
                    }
                    for (key, set) in &set_filters_clone {
                        match get_flattened(doc, key) {
                            Some(v) if set.contains(&v) => {}
                            _ => return None,
                        }
                    }
                    let mut record: Vec<String> = Vec::with_capacity(fields.len());
                    for f in &fields {
                        record.push(get_flattened(doc, f).unwrap_or_default());
                    }
                    Some(record)
                })
                .collect();
            if out_tx.send(ProcessedChunk { records, total }).is_err() {
                break;
            }
        }
        Ok(())
    });

    // --- Writer (main thread) -----------------------------------------------
    let mut total: u64 = 0;
    let mut kept: u64 = 0;
    let mut last_reported: u64 = 0;
    while let Ok(chunk) = out_rx.recv() {
        for record in &chunk.records {
            csv_writer.write_record(record)?;
        }
        kept += chunk.records.len() as u64;
        total += chunk.total;

        if cli.progress_every > 0 && total - last_reported >= cli.progress_every {
            eprintln!("Processed {total} documents, kept {kept}");
            last_reported = total;
        }
    }

    // Propagate errors from background threads.
    reader_handle
        .join()
        .map_err(|_| anyhow::anyhow!("Reader thread panicked"))??;
    worker_handle
        .join()
        .map_err(|_| anyhow::anyhow!("Worker thread panicked"))??;

    csv_writer.flush()?;
    eprintln!("Done. Total: {total} documents, kept: {kept}");
    Ok(())
}

/// Parse a filter argument of the form `key=value`.
fn parse_filter(s: &str) -> Result<(String, String)> {
    let (k, v) = s
        .split_once('=')
        .context("--filter must be in the form key=value")?;
    Ok((k.to_string(), v.to_string()))
}

/// Parse a `--filter-in KEY=PATH` argument and eagerly load the file into
/// a hash set. The set is wrapped in an `Arc` so it can be cheaply shared
/// between rayon worker threads.
fn parse_filter_in(s: &str) -> Result<(String, Arc<HashSet<String>>)> {
    let (key, path) = s
        .split_once('=')
        .context("--filter-in must be in the form key=path/to/file")?;
    if key.is_empty() {
        anyhow::bail!("--filter-in key cannot be empty");
    }
    if path.is_empty() {
        anyhow::bail!("--filter-in path cannot be empty");
    }
    let set = load_filter_set(Path::new(path))
        .with_context(|| format!("Failed to load filter file: {path}"))?;
    eprintln!(
        "Loaded {} distinct value(s) for filter --filter-in {}=...",
        set.len(),
        key
    );
    Ok((key.to_string(), Arc::new(set)))
}

/// Load a text file into a `HashSet<String>`, one value per line.
/// Blank lines and lines that become empty after trimming are skipped.
fn load_filter_set(path: &Path) -> Result<HashSet<String>> {
    let file = File::open(path)?;
    let reader = BufReader::with_capacity(1 << 20, file);
    let mut set = HashSet::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        set.insert(trimmed.to_string());
    }
    Ok(set)
}

/// Read the next BSON document from the stream, or return `Ok(None)` at EOF.
///
/// A BSON document begins with a little-endian i32 that encodes its total
/// size in bytes (including the size prefix itself). We peek the first 4
/// bytes to detect EOF cleanly, then let the `bson` crate parse the rest.
fn read_next_document<R: Read>(reader: &mut R) -> Result<Option<Document>> {
    let mut len_buf = [0u8; 4];
    match read_exact_or_eof(reader, &mut len_buf)? {
        ReadState::Eof => return Ok(None),
        ReadState::Ok => {}
    }

    let doc_len = i32::from_le_bytes(len_buf);
    if doc_len < 5 {
        anyhow::bail!("Invalid BSON document length: {doc_len}");
    }

    // Reconstruct a full document byte slice for the bson crate.
    let mut buf = Vec::with_capacity(doc_len as usize);
    buf.extend_from_slice(&len_buf);
    buf.resize(doc_len as usize, 0);
    reader
        .read_exact(&mut buf[4..])
        .context("Unexpected EOF while reading BSON document body")?;

    let doc = Document::from_reader(&mut buf.as_slice())
        .context("Failed to parse BSON document")?;
    Ok(Some(doc))
}

enum ReadState {
    Ok,
    Eof,
}

/// Like `read_exact` but returns `Eof` if nothing was read at all.
fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<ReadState> {
    let mut read = 0;
    while read < buf.len() {
        match reader.read(&mut buf[read..]) {
            Ok(0) => {
                if read == 0 {
                    return Ok(ReadState::Eof);
                }
                anyhow::bail!("Unexpected EOF in the middle of BSON size prefix");
            }
            Ok(n) => read += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(ReadState::Ok)
}

/// Resolve a dotted path against a BSON document and stringify the resulting
/// scalar. Returns `None` if the path does not exist.
fn get_flattened(doc: &Document, path: &str) -> Option<String> {
    // Walk without cloning intermediate values.
    let mut segments = path.split('.');
    let first = segments.next()?;
    let mut current: &Bson = doc.get(first)?;
    for segment in segments {
        current = match current {
            Bson::Document(d) => d.get(segment)?,
            Bson::Array(a) => {
                let idx: usize = segment.parse().ok()?;
                a.get(idx)?
            }
            _ => return None,
        };
    }
    Some(bson_to_string(current))
}

/// Convert a BSON scalar into its string representation.
/// Non-scalar values (documents, arrays) are serialized as their JSON form
/// so the CSV cell still carries the information.
fn bson_to_string(b: &Bson) -> String {
    match b {
        Bson::Double(v) => v.to_string(),
        Bson::String(v) => v.clone(),
        Bson::Boolean(v) => v.to_string(),
        Bson::Null => String::new(),
        Bson::Int32(v) => v.to_string(),
        Bson::Int64(v) => v.to_string(),
        Bson::ObjectId(v) => v.to_hex(),
        Bson::DateTime(v) => v.try_to_rfc3339_string().unwrap_or_else(|_| v.to_string()),
        Bson::Timestamp(v) => format!("{}:{}", v.time, v.increment),
        Bson::Decimal128(v) => v.to_string(),
        Bson::Symbol(v) => v.clone(),
        Bson::RegularExpression(r) => format!("/{}/{}", r.pattern, r.options),
        Bson::Binary(b) => bson::Bson::Binary(b.clone()).to_string(),
        Bson::Undefined => String::new(),
        Bson::MinKey | Bson::MaxKey => format!("{b:?}"),
        Bson::JavaScriptCode(s) => s.clone(),
        Bson::JavaScriptCodeWithScope(s) => s.code.clone(),
        Bson::DbPointer(_) => format!("{b:?}"),
        // Documents / arrays: JSON serialization keeps the info in a single cell.
        Bson::Document(_) | Bson::Array(_) => b.to_string(),
    }
}
