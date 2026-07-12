//! Output sinks: CSV / Parquet, with optional file rotation and a
//! post-close upload hook.
//!
//! The core abstraction is [`ChunkSink`], which is fed one batch of records
//! at a time and finalized once at the end. [`RotatingSink`] wraps a factory
//! that produces per-chunk sinks and takes care of splitting the output by
//! row count and triggering an optional shell command after each chunk file
//! is closed (typically to upload it to a cloud bucket).

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bson::Bson;
use clap::ValueEnum;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as PqCompression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;

/// Output file format.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum OutputFormat {
    Csv,
    Parquet,
}

/// Parquet compression codec.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum ParquetCompression {
    None,
    Snappy,
    Gzip,
    Zstd,
    Lz4,
}

impl ParquetCompression {
    fn to_parquet(self) -> PqCompression {
        match self {
            ParquetCompression::None => PqCompression::UNCOMPRESSED,
            ParquetCompression::Snappy => PqCompression::SNAPPY,
            ParquetCompression::Gzip => PqCompression::GZIP(Default::default()),
            ParquetCompression::Zstd => {
                PqCompression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default())
            }
            ParquetCompression::Lz4 => PqCompression::LZ4,
        }
    }
}

/// Field type override, used both for CSV formatting and Parquet schema.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FieldType {
    String,
    Int32,
    Int64,
    Double,
    Bool,
    /// Milliseconds since epoch, UTC.
    Timestamp,
}

impl FieldType {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "string" | "str" | "utf8" => FieldType::String,
            "int32" | "i32" => FieldType::Int32,
            "int64" | "i64" | "long" => FieldType::Int64,
            "double" | "f64" | "float" => FieldType::Double,
            "bool" | "boolean" => FieldType::Bool,
            "timestamp" | "datetime" | "date" => FieldType::Timestamp,
            other => anyhow::bail!(
                "Unknown field type '{other}'. \
                 Supported: string, int32, int64, double, bool, timestamp"
            ),
        })
    }
}

/// A single typed value in a record. `Null` is used both for missing paths
/// and for values that could not be coerced into the requested type.
#[derive(Clone, Debug)]
pub enum TypedValue {
    Null,
    String(String),
    Int32(i32),
    Int64(i64),
    Double(f64),
    Bool(bool),
    /// Milliseconds since epoch, UTC.
    TimestampMs(i64),
}

impl TypedValue {
    /// Render as a CSV cell.
    fn to_csv_string(&self) -> String {
        match self {
            TypedValue::Null => String::new(),
            TypedValue::String(s) => s.clone(),
            TypedValue::Int32(v) => v.to_string(),
            TypedValue::Int64(v) => v.to_string(),
            TypedValue::Double(v) => v.to_string(),
            TypedValue::Bool(v) => v.to_string(),
            TypedValue::TimestampMs(ms) => {
                // Format as RFC3339 UTC for CSV readability.
                match bson::DateTime::from_millis(*ms).try_to_rfc3339_string() {
                    Ok(s) => s,
                    Err(_) => ms.to_string(),
                }
            }
        }
    }
}

/// Parse `--field-type` entries of the form `name:type` into `map`.
pub fn parse_field_types(entries: &[String], map: &mut HashMap<String, FieldType>) -> Result<()> {
    for e in entries {
        let (name, ty) = e
            .split_once(':')
            .with_context(|| format!("--field-type must be name:type (got '{e}')"))?;
        let name = name.trim();
        let ty = ty.trim();
        if name.is_empty() {
            anyhow::bail!("--field-type name cannot be empty (in '{e}')");
        }
        map.insert(name.to_string(), FieldType::parse(ty)?);
    }
    Ok(())
}

/// Coerce a BSON value into the requested `FieldType`. Returns `Null` on any
/// mismatch; the goal is to be permissive at extract time (best-effort) so
/// a single malformed document doesn't abort the whole run.
pub fn coerce(b: &Bson, ty: FieldType) -> TypedValue {
    match ty {
        FieldType::String => TypedValue::String(crate::bson_to_string(b)),
        FieldType::Int32 => match b {
            Bson::Int32(v) => TypedValue::Int32(*v),
            Bson::Int64(v) => i32::try_from(*v).map(TypedValue::Int32).unwrap_or(TypedValue::Null),
            Bson::Double(v) if v.fract() == 0.0 && *v >= i32::MIN as f64 && *v <= i32::MAX as f64 => {
                TypedValue::Int32(*v as i32)
            }
            Bson::String(s) => s.parse::<i32>().map(TypedValue::Int32).unwrap_or(TypedValue::Null),
            Bson::Boolean(v) => TypedValue::Int32(if *v { 1 } else { 0 }),
            _ => TypedValue::Null,
        },
        FieldType::Int64 => match b {
            Bson::Int32(v) => TypedValue::Int64(*v as i64),
            Bson::Int64(v) => TypedValue::Int64(*v),
            Bson::Double(v) if v.fract() == 0.0 => TypedValue::Int64(*v as i64),
            Bson::String(s) => s.parse::<i64>().map(TypedValue::Int64).unwrap_or(TypedValue::Null),
            Bson::Boolean(v) => TypedValue::Int64(if *v { 1 } else { 0 }),
            Bson::DateTime(dt) => TypedValue::Int64(dt.timestamp_millis()),
            _ => TypedValue::Null,
        },
        FieldType::Double => match b {
            Bson::Double(v) => TypedValue::Double(*v),
            Bson::Int32(v) => TypedValue::Double(*v as f64),
            Bson::Int64(v) => TypedValue::Double(*v as f64),
            Bson::String(s) => s.parse::<f64>().map(TypedValue::Double).unwrap_or(TypedValue::Null),
            _ => TypedValue::Null,
        },
        FieldType::Bool => match b {
            Bson::Boolean(v) => TypedValue::Bool(*v),
            Bson::Int32(v) => TypedValue::Bool(*v != 0),
            Bson::Int64(v) => TypedValue::Bool(*v != 0),
            Bson::String(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "y" => TypedValue::Bool(true),
                "false" | "0" | "no" | "n" => TypedValue::Bool(false),
                _ => TypedValue::Null,
            },
            _ => TypedValue::Null,
        },
        FieldType::Timestamp => match b {
            Bson::DateTime(dt) => TypedValue::TimestampMs(dt.timestamp_millis()),
            Bson::Int64(v) => TypedValue::TimestampMs(*v),
            Bson::Int32(v) => TypedValue::TimestampMs(*v as i64),
            Bson::String(s) => {
                // Try RFC3339 first via bson's parser.
                match bson::DateTime::parse_rfc3339_str(s) {
                    Ok(dt) => TypedValue::TimestampMs(dt.timestamp_millis()),
                    Err(_) => TypedValue::Null,
                }
            }
            _ => TypedValue::Null,
        },
    }
}

/// A sink accepting record batches. Implementations may buffer internally.
pub trait ChunkSink: Send {
    /// Write a batch of records. Each inner vec has the same length as the
    /// declared field list.
    fn write_records(&mut self, records: &[Vec<TypedValue>]) -> Result<()>;

    /// Flush and finalize the sink. Must be called exactly once.
    fn finish(&mut self) -> Result<()>;
}

// ============================================================================
// CSV
// ============================================================================

pub struct CsvSink {
    writer: csv::Writer<Box<dyn Write + Send>>,
    finished: bool,
}

impl CsvSink {
    pub fn new(
        writer: Box<dyn Write + Send>,
        fields: &[String],
        delimiter: char,
        no_header: bool,
    ) -> Result<Self> {
        let mut w = csv::WriterBuilder::new()
            .delimiter(delimiter as u8)
            .from_writer(writer);
        if !no_header {
            w.write_record(fields)?;
        }
        Ok(Self {
            writer: w,
            finished: false,
        })
    }
}

impl ChunkSink for CsvSink {
    fn write_records(&mut self, records: &[Vec<TypedValue>]) -> Result<()> {
        let mut row: Vec<String> = Vec::new();
        for rec in records {
            row.clear();
            row.reserve(rec.len());
            for v in rec {
                row.push(v.to_csv_string());
            }
            self.writer.write_record(&row)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !self.finished {
            self.writer.flush()?;
            self.finished = true;
        }
        Ok(())
    }
}

impl Drop for CsvSink {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

// ============================================================================
// Parquet
// ============================================================================

pub struct ParquetSink {
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    field_types: Vec<FieldType>,
    finished: bool,
}

impl ParquetSink {
    pub fn new(
        file: File,
        fields: &[String],
        field_types: &[FieldType],
        compression: ParquetCompression,
    ) -> Result<Self> {
        assert_eq!(fields.len(), field_types.len());
        let arrow_fields: Vec<Field> = fields
            .iter()
            .zip(field_types.iter())
            .map(|(name, ty)| Field::new(name, arrow_data_type(*ty), true))
            .collect();
        let schema = Arc::new(Schema::new(arrow_fields));

        let props = WriterProperties::builder()
            .set_compression(compression.to_parquet())
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .context("Failed to create Parquet writer")?;
        Ok(Self {
            writer: Some(writer),
            schema,
            field_types: field_types.to_vec(),
            finished: false,
        })
    }
}

fn arrow_data_type(ty: FieldType) -> DataType {
    match ty {
        FieldType::String => DataType::Utf8,
        FieldType::Int32 => DataType::Int32,
        FieldType::Int64 => DataType::Int64,
        FieldType::Double => DataType::Float64,
        FieldType::Bool => DataType::Boolean,
        FieldType::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
    }
}

impl ChunkSink for ParquetSink {
    fn write_records(&mut self, records: &[Vec<TypedValue>]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let n_cols = self.field_types.len();
        // Build columnar arrays. We allocate per-batch, sized exactly.
        let n = records.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(n_cols);
        for (col_idx, ty) in self.field_types.iter().enumerate() {
            let arr: ArrayRef = match ty {
                FieldType::String => {
                    let mut b: Vec<Option<&str>> = Vec::with_capacity(n);
                    for rec in records {
                        b.push(match &rec[col_idx] {
                            TypedValue::Null => None,
                            TypedValue::String(s) => Some(s.as_str()),
                            _ => None, // shouldn't happen: coerce respects the type
                        });
                    }
                    Arc::new(StringArray::from(b))
                }
                FieldType::Int32 => {
                    let mut b: Vec<Option<i32>> = Vec::with_capacity(n);
                    for rec in records {
                        b.push(match &rec[col_idx] {
                            TypedValue::Int32(v) => Some(*v),
                            _ => None,
                        });
                    }
                    Arc::new(Int32Array::from(b))
                }
                FieldType::Int64 => {
                    let mut b: Vec<Option<i64>> = Vec::with_capacity(n);
                    for rec in records {
                        b.push(match &rec[col_idx] {
                            TypedValue::Int64(v) => Some(*v),
                            _ => None,
                        });
                    }
                    Arc::new(Int64Array::from(b))
                }
                FieldType::Double => {
                    let mut b: Vec<Option<f64>> = Vec::with_capacity(n);
                    for rec in records {
                        b.push(match &rec[col_idx] {
                            TypedValue::Double(v) => Some(*v),
                            _ => None,
                        });
                    }
                    Arc::new(Float64Array::from(b))
                }
                FieldType::Bool => {
                    let mut b: Vec<Option<bool>> = Vec::with_capacity(n);
                    for rec in records {
                        b.push(match &rec[col_idx] {
                            TypedValue::Bool(v) => Some(*v),
                            _ => None,
                        });
                    }
                    Arc::new(BooleanArray::from(b))
                }
                FieldType::Timestamp => {
                    let mut b: Vec<Option<i64>> = Vec::with_capacity(n);
                    for rec in records {
                        b.push(match &rec[col_idx] {
                            TypedValue::TimestampMs(v) => Some(*v),
                            _ => None,
                        });
                    }
                    Arc::new(
                        TimestampMillisecondArray::from(b).with_timezone(Arc::from("UTC")),
                    )
                }
            };
            arrays.push(arr);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .context("Failed to build Arrow RecordBatch")?;
        self.writer
            .as_mut()
            .expect("writer available before finish")
            .write(&batch)
            .context("Failed to write Parquet batch")?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if !self.finished {
            if let Some(w) = self.writer.take() {
                w.close().context("Failed to close Parquet writer")?;
            }
            self.finished = true;
        }
        Ok(())
    }
}

impl Drop for ParquetSink {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

// ============================================================================
// Rotation + upload hook
// ============================================================================

/// Factory that materializes a fresh underlying sink for a chunk file
/// (or, in the `None` case, stdout - only used without rotation).
pub type SinkFactory = Box<dyn Fn(Option<&Path>) -> Result<Box<dyn ChunkSink>> + Send>;

/// Splits output into multiple files based on a max row count, and optionally
/// runs a shell command after each chunk is closed (typically to move the
/// file to a bucket).
pub struct RotatingSink {
    prefix: PathBuf,
    extension: String,
    max_rows: u64,
    factory: SinkFactory,
    upload_cmd: Option<String>,
    remove_after_upload: bool,

    current: Option<Box<dyn ChunkSink>>,
    current_path: Option<PathBuf>,
    current_rows: u64,
    chunk_index: u64,
    finished: bool,
}

impl RotatingSink {
    pub fn new(
        prefix: PathBuf,
        extension: &str,
        max_rows: u64,
        factory: SinkFactory,
        upload_cmd: Option<String>,
        remove_after_upload: bool,
    ) -> Self {
        Self {
            prefix,
            extension: extension.to_string(),
            max_rows,
            factory,
            upload_cmd,
            remove_after_upload,
            current: None,
            current_path: None,
            current_rows: 0,
            chunk_index: 0,
            finished: false,
        }
    }

    /// Build the path for a given chunk index: `<prefix>-NNNNN.<ext>`.
    /// The prefix may itself carry an extension; we ignore it and always
    /// append the format-appropriate one.
    fn chunk_path(&self, index: u64) -> PathBuf {
        let stem = self
            .prefix
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "part".to_string());
        let parent = self.prefix.parent().unwrap_or_else(|| Path::new(""));
        let name = format!("{stem}-{index:05}.{ext}", ext = self.extension);
        parent.join(name)
    }

    fn open_new_chunk(&mut self) -> Result<()> {
        debug_assert!(self.current.is_none());
        let path = self.chunk_path(self.chunk_index);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create output directory: {}", parent.display())
                })?;
            }
        }
        let sink = (self.factory)(Some(&path))?;
        eprintln!("Opened chunk file: {}", path.display());
        self.current = Some(sink);
        self.current_path = Some(path);
        self.current_rows = 0;
        Ok(())
    }

    fn close_current_chunk(&mut self) -> Result<()> {
        let Some(mut sink) = self.current.take() else {
            return Ok(());
        };
        sink.finish()?;
        drop(sink);
        let path = self
            .current_path
            .take()
            .expect("current_path set when current is set");
        eprintln!(
            "Closed chunk {} ({} rows): {}",
            self.chunk_index,
            self.current_rows,
            path.display()
        );

        if let Some(cmd) = &self.upload_cmd {
            run_upload_cmd(cmd, &path, self.chunk_index)?;
            if self.remove_after_upload {
                fs::remove_file(&path).with_context(|| {
                    format!("Failed to remove uploaded chunk: {}", path.display())
                })?;
                eprintln!("Removed local chunk: {}", path.display());
            }
        }

        self.chunk_index += 1;
        Ok(())
    }
}

impl ChunkSink for RotatingSink {
    fn write_records(&mut self, records: &[Vec<TypedValue>]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut start = 0;
        while start < records.len() {
            if self.current.is_none() {
                self.open_new_chunk()?;
            }
            let remaining = self.max_rows.saturating_sub(self.current_rows) as usize;
            if remaining == 0 {
                // Current chunk is exactly full: rotate before writing.
                self.close_current_chunk()?;
                continue;
            }
            let end = (start + remaining).min(records.len());
            let slice = &records[start..end];
            self.current
                .as_mut()
                .expect("just opened")
                .write_records(slice)?;
            self.current_rows += slice.len() as u64;
            start = end;
            if self.current_rows >= self.max_rows {
                self.close_current_chunk()?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.close_current_chunk()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for RotatingSink {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

/// Execute the upload hook. `{file}` and `{index}` placeholders are
/// substituted before the command is handed to the system shell.
fn run_upload_cmd(template: &str, path: &Path, index: u64) -> Result<()> {
    let idx = format!("{index:05}");
    let cmd = template
        .replace("{file}", &path.to_string_lossy())
        .replace("{index}", &idx);
    eprintln!("Running upload hook: {cmd}");

    let status = if cfg!(target_os = "windows") {
        Command::new("cmd").args(["/C", &cmd]).status()
    } else {
        Command::new("sh").args(["-c", &cmd]).status()
    }
    .with_context(|| format!("Failed to spawn upload command: {cmd}"))?;

    if !status.success() {
        anyhow::bail!(
            "Upload command failed with status {:?}: {}",
            status.code(),
            cmd
        );
    }
    Ok(())
}
